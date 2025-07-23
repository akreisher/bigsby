package sstable

import (
	"bigsby/bloom"
	"bigsby/storage"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

type Table struct {
	FilePath string
	// index  map[string, int]
	filter         bloom.Filter
	dataStartIndex int
}

const DataFileName = "segment_table"
const segmentCookie = "BIGSBYSEGMENT"
const segmentFileFormat = 1

// SSTable Requirements:
// - Immutable
// - Sorted
// - Can look up value
// - Can print all values
// - Can create new segment from memtable
// - Can merge two segments together

func Create(filePath string, data []storage.EntryData) (*Table, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("Could not create segment file: %w", err)
	}

	filter := bloom.Filter{}
	for _, entry := range data {
		filter.Insert(entry.Key)
	}

	// Write bigsby segment cookie
	// cookie + version + bloom filter len + bloom filter data
	headerLen := len(segmentCookie) + 2 + 4 + len(filter.Buf)
	header := make([]byte, headerLen)
	idx := 0

	copy(header[idx:], segmentCookie)
	idx += len(segmentCookie)
	binary.BigEndian.PutUint16(header[idx:], segmentFileFormat)
	idx += 2
	binary.BigEndian.PutUint32(header[idx:], uint32(len(filter.Buf)))
	idx += 4
	copy(header[idx:], filter.Buf[:])
	idx += len(filter.Buf)

	f.Write(header)

	// TODO: Write index to disk for loading.
	for _, entry := range data {
		_, err := f.Write(storage.EncodeLogEntry(entry))
		if err != nil {
			return nil, fmt.Errorf("Failed to write segment data: %w", err)
		}
	}

	return &Table{
		FilePath:       filePath,
		filter:         filter,
		dataStartIndex: headerLen,
	}, nil
}

func Load(filePath string) (*Table, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("Cannot open segment file: %w", err)
	}

	dataStartIndex := 0
	cookie := make([]byte, len(segmentCookie))
	n, err := f.Read(cookie)
	if err != nil {
		return nil, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if n != len(segmentCookie) || !bytes.Equal(cookie, []byte(segmentCookie)) {
		return nil, fmt.Errorf("Failed to read cookie in segment file")
	}
	dataStartIndex += n

	versionBuf := make([]byte, 2)
	n, err = f.Read(versionBuf)
	if err != nil {
		return nil, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if n != 2 {
		return nil, fmt.Errorf("Failed to read version in segment file")
	}
	dataStartIndex += n
	version := binary.BigEndian.Uint16(versionBuf)
	if version != segmentFileFormat {
		return nil, fmt.Errorf("Could not read segment file with version %d", version)
	}

	filterLenBuf := make([]byte, 4)
	n, err = f.Read(filterLenBuf)
	if err != nil {
		return nil, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if n != 4 {
		return nil, fmt.Errorf("Failed to read filter length in segment file")
	}
	dataStartIndex += n
	filterLen := binary.BigEndian.Uint32(filterLenBuf)
	if filterLen != bloom.Size {
		return nil, fmt.Errorf("Non-%d filter size is not supported", bloom.Size)
	}

	filterBuf := make([]byte, filterLen)
	n, err = f.Read(filterBuf)
	if err != nil {
		return nil, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if n != len(filterBuf) {
		return nil, fmt.Errorf("Failed to read filter in segment file")
	}
	dataStartIndex += n

	// TODO: Read index from disk.
	return &Table{
		FilePath: filePath,
		filter: bloom.Filter{
			Buf: [bloom.Size]byte(filterBuf),
		},
		dataStartIndex: dataStartIndex,
	}, nil
}

func Merge(newer *Table, older *Table, newFilePath string, last bool) (*Table, error) {
	newerEntriesPtr, err := newer.Read()
	if err != nil {
		return nil, err
	}

	olderEntriesPtr, err := older.Read()
	if err != nil {
		return nil, err
	}

	olderEntries, newerEntries := *olderEntriesPtr, *newerEntriesPtr
	newerSize, olderSize := len(newerEntries), len(olderEntries)
	newerIndex, olderIndex := 0, 0
	merged := make([]storage.EntryData, 0, olderSize+newerSize)

	for newerIndex < newerSize || olderIndex < olderSize {
		if newerIndex >= newerSize {
			merged = append(merged, olderEntries[olderIndex])
			olderIndex += 1
			continue
		}

		if olderIndex >= olderSize {
			new := newerEntries[newerIndex]
			if !last || new.Value != storage.Tombstone {
				merged = append(merged, new)
			}
			newerIndex += 1
			continue
		}

		new, old := newerEntries[newerIndex], olderEntries[olderIndex]

		if new.Key > old.Key {
			merged = append(merged, old)
			olderIndex += 1
		} else if new.Key == old.Key {
			if !last || new.Value != storage.Tombstone {
				merged = append(merged, new)
			}
			olderIndex += 1
			newerIndex += 1
		} else {
			if !last || new.Value != storage.Tombstone {
				merged = append(merged, new)
			}
			newerIndex += 1
		}
	}
	return Create(newFilePath, merged)
}

func (t *Table) Search(key string) (*string, error) {

	// If not found in bloom filter, no lookup needed.
	if !t.filter.Search(key) {
		return nil, nil
	}

	// TODO: Start from index location.
	data, err := os.ReadFile(t.FilePath)
	if err != nil {
		return nil, fmt.Errorf("Could not read segment file: %w", err)
	}
	ptr := t.dataStartIndex
	for ptr < len(data) {
		entry, read, err := storage.DecodeLogEntry(data[ptr:])
		if err != nil {
			return nil, fmt.Errorf("Could not read segment file: %w", err)
		}
		if entry.Key == key {
			return &entry.Value, nil
		}
		ptr += read
	}
	return nil, nil
}

func (t *Table) Read() (*[]storage.EntryData, error) {
	data, err := os.ReadFile(t.FilePath)
	if err != nil {
		return nil, fmt.Errorf("Could not read segment file: %w", err)
	}
	ptr := t.dataStartIndex
	entries := make([]storage.EntryData, 0)
	for ptr < len(data) {
		entry, read, err := storage.DecodeLogEntry(data[ptr:])
		if err != nil {
			return nil, fmt.Errorf("Could not read segment file: %w", err)
		}
		ptr += read
		entries = append(entries, *entry)
	}
	return &entries, nil
}
