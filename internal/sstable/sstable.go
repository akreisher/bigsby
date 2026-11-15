package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/akreisher/bigsby/internal/bloom"
	"github.com/akreisher/bigsby/internal/storage"
	"io"
	"math"
	"os"
)

const segmentCookie = "BIGSBYSEGMENT"

type indexEntry struct {
	Key      string
	Position int
}

type Table struct {
	FilePath       string
	index          []indexEntry
	filter         bloom.Filter
	dataStartIndex int
}

const DataFileName = "segment_table"

// TODO: Non-ascii leading char to avoid reading file as text
const segmentFileFormat = 1
const sparsityFactor = 0.01

// SSTable Requirements:
// - Immutable
// - Sorted
// - Can look up value
// - Can print all values
// - Can create new segment from memtable
// - Can merge two segments together

func writeHeader(f *os.File) (int, error) {
	// Header format:
	//    cookie string "BIGSBYSEGMENT"
	//    16-bit version number
	header := make([]byte, len(segmentCookie)+2)
	copy(header, segmentCookie)
	binary.BigEndian.PutUint16(header[len(segmentCookie):], segmentFileFormat)
	return f.Write(header)
}

func writeBloomFilter(f *os.File, filter bloom.Filter) (int, error) {
	buf := make([]byte, len(filter.Buf)+4)
	binary.BigEndian.PutUint32(buf, uint32(len(filter.Buf)))
	copy(buf[4:], filter.Buf[:])
	return f.Write(buf)
}

func writeIndex(f *os.File, index []indexEntry) (int, error) {
	total := 0

	// Write index size first.
	numEntries := make([]byte, 4)
	binary.BigEndian.PutUint32(numEntries, uint32(len(index)))
	f.Write(numEntries)
	total += 4

	// Write each entry.
	for _, entry := range index {
		// Key length.
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(len(entry.Key)))
		n, err := f.Write(buf)
		if err != nil {
			return 0, err
		}
		total += n

		// Key.
		n, err = f.WriteString(entry.Key)
		if err != nil {
			return 0, err
		}
		total += n

		// Position.
		binary.BigEndian.PutUint32(buf, uint32(entry.Position))
		n, err = f.Write(buf)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// Create a new SStable and return it.
func Create(filePath string, data []storage.EntryData) (*Table, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("Could not create segment file: %w", err)
	}

	defer f.Close()

	position := 0

	n, err := writeHeader(f)
	if err != nil {
		return nil, fmt.Errorf("Failed to write header: %w", err)
	}

	position += n

	// Create bloom filter and index.
	filter := bloom.Filter{}
	indexLimit := int(math.Floor(sparsityFactor * float64(len(data))))
	index := make([]indexEntry, indexLimit)
	seekPos := 0
	for i, entry := range data {
		filter.Insert(entry.Key)
		if indexLimit != 0 && i%indexLimit == 0 {
			index = append(index, indexEntry{
				Position: seekPos,
			})
		}
		seekPos += 8 + len(entry.Key) + len(entry.Value)
	}

	// Write bloom filter to file.
	n, err = writeBloomFilter(f, filter)
	if err != nil {
		return nil, fmt.Errorf("Failed to write bloom filter: %w", err)
	}
	position += n

	// Write index to file.
	n, err = writeIndex(f, index)
	if err != nil {
		return nil, fmt.Errorf("Failed to write index: %w", err)
	}
	position += n

	for _, entry := range data {
		_, err := f.Write(storage.EncodeLogEntry(entry))
		if err != nil {
			return nil, fmt.Errorf("Failed to write segment data: %w", err)
		}
	}

	return &Table{
		FilePath:       filePath,
		filter:         filter,
		dataStartIndex: position,
		index:          index,
	}, nil
}

func readExactly(f *os.File, n int) ([]byte, error) {
	buf := make([]byte, n)
	numRead, err := f.Read(buf)
	if err != nil {
		return nil, err
	}
	if numRead != n {
		return nil, fmt.Errorf("Failed to exactly %d bytes, (read %d instead)", n, numRead)
	}
	return buf, nil
}

func readHeader(f *os.File) (int, error) {
	res := 0

	// Read file cookie.
	cookie, err := readExactly(f, len(segmentCookie))
	if err != nil {
		return 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if !bytes.Equal(cookie, []byte(segmentCookie)) {
		return 0, fmt.Errorf("Failed to read cookie in segment file")
	}
	res += len(segmentCookie)

	// Read file version.
	versionBuf, err := readExactly(f, 2)
	if err != nil {
		return 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	res += 2
	version := binary.BigEndian.Uint16(versionBuf)
	if version != segmentFileFormat {
		return 0, fmt.Errorf("Could not read segment file with version %d", version)
	}
	return res, nil
}

func readFilter(f *os.File) (*bloom.Filter, int, error) {
	res := 0
	filterLenBuf, err := readExactly(f, 4)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	res += 4
	filterLen := int(binary.BigEndian.Uint32(filterLenBuf))
	if filterLen != bloom.Size {
		return nil, 0, fmt.Errorf("Non-%d filter size is not supported", bloom.Size)
	}

	filterBuf, err := readExactly(f, filterLen)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	res += filterLen

	return &bloom.Filter{
		Buf: [bloom.Size]byte(filterBuf),
	}, res, nil
}

func readIndexEntry(f *os.File) (*indexEntry, int, error) {
	res := 0

	// Key length.
	keyLenBuf := make([]byte, 4)
	n, err := f.Read(keyLenBuf)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if n != 4 {
		return nil, 0, fmt.Errorf("Failed to read index entry len in segment file")
	}
	res += n
	keyLen := binary.BigEndian.Uint32(keyLenBuf)

	// Key.
	entry := make([]byte, keyLen)
	n, err = f.Read(entry)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if n != int(keyLen) {
		return nil, 0, fmt.Errorf("Failed to read index entry key in segment file")
	}
	res += n

	// Position.
	posBuf := make([]byte, 4)
	n, err = f.Read(posBuf)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if n != 4 {
		return nil, 0, fmt.Errorf("Failed to read index entry position in segment file")
	}
	res += n

	return &indexEntry{
		Key:      string(entry),
		Position: int(binary.BigEndian.Uint32(posBuf)),
	}, n, nil
}

func readIndex(f *os.File) ([]indexEntry, int, error) {
	res := 0

	numEntriesBuf, err := readExactly(f, 4)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	res += 4

	numEntries := binary.BigEndian.Uint32(numEntriesBuf)
	index := make([]indexEntry, numEntries)

	for range numEntries {
		entry, n, err := readIndexEntry(f)
		if err != nil {
			return nil, 0, fmt.Errorf("Failed to read index entry: %w", err)
		}
		index = append(index, *entry)
		res += n
	}

	return index, res, nil
}

// Load a SSTable from a file.
func Load(filePath string) (*Table, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("Cannot open segment file: %w", err)
	}

	defer f.Close()

	dataStartIndex := 0
	n, err := readHeader(f)
	if err != nil {
		return nil, fmt.Errorf("Failed to read segment header: %w", err)
	}
	dataStartIndex += n

	filter, n, err := readFilter(f)
	if err != nil {
		return nil, fmt.Errorf("Failed to read segment filter: %w", err)
	}
	dataStartIndex += n

	index, n, err := readIndex(f)
	if err != nil {
		return nil, fmt.Errorf("Failed to read segment index: %w", err)
	}
	dataStartIndex += n

	return &Table{
		FilePath:       filePath,
		filter:         *filter,
		index:          index,
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
			return nil, fmt.Errorf("Could not decode segment file entry: %w", err)
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

func (t *Table) Print(out io.Writer) error {
	data, err := t.Read()
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(out, "Segment path: %s\n", t.FilePath)
	if err != nil {
		return err
	}

	_, err = io.WriteString(out, "Index:")
	for _, entry := range t.index {
		if entry.Position == 0 {
			continue
		}
		_, err = fmt.Fprintf(out, "  [%s]: %d\n", entry.Key, entry.Position)
		if err != nil {
			return err
		}
	}
	_, err = fmt.Fprint(out, "Table:\n\n")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%v\n", data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "")
	if err != nil {
		return err
	}
	return nil
}
