package sstable

import (
	"bigsby/storage"
	"fmt"
	"os"
)

type Table struct {
	FilePath string
	// index  map[string, int]
	// filter BloomFilter
}

const DataFileName = "segment_table"
const segmentCookie = "BIGSBY"

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

	// TODO: Write bloom table and index to disk for loading.
	for _, entry := range data {
		_, err := f.Write(storage.EncodeLogEntry(entry))
		if err != nil {
			return nil, fmt.Errorf("Failed to write segment data: %w", err)
		}
	}

	return &Table{
		FilePath: filePath,
	}, nil
}

func Load(filePath string) (*Table, error) {

	_, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("Cannot read segment file: %w", err)
	}

	// TODO: Read bloom table and index to disk.
	return &Table{
		FilePath: filePath,
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

	// TODO: Read after bloom table and index when added.
	// TODO: Start from index location.
	data, err := os.ReadFile(t.FilePath)
	if err != nil {
		return nil, fmt.Errorf("Could not read segment file: %w", err)
	}
	ptr := 0
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
	ptr := 0
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
