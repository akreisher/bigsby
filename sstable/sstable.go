package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Table struct {
	SegmentDirectory string
	dataDirectory    string
	// index  map[string, int]
	// filter BloomFilter
}

const DataFileName = "segment_data"

func New(segmentName string, dataDirectory string) (*Table, error) {

	segmentDirectory := filepath.Join(dataDirectory, segmentName)
	os.MkdirAll(segmentDirectory, os.ModePerm)
	// TODO: Read segment meta-data for existing
	// segment to initialize index/bloom filter.
	return &Table{
		SegmentDirectory: segmentDirectory,
	}, nil
}

func (t *Table) Read() ([]string, error) {
	dat, err := os.ReadFile(filepath.Join(t.SegmentDirectory, DataFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		} else {
			return nil, fmt.Errorf("Could not read segment file (data %s): %w", dat, err)
		}

	}
	data := string(dat)
	if data == "" {
		return []string{}, nil
	}
	return strings.Split(strings.TrimRight(string(dat), "\n"), "\n"), nil
}

func (t *Table) Write(data []string) error {
	f, err := os.Create(filepath.Join(t.SegmentDirectory, DataFileName))
	if err != nil {
		return fmt.Errorf("Failed to open segment: %w", err)
	}
	for _, entry := range data {
		f.WriteString(entry)
	}
	return nil
}
