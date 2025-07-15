package sstable

import (
	"fmt"
	"os"
	"strings"
)

type Table struct {
	FileName string
	// index  map[string, int]
	// filter BloomFilter
}

func New(fileName string) (*Table, error) {
	// TODO: Read segment meta-data for existing
	// segment to initialize index/bloom filter.
	return &Table{
		FileName: fileName,
	}, nil
}

func (t *Table) Read() ([]string, error) {
	dat, err := os.ReadFile(t.FileName)
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
	f, err := os.Create(t.FileName)
	if err != nil {
		return fmt.Errorf("Failed to open segment: %w", err)
	}
	for _, entry := range data {
		f.WriteString(entry)
	}
	return nil
}
