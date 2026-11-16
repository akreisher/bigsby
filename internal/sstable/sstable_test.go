package sstable

import (
	"github.com/akreisher/bigsby/internal/storage"
	"os"
	"path/filepath"
	"testing"
)

func TestCreate(t *testing.T) {
	segmentDirectory := t.TempDir()
	segmentFile := filepath.Join(segmentDirectory, "tester")

	expectedEntries := []storage.EntryData{
		{Key: "good", Value: "world"},
		{Key: "hello", Value: "world"},
		{Key: "zzz", Value: "world"},
	}

	table, err := Create(segmentFile, expectedEntries)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = os.Stat(segmentFile); err != nil {
		t.Fatal(err)
	}

	for _, entry := range expectedEntries {
		found, err := table.Search(entry.Key)
		if err != nil {
			t.Error(err)
			continue
		}
		if found == nil {
			t.Errorf("Got no segment value for key %s", entry.Key)
			continue
		}
		if entry.Value != found.Value {
			t.Errorf("Got unexpected segment value: %s (expected %s)", found.Value, entry.Value)
			continue
		}
	}
}

func TestLoad(t *testing.T) {
	segmentDirectory := t.TempDir()
	segmentFile := filepath.Join(segmentDirectory, "tester")

	expectedEntries := []storage.EntryData{
		{Key: "good", Value: "world"},
		{Key: "hello", Value: "world"},
		{Key: "zzz", Value: "world"},
	}

	_, err := Create(segmentFile, expectedEntries)
	if err != nil {
		t.Fatal(err)
	}

	table, err := Load(segmentFile)
	if err != nil {
		t.Fatal(err)
	}

	for _, entry := range expectedEntries {
		found, err := table.Search(entry.Key)
		if err != nil {
			t.Error(err)
			continue
		}
		if found == nil {
			t.Errorf("Got no segment value for key %s", entry.Key)
			continue
		}
		if entry.Value != found.Value {
			t.Errorf("Got unexpected segment value: %s (expected %s)", found.Value, entry.Value)
			continue
		}
	}
}
