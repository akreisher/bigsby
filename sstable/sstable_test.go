package sstable

import (
	"bigsby/storage"
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
		val, err := table.Search(entry.Key)
		if err != nil {
			t.Error(err)
			continue
		}
		if val == nil {
			t.Errorf("Got no segment value for key %s", entry.Key)
			continue
		}
		if entry.Value != *val {
			t.Errorf("Got unexpected segment value: %s (expected %s)", *val, entry.Value)
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
		val, err := table.Search(entry.Key)
		if err != nil {
			t.Error(err)
			continue
		}
		if val == nil {
			t.Errorf("Got no segment value for key %s", entry.Key)
			continue
		}
		if entry.Value != *val {
			t.Errorf("Got unexpected segment value: %s (expected %s)", *val, entry.Value)
			continue
		}
	}
}
