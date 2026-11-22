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

	// Test segment file is actually created.
	table, err := Create(segmentFile, expectedEntries)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = os.Stat(segmentFile); err != nil {
		t.Fatal(err)
	}

	// Test searching segment file.
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

	// Should fail on unsorted data.
	expectedEntries = []storage.EntryData{
		{Key: "good", Value: "world"},
		{Key: "hello", Value: "world"},
		{Key: "bla", Value: "blu"},
		{Key: "zzz", Value: "world"},
	}

	table, err = Create(segmentFile, expectedEntries)
	if err == nil {
		t.Errorf("Expected error on unsorted data")
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

func TestMerge(t *testing.T) {
	segmentDirectory := t.TempDir()
	segmentFile1 := filepath.Join(segmentDirectory, "segment1")
	segmentFile2 := filepath.Join(segmentDirectory, "segment2")
	segmentFileMerged := filepath.Join(segmentDirectory, "segment3")

	expectedEntries1 := []storage.EntryData{
		{Key: "foo", Value: "baz"},
		{Key: "good", Value: "world"},
		{Key: "hello", Value: "world"},
		{Key: "zzz", Value: "world"},
	}

	expectedEntries2 := []storage.EntryData{
		{Key: "bla", Value: "blu"},
		{Key: "foo", Value: "bar"},
		{Key: "quz", Value: "qux"},
	}

	combinedEntries := []storage.EntryData{
		{Key: "bla", Value: "blu"},
		{Key: "foo", Value: "bar"},
		{Key: "good", Value: "world"},
		{Key: "hello", Value: "world"},
		{Key: "quz", Value: "qux"},
		{Key: "zzz", Value: "world"},
	}

	table1, err := Create(segmentFile1, expectedEntries1)
	if err != nil {
		t.Fatal(err)
	}

	table2, err := Create(segmentFile2, expectedEntries2)
	if err != nil {
		t.Fatal(err)
	}

	merged, err := Merge(table1, table2, segmentFileMerged, false)
	if err != nil {
		t.Fatal(err)
	}

	mergedEntries, err := merged.Read()
	if err != nil {
		t.Fatal(err)
	}

	// Ensure ordering.
	for i, entry := range *mergedEntries {
		expected := combinedEntries[i]
		if expected.Key != entry.Key {
			t.Errorf("Got unexpected segment key: %s (expected %s)", entry.Key, expected.Key)
			continue
		}
		if expected.Value != entry.Value {
			t.Errorf("Got unexpected segment value: %s (expected %s)", entry.Value, expected.Value)
			continue
		}
	}
}
