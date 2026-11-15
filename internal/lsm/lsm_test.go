package lsm

import (
	"errors"
	"github.com/akreisher/bigsby/internal/storage"
	"os"
	"testing"
)

// Test flushing the memtable.
func TestFlush(t *testing.T) {

	segmentDirectory := t.TempDir()

	tree, err := New(
		&Settings{
			CompactionLimit: 1000,
			DataDirectory:   segmentDirectory,
		},
	)
	if err != nil {
		t.Error(err)
	}

	expectedEntries := []storage.EntryData{
		{Key: "good", Value: "world"},
		{Key: "hello", Value: "world"},
		{Key: "zzz", Value: "world"},
	}
	for _, entry := range expectedEntries {
		tree.Insert(entry.Key, entry.Value)
	}
	err = tree.Flush()
	if err != nil {
		t.Errorf("Error flusing: %v", err)
	}

	// Memtable should now be empty.
	if tree.memtable.Height() != 0 {
		tree.PrintMemtable(os.Stdout)
		t.Errorf("Expected empty memtable after write, height:%d", tree.memtable.Height())
	}
	if tree.memtableSize != 0 {
		t.Errorf("Expected empty memtable after write, size: %d", tree.memtableSize)
	}

	// WAL should be cleared.
	if _, err := os.Stat(getWalPath(tree.settings.DataDirectory)); err == nil {
		t.Error("WAL still exists after flush.")
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Error(err)
	}

	// Segment 0 should exist and contain the newly flushed data.
	entries, err := tree.segments[0][0].Read()
	if err != nil {
		t.Errorf("Failed to read segment file: %v", err)
	}
	for i, entry := range *entries {
		if entry.Key != expectedEntries[i].Key {
			t.Errorf("Got unexpected segment key: %s (expected %s)", entry.Key, expectedEntries[i].Key)
		}
		if entry.Value != expectedEntries[i].Value {
			t.Errorf("Got unexpected segment value: %s (expected %s)", entry.Value, expectedEntries[i].Value)
		}
	}

	// Make sure we can still read entries from the tree after flush.
	for _, entry := range expectedEntries {
		val, err := tree.Search(entry.Key)
		if err != nil {
			t.Error(err)
		}
		if *val != entry.Value {
			t.Errorf("Got bad value (expected %s, got %s)", entry.Value, *val)
		}
	}

}

func TestMergeInserts(t *testing.T) {

	segmentDirectory := t.TempDir()

	tree, err := New(
		&Settings{
			CompactionLimit: 1000,
			DataDirectory:   segmentDirectory,
		},
	)
	if err != nil {
		t.Error(err)
	}

	expectedEntries := []storage.EntryData{
		{Key: "good", Value: "world"},
		{Key: "hello", Value: "world"},
		{Key: "zzz", Value: "world"},
	}
	for _, entry := range expectedEntries {
		tree.Insert(entry.Key, entry.Value)
	}
	tree.Flush()

	if tree.memtable.Height() != 0 || tree.memtableSize != 0 {
		t.Error("Expected empty memtable after write")
	}

	entries, err := tree.segments[0][0].Read()
	if err != nil {
		t.Errorf("Failed to read segment file: %v", err)
	}
	for i, entry := range *entries {
		if entry.Key != expectedEntries[i].Key {
			t.Errorf("Got unexpected segment key: %s (expected %s)", entry.Key, expectedEntries[i].Key)
		}
		if entry.Value != expectedEntries[i].Value {
			t.Errorf("Got unexpected segment value: %s (expected %s)", entry.Value, expectedEntries[i].Value)
		}
	}

	expectedEntries = []storage.EntryData{
		{Key: "good", Value: "bye"},
		{Key: "hello", Value: "world"},
		{Key: "new", Value: "entry"},
		{Key: "zzz", Value: "sleep"},
	}
	for _, entry := range expectedEntries {
		tree.Insert(entry.Key, entry.Value)
	}
	tree.Flush()

	entries, err = tree.segments[0][1].Read()
	if err != nil {
		t.Errorf("Failed to read segment file: %v", err)
	}

	for i, entry := range *entries {
		if entry.Key != expectedEntries[i].Key {
			t.Errorf("Got unexpected segment key: %s (expected %s)", entry.Key, expectedEntries[i].Key)
		}
		if entry.Value != expectedEntries[i].Value {
			t.Errorf("Got unexpected segment value: %s (expected %s)", entry.Value, expectedEntries[i].Value)
		}
	}
}

// Write to the memtable, and then test reading back.
func TestReadFromMemtable(t *testing.T) {

	segmentDirectory := t.TempDir()

	tree, err := New(
		&Settings{
			CompactionLimit: 10000,
			DataDirectory:   segmentDirectory,
		},
	)
	if err != nil {
		t.Error(err)
	}

	expectedEntries := []storage.EntryData{
		{Key: "foo", Value: "bar"},
		{Key: "hello", Value: "world"},
		{Key: "test", Value: "entry"},
	}
	for _, entry := range expectedEntries {
		tree.Insert(entry.Key, entry.Value)
	}

	// No segments should have been written.
	if len(tree.segments) != 0 {
		t.Errorf("Segment data is not empty?")
	}

	for _, entry := range expectedEntries {
		val, err := tree.Search(entry.Key)
		if err != nil {
			t.Error(err)
		}
		if *val != entry.Value {
			t.Errorf("Got bad value (expected %s, got %s)", entry.Value, *val)
		}
	}
}

// Test that we can remove an entry that is only in the memtable.
func TestRemoveFromMemtable(t *testing.T) {

	segmentDirectory := t.TempDir()

	tree, err := New(
		&Settings{
			CompactionLimit: 1000,
			DataDirectory:   segmentDirectory,
		},
	)

	if err != nil {

		t.Error(err)
	}

	tree.Insert("hello", "world")
	valPtr, err := tree.Search("hello")
	if valPtr == nil || err != nil {
		t.Error("Could not find key after insert")
	}

	if *valPtr != "world" {
		t.Error("Could not insert correct value")
	}

	err = tree.Remove("hello")
	if err != nil {
		t.Error(err)
	}

	valPtr, err = tree.Search("hello")
	if err != nil {
		t.Error(err)
	}

	if valPtr != nil {
		t.Errorf("Expected nil value for key after delete, got %s", *valPtr)
	}

	// Try to write more values and flush to memtable.
	tree.Insert("good", "bye")
	tree.Insert("something", "else")
	tree.Insert("new", "values")
	tree.Flush()

	// Still should not be able to find the value.
	if tree.memtable.Search("hello") != nil {
		t.Error("Found value for hello in memtable after delete/compaction")
	}
}

// Test that we can remove an entry that has already been flushed to a segment.
func TestRemoveFromSegment(t *testing.T) {
	segmentDirectory := t.TempDir()

	tree, err := New(
		&Settings{
			CompactionLimit: 1000,
			DataDirectory:   segmentDirectory,
		},
	)

	if err != nil {

		t.Error(err)
	}

	tree.Insert("hello", "world")
	tree.Insert("good", "bye")
	tree.Insert("something", "else")
	tree.Insert("new", "values")
	tree.Flush()

	// Should have moved hello out of memtable
	if tree.memtable.Search("hello") != nil {
		t.Error("Found value for hello in memtable after flush")
	}

	// But we should still be able to find it in the segment.
	valPtr, err := tree.Search("hello")
	if valPtr == nil || err != nil {
		t.Error("Could not find key after insert")
	}

	if *valPtr != "world" {
		t.Error("Could not insert correct value")
	}

	err = tree.Remove("hello")
	if err != nil {
		t.Error(err)
	}
	tree.Flush()

	valPtr, err = tree.Search("hello")
	if err != nil {
		t.Error(err)
	}

	if valPtr != nil {
		t.Errorf("Expected nil value for key after delete, got %s", *valPtr)
	}

	// Should not be able to find the value.
	if tree.memtable.Search("hello") != nil {
		t.Error("Found value for hello in memtable after delete/compaction")
	}
}

// Test that the WAL is read at startup and can be rewritten to.
func TestRecoverWAL(t *testing.T) {
	segmentDirectory := t.TempDir()

	tree, err := New(
		&Settings{
			CompactionLimit: 1000,
			DataDirectory:   segmentDirectory,
		},
	)
	if err != nil {

		t.Error(err)
	}

	expectedEntries := []storage.EntryData{
		{Key: "foo", Value: "bar"},
		{Key: "hello", Value: "world"},
		{Key: "red", Value: "blue"},
	}

	for _, entry := range expectedEntries {
		tree.Insert(entry.Key, entry.Value)
	}

	// Read new tree recovered from WAL.
	tree, err = New(
		&Settings{
			CompactionLimit: 1000,
			DataDirectory:   segmentDirectory,
		},
	)
	if err != nil {

		t.Error(err)
	}

	for _, entry := range expectedEntries {
		found, err := tree.Search(entry.Key)
		if err != nil {
			t.Error(err)
		}
		if found == nil {
			t.Errorf("No value found for key %s", entry.Key)
		}

		if *found != entry.Value {
			t.Errorf("Expected entry value %s, got %s", *found, entry.Value)
		}
	}

	// Test write existing wal.
	newEntries := []storage.EntryData{
		{Key: "baz", Value: "qux"},
		{Key: "true", Value: "false"},
	}

	for _, entry := range newEntries {
		tree.Insert(entry.Key, entry.Value)
	}

	// Read new tree recovered from WAL.
	tree, err = New(
		&Settings{
			CompactionLimit: 1000,
			DataDirectory:   segmentDirectory,
		},
	)
	if err != nil {

		t.Error(err)
	}

	for _, entry := range expectedEntries {
		found, err := tree.Search(entry.Key)
		if err != nil {
			t.Error(err)
		}
		if found == nil {
			t.Errorf("No value found for key %s", entry.Key)
		}

		if *found != entry.Value {
			t.Errorf("Expected entry value %s, got %s", *found, entry.Value)
		}
	}

	for _, entry := range newEntries {
		found, err := tree.Search(entry.Key)
		if err != nil {
			t.Error(err)
		}
		if found == nil {
			t.Errorf("No value found for key %s", entry.Key)
		}

		if *found != entry.Value {
			t.Errorf("Expected entry value %s, got %s", *found, entry.Value)
		}
	}
}
