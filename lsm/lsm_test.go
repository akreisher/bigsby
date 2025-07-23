package lsm

import (
	"bigsby/storage"
	"testing"
)

func TestWriteSegment(t *testing.T) {

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

	tree.Insert("zzz", "world")
	tree.Insert("good", "world")
	tree.Insert("hello", "world")
	tree.Flush()
	expectedEntries := []storage.EntryData{
		{Key: "good", Value: "world"},
		{Key: "hello", Value: "world"},
		{Key: "zzz", Value: "world"},
	}

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

	tree.Insert("zzz", "world")
	tree.Insert("good", "world")
	tree.Insert("hello", "world")
	tree.Flush()
	expectedEntries := []storage.EntryData{
		{Key: "good", Value: "world"},
		{Key: "hello", Value: "world"},
		{Key: "zzz", Value: "world"},
	}

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

	tree.Insert("zzz", "sleep")
	tree.Insert("good", "bye")
	tree.Insert("hello", "world")
	tree.Insert("new", "entry")
	tree.Flush()

	expectedEntries = []storage.EntryData{
		{Key: "good", Value: "bye"},
		{Key: "hello", Value: "world"},
		{Key: "new", Value: "entry"},
		{Key: "zzz", Value: "sleep"},
	}

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

	tree.Insert("dead", "stick")
	tree.Insert("good", "night")
	tree.Insert("hello", "world")

	if len(tree.segments) != 0 {
		t.Errorf("Segment data is not empty?")
	}

	val, err := tree.Search("hello")
	if err != nil {
		t.Error(err)
	}

	if *val != "world" {
		t.Errorf("Got bad value (expected world, got %s)", *val)
	}
}

func TestReadFromSegment(t *testing.T) {

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

	err = tree.Insert("hello", "first")
	if err != nil {
		t.Error("Failed to insert", err)
	}

	err = tree.Insert("zzz", "world")
	if err != nil {
		t.Error("Failed to insert")
	}
	err = tree.Insert("good", "world")
	if err != nil {
		t.Error("Failed to insert", err)
	}
	err = tree.Insert("hello", "world")
	if err != nil {
		t.Error("Failed to insert", err)
	}
	tree.Flush()

	if tree.memtable.Height() != 0 || tree.memtableSize != 0 {
		t.Error("Expected empty memtable after write")
	}

	val, err := tree.Search("hello")
	if err != nil {
		t.Error(err)
	}

	if *val != "world" {
		t.Errorf("Got bad value (expected world, got %s)", *val)
	}
}

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
