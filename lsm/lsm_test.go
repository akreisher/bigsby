package lsm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteSegment(t *testing.T) {

	segmentDirectory := t.TempDir()

	tree, err := New(
		&Settings{
			CompactionLimit: 20,
			DataDirectory:   segmentDirectory,
		},
	)

	if err != nil {

		t.Error(err)
	}

	tree.Insert("zzz", "world")
	tree.Insert("good", "world")
	tree.Insert("hello", "world")

	if tree.memtable.Height() != 0 || tree.memtableSize != 0 {
		t.Error("Expected empty memtable after write")
	}

	entries, err := tree.segment.Read()
	if err != nil {
		t.Errorf("Failed to read segment file: %v", err)
	}

	// Make sure data is sorted.
	expectedEntries := []string{"good,world", "hello,world", "zzz,world"}
	for i, expected := range expectedEntries {
		if entries[i] != expected {
			t.Errorf("Got unexpected segment data: %s (expected %s)", entries[i], expected)
		}
	}
}

func TestMergeInserts(t *testing.T) {

	segmentDirectory := t.TempDir()

	tree, err := New(
		&Settings{
			CompactionLimit: 1,
			DataDirectory:   segmentDirectory,
		},
	)

	if err != nil {

		t.Error(err)
	}

	tree.Insert("zzz", "world")
	tree.Insert("good", "world")
	tree.Insert("hello", "world")

	if tree.memtable.Height() != 0 || tree.memtableSize != 0 {
		t.Error("Expected empty memtable after write")
	}
	entries, err := tree.segment.Read()
	if err != nil {
		t.Errorf("Failed to read segment file: %v", err)
	}

	// Make sure data is sorted.
	expectedEntries := []string{"good,world", "hello,world", "zzz,world"}
	for i, expected := range expectedEntries {
		if entries[i] != expected {
			t.Errorf("Got unexpected segment data: %s (expected %s)", entries[i], expected)
		}
	}

	tree.Insert("zzz", "sleep")
	tree.Insert("good", "bye")
	tree.Insert("hello", "world")
	tree.Insert("new", "entry")

	entries, err = tree.segment.Read()
	if err != nil {
		t.Errorf("Failed to read segment file: %v", err)
	}

	// Make sure data is sorted.
	expectedEntries = []string{"good,bye", "hello,world", "new,entry", "zzz,sleep"}
	for i, expected := range expectedEntries {
		if entries[i] != expected {
			t.Errorf("Got unexpected segment data: %s (expected %s)", entries[i], expected)
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

	data, err := tree.segment.Read()
	if err != nil {
		t.Errorf("Failed to read segment file: %v", err)
	}

	if len(data) != 0 {
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
			CompactionLimit: 1,
			DataDirectory:   segmentDirectory,
		},
	)

	if err != nil {

		t.Error(err)
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

	segmentPath := filepath.Join(segmentDirectory, C1)
	_, err = os.Stat(segmentPath)
	if err != nil {
		t.Errorf("Failed to write segment file: %v", err)
	}

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
			CompactionLimit: 20,
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

	segmentData, err := tree.segment.Read()
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

	// Still should not be able to find the value.
	segmentData, err = tree.segment.Read()
	if err != nil {
		t.Error(err)
	}

	for _, entry := range segmentData {
		if strings.HasPrefix(entry, "hello") {
			t.Error("Found key in segment after delete/compaction")
		}
	}
	if tree.memtable.Search("hello") != nil {
		t.Error("Found value for hello in memtable after delete/compaction")
	}
}

func TestRemoveFromSegment(t *testing.T) {
	segmentDirectory := t.TempDir()

	tree, err := New(
		&Settings{
			CompactionLimit: 1,
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

	valPtr, err = tree.Search("hello")
	if err != nil {
		t.Error(err)
	}

	if valPtr != nil {
		t.Errorf("Expected nil value for key after delete, got %s", *valPtr)
	}

	// Should not be able to find the value.
	segmentData, err := tree.segment.Read()
	if err != nil {
		t.Error(err)
	}

	for _, entry := range segmentData {
		if strings.HasPrefix(entry, "hello") {
			t.Error("Found key in segment after delete/compaction")
		}
	}
	if tree.memtable.Search("hello") != nil {
		t.Error("Found value for hello in memtable after delete/compaction")
	}
}
