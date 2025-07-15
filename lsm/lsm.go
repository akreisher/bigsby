package lsm

import (
	"bigsby/redblack"
	"bigsby/sstable"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

type KeyType = string
type ValueType = string
type Memtable = redblack.Tree[KeyType, ValueType]
type Node = redblack.Node[KeyType, ValueType]

type LSMTree struct {
	memtable     Memtable
	memtableSize int
	settings     *Settings
	segment      *sstable.Table
}

type Settings struct {
	CompactionLimit int
	DataDirectory   string
}

const C1 = "./c1.segment"

// TODO: Out-of-band value for this to avoid mixing up values.
const TOMBSTONE = "<BIGSBY_TOMBSTONE>"

func generateLogString(key KeyType, value ValueType) string {
	// TODO: Length delimited entries instead of CSV.
	return key + "," + value + "\n"
}

func readLogEntry(entry string) (*KeyType, *ValueType, error) {
	parts := strings.Split(entry, ",")
	if len(parts) != 2 {
		return nil, nil, fmt.Errorf("Invalid data found: %s", entry)
	}
	return &parts[0], &parts[1], nil
}

func mergeTreeAndSegment(memtable Memtable, segment []string) ([]string, error) {
	newSegment := make([]string, 0)
	i := 0

	for memKey, memValue := range memtable.InOrder() {
		// While we still have segment values, we need to compare.
		if i < len(segment) {
			segKeyPtr, segValuePtr, err := readLogEntry(segment[i])
			if err != nil {
				return nil, err
			}
			segKey, segValue := *segKeyPtr, *segValuePtr

			// Write segment key.
			for segKey < memKey && i < len(segment) {
				newSegment = append(newSegment, generateLogString(segKey, segValue))
				i += 1
				if i < len(segment) {
					segKeyPtr, segValuePtr, err = readLogEntry(segment[i])
					if err != nil {
						return nil, err
					}
					segKey, segValue = *segKeyPtr, *segValuePtr
				}
			}

			// For duplicates, memtable value takes priority, we still shift the segment.
			if memValue != TOMBSTONE {
				newSegment = append(newSegment, generateLogString(memKey, memValue))
			}
			if segKey == memKey {
				i += 1
			}

		} else {
			if memValue != TOMBSTONE {
				newSegment = append(newSegment, generateLogString(memKey, memValue))
			}
		}
	}

	for i < len(segment) {
		segKeyPtr, segValuePtr, err := readLogEntry(segment[i])
		if err != nil {
			return nil, err
		}
		segKey, segValue := *segKeyPtr, *segValuePtr
		newSegment = append(newSegment, generateLogString(segKey, segValue))
		i += 1
	}

	return newSegment, nil
}

func (t *LSMTree) Flush() error {

	// Read next level segment data.
	segmentData, err := t.segment.Read()
	if err != nil {
		return err
	}

	mergedData, err := mergeTreeAndSegment(t.memtable, segmentData)
	if err != nil {
		return err
	}

	// TODO: Write to new segment so we can keep serving the old one.
	t.segment.Write(mergedData)

	// Reset memtable.
	// TODO: Make memtable immutable while writing segment
	// and make a new one for incoming writes.
	t.memtable = Memtable{}
	t.memtableSize = 0
	return nil
}

func New(settings *Settings) (*LSMTree, error) {
	segment, err := sstable.New(filepath.Join(settings.DataDirectory, C1))
	if err != nil {
		return nil, err
	}

	return &LSMTree{
		settings: settings,
		segment:  segment,
	}, nil
}

func (t *LSMTree) Insert(key KeyType, value ValueType) error {
	// TODO: WAL before inserting to memtable.
	t.memtable.Insert(key, value)
	t.memtableSize += len(key) + len(value)

	if t.memtableSize > t.settings.CompactionLimit {
		err := t.Flush()
		if err != nil {
			return fmt.Errorf("Error flushing memtable: %w", err)
		}
	}
	return nil
}

func (t *LSMTree) Search(key KeyType) (*ValueType, error) {

	value := t.memtable.Search(key)
	if value != nil {
		if *value == TOMBSTONE {
			return nil, nil
		}

		return value, nil
	}

	// Search through segments.
	entries, err := t.segment.Read()
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		k, v, err := readLogEntry(entry)
		if err != nil {
			return nil, err
		}

		if key == *k {
			return v, nil
		}
	}
	return nil, nil
}

func (t *LSMTree) Remove(key KeyType) error {
	return t.Insert(key, TOMBSTONE)
}

func (t *LSMTree) PrintMemtable(out io.Writer) {
	io.WriteString(out, fmt.Sprintf("Size: %d\n", t.memtableSize))
	io.WriteString(out, fmt.Sprintf("Height: %d\n", t.memtable.Height()))
	io.WriteString(out, "Tree:\n\n")
	t.memtable.Print(out)
	io.WriteString(out, "\n")
}

func (t *LSMTree) PrintSegment(out io.Writer) {
	data, err := t.segment.Read()
	if err != nil {
		panic(err)
	}

	io.WriteString(out, fmt.Sprintf("Segment name: %s\n", t.segment.FileName))
	io.WriteString(out, "Table:\n\n")
	io.WriteString(out, fmt.Sprintf("%v\n", data))
	io.WriteString(out, "\n")
}
