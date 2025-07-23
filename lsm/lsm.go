package lsm

import (
	"bigsby/redblack"
	"bigsby/sstable"
	"bigsby/storage"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type KeyType = string
type ValueType = string
type Memtable = redblack.Tree[KeyType, ValueType]
type Node = redblack.Node[KeyType, ValueType]

type LSMTree struct {
	memtable     Memtable
	levels       [][]sstable.Table
	memtableSize int
	settings     *Settings
	segments     [][]sstable.Table
}

type Settings struct {
	CompactionLimit      int
	DataDirectory        string
	LevelZeroMaxSegments int
}

const segmentNameLetters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const segmentSuffix = ".segment"

func getSegmentDirectory(dataDirectory string) string {
	return filepath.Join(dataDirectory, "segments")
}

func (t *LSMTree) generateNewSegmentPath(level int) (*string, error) {
	levelDir := filepath.Join(getSegmentDirectory(t.settings.DataDirectory), strconv.Itoa(level))
	err := os.MkdirAll(levelDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	for {
		b := make([]byte, 16)
		for i := range b {
			b[i] = segmentNameLetters[rand.Intn(len(segmentNameLetters))]
		}

		path := filepath.Join(levelDir, string(b)+segmentSuffix)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return &path, nil
		}
	}
}

func (t *LSMTree) Flush() error {

	// segmentData, err := t.segment.Read()
	// if err != nil {
	// 	return err
	// }

	entries := make([]storage.EntryData, 0)
	for k, v := range t.memtable.InOrder() {
		// While we still have segment values, we need to compare.
		entries = append(entries, storage.EntryData{
			Key:   k,
			Value: v,
		})
	}

	path, err := t.generateNewSegmentPath(0)
	if err != nil {
		return fmt.Errorf("Error getting level 0 segment path: %w", err)
	}

	segment, err := sstable.Create(*path, entries)
	if err != nil {
		return err
	}

	if len(t.segments) == 0 {
		t.segments = append(t.segments, make([]sstable.Table, 0))
	}
	t.segments[0] = append(t.segments[0], *segment)

	// TODO: Merge/Compact

	// Reset memtable.
	// TODO: Make memtable immutable while writing segment
	// and make a new one for incoming writes.
	t.memtable = Memtable{}
	t.memtableSize = 0
	return nil
}

func New(settings *Settings) (*LSMTree, error) {
	segmentDirectory := getSegmentDirectory(settings.DataDirectory)

	err := os.MkdirAll(segmentDirectory, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("Failed to make segment dir: %w", err)
	}

	segments := make([][]sstable.Table, 0)
	level := 0
	for {
		levelDir := filepath.Join(segmentDirectory, strconv.Itoa(level))
		files, err := os.ReadDir(levelDir)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
		}

		sort.Slice(files, func(i, j int) bool {
			iInfo, err := files[i].Info()
			if err != nil {
				return false
			}

			jInfo, err := files[j].Info()
			if err != nil {
				return true
			}

			return iInfo.ModTime().After(jInfo.ModTime())
		})

		segments = append(segments, make([]sstable.Table, 0))
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			if !strings.HasSuffix(f.Name(), segmentSuffix) {
				continue
			}

			segment, err := sstable.Load(filepath.Join(levelDir, f.Name()))
			if err != nil {
				return nil, fmt.Errorf("Failed to read segment: %w", err)
			}
			segments[level] = append(segments[level], *segment)
		}
		level++
	}

	return &LSMTree{
		settings: settings,
		segments: segments,
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

func (t *LSMTree) searchSegments(key KeyType) (*ValueType, error) {
	for _, level := range t.segments {
		for _, segment := range level {
			valuePtr, err := segment.Search(key)
			if err != nil {
				return nil, err
			}

			if valuePtr != nil {
				return valuePtr, nil
			}
		}
	}
	return nil, nil
}

func (t *LSMTree) Search(key KeyType) (*ValueType, error) {
	value := t.memtable.Search(key)
	if value != nil {
		if *value == storage.Tombstone {
			return nil, nil
		}

		return value, nil
	}
	return t.searchSegments(key)
}

func (t *LSMTree) Remove(key KeyType) error {
	return t.Insert(key, storage.Tombstone)
}

func (t *LSMTree) PrintMemtable(out io.Writer) {
	io.WriteString(out, fmt.Sprintf("Size: %d\n", t.memtableSize))
	io.WriteString(out, fmt.Sprintf("Height: %d\n", t.memtable.Height()))
	io.WriteString(out, "Tree:\n\n")
	t.memtable.Print(out)
	io.WriteString(out, "\n")
}

func (t *LSMTree) PrintSegments(out io.Writer) {
	for level, segments := range t.segments {
		io.WriteString(out, fmt.Sprintf("Level %d:\n", level))
		for _, segment := range segments {
			data, err := segment.Read()
			if err != nil {
				panic(err)
			}

			io.WriteString(out, fmt.Sprintf("Segment path: %s\n", segment.FilePath))
			io.WriteString(out, "Table:\n\n")
			io.WriteString(out, fmt.Sprintf("%v\n", data))
			io.WriteString(out, "\n")

		}
	}
}
