package lsm

import (
	"bigsby/redblack"
	"bigsby/sstable"
	"bigsby/storage"
	"errors"
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
	memtable     *Memtable
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

func getWalPath(dataDirectory string) string {
	return filepath.Join(dataDirectory, "wal")
}

func readWal(filePath string) (*Memtable, error) {
	memtable := Memtable{}

	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		return &memtable, nil
	} else if err != nil {
		return nil, fmt.Errorf("Could not open wal: %w", err)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	ptr := 0
	for ptr < len(data) {
		entry, read, err := storage.DecodeLogEntry(data[ptr:])
		if err != nil {
			return nil, fmt.Errorf("Could not read wal: %w", err)
		}
		ptr += read
		memtable.Insert(entry.Key, entry.Value)
	}

	return &memtable, nil
}

func writeToWal(filePath string, entry storage.EntryData) error {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0660)
	if err != nil {
		return fmt.Errorf("Error opening WAL file: %w", err)
	}

	defer f.Close()

	_, err = f.Write(storage.EncodeLogEntry(entry))
	if err != nil {
		return fmt.Errorf("Error writing to WAL file: %w", err)
	}

	err = f.Sync()
	if err != nil {
		return fmt.Errorf("Error flushing WAL file: %w", err)
	}
	return nil
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
	t.memtable = &Memtable{}
	t.memtableSize = 0

	err = os.RemoveAll(getWalPath(t.settings.DataDirectory))
	if err != nil {
		return err
	}

	return nil
}

func New(settings *Settings) (*LSMTree, error) {
	segmentDirectory := getSegmentDirectory(settings.DataDirectory)

	err := os.MkdirAll(segmentDirectory, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("Failed to make segment dir: %w", err)
	}

	memtable, err := readWal(getWalPath(settings.DataDirectory))
	if err != nil {
		return nil, err
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
				return true
			}

			jInfo, err := files[j].Info()
			if err != nil {
				return false
			}

			return iInfo.ModTime().Before(jInfo.ModTime())
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
		memtable: memtable,
		settings: settings,
		segments: segments,
	}, nil
}

func (t *LSMTree) Insert(key KeyType, value ValueType) error {

	err := writeToWal(getWalPath(t.settings.DataDirectory), storage.EntryData{
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}

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
		for i := len(level) - 1; i >= 0; i-- {
			valuePtr, err := level[i].Search(key)
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
	var err error
	value := t.memtable.Search(key)
	if value == nil {
		value, err = t.searchSegments(key)
		if err != nil {
			return nil, err
		}
	}

	if value != nil && *value == storage.Tombstone {
		return nil, nil
	}
	return value, nil
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

func (t *LSMTree) PrintSegment(out io.Writer, segmentName string) {
	for _, segments := range t.segments {
		for _, segment := range segments {
			base, _ := strings.CutSuffix(filepath.Base(segment.FilePath), ".segment")
			if base == segmentName {
				segment.Print(out)
				return
			}
		}
	}
	fmt.Fprintf(out, "Could not find segment: %s\n", segmentName)
}

func (t *LSMTree) PrintSegments(out io.Writer) {
	for level, segments := range t.segments {
		io.WriteString(out, fmt.Sprintf("Level %d:\n", level))
		for _, segment := range segments {
			base, _ := strings.CutSuffix(filepath.Base(segment.FilePath), ".segment")
			fmt.Fprintln(out, "\t", base)
		}
	}
}
