package lsm

import (
	"errors"
	"fmt"
	"github.com/akreisher/bigsby/internal/manifest"
	"github.com/akreisher/bigsby/internal/redblack"
	"github.com/akreisher/bigsby/internal/sstable"
	"github.com/akreisher/bigsby/internal/storage"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type KeyType = string
type ValueType struct {
	Value   string
	Deleted bool
}
type Memtable = redblack.Tree[KeyType, ValueType]
type Node = redblack.Node[KeyType, ValueType]

type LSMTree struct {
	manifest     *manifest.Manifest
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

func getManifestPath(dataDirectory string) string {
	return filepath.Join(dataDirectory, "manifest.json")
}

func getSegmentPath(dataDirectory string, segmentFile string) string {
	return filepath.Join(getSegmentDirectory(dataDirectory), segmentFile)
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
		memtable.Insert(entry.Key, ValueType{Value: entry.Value, Deleted: entry.Deleted})
	}

	return &memtable, nil
}

func writeToWal(filePath string, entry storage.EntryData) error {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
	if err != nil {
		return fmt.Errorf("Error opening WAL file: %w", err)
	}

	defer f.Close()

	data, err := storage.EncodeLogEntry(entry)
	if err != nil {
		return fmt.Errorf("Failed to encode wal data: %w", err)
	}

	_, err = f.Write(data)
	if err != nil {
		return fmt.Errorf("Error writing to WAL file: %w", err)
	}

	err = f.Sync()
	if err != nil {
		return fmt.Errorf("Error flushing WAL file: %w", err)
	}
	return nil
}

func (t *LSMTree) Flush() error {

	entries := make([]storage.EntryData, 0)
	for k, v := range t.memtable.InOrder() {
		entries = append(entries, storage.EntryData{
			Key:     k,
			Value:   v.Value,
			Deleted: v.Deleted,
		})
	}

	err := os.MkdirAll(getSegmentDirectory(t.settings.DataDirectory), os.ModePerm)
	if err != nil {
		return fmt.Errorf("Error creating segment dir: %v", err)
	}

	var path string
	segmentId := make([]byte, 16)
	for {
		for i := range segmentId {
			segmentId[i] = segmentNameLetters[rand.Intn(len(segmentNameLetters))]
		}

		path = getSegmentPath(t.settings.DataDirectory, string(segmentId)+segmentSuffix)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			break
		} else if err != nil {
			return fmt.Errorf("Error getting level 1 segment path: %w", err)
		}
	}

	segment, err := sstable.Create(path, entries)
	if err != nil {
		return fmt.Errorf("Error creating segment: %v", err)
	}

	err = t.manifest.AddSegment(string(segmentId), 0)
	if err != nil {
		return fmt.Errorf("Error adding new segment to manifest: %v", err)
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

	// Since the memtable is now flushed, we can safely clear the WAL.
	err = os.RemoveAll(getWalPath(t.settings.DataDirectory))
	if err != nil {
		return fmt.Errorf("Error removing wal: %v", err)
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

	manifest, err := manifest.LoadManifest(getManifestPath(settings.DataDirectory))
	if err != nil {
		return nil, err
	}

	segments := make([][]sstable.Table, 0)
	for l, level := range slices.Backward(manifest.Levels) {
		segments = append(segments, make([]sstable.Table, 0))
		for _, segmentId := range slices.Backward(level.Segments) {
			segment, err := sstable.Load(getSegmentPath(settings.DataDirectory, segmentId+segmentSuffix))
			if err != nil {
				return nil, fmt.Errorf("Failed to read segment: %w", err)
			}
			segments[l] = append(segments[l], *segment)
		}
	}

	return &LSMTree{
		manifest: manifest,
		memtable: memtable,
		settings: settings,
		segments: segments,
	}, nil
}

func (t *LSMTree) Insert(key KeyType, value string) error {
	return t.writeEntry(storage.EntryData{Key: key, Value: value})
}

func (t *LSMTree) writeEntry(entry storage.EntryData) error {
	err := writeToWal(getWalPath(t.settings.DataDirectory), entry)
	if err != nil {
		return err
	}

	t.memtable.Insert(
		entry.Key, ValueType{Value: entry.Value, Deleted: entry.Deleted},
	)
	t.memtableSize += len(entry.Key) + len(entry.Value) + 1

	if t.memtableSize > t.settings.CompactionLimit {
		err := t.Flush()
		if err != nil {
			return fmt.Errorf("Error flushing memtable: %w", err)
		}
	}
	return nil
}

func (t *LSMTree) searchSegments(key KeyType) (*storage.EntryData, error) {
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

func (t *LSMTree) Search(key KeyType) (*string, error) {
	value := t.memtable.Search(key)
	if value != nil {
		if value.Deleted {
			return nil, nil
		}
		return &value.Value, nil
	}

	entry, err := t.searchSegments(key)
	if err != nil {
		return nil, err
	}

	if entry == nil || entry.Deleted {
		return nil, nil
	}
	return &entry.Value, nil
}

func (t *LSMTree) Remove(key KeyType) error {
	return t.writeEntry(storage.EntryData{Key: key, Deleted: true})
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
