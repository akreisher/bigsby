package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/akreisher/bigsby/internal/bloom"
	"github.com/akreisher/bigsby/internal/storage"
)

const segmentCookie = "BIGSBYSEGMENT"

type indexEntry struct {
	Key      string
	Position int
}

type segmentFooter struct {
	NumEntries   uint64
	DataEndIndex uint64
}

type Table struct {
	FilePath       string
	index          []indexEntry
	filter         bloom.Filter
	dataStartIndex int
	dataEndIndex   int
	numEntries     int
}

const DataFileName = "segment_table"

// TODO: Non-ascii leading char to avoid reading file as text
const segmentFileFormat = 1
const sparsityFactor = 0.01

// SSTable Requirements:
// - Immutable
// - Sorted
// - Can look up value
// - Can print all values
// - Can create new segment from memtable
// - Can merge two segments together

func writeHeader(f *os.File) (int, error) {
	// Header format:
	//    cookie string "BIGSBYSEGMENT"
	//    16-bit version number
	header := make([]byte, len(segmentCookie)+2)
	copy(header, segmentCookie)
	binary.BigEndian.PutUint16(header[len(segmentCookie):], segmentFileFormat)
	return f.Write(header)
}

func writeBloomFilter(f *os.File, filter bloom.Filter) (int, error) {
	buf := make([]byte, len(filter.Buf)+4)
	binary.BigEndian.PutUint32(buf, uint32(len(filter.Buf)))
	copy(buf[4:], filter.Buf[:])
	return f.Write(buf)
}

func writeIndex(f *os.File, index []indexEntry) (int, error) {
	total := 0

	// Write index size first.
	numEntries := make([]byte, 4)
	binary.BigEndian.PutUint32(numEntries, uint32(len(index)))
	f.Write(numEntries)
	total += 4

	// Write each entry.
	for _, entry := range index {
		// Key length.
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(len(entry.Key)))
		n, err := f.Write(buf)
		if err != nil {
			return 0, err
		}
		total += n

		// Key.
		n, err = f.WriteString(entry.Key)
		if err != nil {
			return 0, err
		}
		total += n

		// Position.
		binary.BigEndian.PutUint32(buf, uint32(entry.Position))
		n, err = f.Write(buf)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func writeFooter(f *os.File, footer segmentFooter) (int, error) {
	var buf bytes.Buffer

	total := 0
	err := binary.Write(&buf, binary.BigEndian, footer)
	if err != nil {
		return 0, err
	}
	n, err := f.Write(buf.Bytes())
	if err != nil {
		return 0, fmt.Errorf("Failed to write footer: %w", err)
	}
	total += n

	n, err = f.Write([]byte(segmentCookie))
	if err != nil {
		return 0, fmt.Errorf("Failed to write footer: %w", err)
	}
	total += n

	return total, nil
}

// Create a new SStable from a list of entries.
func Create(filePath string, data []storage.EntryData) (*Table, error) {
	f, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("Could not create segment file: %w", err)
	}

	defer f.Close()

	position := 0

	n, err := writeHeader(f)
	if err != nil {
		return nil, fmt.Errorf("Failed to write header: %w", err)
	}

	position += n

	// Create bloom filter.
	filter := bloom.Filter{}

	// Create index.
	indexLimit := int(math.Floor(sparsityFactor * float64(len(data))))
	index := make([]indexEntry, indexLimit)

	dataStartIndex := position
	var lastKey string

	for i, entry := range data {

		// Keys in list must be pre-sorted and unique.
		if i != 0 && entry.Key <= lastKey {
			return nil, fmt.Errorf("Unsorted or repeat key: %s (prev %s)", entry.Key, lastKey)
		}
		lastKey = entry.Key

		data, err := storage.EncodeLogEntry(entry)
		if err != nil {
			return nil, fmt.Errorf("Failed to encode segment data: %w", err)
		}

		// Write entry to the table file.
		_, err = f.Write(data)
		if err != nil {
			return nil, fmt.Errorf("Failed to write segment data: %w", err)
		}

		// Add entry to bloom filter.
		filter.Insert(entry.Key)

		// Check if we are at the index limits.
		if indexLimit != 0 && i%indexLimit == 0 {
			index = append(index, indexEntry{
				Position: position,
			})
		}
		position += entry.Size()
	}
	dataEndIndex := position

	// Write bloom filter to file.
	n, err = writeBloomFilter(f, filter)
	if err != nil {
		return nil, fmt.Errorf("Failed to write bloom filter: %w", err)
	}
	position += n

	// Write index to file.
	n, err = writeIndex(f, index)
	if err != nil {
		return nil, fmt.Errorf("Failed to write index: %w", err)
	}
	position += n

	n, err = writeFooter(f, segmentFooter{
		NumEntries:   uint64(len(data)),
		DataEndIndex: uint64(dataEndIndex),
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to write footer: %w", err)
	}
	position += n

	return &Table{
		FilePath:       filePath,
		filter:         filter,
		dataStartIndex: dataStartIndex,
		dataEndIndex:   dataEndIndex,
		index:          index,
		numEntries:     len(data),
	}, nil
}

func readExactly(f *os.File, n int) ([]byte, error) {
	buf := make([]byte, n)
	numRead, err := f.Read(buf)
	if err != nil {
		return nil, err
	}
	if numRead != n {
		return nil, fmt.Errorf("Failed to exactly %d bytes, (read %d instead)", n, numRead)
	}
	return buf, nil
}

func readHeader(f *os.File) (int, error) {
	res := 0

	// Read file cookie.
	cookie, err := readExactly(f, len(segmentCookie))
	if err != nil {
		return 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if !bytes.Equal(cookie, []byte(segmentCookie)) {
		return 0, fmt.Errorf("Failed to read cookie in segment file")
	}
	res += len(segmentCookie)

	// Read file version.
	versionBuf, err := readExactly(f, 2)
	if err != nil {
		return 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	res += 2
	version := binary.BigEndian.Uint16(versionBuf)
	if version != segmentFileFormat {
		return 0, fmt.Errorf("Could not read segment file with version %d", version)
	}
	return res, nil
}

func readFilter(f *os.File) (*bloom.Filter, int, error) {
	res := 0
	filterLenBuf, err := readExactly(f, 4)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	res += 4
	filterLen := int(binary.BigEndian.Uint32(filterLenBuf))
	if filterLen != bloom.Size {
		return nil, 0, fmt.Errorf("Non-%d filter size is not supported", bloom.Size)
	}

	filterBuf, err := readExactly(f, filterLen)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	res += filterLen

	return &bloom.Filter{
		Buf: [bloom.Size]byte(filterBuf),
	}, res, nil
}

func readIndexEntry(f *os.File) (*indexEntry, int, error) {
	res := 0

	// Key length.
	keyLenBuf := make([]byte, 4)
	n, err := f.Read(keyLenBuf)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if n != 4 {
		return nil, 0, fmt.Errorf("Failed to read index entry len in segment file")
	}
	res += n
	keyLen := binary.BigEndian.Uint32(keyLenBuf)

	// Key.
	entry := make([]byte, keyLen)
	n, err = f.Read(entry)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if n != int(keyLen) {
		return nil, 0, fmt.Errorf("Failed to read index entry key in segment file")
	}
	res += n

	// Position.
	posBuf := make([]byte, 4)
	n, err = f.Read(posBuf)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	if n != 4 {
		return nil, 0, fmt.Errorf("Failed to read index entry position in segment file")
	}
	res += n

	return &indexEntry{
		Key:      string(entry),
		Position: int(binary.BigEndian.Uint32(posBuf)),
	}, n, nil
}

func readIndex(f *os.File) ([]indexEntry, int, error) {
	res := 0

	numEntriesBuf, err := readExactly(f, 4)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read segment file: %w", err)
	}
	res += 4

	numEntries := binary.BigEndian.Uint32(numEntriesBuf)
	index := make([]indexEntry, numEntries)

	for range numEntries {
		entry, n, err := readIndexEntry(f)
		if err != nil {
			return nil, 0, fmt.Errorf("Failed to read index entry: %w", err)
		}
		index = append(index, *entry)
		res += n
	}

	return index, res, nil
}

func readFooter(f *os.File) (*segmentFooter, int, error) {
	res := 0

	// Check cookie.
	_, err := f.Seek(-int64(len(segmentCookie)), io.SeekEnd)
	cookie, err := readExactly(f, len(segmentCookie))
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to read footer: %w", err)
	}
	if !bytes.Equal(cookie, []byte(segmentCookie)) {
		return nil, 0, fmt.Errorf("Failed to read cookie in segment file")
	}
	res += len(segmentCookie)

	var footer segmentFooter
	seekPos := -int64(len(segmentCookie)) - int64(binary.Size(footer))
	_, err = f.Seek(seekPos, io.SeekEnd)
	if err != nil {
		return nil, 0, fmt.Errorf("Failed to seek to pos %d: %w", seekPos, err)
	}

	err = binary.Read(f, binary.BigEndian, &footer)
	if err != nil {
		return nil, res, err
	}
	res += binary.Size(footer)

	return &footer, res, nil
}

// Load a SSTable from a file.
func Load(filePath string) (*Table, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("Cannot open segment file: %w", err)
	}

	defer f.Close()

	dataStartIndex := 0
	n, err := readHeader(f)
	if err != nil {
		return nil, fmt.Errorf("Failed to read segment header: %w", err)
	}
	dataStartIndex += n

	footer, _, err := readFooter(f)

	_, err = f.Seek(int64(footer.DataEndIndex), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("Failed to seek to data end index pos %d: %w", footer.DataEndIndex, err)
	}

	filter, n, err := readFilter(f)
	if err != nil {
		return nil, fmt.Errorf("Failed to read segment filter: %w", err)
	}

	index, n, err := readIndex(f)
	if err != nil {
		return nil, fmt.Errorf("Failed to read segment index: %w", err)
	}

	return &Table{
		FilePath:       filePath,
		filter:         *filter,
		index:          index,
		dataStartIndex: dataStartIndex,
		dataEndIndex:   int(footer.DataEndIndex),
		numEntries:     int(footer.NumEntries),
	}, nil
}

func Merge(newer *Table, older *Table, newFilePath string, last bool) (*Table, error) {
	f, err := os.Create(newFilePath)
	if err != nil {
		return nil, fmt.Errorf("Could not create segment file: %w", err)
	}

	defer f.Close()

	position := 0
	totalEntries := newer.numEntries + older.numEntries

	n, err := writeHeader(f)
	if err != nil {
		return nil, fmt.Errorf("Failed to write header: %w", err)
	}

	position += n

	// Create bloom filter.
	filter := bloom.Filter{}

	// Create index.
	indexLimit := int(math.Floor(sparsityFactor * float64(totalEntries)))
	index := make([]indexEntry, indexLimit)

	numEntries := 0
	insertMerged := func(entry storage.EntryData) error {

		// If we are inserting a deleted entry in the last level,
		// we can safely just skip the data.
		if entry.Deleted && last {
			return nil
		}

		numEntries += 1

		// Add entry to bloom filter.
		filter.Insert(entry.Key)

		// Check if we are at the index limits.
		if indexLimit != 0 && numEntries%indexLimit == 0 {
			index = append(index, indexEntry{
				Position: position,
			})
		}

		data, err := storage.EncodeLogEntry(entry)
		if err != nil {
			return fmt.Errorf("Failed to encode segment data: %w", err)
		}

		// Write entry to the table file.
		n, err = f.Write(data)
		if err != nil {
			return fmt.Errorf("Failed to write segment data: %w", err)
		}
		position += n

		return nil
	}

	var oldEntry *storage.EntryData
	olderIndex := older.dataStartIndex
	olderFile, err := os.Open(older.FilePath)
	if err != nil {
		return nil, fmt.Errorf("Could not open older segment file: %w", err)
	}
	defer olderFile.Close()

	_, err = f.Seek(int64(olderIndex), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("Failed to seek to index pos %d: %w", olderIndex, err)
	}

	var newEntry *storage.EntryData
	newerIndex := newer.dataStartIndex
	newerFile, err := os.Open(newer.FilePath)
	if err != nil {
		return nil, fmt.Errorf("Could not open newer segment file: %w", err)
	}
	defer newerFile.Close()

	_, err = f.Seek(int64(newerIndex), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("Failed to seek to index pos %d: %w", newerIndex, err)
	}

	dataStartIndex := position

	for newerIndex < newer.numEntries || olderIndex < older.numEntries {
		if newerIndex >= newer.dataEndIndex {
			entry, _, err := storage.ReadLogEntry(olderFile)
			if err != nil {
				return nil, fmt.Errorf("Failed to read entry from older table: %w", err)
			}
			olderIndex += 1
			insertMerged(*entry)
			continue
		} else if olderIndex >= older.dataEndIndex {
			entry, _, err := storage.ReadLogEntry(newerFile)
			if err != nil {
				return nil, fmt.Errorf("Failed to read entry from newer table: %w", err)
			}
			newerIndex += 1
			insertMerged(*entry)
			continue
		}

		// Keep latest entries in memory.
		// Compare both, insert in order.
		// Advance whichever one was inserted.
		if newEntry == nil {
			newEntry, _, err = storage.ReadLogEntry(newerFile)
			if err != nil {
				return nil, fmt.Errorf("Failed to read entry from newer table: %w", err)
			}
			newerIndex += 1
		}
		if oldEntry == nil {
			oldEntry, _, err = storage.ReadLogEntry(olderFile)
			if err != nil {
				return nil, fmt.Errorf("Failed to read entry from older table: %w", err)
			}
			olderIndex += 1
		}

		if newEntry.Key > oldEntry.Key {
			insertMerged(*oldEntry)
			oldEntry = nil
		} else if newEntry.Key == oldEntry.Key {
			// Newer value gets priority
			insertMerged(*newEntry)
			newEntry = nil
			oldEntry = nil
		} else {
			insertMerged(*newEntry)
			newEntry = nil
		}
	}

	dataEndIndex := position

	// Write bloom filter to file.
	n, err = writeBloomFilter(f, filter)
	if err != nil {
		return nil, fmt.Errorf("Failed to write bloom filter: %w", err)
	}
	position += n

	// Write index to file.
	n, err = writeIndex(f, index)
	if err != nil {
		return nil, fmt.Errorf("Failed to write index: %w", err)
	}
	position += n

	n, err = writeFooter(f, segmentFooter{
		NumEntries:   uint64(totalEntries),
		DataEndIndex: uint64(dataEndIndex),
	})
	if err != nil {
		return nil, fmt.Errorf("Failed to write footer: %w", err)
	}
	position += n

	return &Table{
		FilePath:       newFilePath,
		filter:         filter,
		dataStartIndex: dataStartIndex,
		dataEndIndex:   dataEndIndex,
		index:          index,
		numEntries:     totalEntries,
	}, nil
}

func (t *Table) Search(key string) (*storage.EntryData, error) {

	// If not found in bloom filter, no lookup needed.
	if !t.filter.Search(key) {
		return nil, nil
	}

	startPos := t.dataStartIndex
	for _, entry := range t.index {
		if entry.Key > key {
			break
		}
		startPos = entry.Position
	}

	f, err := os.Open(t.FilePath)
	if err != nil {
		return nil, fmt.Errorf("Could not open segment file: %w", err)
	}
	_, err = f.Seek(int64(startPos), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("Failed to seek to index pos %d: %w", startPos, err)
	}

	for {
		entry, _, err := storage.ReadLogEntry(f)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("Could not decode segment file entry: %w", err)
		}

		if entry.Key == key {
			return entry, nil
		}
	}
	return nil, nil
}

// Read all entries in the table.
func (t *Table) Read() (*[]storage.EntryData, error) {
	data, err := os.ReadFile(t.FilePath)
	if err != nil {
		return nil, fmt.Errorf("Could not read segment file: %w", err)
	}
	ptr := t.dataStartIndex
	entries := make([]storage.EntryData, 0)
	for ptr < t.dataEndIndex {
		entry, read, err := storage.DecodeLogEntry(data[ptr:])
		if err != nil {
			return nil, fmt.Errorf("Could not read segment file: %w", err)
		}
		ptr += read
		entries = append(entries, *entry)
	}
	return &entries, nil
}

func (t *Table) Print(out io.Writer) error {
	data, err := t.Read()
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(out, "Segment path: %s\n", t.FilePath)
	if err != nil {
		return err
	}

	_, err = io.WriteString(out, "Index:")
	for _, entry := range t.index {
		if entry.Position == 0 {
			continue
		}
		_, err = fmt.Fprintf(out, "  [%s]: %d\n", entry.Key, entry.Position)
		if err != nil {
			return err
		}
	}
	_, err = fmt.Fprint(out, "Table:\n\n")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%v\n", data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "")
	if err != nil {
		return err
	}
	return nil
}
