package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type entryHeader struct {
	KeyLen   uint32
	ValueLen uint32
	Flags    byte
}

type EntryData struct {
	Key     string
	Value   string
	Deleted bool
}

func EncodeLogEntry(entry EntryData) ([]byte, error) {
	hdr := entryHeader{
		KeyLen:   uint32(len(entry.Key)),
		ValueLen: uint32(len(entry.Value)),
	}

	if entry.Deleted {
		hdr.Flags |= 1
		hdr.ValueLen = 0
	}

	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, hdr)
	if err != nil {
		return nil, err
	}

	buf.Write([]byte(entry.Key))
	if !entry.Deleted {
		buf.Write([]byte(entry.Value))
	}
	return buf.Bytes(), nil
}

func ReadLogEntry(r io.Reader) (*EntryData, int, error) {
	var hdr entryHeader

	n := 0
	err := binary.Read(r, binary.BigEndian, &hdr)
	if err != nil {
		return nil, n, err
	}
	n += binary.Size(hdr)

	deleted := hdr.Flags&1 == 1

	// Read key.
	bytesToRead := int(hdr.KeyLen)
	buf := make([]byte, bytesToRead)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, n, fmt.Errorf("Failed to read key: %w", err)
	}
	key := string(buf)
	n += bytesToRead

	// Read value.
	bytesToRead = int(hdr.ValueLen)
	buf = make([]byte, bytesToRead)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, n, fmt.Errorf("Failed to read value: %w", err)
	}
	value := string(buf)
	n += bytesToRead

	return &EntryData{
		Key:     key,
		Value:   value,
		Deleted: deleted,
	}, n, nil
}

func DecodeLogEntry(data []byte) (*EntryData, int, error) {
	return ReadLogEntry(bytes.NewReader(data))
}
