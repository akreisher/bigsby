package storage

import (
	"encoding/binary"
	"fmt"
)

type EntryData struct {
	Key     string
	Value   string
	Deleted bool
}

func EncodeLogEntry(entry EntryData) []byte {
	keySize := len(entry.Key)
	var valSize int = 0
	if !entry.Deleted {
		valSize = len(entry.Value)
	}
	logSize := keySize + valSize + 8 + 1 // key + val + 2 * uint32_len + flags
	buf := make([]byte, logSize)
	binary.BigEndian.PutUint32(buf, uint32(keySize))
	copy(buf[4:], entry.Key)
	if entry.Deleted {
		// Mark tombstone.
		buf[4+keySize] = 1
	} else {
		// If not deleted, copy value.
		binary.BigEndian.PutUint32(buf[4+keySize+1:], uint32(valSize))
		copy(buf[4+keySize+1+4:], entry.Value)
	}
	return buf
}

func DecodeLogEntry(data []byte) (*EntryData, int, error) {
	n := 0
	// Ready key size
	bytesToRead := 4

	if len(data) < n+bytesToRead {
		return nil, 0, fmt.Errorf("Not enough data to read key size")
	}
	keySize := binary.BigEndian.Uint32(data[n:])
	n += bytesToRead

	// Read key
	bytesToRead = int(keySize)
	if len(data) < n+bytesToRead {
		return nil, 0, fmt.Errorf("Not enough data to read key")
	}
	key := string(data[n : n+bytesToRead])
	n += bytesToRead

	// Read tombstone flag.
	bytesToRead = 1
	if len(data) < n+bytesToRead {
		return nil, 0, fmt.Errorf("Not enough data to read flags")
	}
	deleted := data[n]&1 == 1
	n += bytesToRead

	// Read value size.
	bytesToRead = 4
	if len(data) < n+bytesToRead {
		return nil, 0, fmt.Errorf("Not enough data to read value size")
	}
	valueSize := binary.BigEndian.Uint32(data[n:])
	n += bytesToRead

	// Read value
	bytesToRead = int(valueSize)
	if len(data) < n+bytesToRead {
		return nil, 0, fmt.Errorf("Not enough data to read value")
	}
	value := string(data[n : n+bytesToRead])
	n += bytesToRead

	return &EntryData{
		Key:     key,
		Value:   value,
		Deleted: deleted,
	}, n, nil
}
