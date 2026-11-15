package storage

import (
	"encoding/binary"
	"fmt"
)

type EntryData struct {
	Key   string
	Value string
}

// TODO: Out-of-band value for this to avoid mixing up values.
const Tombstone = "<BIGSBY_TOMBSTONE>"

func EncodeLogEntry(entry EntryData) []byte {
	keySize := len(entry.Key)
	valSize := len(entry.Value)
	logSize := keySize + valSize + 8 // key + val + 2 * uint32_len
	buf := make([]byte, logSize)
	binary.BigEndian.PutUint32(buf, uint32(keySize))
	copy(buf[4:], entry.Key)
	binary.BigEndian.PutUint32(buf[4+keySize:], uint32(valSize))
	copy(buf[8+keySize:], entry.Value)
	return buf
}

func DecodeLogEntry(data []byte) (*EntryData, int, error) {

	// Ready key size
	bytesToRead := 4
	if len(data) < bytesToRead {
		return nil, 0, fmt.Errorf("Not enough data to read key size")
	}
	keySize := binary.BigEndian.Uint32(data)

	// Read key
	bytesToRead += int(keySize)
	if len(data) < bytesToRead {
		return nil, 0, fmt.Errorf("Not enough data to read key")
	}
	key := string(data[4 : 4+keySize])

	// Read value size.
	bytesToRead += 4
	if len(data) < bytesToRead {
		return nil, 0, fmt.Errorf("Not enough data to read value size")
	}
	valueSize := binary.BigEndian.Uint32(data[4+keySize:])

	// Read value
	bytesToRead += int(valueSize)
	if len(data) < bytesToRead {
		return nil, 0, fmt.Errorf("Not enough data to read value")
	}
	value := string(data[8+keySize : 8+keySize+valueSize])

	return &EntryData{
		Key:   key,
		Value: value,
	}, bytesToRead, nil
}
