package storage

import "testing"

func TestEncodeDecode(t *testing.T) {

	expectedEntries := []EntryData{
		{Key: "foo", Value: "bar"},
		{Key: "hello", Value: "world"},
		{Key: "red", Value: "black"},
		{Key: "blue", Deleted: true},
		{Key: "bla", Deleted: true},
	}

	for _, entry := range expectedEntries {
		data := EncodeLogEntry(entry)
		decoded, n, err := DecodeLogEntry(data)
		if err != nil {
			t.Error(err)
			continue
		}
		if n != len(data) {
			t.Errorf("Decoded only %d bytes of expected %d", n, len(data))
		}
		if decoded == nil {
			t.Error("No decoded data")
			continue
		}
		if decoded.Key != entry.Key {
			t.Errorf("Decoded key: '%s', but expected '%s' ", decoded.Key, entry.Key)
		}
		if decoded.Value != entry.Value {
			t.Errorf("Decoded value: '%s', but expected '%s' ", decoded.Value, entry.Value)
		}
		if decoded.Deleted != entry.Deleted {
			t.Errorf("Decoded deleted flag: '%t', but expected '%t' ", decoded.Deleted, entry.Deleted)
		}
	}
}
