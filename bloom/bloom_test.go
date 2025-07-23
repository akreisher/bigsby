package bloom

import "testing"

func TestMurmur32(t *testing.T) {

	var data = []struct {
		seed uint32
		h32  uint32
		data string
	}{
		{0x00, 0x00000000, ""},
		{0x00, 0x248bfa47, "hello"},
		{0x00, 0x149bbb7f, "hello, world"},
		{0x00, 0xd5c48bfc, "The quick brown fox jumps over the lazy dog."},
		{0x01, 0x514e28b7, ""},
		{0x01, 0xbb4abcad, "hello"},
		{0x01, 0x6f5cb2e9, "hello, world"},
		{0x01, 0x846f6a36, "The quick brown fox jumps over the lazy dog."},
		{0x2a, 0x087fcd5c, ""},
		{0x2a, 0xe2dbd2e1, "hello"},
		{0x2a, 0x7ec7c6c2, "hello, world"},
		{0x2a, 0xc02d1434, "The quick brown fox jumps over the lazy dog."},
	}

	for _, entry := range data {
		res := murmurhash3([]byte(entry.data), entry.seed)

		if res != entry.h32 {
			t.Errorf("Got unexpected hash for %s (got %d, expected %d)", entry.data, res, entry.h32)
		}
	}
}

func TestBloom(t *testing.T) {
	filter := Filter{}

	filter.Insert("hello")
	filter.Insert("world")

	if !filter.Search("hello") {
		t.Error("Expected to find 'hello' in filter, but could not")
	}

	if !filter.Search("world") {
		t.Error("Expected to find 'hello' in filter, but could not")
	}

	if filter.Search("dog") {
		t.Error("Did not expect to find 'dog' in filter, but found?")
	}

}
