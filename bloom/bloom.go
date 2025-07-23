package bloom

import (
	"encoding/binary"
	"math/bits"
)

// TODO: Implement Hash interface
func murmurhash3(data []byte, seed uint32) uint32 {

	var c1 uint32 = 0xcc9e2d51
	var c2 uint32 = 0x1b873593
	r1 := 15
	r2 := 13
	var m uint32 = 5
	var n uint32 = 0xe6546b64
	var h = seed

	numChunks := len(data) / 4
	i := 0
	for ; i < numChunks*4; i += 4 {
		k := binary.LittleEndian.Uint32(data[i:])
		k *= c1
		k = bits.RotateLeft32(k, r1)
		k *= c2
		h ^= k
		h = bits.RotateLeft32(h, r2)
		h = h*m + n
	}

	var k uint32
	tail := data[i:]
	switch len(tail) & 3 {
	case 3:
		k ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k ^= uint32(tail[0])
		k *= c1
		k = bits.RotateLeft32(k, r1)
		k *= c2
		h ^= k
	}

	h ^= uint32(len(data))
	h ^= (h >> 16)
	h *= 0x85ebca6b
	h ^= (h >> 13)
	h *= 0xc2b2ae35
	h ^= (h >> 16)
	return h
}

// TODO: Make configurable/optimize based on segment sizes.
const Size = 128 // bytes
const kFunctions = 7

type Filter struct {
	Buf [Size]byte
}

func (f *Filter) Insert(key string) {
	for i := range kFunctions {
		h := murmurhash3([]byte(key), uint32(i))
		bitIdx := h % (Size * 8)
		byteIdx, bitShift := bitIdx/8, 7-bitIdx%8
		f.Buf[byteIdx] |= 1 << byte(bitShift)
	}
}

func (f *Filter) Search(key string) bool {
	for i := range kFunctions {
		h := murmurhash3([]byte(key), uint32(i))
		bitIdx := h % (Size * 8)
		byteIdx, bitShift := bitIdx/8, 7-bitIdx%8
		if f.Buf[byteIdx]&(1<<byte(bitShift)) == 0 {
			return false
		}
	}
	return true
}
