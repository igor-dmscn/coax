package ws

import "encoding/binary"

// mask XORs b in place with the 4-byte masking key, starting at position pos
// within the key cycle, and returns the position to resume from. Callers thread
// pos across calls so a masked payload can be processed in arbitrary chunks.
//
// Pure Go, in place, no allocation. The middle of the buffer is processed eight
// bytes at a time by replicating the key into a uint64: little-endian load and
// store preserve byte order, so byte i is XORed with key[i%4] as required.
//
// An assembly implementation would be faster (see docs/go-port-plan.md §9.4),
// but only a benchmark should justify introducing per-architecture code.
func mask(b []byte, key [4]byte, pos int) int {
	// Align to a key boundary one byte at a time.
	for len(b) > 0 && pos != 0 {
		b[0] ^= key[pos]
		b = b[1:]
		pos = (pos + 1) & 3
	}

	if len(b) >= 8 {
		k32 := binary.LittleEndian.Uint32(key[:])
		k64 := uint64(k32) | uint64(k32)<<32
		for len(b) >= 8 {
			binary.LittleEndian.PutUint64(b, binary.LittleEndian.Uint64(b)^k64)
			b = b[8:]
		}
	}

	// Tail. pos is 0 here whenever the aligned path ran, since eight is a
	// multiple of the key length.
	for i := range b {
		b[i] ^= key[pos]
		pos = (pos + 1) & 3
	}
	return pos
}
