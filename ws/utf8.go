package ws

import "unicode/utf8"

// utf8Validator incrementally validates a UTF-8 byte stream.
//
// Text messages must be valid UTF-8 (RFC 6455 §5.6), and a message may be split
// across fragments at any byte offset — including the middle of a multi-byte
// rune. So validation has to carry state between calls rather than run over a
// reassembled buffer, which is also what keeps the read path allocation-free.
//
// The zero value is ready to use. Reset it between messages.
type utf8Validator struct {
	// pending holds the bytes of a rune that was split across writes.
	pending [utf8.UTFMax]byte
	n       int
}

func (v *utf8Validator) reset() {
	v.n = 0
}

// write validates the next chunk of the stream, reporting whether everything
// seen so far is valid UTF-8. Once it returns false the validator must not be
// used again without a reset.
func (v *utf8Validator) write(p []byte) bool {
	// Complete a rune carried over from the previous call, one byte at a time:
	// we cannot know how many more bytes it needs without decoding, and a
	// truncated sequence is only invalid once a wrong byte actually arrives.
	for v.n > 0 && len(p) > 0 {
		v.pending[v.n] = p[0]
		v.n++
		p = p[1:]

		if utf8.FullRune(v.pending[:v.n]) {
			// FullRune treats an invalid encoding as complete, so DecodeRune
			// is what rejects overlongs, surrogates and out-of-range values.
			if r, _ := utf8.DecodeRune(v.pending[:v.n]); r == utf8.RuneError {
				return false
			}
			v.n = 0
			break
		}
		if v.n == len(v.pending) {
			return false // four bytes that still do not form a rune
		}
	}

	for len(p) > 0 {
		if !utf8.FullRune(p) {
			// A valid but incomplete prefix: stash it for the next call.
			v.n = copy(v.pending[:], p)
			return true
		}
		r, size := utf8.DecodeRune(p)
		if r == utf8.RuneError && size == 1 {
			return false
		}
		p = p[size:]
	}
	return true
}

// done reports whether the stream ended on a rune boundary. A message that
// stops mid-rune is invalid even if every byte seen was a legal prefix.
func (v *utf8Validator) done() bool {
	return v.n == 0
}
