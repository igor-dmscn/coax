package ws

import "testing"

func TestUTF8ValidatorWholeInput(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		valid bool
		// complete reports whether the stream ended on a rune boundary.
		complete bool
	}{
		{"empty", nil, true, true},
		{"ascii", []byte("hello"), true, true},
		{"two byte", []byte("é"), true, true},
		{"three byte", []byte("→"), true, true},
		{"four byte", []byte("🎉"), true, true},
		{"mixed", []byte("héllo → 世界 🎉"), true, true},
		{"nul byte", []byte{0x00}, true, true},
		{"max valid", []byte{0xF4, 0x8F, 0xBF, 0xBF}, true, true}, // U+10FFFF

		{"lone continuation byte", []byte{0x80}, false, false},
		{"invalid byte FF", []byte{0xFF}, false, false},
		{"invalid byte FE", []byte{0xFE}, false, false},
		{"overlong two byte", []byte{0xC0, 0x80}, false, false},
		{"overlong three byte", []byte{0xE0, 0x80, 0x80}, false, false},
		{"surrogate half", []byte{0xED, 0xA0, 0x80}, false, false}, // U+D800
		{"beyond U+10FFFF", []byte{0xF5, 0x80, 0x80, 0x80}, false, false},
		{"five byte sequence", []byte{0xF8, 0x88, 0x80, 0x80, 0x80}, false, false},
		{"truncated two byte", []byte{0xC3}, true, false},
		{"truncated three byte", []byte{0xE2, 0x86}, true, false},
		{"truncated four byte", []byte{0xF0, 0x9F, 0x8E}, true, false},
		{"valid then invalid", []byte{'a', 0xFF}, false, false},
		{"continuation after ascii", []byte{'a', 0x80}, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v utf8Validator
			if got := v.write(tt.input); got != tt.valid {
				t.Fatalf("write() = %v, want %v", got, tt.valid)
			}
			if !tt.valid {
				return
			}
			if got := v.done(); got != tt.complete {
				t.Errorf("done() = %v, want %v", got, tt.complete)
			}
		})
	}
}

// TestUTF8ValidatorSplitRunes feeds valid text one byte at a time and at every
// possible split point. A message may be fragmented mid-rune, so validation has
// to carry state rather than assume it sees whole runes.
func TestUTF8ValidatorSplitRunes(t *testing.T) {
	const text = "aé→🎉z"

	t.Run("byte at a time", func(t *testing.T) {
		var v utf8Validator
		for i := range len(text) {
			if !v.write([]byte(text[i : i+1])) {
				t.Fatalf("write() rejected valid text at byte %d", i)
			}
		}
		if !v.done() {
			t.Error("done() = false after complete text")
		}
	})

	t.Run("every split point", func(t *testing.T) {
		for split := range len(text) + 1 {
			var v utf8Validator
			if !v.write([]byte(text[:split])) {
				t.Fatalf("split %d: first half rejected", split)
			}
			if !v.write([]byte(text[split:])) {
				t.Fatalf("split %d: second half rejected", split)
			}
			if !v.done() {
				t.Errorf("split %d: done() = false", split)
			}
		}
	})
}

// TestUTF8ValidatorRejectsAcrossSplit checks that an invalid sequence is caught
// even when the bad byte arrives in a later chunk than the lead byte.
func TestUTF8ValidatorRejectsAcrossSplit(t *testing.T) {
	tests := []struct {
		name  string
		first []byte
		rest  []byte
	}{
		{"bad continuation", []byte{0xC3}, []byte{'a'}},
		{"surrogate split", []byte{0xED}, []byte{0xA0, 0x80}},
		{"overlong split", []byte{0xC0}, []byte{0x80}},
		{"too many bytes", []byte{0xF0}, []byte{0x9F, 0x8E, 0x89, 0x89}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v utf8Validator
			if !v.write(tt.first) {
				// A lone lead byte may be rejected immediately if it can never
				// start a valid rune; that is also correct.
				return
			}
			if v.write(tt.rest) {
				t.Error("write() accepted an invalid sequence split across calls")
			}
		})
	}
}

func TestUTF8ValidatorReset(t *testing.T) {
	var v utf8Validator

	if !v.write([]byte{0xC3}) { // valid but incomplete
		t.Fatal("write() rejected a valid prefix")
	}
	if v.done() {
		t.Fatal("done() = true with a pending partial rune")
	}

	v.reset()
	if !v.done() {
		t.Error("done() = false after reset")
	}
	if !v.write([]byte("clean")) {
		t.Error("write() failed after reset")
	}
}
