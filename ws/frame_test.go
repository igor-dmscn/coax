package ws

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestFrameHeaderRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		h    frameHeader
	}{
		{"empty text", frameHeader{fin: true, opcode: opText}},
		{"7-bit length", frameHeader{fin: true, opcode: opBinary, payloadLen: 125}},
		{"16-bit length", frameHeader{fin: true, opcode: opBinary, payloadLen: 126}},
		{"16-bit length max", frameHeader{fin: true, opcode: opBinary, payloadLen: 0xFFFF}},
		{"64-bit length", frameHeader{fin: true, opcode: opBinary, payloadLen: 0x10000}},
		{"not final", frameHeader{opcode: opText, payloadLen: 10}},
		{"continuation", frameHeader{fin: true, opcode: opContinuation, payloadLen: 10}},
		{"masked", frameHeader{fin: true, opcode: opText, masked: true, payloadLen: 4, maskKey: [4]byte{1, 2, 3, 4}}},
		{"ping", frameHeader{fin: true, opcode: opPing, payloadLen: 5}},
		{"reserved bits", frameHeader{fin: true, rsv1: true, rsv2: true, rsv3: true, opcode: opText}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf [maxHeaderSize]byte
			encoded := appendFrameHeader(buf[:0], tt.h)

			got, err := readFrameHeader(bytes.NewReader(encoded), make([]byte, maxHeaderSize))
			if err != nil {
				t.Fatalf("readFrameHeader() error = %v", err)
			}
			if got != tt.h {
				t.Errorf("round trip = %+v, want %+v", got, tt.h)
			}
		})
	}
}

func TestReadFrameHeaderRejects(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
	}{
		{
			// A 16-bit length below 126 must use the 7-bit form.
			name: "non-minimal 16-bit length",
			raw:  []byte{0x82, 0x7E, 0x00, 0x10},
		},
		{
			// A 64-bit length must exceed 0xFFFF.
			name: "non-minimal 64-bit length",
			raw:  []byte{0x82, 0x7F, 0, 0, 0, 0, 0, 0, 0x01, 0x00},
		},
		{
			name: "64-bit length with the high bit set",
			raw:  []byte{0x82, 0x7F, 0x80, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name: "truncated header",
			raw:  []byte{0x82},
		},
		{
			name: "truncated extended length",
			raw:  []byte{0x82, 0x7E, 0x00},
		},
		{
			name: "truncated mask key",
			raw:  []byte{0x82, 0x84, 0x01, 0x02},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := readFrameHeader(bytes.NewReader(tt.raw), make([]byte, maxHeaderSize)); err == nil {
				t.Error("readFrameHeader() = nil error, want a failure")
			}
		})
	}
}

func TestFrameHeaderValidate(t *testing.T) {
	noRSV := [3]bool{}

	tests := []struct {
		name         string
		h            frameHeader
		expectMasked bool
		rsvAllowed   [3]bool
		wantErr      bool
	}{
		{
			name:         "masked client text",
			h:            frameHeader{fin: true, opcode: opText, masked: true},
			expectMasked: true,
		},
		{
			name:         "unmasked client frame is rejected",
			h:            frameHeader{fin: true, opcode: opText},
			expectMasked: true,
			wantErr:      true,
		},
		{
			name: "masked server frame is rejected",
			h:    frameHeader{fin: true, opcode: opText, masked: true},
			// expectMasked false: we are the client
			wantErr: true,
		},
		{
			name:    "rsv1 without an extension",
			h:       frameHeader{fin: true, rsv1: true, opcode: opText},
			wantErr: true,
		},
		{
			name:       "rsv1 with a negotiated extension",
			h:          frameHeader{fin: true, rsv1: true, opcode: opText},
			rsvAllowed: [3]bool{true, false, false},
		},
		{
			name:    "rsv2 without an extension",
			h:       frameHeader{fin: true, rsv2: true, opcode: opText},
			wantErr: true,
		},
		{
			name:    "rsv3 without an extension",
			h:       frameHeader{fin: true, rsv3: true, opcode: opText},
			wantErr: true,
		},
		{
			name:    "reserved data opcode 0x3",
			h:       frameHeader{fin: true, opcode: 0x3},
			wantErr: true,
		},
		{
			name:    "reserved control opcode 0xB",
			h:       frameHeader{fin: true, opcode: 0xB},
			wantErr: true,
		},
		{
			name:    "fragmented control frame",
			h:       frameHeader{opcode: opPing},
			wantErr: true,
		},
		{
			name:    "oversized control frame",
			h:       frameHeader{fin: true, opcode: opPing, payloadLen: maxControlPayload + 1},
			wantErr: true,
		},
		{
			name: "control frame at the size limit",
			h:    frameHeader{fin: true, opcode: opClose, payloadLen: maxControlPayload},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rsv := tt.rsvAllowed
			if rsv == ([3]bool{}) {
				rsv = noRSV
			}
			err := tt.h.validate(tt.expectMasked, rsv)
			if (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantErr && err != nil {
				// Every framing violation must close with 1002.
				if got := closeStatusFor(err); got != StatusProtocolError {
					t.Errorf("closeStatusFor() = %v, want %v", got, StatusProtocolError)
				}
			}
		})
	}
}

func TestMaskIsReversibleAcrossChunks(t *testing.T) {
	key := [4]byte{0xDE, 0xAD, 0xBE, 0xEF}

	original := make([]byte, 1000)
	for i := range original {
		original[i] = byte(i)
	}

	// Mask in one pass, then unmask in awkwardly sized chunks: the position
	// must carry across calls or the result diverges.
	masked := append([]byte(nil), original...)
	mask(masked, key, 0)

	pos := 0
	for i := 0; i < len(masked); {
		n := min(1+i%7, len(masked)-i)
		pos = mask(masked[i:i+n], key, pos)
		i += n
	}

	if !bytes.Equal(masked, original) {
		t.Error("masking then unmasking in chunks did not restore the original")
	}
}

func TestMaskStartingAtOffset(t *testing.T) {
	key := [4]byte{1, 2, 3, 4}

	// Masking a buffer in two pieces must equal masking it in one.
	for split := range 16 {
		data := make([]byte, 16)
		for i := range data {
			data[i] = byte(i * 7)
		}
		want := append([]byte(nil), data...)
		mask(want, key, 0)

		got := append([]byte(nil), data...)
		pos := mask(got[:split], key, 0)
		mask(got[split:], key, pos)

		if !bytes.Equal(got, want) {
			t.Errorf("split at %d: got %x, want %x", split, got, want)
		}
	}
}

// FuzzFrameParse asserts the header parser never panics on hostile input and
// never reports a negative or unbounded length.
func FuzzFrameParse(f *testing.F) {
	f.Add([]byte{0x81, 0x00})
	f.Add([]byte{0x82, 0x7E, 0x01, 0x00})
	f.Add([]byte{0x82, 0x7F, 0, 0, 0, 0, 0, 1, 0, 0})
	f.Add([]byte{0x88, 0x82, 1, 2, 3, 4, 0x03, 0xE8})
	f.Add([]byte{0xFF, 0xFF})
	f.Add([]byte{})

	buf := make([]byte, maxHeaderSize)
	f.Fuzz(func(t *testing.T, raw []byte) {
		h, err := readFrameHeader(bytes.NewReader(raw), buf)
		if err != nil {
			// Only io errors and protocol errors are expected.
			var cs *closeSentinel
			if !errors.As(err, &cs) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("unexpected error kind %T: %v", err, err)
			}
			return
		}

		if h.payloadLen < 0 {
			t.Fatalf("negative payload length %d from %x", h.payloadLen, raw)
		}
		// A header that parses must re-encode to something that parses back.
		encoded := appendFrameHeader(make([]byte, 0, maxHeaderSize), h)
		got, err := readFrameHeader(bytes.NewReader(encoded), buf)
		if err != nil {
			t.Fatalf("re-reading own encoding of %+v: %v", h, err)
		}
		if got != h {
			t.Fatalf("re-encode changed header: %+v -> %+v", h, got)
		}
	})
}
