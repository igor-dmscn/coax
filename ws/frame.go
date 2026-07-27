package ws

import (
	"encoding/binary"
	"io"
)

// Frame layout limits from RFC 6455 §5.2.
const (
	// maxHeaderSize is a two-byte prefix, an eight-byte extended length and a
	// four-byte masking key.
	maxHeaderSize = 2 + 8 + 4

	// maxControlPayload caps the payload of ping, pong and close frames.
	maxControlPayload = 125
)

// opcode identifies a frame's kind.
type opcode byte

const (
	opContinuation opcode = 0x0
	opText         opcode = 0x1
	opBinary       opcode = 0x2
	opClose        opcode = 0x8
	opPing         opcode = 0x9
	opPong         opcode = 0xA
)

// isControl reports whether the opcode denotes a control frame. Control frames
// may be interleaved between the fragments of a data message, so the read loop
// has to handle them at any point.
func (o opcode) isControl() bool { return o&0x8 != 0 }

func (o opcode) known() bool {
	switch o {
	case opContinuation, opText, opBinary, opClose, opPing, opPong:
		return true
	default:
		return false
	}
}

// frameHeader is a decoded frame header. It carries no payload.
type frameHeader struct {
	fin        bool
	rsv1       bool
	rsv2       bool
	rsv3       bool
	opcode     opcode
	masked     bool
	payloadLen int64
	maskKey    [4]byte
}

// readFrameHeader decodes one header from r using buf as scratch. buf must be
// at least maxHeaderSize long; no memory is allocated.
func readFrameHeader(r io.Reader, buf []byte) (frameHeader, error) {
	if _, err := io.ReadFull(r, buf[:2]); err != nil {
		return frameHeader{}, err
	}

	h := frameHeader{
		fin:    buf[0]&0x80 != 0,
		rsv1:   buf[0]&0x40 != 0,
		rsv2:   buf[0]&0x20 != 0,
		rsv3:   buf[0]&0x10 != 0,
		opcode: opcode(buf[0] & 0x0F),
		masked: buf[1]&0x80 != 0,
	}

	switch n := buf[1] & 0x7F; n {
	case 126:
		if _, err := io.ReadFull(r, buf[:2]); err != nil {
			return frameHeader{}, err
		}
		h.payloadLen = int64(binary.BigEndian.Uint16(buf[:2]))
		// RFC 6455 §5.2: the minimal number of bytes must be used, so a
		// two-byte length below 126 is a violation rather than a synonym.
		if h.payloadLen < 126 {
			return frameHeader{}, protocolErrorf("16-bit length %d is not minimally encoded", h.payloadLen)
		}
	case 127:
		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return frameHeader{}, err
		}
		v := binary.BigEndian.Uint64(buf[:8])
		// The most significant bit must be zero, which also keeps the value
		// inside int64 so no conversion can wrap negative.
		if v>>63 != 0 {
			return frameHeader{}, protocolErrorf("64-bit length has the high bit set")
		}
		h.payloadLen = int64(v)
		if h.payloadLen <= 0xFFFF {
			return frameHeader{}, protocolErrorf("64-bit length %d is not minimally encoded", h.payloadLen)
		}
	default:
		h.payloadLen = int64(n)
	}

	if h.masked {
		if _, err := io.ReadFull(r, buf[:4]); err != nil {
			return frameHeader{}, err
		}
		copy(h.maskKey[:], buf[:4])
	}

	return h, nil
}

// validate applies the framing rules that do not depend on connection state.
//
// expectMasked is true on a server (clients must mask) and false on a client
// (servers must not). rsvAllowed carries the RSV bits permitted by negotiated
// extensions — zero today, since no extension is implemented; deflate would set
// RSV1 here rather than requiring this check to be rewritten.
func (h frameHeader) validate(expectMasked bool, rsvAllowed [3]bool) error {
	if (h.rsv1 && !rsvAllowed[0]) || (h.rsv2 && !rsvAllowed[1]) || (h.rsv3 && !rsvAllowed[2]) {
		return protocolErrorf("reserved bit set without a negotiated extension")
	}
	if !h.opcode.known() {
		return protocolErrorf("reserved opcode %#x", byte(h.opcode))
	}
	if h.masked != expectMasked {
		if expectMasked {
			return protocolErrorf("client frame is not masked")
		}
		return protocolErrorf("server frame is masked")
	}
	if h.opcode.isControl() {
		if !h.fin {
			return protocolErrorf("fragmented control frame")
		}
		if h.payloadLen > maxControlPayload {
			return protocolErrorf("control frame payload of %d bytes exceeds %d", h.payloadLen, maxControlPayload)
		}
	}
	return nil
}

// appendFrameHeader encodes h into buf, which must have room for
// maxHeaderSize bytes, and returns the encoded slice.
func appendFrameHeader(buf []byte, h frameHeader) []byte {
	buf = buf[:0]

	var b0 byte
	if h.fin {
		b0 |= 0x80
	}
	if h.rsv1 {
		b0 |= 0x40
	}
	if h.rsv2 {
		b0 |= 0x20
	}
	if h.rsv3 {
		b0 |= 0x10
	}
	buf = append(buf, b0|byte(h.opcode))

	var b1 byte
	if h.masked {
		b1 |= 0x80
	}
	switch {
	case h.payloadLen <= 125:
		buf = append(buf, b1|byte(h.payloadLen))
	case h.payloadLen <= 0xFFFF:
		buf = append(buf, b1|126)
		buf = binary.BigEndian.AppendUint16(buf, uint16(h.payloadLen))
	default:
		buf = append(buf, b1|127)
		buf = binary.BigEndian.AppendUint64(buf, uint64(h.payloadLen))
	}

	if h.masked {
		buf = append(buf, h.maskKey[:]...)
	}
	return buf
}
