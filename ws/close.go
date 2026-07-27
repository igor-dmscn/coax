package ws

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"
)

// StatusCode is a WebSocket close status code as defined in RFC 6455 §7.4.
type StatusCode int

const (
	StatusNormalClosure   StatusCode = 1000
	StatusGoingAway       StatusCode = 1001
	StatusProtocolError   StatusCode = 1002
	StatusUnsupportedData StatusCode = 1003

	// StatusNoStatusRcvd is reported when a peer closed without a status code.
	// It must never be sent on the wire.
	StatusNoStatusRcvd StatusCode = 1005

	// StatusAbnormalClosure is reported when the connection dropped without a
	// close frame. It must never be sent on the wire.
	StatusAbnormalClosure StatusCode = 1006

	StatusInvalidFramePayloadData StatusCode = 1007
	StatusPolicyViolation         StatusCode = 1008
	StatusMessageTooBig           StatusCode = 1009
	StatusMandatoryExtension      StatusCode = 1010
	StatusInternalError           StatusCode = 1011
	StatusServiceRestart          StatusCode = 1012
	StatusTryAgainLater           StatusCode = 1013
	StatusBadGateway              StatusCode = 1014
)

// maxCloseReason is the longest reason a close frame can carry: control frame
// payloads are capped at 125 bytes and the status code takes the first two.
const maxCloseReason = maxControlPayload - 2

// CloseError is returned by Conn's read and write methods once the connection
// has closed, carrying the status and reason the peer sent.
type CloseError struct {
	Code   StatusCode
	Reason string
}

func (e CloseError) Error() string {
	return fmt.Sprintf("ws: closed with status %d: %q", e.Code, e.Reason)
}

// CloseStatus returns the status code from a CloseError anywhere in err's
// chain, or -1 if err does not describe a close.
//
//	if ws.CloseStatus(err) == ws.StatusNormalClosure { ... }
func CloseStatus(err error) StatusCode {
	var ce CloseError
	if errors.As(err, &ce) {
		return ce.Code
	}
	return -1
}

// validForSend reports whether a status code may be put on the wire. The
// codes reserved for local reporting (1005, 1006, 1015), the unassigned 1004,
// and everything outside the registered and private ranges are rejected.
func (c StatusCode) validForSend() bool {
	switch {
	case c >= 3000 && c <= 4999: // private use
		return true
	case c >= 1000 && c <= 1003:
		return true
	case c >= 1007 && c <= 1014:
		return true
	default:
		return false
	}
}

// validForReceive reports whether a status code is acceptable in a close frame
// received from a peer. It is the same set as validForSend: a peer sending
// 1005, 1006 or 1004 is a protocol error, not a status to report onward.
func (c StatusCode) validForReceive() bool {
	return c.validForSend()
}

// parseClosePayload decodes the payload of a close frame.
//
// An empty payload is legal and means "no status", reported as
// StatusNoStatusRcvd. A single byte cannot hold a status code and is a protocol
// error, as is an invalid code or a reason that is not valid UTF-8.
func parseClosePayload(p []byte) (CloseError, error) {
	switch len(p) {
	case 0:
		return CloseError{Code: StatusNoStatusRcvd}, nil
	case 1:
		return CloseError{}, protocolErrorf("close payload of one byte")
	}

	code := StatusCode(binary.BigEndian.Uint16(p))
	if !code.validForReceive() {
		return CloseError{}, protocolErrorf("close status %d is not valid on the wire", code)
	}

	reason := p[2:]
	if !utf8.Valid(reason) {
		return CloseError{}, &closeSentinel{
			code: StatusInvalidFramePayloadData,
			err:  errors.New("ws: close reason is not valid UTF-8"),
		}
	}

	return CloseError{Code: code, Reason: string(reason)}, nil
}

// appendClosePayload encodes a status and reason into buf, which must have room
// for maxControlPayload bytes.
func appendClosePayload(buf []byte, code StatusCode, reason string) ([]byte, error) {
	if !code.validForSend() {
		return nil, fmt.Errorf("ws: status %d may not be sent", code)
	}
	if len(reason) > maxCloseReason {
		return nil, fmt.Errorf("ws: close reason is %d bytes, limit is %d", len(reason), maxCloseReason)
	}

	buf = binary.BigEndian.AppendUint16(buf, uint16(code))
	return append(buf, reason...), nil
}

// closeSentinel is an internal error carrying the status code the connection
// should be closed with. Read and write paths return these; the connection
// turns them into a close frame and a CloseError for the caller.
type closeSentinel struct {
	code StatusCode
	err  error
}

func (e *closeSentinel) Error() string { return e.err.Error() }
func (e *closeSentinel) Unwrap() error { return e.err }

// protocolErrorf builds a closeSentinel that closes with StatusProtocolError,
// which is the response to every framing violation in RFC 6455 §5.
func protocolErrorf(format string, args ...any) error {
	return &closeSentinel{
		code: StatusProtocolError,
		err:  fmt.Errorf("ws: protocol error: "+format, args...),
	}
}

// closeStatusFor reports the status a failure should close the connection with.
func closeStatusFor(err error) StatusCode {
	var cs *closeSentinel
	if errors.As(err, &cs) {
		return cs.code
	}
	return StatusInternalError
}
