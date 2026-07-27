package redispubsub

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// RESP2 type bytes. Redis pub/sub needs all five and nothing else: staying on
// RESP2 is a choice, made by never sending HELLO, so the server never switches to
// RESP3's push types.
// https://redis.io/docs/latest/develop/reference/protocol-spec/
const (
	typeSimpleString = '+'
	typeError        = '-'
	typeInteger      = ':'
	typeBulkString   = '$'
	typeArray        = '*'
)

// Limits applied while parsing, before anything is allocated. A reply is at most
// a few hundred bytes for control traffic and one broadcast payload otherwise, so
// these are guards against a hostile or broken server rather than tuning knobs.
const (
	// maxArrayLen bounds an array's element count. Pub/sub replies have at most
	// four elements.
	maxArrayLen = 1024

	// maxDepth bounds nesting. Pub/sub replies nest one level; without a limit,
	// "*1" repeated recurses once per three bytes of input.
	maxDepth = 8
)

// errProtocol reports a reply that is not valid RESP2. It always means the
// connection is unusable: the parser cannot know where the next reply starts.
var errProtocol = errors.New("redispubsub: protocol error")

// Error is an error reply from Redis, such as "NOAUTH Authentication required" or
// "WRONGPASS invalid username-password pair". The connection is still usable, so
// this is reported to the caller rather than treated as a failure.
type Error struct{ Message string }

func (e *Error) Error() string { return "redis: " + e.Message }

// value is a parsed RESP2 reply.
//
// One struct rather than an interface: replies are small, shallow, and always
// inspected immediately, so a type switch on an interface would only add
// allocation and indirection.
type value struct {
	typ  byte
	text []byte  // simple string, error message, or bulk string
	num  int64   // integer
	arr  []value // array elements
	null bool    // the null bulk string or array ($-1, *-1)
}

// str returns a value's text, whatever shape carried it. Pub/sub replies mix bulk
// strings (real Redis) and simple strings (+PONG), and callers care about the
// content rather than the encoding.
func (v value) str() string { return string(v.text) }

// isString reports whether v carried text at all.
func (v value) isString() bool {
	return v.typ == typeBulkString || v.typ == typeSimpleString
}

// readValue parses one reply. maxSize bounds a single bulk string, which is what
// stops a bogus length header from allocating gigabytes.
func readValue(br *bufio.Reader, maxSize int) (value, error) {
	return readValueDepth(br, maxSize, 0)
}

func readValueDepth(br *bufio.Reader, maxSize, depth int) (value, error) {
	if depth > maxDepth {
		return value{}, fmt.Errorf("%w: nested deeper than %d", errProtocol, maxDepth)
	}

	line, err := readLine(br)
	if err != nil {
		return value{}, err
	}
	if len(line) == 0 {
		return value{}, fmt.Errorf("%w: empty reply line", errProtocol)
	}

	typ, rest := line[0], line[1:]
	switch typ {
	case typeSimpleString, typeError:
		// Copied: the next read invalidates the reader's buffer.
		return value{typ: typ, text: clone(rest)}, nil

	case typeInteger:
		n, err := parseInt(rest)
		if err != nil {
			return value{}, err
		}
		return value{typ: typ, num: n}, nil

	case typeBulkString:
		return readBulkString(br, rest, maxSize)

	case typeArray:
		return readArray(br, rest, maxSize, depth)

	default:
		return value{}, fmt.Errorf("%w: unknown type byte %q", errProtocol, typ)
	}
}

func readBulkString(br *bufio.Reader, header []byte, maxSize int) (value, error) {
	n, err := parseInt(header)
	if err != nil {
		return value{}, err
	}
	if n < 0 {
		// -1 is the null bulk string. Anything else is nonsense.
		if n != -1 {
			return value{}, fmt.Errorf("%w: bulk string length %d", errProtocol, n)
		}
		return value{typ: typeBulkString, null: true}, nil
	}
	if n > int64(maxSize) {
		return value{}, fmt.Errorf("redispubsub: bulk string of %d bytes exceeds the %d byte limit", n, maxSize)
	}

	// Read the trailing CRLF with the payload: one allocation, one read.
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(br, buf); err != nil {
		return value{}, err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return value{}, fmt.Errorf("%w: bulk string is not followed by CRLF", errProtocol)
	}
	return value{typ: typeBulkString, text: buf[:n:n]}, nil
}

func readArray(br *bufio.Reader, header []byte, maxSize, depth int) (value, error) {
	n, err := parseInt(header)
	if err != nil {
		return value{}, err
	}
	if n < 0 {
		if n != -1 {
			return value{}, fmt.Errorf("%w: array length %d", errProtocol, n)
		}
		return value{typ: typeArray, null: true}, nil
	}
	if n > maxArrayLen {
		return value{}, fmt.Errorf("%w: array of %d elements exceeds the %d element limit", errProtocol, n, maxArrayLen)
	}

	arr := make([]value, n)
	for i := range arr {
		if arr[i], err = readValueDepth(br, maxSize, depth+1); err != nil {
			return value{}, err
		}
	}
	return value{typ: typeArray, arr: arr}, nil
}

// readLine reads one CRLF-terminated line and returns it without the terminator.
// The result points into the reader's buffer and is only valid until the next
// read.
func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, fmt.Errorf("%w: reply line longer than %d bytes", errProtocol, br.Size())
		}
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("%w: line is not CRLF terminated", errProtocol)
	}
	return line[:len(line)-2], nil
}

// parseInt reads a RESP integer. Written out rather than calling
// strconv.ParseInt so that no string is allocated per reply, and so that
// anything Redis would never send — a sign in the middle, a leading plus,
// whitespace — is a protocol error rather than a silently different number.
func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("%w: empty integer", errProtocol)
	}

	negative := b[0] == '-'
	if negative {
		b = b[1:]
		if len(b) == 0 {
			return 0, fmt.Errorf("%w: integer is just a sign", errProtocol)
		}
	}

	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: %q in an integer", errProtocol, c)
		}
		digit := int64(c - '0')
		// Overflow check before it happens: a length header is untrusted input.
		if n > (1<<63-1-digit)/10 {
			return 0, fmt.Errorf("%w: integer out of range", errProtocol)
		}
		n = n*10 + digit
	}

	if negative {
		return -n, nil
	}
	return n, nil
}

func clone(b []byte) []byte {
	if len(b) == 0 {
		// Distinguishable from nil, which is what a null bulk string means.
		return []byte{}
	}
	return append(make([]byte, 0, len(b)), b...)
}

// appendCommand encodes a command as a RESP2 array of bulk strings, appending to
// buf so a caller can reuse one buffer for every command it sends.
func appendCommand(buf []byte, args ...string) []byte {
	buf = appendHeader(buf, typeArray, len(args))
	for _, arg := range args {
		buf = appendBulkString(buf, arg)
	}
	return buf
}

// appendPublish encodes PUBLISH separately because its payload is bytes: routing
// it through appendCommand would mean converting to string, which copies.
func appendPublish(buf []byte, broadcasting string, payload []byte) []byte {
	buf = appendHeader(buf, typeArray, 3)
	buf = appendBulkString(buf, "PUBLISH")
	buf = appendBulkString(buf, broadcasting)
	buf = appendHeader(buf, typeBulkString, len(payload))
	buf = append(buf, payload...)
	return append(buf, '\r', '\n')
}

func appendBulkString(buf []byte, s string) []byte {
	buf = appendHeader(buf, typeBulkString, len(s))
	buf = append(buf, s...)
	return append(buf, '\r', '\n')
}

func appendHeader(buf []byte, typ byte, n int) []byte {
	buf = append(buf, typ)
	buf = strconv.AppendInt(buf, int64(n), 10)
	return append(buf, '\r', '\n')
}
