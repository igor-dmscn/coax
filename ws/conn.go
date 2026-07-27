package ws

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// MessageType is the kind of a WebSocket data message.
type MessageType int

const (
	// MessageText is a UTF-8 encoded message. The connection validates the
	// encoding and fails the connection if it is not valid UTF-8.
	MessageText MessageType = MessageType(opText)

	// MessageBinary is an opaque message.
	MessageBinary MessageType = MessageType(opBinary)
)

func (t MessageType) String() string {
	switch t {
	case MessageText:
		return "text"
	case MessageBinary:
		return "binary"
	default:
		return fmt.Sprintf("MessageType(%d)", int(t))
	}
}

// Default limits, overridable through AcceptOptions and DialOptions.
const (
	defaultMaxFrameSize   = 1 << 20  // 1 MiB
	defaultMaxMessageSize = 32 << 20 // 32 MiB
	streamWriteBuffer     = 4096
	closeGracePeriod      = 5 * time.Second
)

var (
	// errWriterClosed is returned when a streaming writer is used after Close.
	errWriterClosed = errors.New("ws: writer is closed")

	// errWriterOpen is returned when a message is started before the previous
	// streaming message was closed.
	errWriterOpen = errors.New("ws: previous message is still open")
)

// Conn is a WebSocket connection.
//
// A Conn supports one concurrent reader and one concurrent writer. Reading and
// writing may proceed concurrently with each other, but the read methods must
// not be called from two goroutines at once, and neither must the write methods.
//
// Reader and Writer are the allocation-free path. Read and Write are
// conveniences built on them that allocate per message.
//
// Context handling: the read and write methods apply ctx's deadline to the
// underlying connection and return early if ctx is already cancelled, but they
// do not poll ctx while blocked. To interrupt a blocked call, close the
// connection with CloseNow from another goroutine.
type Conn struct {
	rwc         net.Conn
	br          *bufio.Reader
	bw          *bufio.Writer
	client      bool
	subprotocol string
	maxFrame    int64
	maxMessage  int64
	rsvAllowed  [3]bool

	// Read state, owned by the goroutine calling Reader or Read.
	readHeaderBuf [maxHeaderSize]byte
	controlBuf    [maxControlPayload]byte
	reader        msgReader
	validator     utf8Validator

	// Write state. writeMu serialises individual frames, not whole messages: a
	// streaming writer takes it per fragment and releases it in between, so an
	// automatic pong or a close frame never waits on a slow message. RFC 6455
	// §5.4 permits control frames between fragments, so this is also the
	// behaviour the spec describes.
	//
	// msgWriterOpen guards against a second message starting before the current
	// one is closed, which would interleave data frames illegally.
	writeMu        sync.Mutex
	msgWriterOpen  bool
	writeHeaderBuf [maxHeaderSize]byte
	maskScratch    []byte // client only: masking must not mutate a caller's buffer
	writer         msgWriter

	closeMu  sync.Mutex
	closeErr error
}

func newConn(rwc net.Conn, br *bufio.Reader, bw *bufio.Writer, client bool, subprotocol string, maxFrame, maxMessage int64) *Conn {
	if maxFrame <= 0 {
		maxFrame = defaultMaxFrameSize
	}
	if maxMessage <= 0 {
		maxMessage = defaultMaxMessageSize
	}

	c := &Conn{
		rwc:         rwc,
		br:          br,
		bw:          bw,
		client:      client,
		subprotocol: subprotocol,
		maxFrame:    maxFrame,
		maxMessage:  maxMessage,
	}
	c.reader.c = c
	c.writer.c = c
	if client {
		c.maskScratch = make([]byte, streamWriteBuffer)
	}
	return c
}

// Subprotocol returns the subprotocol negotiated during the handshake, or the
// empty string if none was.
func (c *Conn) Subprotocol() string { return c.subprotocol }

// err returns the terminal error if the connection has closed.
func (c *Conn) err() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closeErr
}

// Reader returns the type of the next message and a reader for its payload.
//
// The returned reader is only valid until the next call to Reader or Read, and
// stops with io.EOF at the end of the message. It reads directly into the
// caller's buffer and allocates nothing.
//
// Control frames arriving before or during the message are handled internally:
// pings are answered, pongs ignored, and a close frame ends the connection and
// is reported as a CloseError.
func (c *Conn) Reader(ctx context.Context) (MessageType, io.Reader, error) {
	if err := c.err(); err != nil {
		return 0, nil, err
	}
	if err := c.applyReadDeadline(ctx); err != nil {
		return 0, nil, err
	}

	// Drain any unread remainder of the previous message so the stream stays
	// framed correctly even if the caller abandoned its reader.
	if c.reader.active {
		if err := c.reader.drain(); err != nil {
			return 0, nil, err
		}
	}

	for {
		h, err := c.readFrameHeader()
		if err != nil {
			return 0, nil, err
		}

		if h.opcode.isControl() {
			if err := c.handleControl(h); err != nil {
				return 0, nil, err
			}
			continue
		}

		switch h.opcode {
		case opContinuation:
			return 0, nil, c.failRead(protocolErrorf("continuation frame with no message in progress"))
		case opText, opBinary:
		}

		if h.payloadLen > c.maxMessage {
			return 0, nil, c.failRead(&closeSentinel{
				code: StatusMessageTooBig,
				err:  fmt.Errorf("ws: message of %d bytes exceeds limit of %d", h.payloadLen, c.maxMessage),
			})
		}

		c.validator.reset()
		c.reader.reset(MessageType(h.opcode), h)
		return MessageType(h.opcode), &c.reader, nil
	}
}

// Read reads the next message into a newly allocated buffer.
//
// It is a convenience wrapper around Reader and allocates proportionally to the
// message size; use Reader on hot paths.
func (c *Conn) Read(ctx context.Context) (MessageType, []byte, error) {
	typ, r, err := c.Reader(ctx)
	if err != nil {
		return 0, nil, err
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, nil, err
	}
	return typ, b, nil
}

// Write sends p as a single message.
//
// It writes one frame regardless of size and allocates nothing on a server
// connection. Text messages are not validated: sending invalid UTF-8 is the
// caller's error to avoid.
func (c *Conn) Write(ctx context.Context, typ MessageType, p []byte) error {
	if typ != MessageText && typ != MessageBinary {
		return fmt.Errorf("ws: invalid message type %v", typ)
	}
	if err := c.err(); err != nil {
		return err
	}
	if err := c.applyWriteDeadline(ctx); err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.msgWriterOpen {
		return errWriterOpen
	}
	return c.writeFrameLocked(opcode(typ), p, true)
}

// Writer returns a writer for a single message of the given type.
//
// Bytes are buffered and emitted as one frame when the message fits the
// internal buffer, or as continuation fragments when it does not. The message
// is not complete until Close is called, and no other message may be written
// until then.
func (c *Conn) Writer(ctx context.Context, typ MessageType) (io.WriteCloser, error) {
	if typ != MessageText && typ != MessageBinary {
		return nil, fmt.Errorf("ws: invalid message type %v", typ)
	}
	if err := c.err(); err != nil {
		return nil, err
	}
	if err := c.applyWriteDeadline(ctx); err != nil {
		return nil, err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.msgWriterOpen {
		return nil, errWriterOpen
	}
	c.msgWriterOpen = true
	c.writer.reset(opcode(typ))
	return &c.writer, nil
}

// Close sends a close frame with the given status and reason, waits briefly for
// the peer to answer, and closes the underlying connection.
//
// The wait reads from the connection, so Close must not be called while another
// goroutine is reading: use CloseSend there instead.
//
// It is safe to call Close more than once; later calls return the error from
// the first. reason must not exceed 123 bytes.
func (c *Conn) Close(code StatusCode, reason string) error {
	return c.closeHandshake(code, reason, CloseError{Code: code, Reason: reason})
}

// CloseSend sends a close frame and then closes the underlying connection,
// without waiting for the peer to answer.
//
// It exists for the common server shape where one goroutine owns reads: Close
// completes the handshake, which means reading, so calling it from anywhere else
// would race that reader. This tells the peer why the connection is ending —
// which CloseNow does not — and gives up the right to hear its answer.
//
// It is safe to call more than once; later calls return the error from the first.
// reason must not exceed 123 bytes.
func (c *Conn) CloseSend(code StatusCode, reason string) error {
	if err := c.startClosing(CloseError{Code: code, Reason: reason}); err != nil {
		return err
	}

	writeErr := c.writeClose(code, reason)
	closeErr := c.rwc.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

// CloseNow closes the underlying connection without a close handshake and
// without telling the peer anything. Use it to abandon a connection, or to
// unblock a reader or writer.
func (c *Conn) CloseNow() error {
	if err := c.startClosing(CloseError{Code: StatusAbnormalClosure}); err != nil {
		return err
	}
	return c.rwc.Close()
}

// startClosing records why the connection is ending, or reports the reason
// already recorded, which is what makes the Close methods callable repeatedly and
// from more than one goroutine.
func (c *Conn) startClosing(cause error) error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	if c.closeErr != nil {
		return c.closeErr
	}
	c.closeErr = cause
	return nil
}

// closeHandshake writes a close frame, drains until the peer's close arrives or
// the grace period expires, then closes the socket. cause becomes the terminal
// error reported by later calls.
func (c *Conn) closeHandshake(code StatusCode, reason string, cause error) error {
	if err := c.startClosing(cause); err != nil {
		return err
	}

	writeErr := c.writeClose(code, reason)

	// Give the peer a moment to answer so the TCP close does not race the close
	// frame, then stop regardless: a peer that never answers must not hold the
	// connection open.
	_ = c.rwc.SetReadDeadline(time.Now().Add(closeGracePeriod))
	for {
		h, err := readFrameHeader(c.br, c.readHeaderBuf[:])
		if err != nil {
			break
		}
		if h.payloadLen > maxControlPayload {
			// A data frame. We are closing, so there is nothing worth reading
			// and it will not fit the control buffer.
			break
		}
		if _, err := c.readPayload(h); err != nil {
			break
		}
		if h.opcode == opClose {
			break
		}
	}

	closeErr := c.rwc.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func (c *Conn) writeClose(code StatusCode, reason string) error {
	payload, err := appendClosePayload(c.controlBuf[:0], code, reason)
	if err != nil {
		return err
	}

	// The deadline is set before taking the lock on purpose: if an application
	// write is in flight and blocked, this bounds how long tearing down waits
	// for it to release the lock.
	_ = c.rwc.SetWriteDeadline(time.Now().Add(closeGracePeriod))

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeFrameLocked(opClose, payload, true)
}

// failRead turns a read-path failure into a closed connection, sending the
// close frame the failure calls for, and returns the error to report.
func (c *Conn) failRead(err error) error {
	var cs *closeSentinel
	if !errors.As(err, &cs) {
		// An I/O failure: nothing can be sent, so just tear down.
		c.closeMu.Lock()
		if c.closeErr == nil {
			c.closeErr = err
		}
		reported := c.closeErr
		c.closeMu.Unlock()
		_ = c.rwc.Close()
		return reported
	}

	c.closeMu.Lock()
	if c.closeErr != nil {
		reported := c.closeErr
		c.closeMu.Unlock()
		return reported
	}
	c.closeErr = err
	c.closeMu.Unlock()

	_ = c.writeClose(cs.code, "")
	_ = c.bw.Flush()
	_ = c.rwc.Close()
	return err
}

func (c *Conn) applyReadDeadline(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline() // zero clears any previous deadline
	return c.rwc.SetReadDeadline(deadline)
}

func (c *Conn) applyWriteDeadline(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	return c.rwc.SetWriteDeadline(deadline)
}

// readFrameHeader reads and validates one header.
func (c *Conn) readFrameHeader() (frameHeader, error) {
	h, err := readFrameHeader(c.br, c.readHeaderBuf[:])
	if err != nil {
		return frameHeader{}, c.failRead(err)
	}
	if err := h.validate(!c.client, c.rsvAllowed); err != nil {
		return frameHeader{}, c.failRead(err)
	}
	if h.payloadLen > c.maxFrame {
		return frameHeader{}, c.failRead(&closeSentinel{
			code: StatusMessageTooBig,
			err:  fmt.Errorf("ws: frame of %d bytes exceeds limit of %d", h.payloadLen, c.maxFrame),
		})
	}
	return h, nil
}

// readPayload reads a control frame payload into the shared control buffer and
// unmasks it. Only valid for frames of at most maxControlPayload bytes; the
// guard keeps a mistaken caller from overrunning the buffer.
func (c *Conn) readPayload(h frameHeader) ([]byte, error) {
	if h.payloadLen > maxControlPayload {
		return nil, protocolErrorf("payload of %d bytes does not fit the control buffer", h.payloadLen)
	}
	p := c.controlBuf[:h.payloadLen]
	if _, err := io.ReadFull(c.br, p); err != nil {
		return nil, err
	}
	if h.masked {
		mask(p, h.maskKey, 0)
	}
	return p, nil
}

// handleControl processes one control frame. A close frame ends the connection
// and is reported as an error; ping and pong return nil so the caller resumes
// reading data.
func (c *Conn) handleControl(h frameHeader) error {
	payload, err := c.readPayload(h)
	if err != nil {
		return c.failRead(err)
	}

	switch h.opcode {
	case opPing:
		c.writeMu.Lock()
		err := c.writeFrameLocked(opPong, payload, true)
		c.writeMu.Unlock()
		if err != nil {
			return c.failRead(err)
		}
		return nil

	case opPong:
		return nil

	case opClose:
		ce, err := parseClosePayload(payload)
		if err != nil {
			return c.failRead(err)
		}
		// Mirror the peer's status back, then tear down. A peer that sent no
		// status gets an empty close in return.
		//
		// Rewriting controlBuf here is safe: parseClosePayload already copied
		// the reason into ce, so nothing still refers to the received bytes.
		var echo []byte
		if ce.Code != StatusNoStatusRcvd {
			echo, err = appendClosePayload(c.controlBuf[:0], ce.Code, "")
			if err != nil {
				return c.failRead(err)
			}
		}
		return c.finishPeerClose(ce, echo)

	default:
		return c.failRead(protocolErrorf("unexpected control opcode %#x", byte(h.opcode)))
	}
}

// finishPeerClose records the peer's close, sends echo as the reply payload,
// and closes the socket.
func (c *Conn) finishPeerClose(ce CloseError, echo []byte) error {
	c.closeMu.Lock()
	if c.closeErr != nil {
		reported := c.closeErr
		c.closeMu.Unlock()
		return reported
	}
	c.closeErr = ce
	c.closeMu.Unlock()

	_ = c.rwc.SetWriteDeadline(time.Now().Add(closeGracePeriod))
	_ = c.writeRawClose(echo)
	_ = c.rwc.Close()
	return ce
}

func (c *Conn) writeRawClose(payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeFrameLocked(opClose, payload, true)
}

// writeFrameLocked writes one complete frame. The caller must hold writeMu.
func (c *Conn) writeFrameLocked(op opcode, payload []byte, fin bool) error {
	h := frameHeader{
		fin:        fin,
		opcode:     op,
		masked:     c.client,
		payloadLen: int64(len(payload)),
	}
	if c.client {
		if err := newMaskKey(&h.maskKey); err != nil {
			return err
		}
	}

	if _, err := c.bw.Write(appendFrameHeader(c.writeHeaderBuf[:0], h)); err != nil {
		return err
	}

	if !c.client {
		// Server frames are unmasked, so the caller's bytes go out untouched.
		if _, err := c.bw.Write(payload); err != nil {
			return err
		}
		return c.bw.Flush()
	}

	// Masking mutates, so copy through scratch rather than touching payload.
	pos := 0
	for len(payload) > 0 {
		n := copy(c.maskScratch, payload)
		payload = payload[n:]
		pos = mask(c.maskScratch[:n], h.maskKey, pos)
		if _, err := c.bw.Write(c.maskScratch[:n]); err != nil {
			return err
		}
	}
	return c.bw.Flush()
}

// msgReader reads one message, transparently crossing fragment boundaries and
// handling control frames that arrive in between.
type msgReader struct {
	c   *Conn
	typ MessageType

	remaining int64 // bytes left in the current frame
	fin       bool  // current frame is the last of the message
	maskKey   [4]byte
	masked    bool
	maskPos   int

	total  int64
	active bool // a message is in progress; the zero value means none
	err    error
}

func (r *msgReader) reset(typ MessageType, h frameHeader) {
	r.typ = typ
	r.remaining = h.payloadLen
	r.fin = h.fin
	r.maskKey = h.maskKey
	r.masked = h.masked
	r.maskPos = 0
	r.total = 0
	r.active = true
	r.err = nil
}

func (r *msgReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	for r.remaining == 0 {
		if r.fin {
			if r.typ == MessageText && !r.c.validator.done() {
				r.err = r.c.failRead(&closeSentinel{
					code: StatusInvalidFramePayloadData,
					err:  errors.New("ws: text message ends mid-rune"),
				})
				return 0, r.err
			}
			r.active = false
			r.err = io.EOF
			return 0, io.EOF
		}
		if err := r.nextFragment(); err != nil {
			r.err = err
			return 0, err
		}
	}

	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.c.br.Read(p)
	if n > 0 {
		if r.masked {
			r.maskPos = mask(p[:n], r.maskKey, r.maskPos)
		}
		r.remaining -= int64(n)
		r.total += int64(n)

		if r.typ == MessageText && !r.c.validator.write(p[:n]) {
			r.err = r.c.failRead(&closeSentinel{
				code: StatusInvalidFramePayloadData,
				err:  errors.New("ws: text message is not valid UTF-8"),
			})
			return n, r.err
		}
	}
	if err != nil {
		r.err = r.c.failRead(err)
		return n, r.err
	}
	return n, nil
}

// nextFragment advances to the next data fragment of this message, handling any
// control frames in between.
func (r *msgReader) nextFragment() error {
	for {
		h, err := r.c.readFrameHeader()
		if err != nil {
			return err
		}

		if h.opcode.isControl() {
			if err := r.c.handleControl(h); err != nil {
				return err
			}
			continue
		}
		if h.opcode != opContinuation {
			return r.c.failRead(protocolErrorf("expected a continuation frame, got opcode %#x", byte(h.opcode)))
		}
		if r.total+h.payloadLen > r.c.maxMessage {
			return r.c.failRead(&closeSentinel{
				code: StatusMessageTooBig,
				err:  fmt.Errorf("ws: message exceeds limit of %d bytes", r.c.maxMessage),
			})
		}

		r.remaining = h.payloadLen
		r.fin = h.fin
		r.maskKey = h.maskKey
		r.masked = h.masked
		r.maskPos = 0
		return nil
	}
}

// drain discards the rest of the message so the framing stays in step.
func (r *msgReader) drain() error {
	for {
		_, err := r.Read(r.c.controlBuf[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// msgWriter streams one message, emitting continuation fragments when its
// buffer fills so a message of unknown length needs no unbounded buffering.
type msgWriter struct {
	c            *Conn
	op           opcode
	buf          []byte
	n            int
	sentFragment bool
	closed       bool
}

func (w *msgWriter) reset(op opcode) {
	if w.buf == nil {
		w.buf = make([]byte, streamWriteBuffer)
	}
	w.op = op
	w.n = 0
	w.sentFragment = false
	w.closed = false
}

func (w *msgWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errWriterClosed
	}
	if err := w.c.err(); err != nil {
		return 0, err
	}

	written := 0
	for len(p) > 0 {
		n := copy(w.buf[w.n:], p)
		w.n += n
		p = p[n:]
		written += n

		if w.n == len(w.buf) {
			if err := w.flushFragment(false); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

// Close finishes the message, emitting the final frame.
func (w *msgWriter) Close() error {
	if w.closed {
		return errWriterClosed
	}
	w.closed = true

	err := w.flushFragment(true)

	w.c.writeMu.Lock()
	w.c.msgWriterOpen = false
	w.c.writeMu.Unlock()
	return err
}

// flushFragment writes the buffered bytes as one frame. It holds writeMu only
// for that frame so control frames can be sent between fragments.
func (w *msgWriter) flushFragment(fin bool) error {
	op := w.op
	if w.sentFragment {
		op = opContinuation
	}

	w.c.writeMu.Lock()
	err := w.c.writeFrameLocked(op, w.buf[:w.n], fin)
	w.c.writeMu.Unlock()

	w.sentFragment = true
	w.n = 0
	return err
}
