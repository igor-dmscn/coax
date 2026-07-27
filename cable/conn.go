package cable

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"go-cable/ws"
)

// writeTimeout bounds a single frame write, so one unresponsive client cannot
// hold its writer goroutine forever.
const writeTimeout = 10 * time.Second

// Connection is one client's connection to the server. It is created after a
// successful handshake and lives until the socket closes, which may be days.
//
// One goroutine reads and dispatches, one writes. Commands are handled inline on
// the reader, so a connection's messages are processed in order and its own
// state needs no locking.
//
// ← actioncable/lib/action_cable/connection/base.rb
type Connection struct {
	server      *Server
	sock        *ws.Conn
	identifiers Identifiers
	logger      *slog.Logger
	request     *http.Request
	startedAt   time.Time

	// send carries encoded frames to the writer goroutine. It is bounded: a
	// client that cannot keep up is dropped rather than buffered without limit.
	send chan []byte

	// subscriptions is keyed by the client's raw identifier string. Only the
	// reader goroutine touches it, so it needs no lock.
	subscriptions map[string]*Subscription

	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func newConnection(s *Server, sock *ws.Conn, r *http.Request, ids Identifiers, logger *slog.Logger) *Connection {
	ctx, cancel := context.WithCancel(r.Context())
	return &Connection{
		server:      s,
		sock:        sock,
		identifiers: ids,
		logger:      logger,
		request:     r,
		startedAt:   time.Now(),

		send:          make(chan []byte, s.opts.SendBuffer),
		subscriptions: make(map[string]*Subscription),

		ctx:    ctx,
		cancel: cancel,
	}
}

// Identifiers returns who this connection belongs to, as set by the
// Authenticator. The map must not be modified.
func (c *Connection) Identifiers() Identifiers { return c.identifiers }

// Request returns the HTTP request that opened the connection. Its body has
// already been consumed; it is useful for headers, cookies and query parameters.
func (c *Connection) Request() *http.Request { return c.request }

// readLoop reads messages until the connection ends. It runs on the HTTP
// handler's goroutine, so the handler naturally stays alive for the connection's
// lifetime.
func (c *Connection) readLoop() {
	for {
		// Read allocates per message. Commands are small and the JSON decode
		// allocates regardless, so the streaming API would buy nothing here.
		typ, data, err := c.sock.Read(c.ctx)
		if err != nil {
			c.logClosed(err)
			return
		}
		if typ != ws.MessageText {
			// Action Cable is JSON over text frames.
			// ← actioncable/lib/action_cable/server/socket/message_buffer.rb:21
			c.logger.Error("cable: ignoring non-text message", "type", typ.String())
			continue
		}

		c.handleMessage(data)
	}
}

// handleMessage processes one client message.
//
// Failures are logged and the connection continues: Rails answers an
// unintelligible command with nothing at all, so a client is never told that its
// command was rejected.
// ← actioncable/lib/action_cable/server/socket.rb:79 (dispatch_websocket_message)
func (c *Connection) handleMessage(data []byte) {
	cmd, err := decodeCommand(data)
	if err != nil {
		c.logger.Error("cable: could not handle incoming message", "error", err)
		return
	}

	switch cmd.Command {
	case commandSubscribe:
		c.addSubscription(cmd.Identifier)
	case commandUnsubscribe:
		c.removeSubscription(cmd.Identifier)
	case commandMessage:
		c.performAction(cmd)
	}
	// No default: decodeCommand rejects anything else.
}

// writeLoop owns the socket's write side. It is the only writer, which is what
// the WebSocket layer requires.
func (c *Connection) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case frame := <-c.send:
			ctx, cancel := context.WithTimeout(c.ctx, writeTimeout)
			err := c.sock.Write(ctx, ws.MessageText, frame)
			cancel()

			if err != nil {
				// A failed write means the connection is gone: closing here also
				// unblocks the reader.
				if !errors.Is(err, context.Canceled) {
					c.logger.Debug("cable: write failed", "error", err)
				}
				c.closeNow()
				return
			}
		}
	}
}

// transmit queues an already-encoded frame. It never blocks: a full buffer means
// the client is not reading, so the connection is dropped instead of growing the
// queue.
func (c *Connection) transmit(frame []byte) {
	select {
	case c.send <- frame:
	case <-c.ctx.Done():
	default:
		c.logger.Warn("cable: send buffer full, dropping connection", "buffered", len(c.send))
		c.closeNow()
	}
}

// transmitMessage encodes and queues a message, reporting whether it was queued.
func (c *Connection) transmitMessage(m serverMessage) bool {
	frame, err := m.encode()
	if err != nil {
		c.logger.Error("cable: encoding outbound message", "error", err)
		return false
	}
	c.transmit(frame)
	return true
}

// closeNow tears the connection down without a disconnect message, unblocking
// both goroutines. It is safe to call from any goroutine, repeatedly.
func (c *Connection) closeNow() {
	c.closeOnce.Do(func() {
		c.cancel()
		_ = c.sock.CloseNow()
	})
}

// shutdown releases the connection's resources. Called once the reader stops,
// which is also the only goroutine that touches the subscriptions.
// ← actioncable/lib/action_cable/connection/base.rb:214 (on_close)
func (c *Connection) shutdown() {
	c.closeNow()
	c.unsubscribeAll()
	c.logger.Debug("cable: connection finished", "duration", time.Since(c.startedAt))
}

// logClosed records why a connection ended, distinguishing the ordinary cases
// from real failures.
func (c *Connection) logClosed(err error) {
	switch {
	case errors.Is(err, context.Canceled):
		c.logger.Debug("cable: connection cancelled")
	case ws.CloseStatus(err) == ws.StatusNormalClosure,
		ws.CloseStatus(err) == ws.StatusGoingAway,
		ws.CloseStatus(err) == ws.StatusNoStatusRcvd,
		ws.CloseStatus(err) == ws.StatusAbnormalClosure:
		c.logger.Debug("cable: client disconnected", "status", int(ws.CloseStatus(err)))
	default:
		c.logger.Info("cable: connection closed", "error", err)
	}
}

// shortContext bounds a best-effort write during teardown.
func shortContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), writeTimeout)
}
