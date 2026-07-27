package coax

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/igor-dmscn/coax-claude-impl/ws"
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
	send chan outbound

	// subscriptions is keyed by the client's raw identifier string. Only the
	// reader goroutine touches it, so it needs no lock.
	subscriptions map[string]*Subscription

	// stopInternalChannel ends this connection's pub/sub subscription to its own
	// identity. Reader goroutine only, like the subscriptions.
	stopInternalChannel func()

	ctx         context.Context
	cancel      context.CancelFunc
	closeOnce   sync.Once
	closingOnce sync.Once
}

// outbound is one queued frame. final marks the last one: the connection is torn
// down once it has been written, which is how a disconnect message reaches a
// client before its socket disappears.
type outbound struct {
	frame []byte
	final bool
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

		send:          make(chan outbound, s.opts.SendBuffer),
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
			c.logger.Error("coax: ignoring non-text message", "type", typ.String())
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
		c.logger.Error("coax: could not handle incoming message", "error", err)
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
		case out := <-c.send:
			ctx, cancel := context.WithTimeout(c.ctx, writeTimeout)
			err := c.sock.Write(ctx, ws.MessageText, out.frame)
			cancel()

			if err != nil {
				// A failed write means the connection is gone: closing here also
				// unblocks the reader.
				if !errors.Is(err, context.Canceled) {
					c.logger.Debug("coax: write failed", "error", err)
				}
				c.closeNow()
				return
			}

			if out.final {
				// The client has its explanation, so the connection can go. Sent
				// rather than dropped: a close frame turns the client's onclose
				// into an ordinary event instead of an error. CloseSend rather
				// than Close because the reader goroutine owns reads and a close
				// handshake would have to read.
				c.closeSend()
				return
			}
		}
	}
}

// transmit queues an already-encoded frame. It never blocks: a full buffer means
// the client is not reading, so the connection is dropped instead of growing the
// queue.
func (c *Connection) transmit(frame []byte) {
	c.enqueue(outbound{frame: frame})
}

func (c *Connection) enqueue(out outbound) {
	select {
	case c.send <- out:
	case <-c.ctx.Done():
	default:
		c.logger.Warn("coax: send buffer full, dropping connection", "buffered", len(c.send))
		c.closeNow()
	}
}

// transmitMessage encodes and queues a message, reporting whether it was queued.
func (c *Connection) transmitMessage(m serverMessage) bool {
	frame, err := m.encode()
	if err != nil {
		c.logger.Error("coax: encoding outbound message", "error", err)
		return false
	}
	c.transmit(frame)
	return true
}

// close tells the client why it is going away and then disconnects it, which is
// what the client needs in order to decide whether to come back. Both reasons for
// doing this — a server shutting down and a disconnect from another process —
// arrive from other goroutines, so it is safe to call from anywhere, and only the
// first call is acted on.
//
// It returns as soon as the message is queued. The connection ends once the
// writer has flushed it, or immediately if the queue is too full to accept it.
// ← actioncable/lib/action_cable/connection/base.rb:118 (close)
func (c *Connection) close(reason string, reconnect bool) {
	c.closingOnce.Do(func() {
		frame, err := newDisconnect(reason, reconnect).encode()
		if err != nil {
			c.logger.Error("coax: encoding a disconnect message", "error", err)
			c.closeNow()
			return
		}

		c.logger.Debug("coax: disconnecting", "reason", reason, "reconnect", reconnect)
		c.enqueue(outbound{frame: frame, final: true})
	})
}

// waitForClose waits until a close started by close() has been delivered, so a
// caller that is about to tear the socket down does not do it under the message.
// Bounded, because a client that has stopped reading must not hold anyone up.
func (c *Connection) waitForClose() {
	timer := time.NewTimer(writeTimeout)
	defer timer.Stop()

	select {
	case <-c.ctx.Done():
	case <-timer.C:
		c.closeNow()
	}
}

// closeNow tears the connection down without a disconnect message, unblocking
// both goroutines. It is safe to call from any goroutine, repeatedly.
func (c *Connection) closeNow() {
	c.closeOnce.Do(func() {
		c.cancel()
		_ = c.sock.CloseNow()
	})
}

// closeSend ends the connection with a WebSocket close frame, for when the client
// has already been told why in a disconnect message.
func (c *Connection) closeSend() {
	c.closeOnce.Do(func() {
		c.cancel()
		_ = c.sock.CloseSend(ws.StatusNormalClosure, "")
	})
}

// shutdown releases the connection's resources. Called once the reader stops,
// which is also the only goroutine that touches the subscriptions.
// ← actioncable/lib/action_cable/connection/base.rb:214 (on_close)
func (c *Connection) shutdown() {
	c.closeNow()
	c.unsubscribeAll()
	c.unsubscribeFromInternalChannel()
	c.logger.Debug("coax: connection finished", "duration", time.Since(c.startedAt))
}

// logClosed records why a connection ended, distinguishing the ordinary cases
// from real failures.
func (c *Connection) logClosed(err error) {
	switch {
	case errors.Is(err, context.Canceled):
		c.logger.Debug("coax: connection cancelled")
	case ws.CloseStatus(err) == ws.StatusNormalClosure,
		ws.CloseStatus(err) == ws.StatusGoingAway,
		ws.CloseStatus(err) == ws.StatusNoStatusRcvd,
		ws.CloseStatus(err) == ws.StatusAbnormalClosure:
		c.logger.Debug("coax: client disconnected", "status", int(ws.CloseStatus(err)))
	default:
		c.logger.Info("coax: connection closed", "error", err)
	}
}

// shortContext bounds a best-effort write during teardown.
func shortContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), writeTimeout)
}
