package coax

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/igor-dmscn/coax-claude-impl/ws"
)

// Server is an Action Cable endpoint. It implements http.Handler, so it is
// mounted like any other route:
//
//	srv := coax.New(&coax.Options{Authenticate: authenticate})
//	defer srv.Close()
//	http.Handle(coax.DefaultMountPath, srv)
//
// A Server holds every connection made to it, in this process only. Reaching
// connections on other processes goes through the pub/sub backend instead.
type Server struct {
	opts Options

	// ownsPubSub records that the default one was created here, so Close only
	// closes what it made rather than the caller's backend.
	ownsPubSub bool

	// mu guards both registries. Connections come and go constantly; channels
	// are registered once at start-up.
	mu       sync.RWMutex
	conns    map[*Connection]struct{}
	channels map[string]ChannelFactory

	// live counts the connections being served, so shutdown can wait for them.
	live sync.WaitGroup

	heartbeatOnce sync.Once
	stopOnce      sync.Once

	// stop is closed once the server is going away. It stops the heartbeat and
	// makes the server refuse new connections.
	stop chan struct{}
}

// New returns a Server. opts may be nil.
func New(opts *Options) *Server {
	if opts == nil {
		opts = &Options{}
	}
	s := &Server{
		opts:     opts.withDefaults(),
		conns:    make(map[*Connection]struct{}),
		channels: make(map[string]ChannelFactory),
		stop:     make(chan struct{}),
	}
	if s.opts.PubSub == nil {
		s.opts.PubSub = NewMemoryPubSub()
		s.ownsPubSub = true
	}
	return s
}

// ServeHTTP upgrades a request to a WebSocket connection and serves it until it
// closes. It does not return until then, which is what hijacking the connection
// requires.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// A request that is not a WebSocket upgrade, or comes from an origin that is
	// not allowed, gets a plain 404 with no WebSocket and no disconnect message.
	// ← actioncable/lib/action_cable/server/socket.rb:145 (respond_to_invalid_request)
	if !isUpgrade(r) {
		s.opts.Logger.Debug("coax: not a websocket upgrade", "path", r.URL.Path, "remote", r.RemoteAddr)
		writePageNotFound(w)
		return
	}
	if !s.originAllowed(r) {
		s.opts.Logger.Warn("coax: origin not allowed", "origin", r.Header.Get("Origin"), "remote", r.RemoteAddr)
		writePageNotFound(w)
		return
	}

	// A server that is going away must not take on more work, or a shutdown can
	// never finish. Answered with a status rather than a WebSocket, since there is
	// no point completing a handshake only to end it.
	if s.stopping() {
		s.opts.Logger.Debug("coax: refusing a connection during shutdown", "remote", r.RemoteAddr)
		writeUnavailable(w)
		return
	}

	conn, err := ws.Accept(w, r, &ws.AcceptOptions{
		Subprotocols: []string{Subprotocol, subprotocolUnsupported},
		// Origin was already checked above with Action Cable's rules.
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.opts.Logger.Warn("coax: handshake failed", "error", err, "remote", r.RemoteAddr)
		return
	}

	s.serve(r, conn)
}

// serve runs one connection's whole life: authenticate, welcome, register, read
// until it ends.
func (s *Server) serve(r *http.Request, sock *ws.Conn) {
	logger := s.opts.Logger.With("remote", r.RemoteAddr)

	// Authentication runs after the handshake, so a rejection reaches the client
	// as a disconnect message rather than an HTTP status.
	// ← actioncable/lib/action_cable/connection/base.rb:91 (handle_open)
	identifiers, err := s.opts.Authenticate(r)
	if err != nil {
		logger.Info("coax: connection rejected", "error", err)
		rejectUnauthorized(sock)
		return
	}

	c := newConnection(s, sock, r, identifiers, logger)
	defer c.shutdown()

	// The writer goroutine owns the socket's write side from here on.
	go c.writeLoop()

	// Before the welcome, so that a remote disconnect published the moment a
	// client believes it is connected cannot be missed.
	c.subscribeToInternalChannel()

	if !c.transmitMessage(newWelcome()) {
		return
	}

	// Registration follows the welcome, so a connection is never sent a
	// heartbeat before it has been told it is usable.
	if !s.add(c) {
		// Shutdown began while this connection was being set up. It is told the
		// same thing every other connection was told, and waited for, since
		// returning here would close the socket under the message.
		c.close(reasonServerRestart, true)
		c.waitForClose()
		return
	}
	defer s.remove(c)
	s.startHeartbeat()

	c.readLoop()
}

// Close drops every connection without explanation and stops the server. Clients
// see a broken socket and reconnect on their own schedule. It is safe to call more
// than once, and after Shutdown.
//
// Prefer Shutdown, which tells clients what happened. Close is for when there is
// no time left, and for tests.
//
// A PubSub supplied through Options is left open, since the server did not open it.
func (s *Server) Close() error {
	s.beginShutdown()
	for _, c := range s.snapshot(nil) {
		c.closeNow()
	}
	return s.closePubSub()
}

// Shutdown ends the server gracefully: every connection is told the server is
// restarting and that it should come back, then closed once that message has been
// delivered. New connections are refused from the moment it is called.
//
// It waits for connections to finish, bounded by ctx. On expiry the remaining ones
// are dropped abruptly and ctx's error is returned, so a client that has stopped
// reading cannot hold a deployment open.
//
// Clients reconnect, which means this is what a rolling deploy wants: a connection
// moved to another process rather than a client left wondering.
//
// ← actioncable/lib/action_cable/server/base.rb:44 (restart)
func (s *Server) Shutdown(ctx context.Context) error {
	s.beginShutdown()

	for _, c := range s.snapshot(nil) {
		c.close(reasonServerRestart, true)
	}

	err := s.waitForConnections(ctx)
	if err != nil {
		s.opts.Logger.Warn("coax: shutdown ran out of time, dropping connections",
			"remaining", s.ConnectionCount(), "error", err)
		for _, c := range s.snapshot(nil) {
			c.closeNow()
		}
	}

	if closeErr := s.closePubSub(); err == nil {
		err = closeErr
	}
	return err
}

// beginShutdown makes the server refuse new connections and stops the heartbeat.
// The lock is held so that a connection which has just been accepted is either
// registered before shutdown starts waiting, or refused.
func (s *Server) beginShutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopOnce.Do(func() { close(s.stop) })
}

func (s *Server) stopping() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

// waitForConnections waits until every connection has stopped being served. A
// connection may still be releasing its subscriptions when this returns, which is
// harmless: a backend ignores an unsubscribe after it has been closed.
func (s *Server) waitForConnections(ctx context.Context) error {
	// Checked first so that shutting down an idle server with an expired context
	// succeeds rather than depending on which case the select picks.
	if s.ConnectionCount() == 0 {
		return nil
	}

	done := make(chan struct{})
	go func() {
		s.live.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) closePubSub() error {
	if s.ownsPubSub {
		return s.opts.PubSub.Close()
	}
	return nil
}

// ConnectionCount reports how many connections this process is serving.
func (s *Server) ConnectionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.conns)
}

// add registers a connection, reporting false if the server is going away. It
// takes the same lock beginShutdown does, which is what makes the answer binding:
// a connection either counts towards the shutdown wait or is turned away.
func (s *Server) add(c *Connection) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopping() {
		return false
	}
	s.conns[c] = struct{}{}
	s.live.Add(1)
	return true
}

func (s *Server) remove(c *Connection) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()

	s.live.Done()
}

// snapshot copies the connection set into dst so callers can iterate without
// holding the lock while transmitting. Passing a reused slice avoids allocating
// on every heartbeat tick.
func (s *Server) snapshot(dst []*Connection) []*Connection {
	dst = dst[:0]
	s.mu.RLock()
	for c := range s.conns {
		dst = append(dst, c)
	}
	s.mu.RUnlock()
	return dst
}

// startHeartbeat launches the ping ticker on first use, matching Rails' lazy
// setup: a server that never accepts a connection runs no goroutine.
// ← actioncable/lib/action_cable/server/connections.rb:38 (setup_heartbeat_timer)
func (s *Server) startHeartbeat() {
	s.heartbeatOnce.Do(func() { go s.heartbeat() })
}

func (s *Server) heartbeat() {
	ticker := time.NewTicker(s.opts.HeartbeatInterval)
	defer ticker.Stop()

	var conns []*Connection
	for {
		select {
		case <-s.stop:
			return
		case now := <-ticker.C:
			frame, err := newPing(now).encode()
			if err != nil {
				s.opts.Logger.Error("coax: encoding heartbeat", "error", err)
				continue
			}

			conns = s.sweep(frame, conns)
		}
	}
}

// sweep queues one frame on every connection, reusing dst to hold the snapshot.
//
// One encode per tick, shared by every connection: nothing mutates the frame, and
// server-side frames are written unmasked, so the bytes go out untouched. In-place
// masking would make this sharing a data race.
//
// It is the one piece of work that scales with the number of connections on a
// fixed schedule, which is why it does no I/O: each transmit is a send on a
// buffered channel, so a slow client cannot slow the sweep down.
func (s *Server) sweep(frame []byte, dst []*Connection) []*Connection {
	dst = s.snapshot(dst)
	for _, c := range dst {
		c.transmit(frame)
	}
	return dst
}

// originAllowed applies Action Cable's origin rules: the server's own host is
// always allowed, as is a request with no Origin header, since only browsers
// send one.
func (s *Server) originAllowed(r *http.Request) bool {
	if s.opts.DisableOriginCheck {
		return true
	}

	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}

	u, err := url.Parse(origin)
	if err != nil {
		return false
	}

	host := strings.ToLower(u.Host)
	if host == strings.ToLower(r.Host) {
		return true
	}
	for _, pattern := range s.opts.AllowedOrigins {
		if ok, err := path.Match(strings.ToLower(pattern), host); err == nil && ok {
			return true
		}
	}
	return false
}

// isUpgrade reports whether the request asks for a WebSocket upgrade. The
// handshake details are validated by ws.Accept; this only decides between
// serving a WebSocket and returning 404.
func isUpgrade(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	for _, value := range r.Header.Values("Upgrade") {
		for value != "" {
			var token string
			token, value, _ = strings.Cut(value, ",")
			if strings.EqualFold(strings.TrimSpace(token), "websocket") {
				return true
			}
		}
	}
	return false
}

// writePageNotFound reproduces Rails' response to a request that cannot become a
// cable connection, byte for byte.
func writePageNotFound(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusNotFound)
	io.WriteString(w, "Page not found")
}

// writeUnavailable answers a connection attempt made while the server is going
// away. Unlike the 404, this one is not copied from Rails: a Rails server being
// restarted stops listening altogether, while this is an http.Handler that may
// well be mounted in a process still serving other routes.
func writeUnavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	io.WriteString(w, "Server shutting down")
}

// rejectUnauthorized tells a client its connection was refused and closes.
func rejectUnauthorized(sock *ws.Conn) {
	if frame, err := newDisconnect(reasonUnauthorized, false).encode(); err == nil {
		ctx, cancel := shortContext()
		defer cancel()
		_ = sock.Write(ctx, ws.MessageText, frame)
	}
	_ = sock.Close(ws.StatusNormalClosure, "")
}
