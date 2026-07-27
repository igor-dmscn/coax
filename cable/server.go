package cable

import (
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"go-cable/ws"
)

// Server is an Action Cable endpoint. It implements http.Handler, so it is
// mounted like any other route:
//
//	srv := cable.New(&cable.Options{Authenticate: authenticate})
//	defer srv.Close()
//	http.Handle(cable.DefaultMountPath, srv)
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

	heartbeatOnce sync.Once
	stopOnce      sync.Once
	stop          chan struct{}
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
		s.opts.Logger.Debug("cable: not a websocket upgrade", "path", r.URL.Path, "remote", r.RemoteAddr)
		writePageNotFound(w)
		return
	}
	if !s.originAllowed(r) {
		s.opts.Logger.Warn("cable: origin not allowed", "origin", r.Header.Get("Origin"), "remote", r.RemoteAddr)
		writePageNotFound(w)
		return
	}

	conn, err := ws.Accept(w, r, &ws.AcceptOptions{
		Subprotocols: []string{Subprotocol, subprotocolUnsupported},
		// Origin was already checked above with Action Cable's rules.
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.opts.Logger.Warn("cable: handshake failed", "error", err, "remote", r.RemoteAddr)
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
		logger.Info("cable: connection rejected", "error", err)
		rejectUnauthorized(sock)
		return
	}

	c := newConnection(s, sock, r, identifiers, logger)
	defer c.shutdown()

	// The writer goroutine owns the socket's write side from here on.
	go c.writeLoop()

	if !c.transmitMessage(newWelcome()) {
		return
	}

	// Registration follows the welcome, so a connection is never sent a
	// heartbeat before it has been told it is usable.
	s.add(c)
	defer s.remove(c)
	s.startHeartbeat()

	c.readLoop()
}

// Close stops the heartbeat and drops every connection. It is safe to call more
// than once. A PubSub supplied through Options is left open, since the server did
// not open it.
//
// This is the abrupt form. Graceful shutdown, which tells clients to reconnect
// before going away, arrives with the rest of the disconnect handling.
func (s *Server) Close() error {
	s.stopOnce.Do(func() { close(s.stop) })
	for _, c := range s.snapshot(nil) {
		c.closeNow()
	}
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

func (s *Server) add(c *Connection) {
	s.mu.Lock()
	s.conns[c] = struct{}{}
	s.mu.Unlock()
}

func (s *Server) remove(c *Connection) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
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
				s.opts.Logger.Error("cable: encoding heartbeat", "error", err)
				continue
			}

			// One encode per tick, shared by every connection: nothing mutates
			// the frame, and server-side frames are written unmasked, so the
			// bytes go out untouched.
			conns = s.snapshot(conns)
			for _, c := range conns {
				c.transmit(frame)
			}
		}
	}
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

// rejectUnauthorized tells a client its connection was refused and closes.
func rejectUnauthorized(sock *ws.Conn) {
	if frame, err := newDisconnect(reasonUnauthorized, false).encode(); err == nil {
		ctx, cancel := shortContext()
		defer cancel()
		_ = sock.Write(ctx, ws.MessageText, frame)
	}
	_ = sock.Close(ws.StatusNormalClosure, "")
}
