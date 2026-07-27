package coax

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/igor-dmscn/coax-claude-impl/ws"
)

// quietOptions silences log output and speeds the heartbeat up so tests do not
// wait three seconds for a ping.
func quietOptions(o *Options) *Options {
	if o == nil {
		o = &Options{}
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.HeartbeatInterval == 0 {
		o.HeartbeatInterval = 20 * time.Millisecond
	}
	return o
}

// newTestServer starts a cable server behind an HTTP test server and returns it
// with a ws:// URL.
func newTestServer(t *testing.T, o *Options) (*Server, string) {
	t.Helper()

	srv := New(quietOptions(o))
	t.Cleanup(func() { srv.Close() })

	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)

	return srv, "ws" + strings.TrimPrefix(hs.URL, "http")
}

// dial connects a client speaking the Action Cable subprotocol.
func dial(t *testing.T, url string, header http.Header) *ws.Conn {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	conn, err := ws.Dial(ctx, url, &ws.DialOptions{
		Subprotocols: []string{Subprotocol},
		Header:       header,
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

// readMessage reads and decodes one server message.
func readMessage(t *testing.T, conn *ws.Conn) serverMessage {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if typ != ws.MessageText {
		t.Fatalf("message type = %v, want text", typ)
	}

	var m serverMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("Unmarshal(%s) error = %v", data, err)
	}
	return m
}

func TestWelcomeIsFirstMessage(t *testing.T) {
	_, url := newTestServer(t, nil)
	conn := dial(t, url, nil)

	if got := readMessage(t, conn); got.Type != typeWelcome {
		t.Errorf("first message type = %q, want %q", got.Type, typeWelcome)
	}
}

func TestHeartbeatFollowsWelcome(t *testing.T) {
	_, url := newTestServer(t, nil)
	conn := dial(t, url, nil)

	if got := readMessage(t, conn); got.Type != typeWelcome {
		t.Fatalf("first message type = %q, want %q", got.Type, typeWelcome)
	}

	// Two pings, since the client treats two missed beats as a dead connection.
	for i := range 2 {
		got := readMessage(t, conn)
		if got.Type != typePing {
			t.Fatalf("message %d type = %q, want %q", i, got.Type, typePing)
		}

		var seconds int64
		if err := json.Unmarshal(got.Message, &seconds); err != nil {
			t.Fatalf("ping payload %s is not a number: %v", got.Message, err)
		}
		if delta := time.Since(time.Unix(seconds, 0)).Abs(); delta > time.Minute {
			t.Errorf("ping timestamp is %v away from now", delta)
		}
	}
}

// TestUnauthorizedIsAFrameNotAStatus pins the ordering that surprises people: the
// handshake succeeds first, so a rejected connection is told in a WebSocket frame
// rather than an HTTP status.
func TestUnauthorizedIsAFrameNotAStatus(t *testing.T) {
	_, url := newTestServer(t, &Options{
		Authenticate: func(*http.Request) (Identifiers, error) {
			return nil, errors.New("no credentials")
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// The upgrade itself must succeed.
	conn, err := ws.Dial(ctx, url, &ws.DialOptions{Subprotocols: []string{Subprotocol}})
	if err != nil {
		t.Fatalf("Dial() error = %v, want a successful handshake", err)
	}
	defer conn.CloseNow()

	got := readMessage(t, conn)
	if got.Type != typeDisconnect {
		t.Fatalf("message type = %q, want %q", got.Type, typeDisconnect)
	}
	if got.Reason != reasonUnauthorized {
		t.Errorf("reason = %q, want %q", got.Reason, reasonUnauthorized)
	}
	if got.Reconnect == nil || *got.Reconnect {
		t.Errorf("reconnect = %v, want false so the client stays down", got.Reconnect)
	}

	// The server then closes.
	if _, _, err := conn.Read(ctx); err == nil {
		t.Error("Read() after the disconnect message = nil error, want a close")
	}
}

func TestAuthenticatorReceivesRequestAndSetsIdentifiers(t *testing.T) {
	gotIDs := make(chan Identifiers, 1)

	_, url := newTestServer(t, &Options{
		Authenticate: func(r *http.Request) (Identifiers, error) {
			token := r.URL.Query().Get("token")
			if token != "secret" {
				return nil, errors.New("bad token")
			}
			ids := Identifiers{"current_user": r.Header.Get("X-User")}
			gotIDs <- ids
			return ids, nil
		},
	})

	conn := dial(t, url+"?token=secret", http.Header{"X-User": []string{"42"}})
	if got := readMessage(t, conn); got.Type != typeWelcome {
		t.Fatalf("first message type = %q, want %q", got.Type, typeWelcome)
	}

	select {
	case ids := <-gotIDs:
		if ids["current_user"] != "42" {
			t.Errorf("current_user = %q, want %q", ids["current_user"], "42")
		}
	case <-time.After(time.Second):
		t.Fatal("the authenticator was not called")
	}
}

// TestPageNotFound checks the response to a non-WebSocket request byte for byte,
// because it is what Rails returns and clients may match on it.
func TestPageNotFound(t *testing.T) {
	srv := New(quietOptions(nil))
	defer srv.Close()
	hs := httptest.NewServer(srv)
	defer hs.Close()

	resp, err := http.Get(hs.URL)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
	if got, want := resp.Header.Get("Content-Type"), "text/plain; charset=utf-8"; got != want {
		t.Errorf("Content-Type = %q, want %q", got, want)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got, want := string(body), "Page not found"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

func TestOriginRules(t *testing.T) {
	tests := []struct {
		name    string
		opts    *Options
		origin  string
		wantOK  bool
		sameAs  bool // send the server's own host as the Origin
		noQuery bool
	}{
		{name: "no origin header is allowed", wantOK: true},
		{name: "same origin is allowed", sameAs: true, wantOK: true},
		{name: "cross origin is refused", origin: "http://evil.example", wantOK: false},
		{
			name:   "configured pattern is allowed",
			opts:   &Options{AllowedOrigins: []string{"*.allowed.example"}},
			origin: "http://app.allowed.example",
			wantOK: true,
		},
		{
			name:   "pattern does not match other hosts",
			opts:   &Options{AllowedOrigins: []string{"*.allowed.example"}},
			origin: "http://app.other.example",
			wantOK: false,
		},
		{
			name:   "check can be disabled",
			opts:   &Options{DisableOriginCheck: true},
			origin: "http://evil.example",
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, url := newTestServer(t, tt.opts)

			header := http.Header{}
			switch {
			case tt.sameAs:
				header.Set("Origin", "http"+strings.TrimPrefix(url, "ws"))
			case tt.origin != "":
				header.Set("Origin", tt.origin)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, err := ws.Dial(ctx, url, &ws.DialOptions{
				Subprotocols: []string{Subprotocol},
				Header:       header,
			})
			if err == nil {
				conn.CloseNow()
			}

			if gotOK := err == nil; gotOK != tt.wantOK {
				t.Errorf("connected = %v (err %v), want %v", gotOK, err, tt.wantOK)
			}
		})
	}
}

// TestSubprotocolAlwaysActionCableV1JSON covers plan question 2: the Rails JS
// client offers extra subprotocols but only accepts actioncable-v1-json back, so
// that is what the server must answer regardless of what else was offered.
func TestSubprotocolAlwaysActionCableV1JSON(t *testing.T) {
	offers := [][]string{
		{Subprotocol},
		{Subprotocol, "custom-thing"},
		{"custom-thing", Subprotocol},
		{Subprotocol, subprotocolUnsupported},
	}

	_, url := newTestServer(t, nil)

	for _, offer := range offers {
		t.Run(strings.Join(offer, ","), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			conn, err := ws.Dial(ctx, url, &ws.DialOptions{Subprotocols: offer})
			if err != nil {
				t.Fatalf("Dial() error = %v", err)
			}
			defer conn.CloseNow()

			if got := conn.Subprotocol(); got != Subprotocol {
				t.Errorf("Subprotocol() = %q, want %q", got, Subprotocol)
			}
		})
	}
}

func TestConnectionRegistration(t *testing.T) {
	srv, url := newTestServer(t, nil)

	if got := srv.ConnectionCount(); got != 0 {
		t.Fatalf("ConnectionCount() = %d before any connection, want 0", got)
	}

	conn := dial(t, url, nil)
	if got := readMessage(t, conn); got.Type != typeWelcome {
		t.Fatalf("first message type = %q, want %q", got.Type, typeWelcome)
	}

	// Registration happens after the welcome is queued.
	waitFor(t, "connection to register", func() bool { return srv.ConnectionCount() == 1 })

	conn.CloseNow()
	waitFor(t, "connection to be removed", func() bool { return srv.ConnectionCount() == 0 })
}

func TestNonTextFrameIsIgnored(t *testing.T) {
	_, url := newTestServer(t, nil)
	conn := dial(t, url, nil)
	ctx := context.Background()

	if got := readMessage(t, conn); got.Type != typeWelcome {
		t.Fatalf("first message type = %q, want %q", got.Type, typeWelcome)
	}

	if err := conn.Write(ctx, ws.MessageBinary, []byte{0x01, 0x02}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// The connection survives: the next heartbeat still arrives.
	if got := readMessage(t, conn); got.Type != typePing {
		t.Errorf("message after a binary frame = %q, want %q", got.Type, typePing)
	}
}

func TestMalformedCommandDoesNotCloseTheConnection(t *testing.T) {
	_, url := newTestServer(t, nil)
	conn := dial(t, url, nil)
	ctx := context.Background()

	if got := readMessage(t, conn); got.Type != typeWelcome {
		t.Fatalf("first message type = %q, want %q", got.Type, typeWelcome)
	}

	for _, bad := range []string{`not json`, `{}`, `{"command":"nope","identifier":"x"}`, `{"command":"subscribe"}`} {
		if err := conn.Write(ctx, ws.MessageText, []byte(bad)); err != nil {
			t.Fatalf("Write(%s) error = %v", bad, err)
		}
	}

	// Rails logs and says nothing back, so the proof of survival is the next ping.
	if got := readMessage(t, conn); got.Type != typePing {
		t.Errorf("message after malformed commands = %q, want %q", got.Type, typePing)
	}
}

// wsPipe returns a connected client and server WebSocket pair, bypassing the
// cable server so a test can drive a Connection's internals directly.
func wsPipe(t *testing.T) (client, server *ws.Conn) {
	t.Helper()

	accepted := make(chan *ws.Conn, 1)
	done := make(chan struct{})

	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := ws.Accept(w, r, &ws.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		accepted <- conn
		<-done // hold the handler open for the connection's lifetime
	}))
	// Registered first so it runs last: the handler must return before Close.
	t.Cleanup(hs.Close)
	t.Cleanup(func() { close(done) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	client, err := ws.Dial(ctx, "ws"+strings.TrimPrefix(hs.URL, "http"), nil)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { client.CloseNow() })

	select {
	case server = <-accepted:
		t.Cleanup(func() { server.CloseNow() })
		return client, server
	case <-time.After(5 * time.Second):
		t.Fatal("the server never accepted the connection")
		return nil, nil
	}
}

// TestSlowClientIsDropped exercises the bounded send queue directly: a client
// that stops reading must be disconnected rather than buffered without limit,
// which is where this diverges from Rails deliberately.
func TestSlowClientIsDropped(t *testing.T) {
	srv := New(quietOptions(&Options{SendBuffer: 2}))
	defer srv.Close()

	_, server := wsPipe(t)

	// A real connection whose writer goroutine is deliberately not started, so
	// nothing drains the queue.
	c := newConnection(srv, server, httptest.NewRequest(http.MethodGet, "/cable", nil), nil, srv.opts.Logger)

	frame := []byte(`{"type":"ping","message":1}`)
	for range srv.opts.SendBuffer {
		c.transmit(frame)
	}
	select {
	case <-c.ctx.Done():
		t.Fatal("connection closed while the queue still had room")
	default:
	}

	// The frame that does not fit drops the connection.
	c.transmit(frame)
	select {
	case <-c.ctx.Done():
	case <-time.After(time.Second):
		t.Error("connection was not dropped when its send queue filled")
	}
}

func TestCloseDropsConnections(t *testing.T) {
	srv, url := newTestServer(t, nil)

	conn := dial(t, url, nil)
	if got := readMessage(t, conn); got.Type != typeWelcome {
		t.Fatalf("first message type = %q, want %q", got.Type, typeWelcome)
	}
	waitFor(t, "connection to register", func() bool { return srv.ConnectionCount() == 1 })

	if err := srv.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, _, err := conn.Read(ctx); err == nil {
		t.Error("Read() after Server.Close() = nil error, want a close")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	for range 3 {
		if err := srv.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}
}

// TestNoGoroutineLeak checks that finished connections leave nothing behind. It
// stands in for the goleak dependency, which is not worth taking for one assert.
func TestNoGoroutineLeak(t *testing.T) {
	srv, url := newTestServer(t, nil)

	// Establish and close a batch, twice, so the baseline excludes anything the
	// server or heartbeat starts on first use.
	exercise := func() {
		for range 5 {
			conn := dial(t, url, nil)
			if got := readMessage(t, conn); got.Type != typeWelcome {
				t.Fatalf("first message type = %q, want %q", got.Type, typeWelcome)
			}
			conn.CloseNow()
		}
		waitFor(t, "connections to drain", func() bool { return srv.ConnectionCount() == 0 })
	}

	exercise()
	waitForStableGoroutines(t)
	baseline := runtime.NumGoroutine()

	exercise()
	waitForStableGoroutines(t)

	if got := runtime.NumGoroutine(); got > baseline+2 {
		t.Errorf("goroutines = %d after a second batch, baseline %d", got, baseline)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitForStableGoroutines waits until the goroutine count stops changing, since
// they exit asynchronously after a connection closes.
func waitForStableGoroutines(t *testing.T) {
	t.Helper()

	previous := -1
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		if current := runtime.NumGoroutine(); current == previous {
			return
		} else {
			previous = current
		}
	}
}

// testLogger writes server logs into the test's output, and stops when the test
// ends.
//
// The stopping matters: a hijacked WebSocket connection is not one of the
// "outstanding requests" httptest.Server.Close waits for, so its handler can
// still be shutting down — and logging — after the test function has returned.
// Logging into a finished test is a panic and a data race.
func testLogger(t *testing.T) *slog.Logger {
	w := &testWriter{t: t}
	t.Cleanup(w.stop)
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct {
	mu      sync.Mutex
	t       *testing.T
	stopped bool
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.stopped {
		w.t.Logf("%s", p)
	}
	return len(p), nil
}

// stop runs as a cleanup, which is still inside the test, so anything logged up
// to that point is reported.
func (w *testWriter) stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopped = true
}
