package cable

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestShutdownTellsClientsToReconnect is what makes a rolling deploy survivable:
// the client learns why it is being disconnected and that it should come back,
// rather than seeing a socket break for no reason.
func TestShutdownTellsClientsToReconnect(t *testing.T) {
	srv, url := newTestServer(t, nil)
	conn := connect(t, url)
	waitFor(t, "connection to register", func() bool { return srv.ConnectionCount() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Shutdown does not wait for the client to read anything, so it returns while
	// the message is still in the socket's buffer.
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	got := nextNonPing(t, conn)
	if got.Type != typeDisconnect {
		t.Fatalf("message type = %q, want %q", got.Type, typeDisconnect)
	}
	if got.Reason != reasonServerRestart {
		t.Errorf("reason = %q, want %q", got.Reason, reasonServerRestart)
	}
	if got.Reconnect == nil || !*got.Reconnect {
		t.Errorf("reconnect = %v, want true so the client comes back", got.Reconnect)
	}

	// And then the connection really is gone.
	if _, _, err := conn.Read(ctx); err == nil {
		t.Error("Read() after the disconnect message = nil error, want a close")
	}
	if got := srv.ConnectionCount(); got != 0 {
		t.Errorf("ConnectionCount() = %d after Shutdown, want 0", got)
	}
}

// TestShutdownRefusesNewConnections: without this a shutdown could never finish,
// because arriving clients would keep it busy.
func TestShutdownRefusesNewConnections(t *testing.T) {
	srv := New(quietOptions(nil))
	hs := httptest.NewServer(srv)
	defer hs.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, hs.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got, want := strings.TrimSpace(string(body)), "Server shutting down"; got != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

// TestShutdownRespectsItsDeadline uses a connection that is registered but has no
// goroutines running, so nothing can finish it: a real client that has stopped
// reading looks the same from here, and must not hold a deployment open.
func TestShutdownRespectsItsDeadline(t *testing.T) {
	srv := New(quietOptions(nil))
	defer srv.Close()

	_, server := wsPipe(t)
	c := newConnection(srv, server, httptest.NewRequest(http.MethodGet, "/cable", nil), nil, srv.opts.Logger)
	if !srv.add(c) {
		t.Fatal("add() = false on a running server")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := srv.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Shutdown() took %v, want it bounded by the context", elapsed)
	}

	// The connection it gave up on is dropped rather than left running.
	select {
	case <-c.ctx.Done():
	case <-time.After(time.Second):
		t.Error("the connection was not dropped when the deadline passed")
	}
}

func TestShutdownIsIdempotentAndComposesWithClose(t *testing.T) {
	srv, url := newTestServer(t, nil)
	connect(t, url)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 2 {
		if err := srv.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	}
	// Shutdown then Close is what a process does when it has a hard deadline.
	if err := srv.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestShutdownClosesAnOwnedPubSub(t *testing.T) {
	srv := New(quietOptions(nil))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	if err := srv.Broadcast(ctx, "room_1", 1); !errors.Is(err, ErrPubSubClosed) {
		t.Errorf("Broadcast() after Shutdown = %v, want %v", err, ErrPubSubClosed)
	}
}

// TestShutdownEndsWithTheDisconnect: the disconnect is the last thing a client
// hears. A heartbeat still ticking, or a second disconnect from another shutdown
// path, would show up here.
func TestShutdownEndsWithTheDisconnect(t *testing.T) {
	srv, url := newTestServer(t, nil)
	conn := connect(t, url)

	// Read one ping first, so this cannot pass on a server whose heartbeat never
	// started.
	if got := readMessage(t, conn); got.Type != typePing {
		t.Fatalf("message type = %q, want %q", got.Type, typePing)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	if got := nextNonPing(t, conn); got.Type != typeDisconnect {
		t.Fatalf("message type = %q, want %q", got.Type, typeDisconnect)
	}
	if _, _, err := conn.Read(ctx); err == nil {
		t.Error("a message followed the disconnect, want the connection closed")
	}
}
