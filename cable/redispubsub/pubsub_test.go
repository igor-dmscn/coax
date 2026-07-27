package redispubsub

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"testing"
	"time"

	"go-cable/cable"
	"go-cable/cable/pubsubtest"
)

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

// newTestPubSub returns an adapter pointed at a fake Redis, closed with the test.
func newTestPubSub(t *testing.T) (*PubSub, *fakeRedis) {
	t.Helper()

	f := newFakeRedis(t)
	ps := New(f.options())
	t.Cleanup(func() { ps.Close() })
	return ps, f
}

// received returns a handler that reports payloads on a channel, and the channel.
func received() (cable.Handler, chan string) {
	payloads := make(chan string, 16)
	return func(payload []byte) {
		select {
		case payloads <- string(payload):
		default:
		}
	}, payloads
}

func expect(t *testing.T, payloads chan string, want string) {
	t.Helper()

	select {
	case got := <-payloads:
		if got != want {
			t.Errorf("received %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("nothing received, want %q", want)
	}
}

func expectNothing(t *testing.T, payloads chan string) {
	t.Helper()

	select {
	case got := <-payloads:
		t.Errorf("received %q, want nothing", got)
	case <-time.After(200 * time.Millisecond):
	}
}

// subscribe subscribes with a short deadline, so a test fails rather than hangs.
func subscribe(t *testing.T, ps *PubSub, name string, h cable.Handler) func() {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	unsubscribe, err := ps.Subscribe(ctx, name, h)
	if err != nil {
		t.Fatalf("Subscribe(%q) error = %v", name, err)
	}
	return unsubscribe
}

func broadcast(t *testing.T, ps *PubSub, name, payload string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ps.Broadcast(ctx, name, []byte(payload)); err != nil {
		t.Fatalf("Broadcast(%q) error = %v", name, err)
	}
}

// TestConformance runs the shared PubSub suite, which is the same one the
// in-memory adapter passes.
func TestConformance(t *testing.T) {
	pubsubtest.Run(t, func(t *testing.T) cable.PubSub {
		ps, _ := newTestPubSub(t)
		return ps
	})
}

func TestReceivesPublishedMessages(t *testing.T) {
	ps, f := newTestPubSub(t)

	h, payloads := received()
	subscribe(t, ps, "room_1", h)

	// Pushed by the server rather than through Broadcast, so this tests the
	// receive path alone.
	f.push("room_1", []byte(`{"body":"hello"}`))
	expect(t, payloads, `{"body":"hello"}`)
}

// TestSubscribeWaitsForTheConfirmation is the guarantee the whole design rests
// on: Subscribe does not return until Redis says it is listening.
func TestSubscribeWaitsForTheConfirmation(t *testing.T) {
	f := newFakeRedis(t)
	f.holdSubscribe = make(chan struct{})

	ps := New(f.options())
	defer ps.Close()

	h, payloads := received()

	done := make(chan error, 1)
	go func() {
		_, err := ps.Subscribe(context.Background(), "room_1", h)
		done <- err
	}()

	// The confirmation is held, so Subscribe must still be blocked.
	f.waitForCommand("SUBSCRIBE room_1", 1)
	select {
	case err := <-done:
		t.Fatalf("Subscribe returned %v before Redis confirmed", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(f.holdSubscribe)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Subscribe() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe never returned after the confirmation")
	}

	f.push("room_1", []byte(`1`))
	expect(t, payloads, `1`)
}

// TestOneRedisSubscriptionPerBroadcasting: local subscribers are multiplexed onto
// a single Redis subscription, and it is only given up when the last one leaves.
func TestOneRedisSubscriptionPerBroadcasting(t *testing.T) {
	ps, f := newTestPubSub(t)

	first, firstPayloads := received()
	second, secondPayloads := received()

	stopFirst := subscribe(t, ps, "room_1", first)
	stopSecond := subscribe(t, ps, "room_1", second)

	if got := f.countCommand("SUBSCRIBE room_1"); got != 1 {
		t.Errorf("sent SUBSCRIBE %d times, want 1", got)
	}

	f.push("room_1", []byte(`1`))
	expect(t, firstPayloads, `1`)
	expect(t, secondPayloads, `1`)

	// One leaving must not take the other's subscription with it.
	stopFirst()
	if got := f.countCommand("UNSUBSCRIBE room_1"); got != 0 {
		t.Errorf("sent UNSUBSCRIBE %d times while a subscriber remained, want 0", got)
	}
	f.push("room_1", []byte(`2`))
	expect(t, secondPayloads, `2`)
	expectNothing(t, firstPayloads)

	stopSecond()
	f.waitForCommand("UNSUBSCRIBE room_1", 1)
}

// TestReconnectResubscribes is the failure that matters most in production: a
// Redis restart or failover must not silently leave clients subscribed to
// nothing.
func TestReconnectResubscribes(t *testing.T) {
	ps, f := newTestPubSub(t)

	h, payloads := received()
	subscribe(t, ps, "room_1", h)
	f.push("room_1", []byte(`before`))
	expect(t, payloads, `before`)

	f.killConns()

	// The subscription map is replayed onto the new connection.
	f.waitForCommand("SUBSCRIBE room_1", 2)

	// Wait for the new connection to be the one the fake pushes on.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.push("room_1", []byte(`after`)) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	expect(t, payloads, `after`)
}

// TestSubscribeSurvivesAReconnect covers the awkward case: the connection dies
// while a subscription is still waiting for its confirmation. It must keep
// waiting and be satisfied by the new connection, never returning as if it had
// succeeded.
func TestSubscribeSurvivesAReconnect(t *testing.T) {
	f := newFakeRedis(t)
	f.holdSubscribe = make(chan struct{})

	ps := New(f.options())
	defer ps.Close()

	h, payloads := received()

	done := make(chan error, 1)
	go func() {
		_, err := ps.Subscribe(context.Background(), "room_1", h)
		done <- err
	}()
	f.waitForCommand("SUBSCRIBE room_1", 1)

	// Kill the connection with the subscription still unconfirmed, then let the
	// next one through.
	f.killConns()
	close(f.holdSubscribe)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Subscribe() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe never returned after the reconnection")
	}

	// Confirmed means listening, on whichever connection ended up being used.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.push("room_1", []byte(`1`)) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	expect(t, payloads, `1`)
}

func TestSubscribeGivesUpWithTheContext(t *testing.T) {
	f := newFakeRedis(t)
	f.stopListening() // nothing to connect to

	ps := New(f.options())
	defer ps.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	h, _ := received()
	if _, err := ps.Subscribe(ctx, "room_1", h); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Subscribe() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestBroadcastPublishes(t *testing.T) {
	ps, f := newTestPubSub(t)

	broadcast(t, ps, "room_1", `{"body":"hello"}`)
	f.waitForCommand(`PUBLISH room_1 {"body":"hello"}`, 1)
}

// TestBroadcastRetriesAStaleConnection: Redis closes idle clients and load
// balancers drop quiet connections, so the first publish after a silent spell
// routinely lands on a dead socket. It must succeed on a fresh one rather than
// surfacing an error the caller cannot do anything about.
func TestBroadcastRetriesAStaleConnection(t *testing.T) {
	ps, f := newTestPubSub(t)

	broadcast(t, ps, "room_1", `first`)
	f.waitForCommand("PUBLISH room_1 first", 1)

	f.killConns()

	broadcast(t, ps, "room_1", `second`)
	f.waitForCommand("PUBLISH room_1 second", 1)
}

// TestBroadcastReportsRedisErrors also pins that an error *reply* is not retried:
// the server answered, so the connection is fine and sending it again would only
// produce the same refusal twice. Only a broken connection earns a second attempt.
func TestBroadcastReportsRedisErrors(t *testing.T) {
	f := newFakeRedis(t)
	f.failPublish = "READONLY You can't write against a read only replica."

	ps := New(f.options())
	defer ps.Close()

	err := ps.Broadcast(context.Background(), "room_1", []byte(`1`))

	var redisErr *Error
	if !errors.As(err, &redisErr) {
		t.Fatalf("Broadcast() error = %v, want a *redispubsub.Error", err)
	}
	if redisErr.Message != f.failPublish {
		t.Errorf("message = %q, want %q", redisErr.Message, f.failPublish)
	}
	if got := f.countCommand("PUBLISH room_1 1"); got != 1 {
		t.Errorf("published %d times, want 1: an error reply is not a retry", got)
	}
}

func TestAuthentication(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
		wantCmd  string
	}{
		{name: "password only sends the legacy form", password: "secret", wantCmd: "AUTH secret"},
		{name: "username and password send the ACL form", username: "alice", password: "secret", wantCmd: "AUTH alice secret"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeRedis(t)
			f.password = "secret"

			opts := f.options()
			opts.Username, opts.Password = tt.username, tt.password

			ps := New(opts)
			defer ps.Close()

			h, payloads := received()
			subscribe(t, ps, "room_1", h)
			f.waitForCommand(tt.wantCmd, 1)

			f.push("room_1", []byte(`1`))
			expect(t, payloads, `1`)
		})
	}
}

func TestAuthenticationFailureKeepsRetrying(t *testing.T) {
	f := newFakeRedis(t)
	f.password = "secret"

	opts := f.options()
	opts.Password = "wrong"

	ps := New(opts)
	defer ps.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	h, _ := received()
	if _, err := ps.Subscribe(ctx, "room_1", h); err == nil {
		t.Fatal("Subscribe() error = nil, want a failure")
	}

	// A rejected password is retried rather than latched, since the fix may be a
	// rotated credential on the server.
	if got := f.countCommand("AUTH wrong"); got < 2 {
		t.Errorf("tried AUTH %d times, want repeated attempts", got)
	}
}

func TestKeepalivePings(t *testing.T) {
	ps, f := newTestPubSub(t)

	h, _ := received()
	subscribe(t, ps, "room_1", h)

	// The fake's options use a 20ms interval.
	f.waitForCommand("PING", 2)
}

func TestCloseStopsEverything(t *testing.T) {
	f := newFakeRedis(t)
	ps := New(f.options())

	h, _ := received()
	subscribe(t, ps, "room_1", h)
	broadcast(t, ps, "room_1", `1`)

	before := runtime.NumGoroutine()

	for range 2 {
		if err := ps.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}

	// Both connections are gone, and so are the goroutines behind them.
	waitFor(t, "connections to close", func() bool { return f.connectionCount() == 0 })
	waitFor(t, "goroutines to exit", func() bool { return runtime.NumGoroutine() <= before-2 })

	ctx := context.Background()
	if err := ps.Broadcast(ctx, "room_1", []byte(`1`)); !errors.Is(err, cable.ErrPubSubClosed) {
		t.Errorf("Broadcast() after Close = %v, want %v", err, cable.ErrPubSubClosed)
	}
	if _, err := ps.Subscribe(ctx, "room_1", h); !errors.Is(err, cable.ErrPubSubClosed) {
		t.Errorf("Subscribe() after Close = %v, want %v", err, cable.ErrPubSubClosed)
	}
}

// TestCloseReleasesAPendingSubscribe: closing while a subscription is waiting for
// a confirmation must not leave the caller blocked forever.
func TestCloseReleasesAPendingSubscribe(t *testing.T) {
	f := newFakeRedis(t)
	f.holdSubscribe = make(chan struct{})

	ps := New(f.options())

	done := make(chan error, 1)
	go func() {
		h, _ := received()
		_, err := ps.Subscribe(context.Background(), "room_1", h)
		done <- err
	}()
	f.waitForCommand("SUBSCRIBE room_1", 1)

	go ps.Close()

	select {
	case err := <-done:
		if !errors.Is(err, cable.ErrPubSubClosed) {
			t.Errorf("Subscribe() error = %v, want %v", err, cable.ErrPubSubClosed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a pending Subscribe was not released by Close")
	}
	close(f.holdSubscribe)
}

func TestNilHandlerIsRejected(t *testing.T) {
	ps, _ := newTestPubSub(t)

	if _, err := ps.Subscribe(context.Background(), "room_1", nil); err == nil {
		t.Error("Subscribe(nil) error = nil, want an error")
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
