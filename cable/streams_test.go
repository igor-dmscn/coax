package cable

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"go-cable/ws"
)

// streamFromRoom is a Subscribed hook that streams from the broadcasting named
// in the subscription's own params, so a test drives what it listens to from the
// client side.
func streamFromRoom(ctx context.Context, s *Subscription) error {
	var params struct {
		Room string `json:"room"`
	}
	if err := json.Unmarshal(s.Params(), &params); err != nil {
		return err
	}
	return s.StreamFrom(ctx, params.Room)
}

// subscribeAndConfirm sends a subscribe command and waits for the confirmation.
func subscribeAndConfirm(t *testing.T, conn *ws.Conn, r *recorder, identifier string) *Subscription {
	t.Helper()

	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: identifier})
	sub := waitSubscribed(t, r)

	got := nextNonPing(t, conn)
	if got.Type != typeConfirmSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeConfirmSubscription)
	}
	if got.Identifier != identifier {
		t.Fatalf("confirmed identifier = %q, want %q", got.Identifier, identifier)
	}
	return sub
}

// newStreamingServer starts a server whose channel streams from the room in each
// subscription's params, sharing one MemoryPubSub the test can inspect.
func newStreamingServer(t *testing.T, r *recorder) (*Server, string, *MemoryPubSub) {
	t.Helper()

	r.onSubscribed = streamFromRoom
	ps := NewMemoryPubSub()
	t.Cleanup(func() { ps.Close() })

	srv, url := newRegisteredServer(t, "ChatChannel", r, &Options{PubSub: ps})
	return srv, url, ps
}

func TestBroadcastReachesEveryStreamingClient(t *testing.T) {
	r := newRecorder()
	srv, url, _ := newStreamingServer(t, r)

	first, second := connect(t, url), connect(t, url)
	subscribeAndConfirm(t, first, r, chatIdentifier)
	subscribeAndConfirm(t, second, r, chatIdentifier)

	body := map[string]string{"body": "hello"}
	if err := srv.Broadcast(context.Background(), "1", body); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	for name, conn := range map[string]*ws.Conn{"first": first, "second": second} {
		got := nextNonPing(t, conn)
		if got.Type != "" {
			t.Errorf("%s: type = %q, want empty so the client routes it to received()", name, got.Type)
		}
		if got.Identifier != chatIdentifier {
			t.Errorf("%s: identifier = %q, want %q", name, got.Identifier, chatIdentifier)
		}
		if want := `{"body":"hello"}`; string(got.Message) != want {
			t.Errorf("%s: message = %s, want %s", name, got.Message, want)
		}
	}
}

func TestBroadcastReachesOnlyItsBroadcasting(t *testing.T) {
	r := newRecorder()
	srv, url, _ := newStreamingServer(t, r)

	conn := connect(t, url)
	subscribeAndConfirm(t, conn, r, chatIdentifier) // room "1"

	if err := srv.Broadcast(context.Background(), "2", map[string]string{"body": "elsewhere"}); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	expectOnlyHeartbeats(t, conn)
}

// TestTwoSubscriptionsOnOneBroadcasting matches Rails: subscribers are counted,
// not deduplicated, so a client subscribed twice to the same room is told twice —
// once per subscription.
func TestTwoSubscriptionsOnOneBroadcasting(t *testing.T) {
	r := newRecorder()
	srv, url, ps := newStreamingServer(t, r)

	// Two identifiers, same room: two subscriptions on one connection.
	const (
		asMember = `{"channel":"ChatChannel","room":"1","as":"member"}`
		asAdmin  = `{"channel":"ChatChannel","room":"1","as":"admin"}`
	)

	conn := connect(t, url)
	subscribeAndConfirm(t, conn, r, asMember)
	subscribeAndConfirm(t, conn, r, asAdmin)

	if got := ps.broadcastingCount(); got != 1 {
		t.Fatalf("broadcastingCount() = %d, want 1", got)
	}

	if err := srv.Broadcast(context.Background(), "1", map[string]string{"body": "hello"}); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	seen := map[string]int{}
	for range 2 {
		seen[nextNonPing(t, conn).Identifier]++
	}
	if seen[asMember] != 1 || seen[asAdmin] != 1 {
		t.Errorf("deliveries = %v, want one per subscription", seen)
	}
}

func TestStopStream(t *testing.T) {
	// StopStream belongs to the channel, so it runs on the reader goroutine; here
	// Perform stands in for a real "leave" action. Every hook is set before the
	// client connects, so nothing but the socket crosses goroutines.
	r := newRecorder()
	stopped := make(chan struct{}, 1)
	r.onPerform = func(s *Subscription, _ string, _ json.RawMessage) error {
		s.StopStream("1")
		stopped <- struct{}{}
		return nil
	}

	srv, url, ps := newStreamingServer(t, r)

	conn := connect(t, url)
	sub := subscribeAndConfirm(t, conn, r, chatIdentifier)

	writeCommand(t, conn, clientCommand{
		Command:    commandMessage,
		Identifier: chatIdentifier,
		Data:       `{"action":"leave"}`,
	})
	<-stopped

	if got := ps.broadcastingCount(); got != 0 {
		t.Errorf("broadcastingCount() = %d after StopStream, want 0", got)
	}
	if err := srv.Broadcast(context.Background(), "1", map[string]string{"body": "hello"}); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}
	expectOnlyHeartbeats(t, conn)

	// Stopping a stream that is not running is harmless.
	sub.StopStream("nope")
}

func TestUnsubscribeStopsStreams(t *testing.T) {
	r := newRecorder()
	srv, url, ps := newStreamingServer(t, r)

	conn := connect(t, url)
	subscribeAndConfirm(t, conn, r, chatIdentifier)

	writeCommand(t, conn, clientCommand{Command: commandUnsubscribe, Identifier: chatIdentifier})
	select {
	case <-r.unsubscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("Unsubscribed was never called")
	}

	if got := ps.broadcastingCount(); got != 0 {
		t.Errorf("broadcastingCount() = %d after unsubscribing, want 0", got)
	}
	if err := srv.Broadcast(context.Background(), "1", map[string]string{"body": "hello"}); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}
	expectOnlyHeartbeats(t, conn)
}

// TestClosingConnectionStopsStreams is the leak that matters in production:
// clients vanish without unsubscribing, and the backend must not keep their
// streams.
func TestClosingConnectionStopsStreams(t *testing.T) {
	r := newRecorder()
	_, url, ps := newStreamingServer(t, r)

	conn := connect(t, url)
	subscribeAndConfirm(t, conn, r, chatIdentifier)
	if got := ps.broadcastingCount(); got != 1 {
		t.Fatalf("broadcastingCount() = %d, want 1", got)
	}

	conn.CloseNow()

	waitFor(t, "streams to stop", func() bool { return ps.broadcastingCount() == 0 })
}

// TestRejectedSubscriptionStopsStreams covers a channel that opens a stream and
// then decides to reject: the client is not subscribed, so nothing may be left
// listening on its behalf.
func TestRejectedSubscriptionStopsStreams(t *testing.T) {
	r := newRecorder()
	_, url, ps := newStreamingServer(t, r)

	r.onSubscribed = func(ctx context.Context, s *Subscription) error {
		if err := s.StreamFrom(ctx, "1"); err != nil {
			return err
		}
		return errors.New("changed my mind")
	}

	conn := connect(t, url)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: chatIdentifier})
	waitSubscribed(t, r)

	if got := nextNonPing(t, conn); got.Type != typeRejectSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeRejectSubscription)
	}
	if got := ps.broadcastingCount(); got != 0 {
		t.Errorf("broadcastingCount() = %d after a rejection, want 0", got)
	}
}

// blockingPubSub holds Subscribe until released, so a test can watch what the
// server does while the backend has not acknowledged yet.
type blockingPubSub struct {
	*MemoryPubSub
	release chan struct{}
}

func (p blockingPubSub) Subscribe(ctx context.Context, broadcasting string, h Handler) (func(), error) {
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return p.MemoryPubSub.Subscribe(ctx, broadcasting, h)
}

// TestConfirmationWaitsForTheBackend is the whole point of making Subscribe
// blocking (plan section 4): the confirmation cannot go out before the backend is
// listening, so a broadcast published the instant a client sees it cannot be
// lost. Rails needs a counter and a deferred callback for this.
func TestConfirmationWaitsForTheBackend(t *testing.T) {
	r := newRecorder()
	r.onSubscribed = streamFromRoom

	ps := blockingPubSub{MemoryPubSub: NewMemoryPubSub(), release: make(chan struct{})}
	defer ps.Close()

	srv, url := newRegisteredServer(t, "ChatChannel", r, &Options{PubSub: ps})

	conn := connect(t, url)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: chatIdentifier})
	waitSubscribed(t, r)

	// The backend has not acknowledged, so there is no confirmation yet.
	expectOnlyHeartbeats(t, conn)

	close(ps.release)

	if got := nextNonPing(t, conn); got.Type != typeConfirmSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeConfirmSubscription)
	}

	// Confirmed means listening: a broadcast right now arrives.
	if err := srv.Broadcast(context.Background(), "1", map[string]string{"body": "hello"}); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}
	if got := nextNonPing(t, conn); string(got.Message) != `{"body":"hello"}` {
		t.Errorf("message = %s, want the broadcast", got.Message)
	}
}

// failingPubSub is a backend that cannot subscribe.
type failingPubSub struct{ *MemoryPubSub }

func (failingPubSub) Subscribe(context.Context, string, Handler) (func(), error) {
	return nil, errors.New("backend unavailable")
}

// TestStreamFailureRejectsSubscription: a channel that cannot open its stream
// returns the error, which rejects. Better than confirming a client that would
// then silently receive nothing.
func TestStreamFailureRejectsSubscription(t *testing.T) {
	r := newRecorder()
	r.onSubscribed = streamFromRoom

	ps := failingPubSub{MemoryPubSub: NewMemoryPubSub()}
	defer ps.Close()

	_, url := newRegisteredServer(t, "ChatChannel", r, &Options{PubSub: ps})

	conn := connect(t, url)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: chatIdentifier})
	waitSubscribed(t, r)

	if got := nextNonPing(t, conn); got.Type != typeRejectSubscription {
		t.Errorf("message type = %q, want %q", got.Type, typeRejectSubscription)
	}
}

func TestStreamFromTwiceIsANoop(t *testing.T) {
	r := newRecorder()
	srv, url, ps := newStreamingServer(t, r)

	r.onSubscribed = func(ctx context.Context, s *Subscription) error {
		if err := s.StreamFrom(ctx, "1"); err != nil {
			return err
		}
		return s.StreamFrom(ctx, "1")
	}

	conn := connect(t, url)
	subscribeAndConfirm(t, conn, r, chatIdentifier)

	if got := ps.broadcastingCount(); got != 1 {
		t.Fatalf("broadcastingCount() = %d, want 1", got)
	}
	if err := srv.Broadcast(context.Background(), "1", map[string]string{"body": "hello"}); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	// One delivery, not two: streaming twice within a subscription is ignored.
	nextNonPing(t, conn)
	expectOnlyHeartbeats(t, conn)
}

func TestBroadcastReportsEncodingFailures(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	if err := srv.Broadcast(context.Background(), "1", math.Inf(1)); err == nil {
		t.Error("Broadcast() with an unencodable value = nil error, want an error")
	}
}

// TestServerClosesOnlyItsOwnPubSub: a backend the caller supplied may be shared
// with other servers, so closing it is not the server's business.
func TestServerClosesOnlyItsOwnPubSub(t *testing.T) {
	supplied := NewMemoryPubSub()
	defer supplied.Close()

	srv := New(quietOptions(&Options{PubSub: supplied}))
	if err := srv.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := supplied.Broadcast(context.Background(), "1", []byte(`1`)); err != nil {
		t.Errorf("the supplied PubSub was closed with the server: %v", err)
	}

	// The default one is created here, so it is closed here.
	owned := New(quietOptions(nil))
	if err := owned.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := owned.Broadcast(context.Background(), "1", 1); !errors.Is(err, ErrPubSubClosed) {
		t.Errorf("Broadcast() after Close = %v, want %v", err, ErrPubSubClosed)
	}
}
