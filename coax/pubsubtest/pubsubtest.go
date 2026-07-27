// Package pubsubtest is a conformance suite for coax.PubSub implementations.
//
// A backend is a small interface with a large number of ways to be subtly wrong:
// delivering to the wrong subscribers, deduplicating handlers that should each
// get a copy, leaking a subscription after unsubscribe, or acknowledging a
// subscription before the backend is really listening. Run exercises all of it,
// so a new adapter — in this repository or elsewhere — can prove itself against
// the same suite the built-in ones pass:
//
//	func TestConformance(t *testing.T) {
//		pubsubtest.Run(t, func(t *testing.T) coax.PubSub {
//			ps := myadapter.New(...)
//			t.Cleanup(func() { ps.Close() })
//			return ps
//		})
//	}
//
// Delivery may be asynchronous, so every assertion waits with a timeout rather
// than expecting a payload to have arrived by the time Broadcast returns.
package pubsubtest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/igor-dmscn/coax-claude-impl/coax"
)

// deliveryTimeout is how long an assertion waits for a payload that should
// arrive. Generous: it only costs time when a test is about to fail anyway.
const deliveryTimeout = 10 * time.Second

// quietPeriod is how long an assertion waits to be convinced that a payload that
// should not arrive really did not.
const quietPeriod = 250 * time.Millisecond

// Factory builds a PubSub for one subtest. It is called once per subtest and is
// responsible for cleaning up, normally with t.Cleanup.
type Factory func(*testing.T) coax.PubSub

// Run checks an implementation against the PubSub contract.
func Run(t *testing.T, newPubSub Factory) {
	t.Helper()

	tests := map[string]func(*testing.T, coax.PubSub){
		"Delivers":                  testDelivers,
		"DeliversImmediately":       testDeliversImmediately,
		"IsolatesBroadcastings":     testIsolatesBroadcastings,
		"DeliversToEverySubscriber": testDeliversToEverySubscriber,
		"KeepsPayloadsIntact":       testKeepsPayloadsIntact,
		"Unsubscribes":              testUnsubscribes,
		"UnsubscribeIsIdempotent":   testUnsubscribeIsIdempotent,
		"UnsubscribeLeavesSiblings": testUnsubscribeLeavesSiblings,
		"RefusesUseAfterClose":      testRefusesUseAfterClose,
		"SurvivesConcurrentUse":     testSurvivesConcurrentUse,
	}

	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			run(t, newPubSub(t))
		})
	}
}

func testDelivers(t *testing.T, ps coax.PubSub) {
	got := subscribe(t, ps, "room_1")
	broadcast(t, ps, "room_1", `{"body":"hello"}`)
	got.expect(t, `{"body":"hello"}`)
}

// testDeliversImmediately is the guarantee that lets a caller confirm a client
// the moment Subscribe returns: a payload broadcast in the very next statement
// must not be lost, which means Subscribe cannot return before the backend is
// really listening.
func testDeliversImmediately(t *testing.T, ps coax.PubSub) {
	got := subscribe(t, ps, "room_1")

	// No sleep, no retry: if Subscribe returned early, this payload is gone.
	broadcast(t, ps, "room_1", `1`)
	got.expect(t, `1`)
}

func testIsolatesBroadcastings(t *testing.T, ps coax.PubSub) {
	got := subscribe(t, ps, "room_1")
	other := subscribe(t, ps, "room_2")

	broadcast(t, ps, "room_2", `2`)

	other.expect(t, `2`)
	got.expectNothing(t)
}

// testDeliversToEverySubscriber pins that subscribers are counted, not
// deduplicated: two subscriptions on one broadcasting each get a copy, even with
// identical behaviour.
func testDeliversToEverySubscriber(t *testing.T, ps coax.PubSub) {
	first := subscribe(t, ps, "room_1")
	second := subscribe(t, ps, "room_1")

	broadcast(t, ps, "room_1", `1`)

	first.expect(t, `1`)
	second.expect(t, `1`)
}

// testKeepsPayloadsIntact: payloads are opaque bytes. JSON today, but an adapter
// that assumes text, re-encodes, or truncates at a NUL is broken.
func testKeepsPayloadsIntact(t *testing.T, ps coax.PubSub) {
	payloads := map[string]string{
		"empty":        "",
		"binary":       "\x00\x01\xff\xfe",
		"invalid utf8": "\xc3\x28",
		"newlines":     "a\r\nb\r\n",
		"large":        string(make([]byte, 1<<20)),
	}

	got := subscribe(t, ps, "room_1")
	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			broadcast(t, ps, "room_1", payload)
			got.expect(t, payload)
		})
	}
}

func testUnsubscribes(t *testing.T, ps coax.PubSub) {
	got := subscribe(t, ps, "room_1")
	got.unsubscribe()

	broadcast(t, ps, "room_1", `1`)
	got.expectNothing(t)
}

func testUnsubscribeIsIdempotent(t *testing.T, ps coax.PubSub) {
	got := subscribe(t, ps, "room_1")

	// Teardown paths race, so this happens for real.
	got.unsubscribe()
	got.unsubscribe()

	broadcast(t, ps, "room_1", `1`)
	got.expectNothing(t)
}

func testUnsubscribeLeavesSiblings(t *testing.T, ps coax.PubSub) {
	leaving := subscribe(t, ps, "room_1")
	staying := subscribe(t, ps, "room_1")

	leaving.unsubscribe()
	broadcast(t, ps, "room_1", `1`)

	staying.expect(t, `1`)
	leaving.expectNothing(t)
}

func testRefusesUseAfterClose(t *testing.T, ps coax.PubSub) {
	if err := ps.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	// Close is called from shutdown paths that may run twice.
	if err := ps.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}

	ctx := context.Background()
	if err := ps.Broadcast(ctx, "room_1", []byte(`1`)); !errors.Is(err, coax.ErrPubSubClosed) {
		t.Errorf("Broadcast() after Close = %v, want %v", err, coax.ErrPubSubClosed)
	}
	if _, err := ps.Subscribe(ctx, "room_1", func([]byte) {}); !errors.Is(err, coax.ErrPubSubClosed) {
		t.Errorf("Subscribe() after Close = %v, want %v", err, coax.ErrPubSubClosed)
	}
}

func testSurvivesConcurrentUse(t *testing.T, ps coax.PubSub) {
	ctx := context.Background()

	var wg sync.WaitGroup
	for worker := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := range 10 {
				// Distinct broadcastings per worker, so first- and
				// last-subscriber transitions happen constantly.
				name := "room_" + string(rune('a'+worker)) + string(rune('0'+round%10))

				unsubscribe, err := ps.Subscribe(ctx, name, func([]byte) {})
				if err != nil {
					t.Errorf("Subscribe() error = %v", err)
					return
				}
				if err := ps.Broadcast(ctx, name, []byte(`1`)); err != nil {
					t.Errorf("Broadcast() error = %v", err)
					return
				}
				unsubscribe()
			}
		}()
	}
	wg.Wait()
}

// subscription is one subscriber plus the payloads it has received.
type subscription struct {
	payloads    chan string
	unsubscribe func()
}

// subscribe registers a subscriber that records what it receives.
func subscribe(t *testing.T, ps coax.PubSub, broadcasting string) *subscription {
	t.Helper()

	s := &subscription{payloads: make(chan string, 64)}

	ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
	defer cancel()

	unsubscribe, err := ps.Subscribe(ctx, broadcasting, func(payload []byte) {
		select {
		case s.payloads <- string(payload):
		default: // a test that overflows this is not measuring what it thinks
		}
	})
	if err != nil {
		t.Fatalf("Subscribe(%q) error = %v", broadcasting, err)
	}
	if unsubscribe == nil {
		t.Fatalf("Subscribe(%q) returned a nil unsubscribe function", broadcasting)
	}

	s.unsubscribe = unsubscribe
	t.Cleanup(unsubscribe)
	return s
}

func broadcast(t *testing.T, ps coax.PubSub, broadcasting, payload string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), deliveryTimeout)
	defer cancel()

	if err := ps.Broadcast(ctx, broadcasting, []byte(payload)); err != nil {
		t.Fatalf("Broadcast(%q) error = %v", broadcasting, err)
	}
}

func (s *subscription) expect(t *testing.T, want string) {
	t.Helper()

	select {
	case got := <-s.payloads:
		if got != want {
			t.Errorf("received %q, want %q", truncate(got), truncate(want))
		}
	case <-time.After(deliveryTimeout):
		t.Fatalf("nothing received, want %q", truncate(want))
	}
}

func (s *subscription) expectNothing(t *testing.T) {
	t.Helper()

	select {
	case got := <-s.payloads:
		t.Errorf("received %q, want nothing", truncate(got))
	case <-time.After(quietPeriod):
	}
}

// truncate keeps a failure message readable when a payload is a megabyte long.
func truncate(s string) string {
	const max = 64
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
