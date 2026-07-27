package coax

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// collect returns a handler appending to a slice guarded for the race detector,
// plus a reader for it.
func collect() (Handler, func() []string) {
	var mu sync.Mutex
	var got []string

	return func(payload []byte) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, string(payload))
		}, func() []string {
			mu.Lock()
			defer mu.Unlock()
			return append([]string(nil), got...)
		}
}

func TestMemoryPubSubDelivers(t *testing.T) {
	ps := NewMemoryPubSub()
	defer ps.Close()
	ctx := context.Background()

	first, readFirst := collect()
	second, readSecond := collect()
	other, readOther := collect()

	for _, sub := range []struct {
		broadcasting string
		h            Handler
	}{
		{"room_1", first},
		{"room_1", second},
		{"room_2", other},
	} {
		if _, err := ps.Subscribe(ctx, sub.broadcasting, sub.h); err != nil {
			t.Fatalf("Subscribe(%q) error = %v", sub.broadcasting, err)
		}
	}

	if err := ps.Broadcast(ctx, "room_1", []byte(`{"body":"hi"}`)); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	// Every subscriber of the broadcasting, and only of that broadcasting.
	for name, read := range map[string]func() []string{"first": readFirst, "second": readSecond} {
		if got := read(); len(got) != 1 || got[0] != `{"body":"hi"}` {
			t.Errorf("%s received %v, want one payload", name, got)
		}
	}
	if got := readOther(); len(got) != 0 {
		t.Errorf("the room_2 subscriber received %v, want nothing", got)
	}
}

// TestMemoryPubSubIdenticalHandlers pins that subscribers are counted, not
// deduplicated: two subscriptions with the same behaviour each get a copy.
func TestMemoryPubSubIdenticalHandlers(t *testing.T) {
	ps := NewMemoryPubSub()
	defer ps.Close()
	ctx := context.Background()

	var calls atomic.Int64
	h := Handler(func([]byte) { calls.Add(1) })

	for range 2 {
		if _, err := ps.Subscribe(ctx, "room_1", h); err != nil {
			t.Fatalf("Subscribe() error = %v", err)
		}
	}
	if err := ps.Broadcast(ctx, "room_1", []byte(`1`)); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	if got := calls.Load(); got != 2 {
		t.Errorf("handler called %d times, want 2", got)
	}
}

func TestMemoryPubSubUnsubscribe(t *testing.T) {
	ps := NewMemoryPubSub()
	defer ps.Close()
	ctx := context.Background()

	h, read := collect()
	unsubscribe, err := ps.Subscribe(ctx, "room_1", h)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	// Calling it twice must be harmless: teardown paths can race.
	unsubscribe()
	unsubscribe()

	if err := ps.Broadcast(ctx, "room_1", []byte(`1`)); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}
	if got := read(); len(got) != 0 {
		t.Errorf("received %v after unsubscribing, want nothing", got)
	}

	// The last subscriber leaving takes the broadcasting with it, so a server
	// does not accumulate an entry per room it has ever served.
	if got := ps.broadcastingCount(); got != 0 {
		t.Errorf("broadcastingCount() = %d, want 0", got)
	}
}

func TestMemoryPubSubClose(t *testing.T) {
	ps := NewMemoryPubSub()
	ctx := context.Background()

	h, read := collect()
	unsubscribe, err := ps.Subscribe(ctx, "room_1", h)
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	for range 2 {
		if err := ps.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}

	if err := ps.Broadcast(ctx, "room_1", []byte(`1`)); !errors.Is(err, ErrPubSubClosed) {
		t.Errorf("Broadcast() after Close = %v, want %v", err, ErrPubSubClosed)
	}
	if _, err := ps.Subscribe(ctx, "room_1", h); !errors.Is(err, ErrPubSubClosed) {
		t.Errorf("Subscribe() after Close = %v, want %v", err, ErrPubSubClosed)
	}
	if got := read(); len(got) != 0 {
		t.Errorf("received %v after Close, want nothing", got)
	}

	// Unsubscribing after Close is what a connection tearing down does.
	unsubscribe()
}

func TestMemoryPubSubZeroValue(t *testing.T) {
	var ps MemoryPubSub
	ctx := context.Background()

	h, read := collect()
	if _, err := ps.Subscribe(ctx, "room_1", h); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if err := ps.Broadcast(ctx, "room_1", []byte(`1`)); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}
	if got := read(); len(got) != 1 {
		t.Errorf("received %v, want one payload", got)
	}
}

func TestMemoryPubSubRejectsNilHandler(t *testing.T) {
	ps := NewMemoryPubSub()
	defer ps.Close()

	if _, err := ps.Subscribe(context.Background(), "room_1", nil); err == nil {
		t.Error("Subscribe(nil) error = nil, want an error")
	}
}

func TestMemoryPubSubHonoursContext(t *testing.T) {
	ps := NewMemoryPubSub()
	defer ps.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	h, _ := collect()
	if _, err := ps.Subscribe(ctx, "room_1", h); !errors.Is(err, context.Canceled) {
		t.Errorf("Subscribe() with a cancelled context = %v, want %v", err, context.Canceled)
	}
	if err := ps.Broadcast(ctx, "room_1", []byte(`1`)); !errors.Is(err, context.Canceled) {
		t.Errorf("Broadcast() with a cancelled context = %v, want %v", err, context.Canceled)
	}
}

// TestMemoryPubSubConcurrentUse is here for the race detector: subscribing,
// unsubscribing and broadcasting all happen on different goroutines in a real
// server.
func TestMemoryPubSubConcurrentUse(t *testing.T) {
	ps := NewMemoryPubSub()
	defer ps.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				unsubscribe, err := ps.Subscribe(ctx, "room_1", func([]byte) {})
				if err != nil {
					t.Errorf("Subscribe() error = %v", err)
					return
				}
				if err := ps.Broadcast(ctx, "room_1", []byte(`1`)); err != nil {
					t.Errorf("Broadcast() error = %v", err)
					return
				}
				unsubscribe()
			}
		}()
	}
	wg.Wait()

	if got := ps.broadcastingCount(); got != 0 {
		t.Errorf("broadcastingCount() = %d, want 0", got)
	}
}
