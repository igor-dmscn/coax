package coax

import (
	"context"
	"encoding/json"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestPeriodicallyTransmits: N intervals produce N messages, which is the whole
// contract from a client's side.
func TestPeriodicallyTransmits(t *testing.T) {
	const interval = 20 * time.Millisecond

	r := newRecorder()
	var ticks atomic.Int64
	r.onSubscribed = func(_ context.Context, s *Subscription) error {
		return s.Periodically(interval, func(context.Context) error {
			ticks.Add(1)
			return s.Transmit(map[string]int64{"tick": ticks.Load()})
		})
	}

	// A heartbeat slow enough that the timer's messages are what arrives.
	_, conn := newChannelServer(t, "ClockChannel", r, &Options{HeartbeatInterval: time.Hour})
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: clockIdentifier})
	waitSubscribed(t, r)

	if got := readMessage(t, conn); got.Type != typeConfirmSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeConfirmSubscription)
	}

	for want := 1; want <= 3; want++ {
		got := readMessage(t, conn)
		if got.Type != "" {
			t.Fatalf("message type = %q, want a data message", got.Type)
		}

		var payload struct{ Tick int64 }
		if err := json.Unmarshal(got.Message, &payload); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", got.Message, err)
		}
		if payload.Tick != int64(want) {
			t.Errorf("tick = %d, want %d", payload.Tick, want)
		}
	}
}

// TestPeriodicallyWaitsForTheFirstInterval: firing immediately would surprise a
// channel that has just transmitted its initial state, and Rails does not.
func TestPeriodicallyWaitsForTheFirstInterval(t *testing.T) {
	r := newRecorder()
	var ticks atomic.Int64
	r.onSubscribed = func(_ context.Context, s *Subscription) error {
		return s.Periodically(time.Hour, func(context.Context) error {
			ticks.Add(1)
			return nil
		})
	}

	_, conn := newChannelServer(t, "ClockChannel", r, nil)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: clockIdentifier})
	waitSubscribed(t, r)
	if got := nextNonPing(t, conn); got.Type != typeConfirmSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeConfirmSubscription)
	}

	if got := ticks.Load(); got != 0 {
		t.Errorf("the timer fired %d times before its first interval", got)
	}
}

func TestPeriodicallyRejectsBadArguments(t *testing.T) {
	tests := map[string]func(*Subscription) error{
		"zero interval": func(s *Subscription) error { return s.Periodically(0, func(context.Context) error { return nil }) },
		"negative interval": func(s *Subscription) error {
			return s.Periodically(-time.Second, func(context.Context) error { return nil })
		},
		"nil function": func(s *Subscription) error { return s.Periodically(time.Second, nil) },
	}

	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			r := newRecorder()
			r.onSubscribed = func(_ context.Context, s *Subscription) error { return call(s) }

			_, conn := newChannelServer(t, "ClockChannel", r, nil)
			writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: clockIdentifier})
			waitSubscribed(t, r)

			// The error propagates out of Subscribed, so the subscription is
			// rejected rather than the process being taken down by a panic.
			if got := nextNonPing(t, conn); got.Type != typeRejectSubscription {
				t.Errorf("message type = %q, want %q", got.Type, typeRejectSubscription)
			}
		})
	}
}

// TestPeriodicallyKeepsRunningAfterAFailure: a tick that fails is not a reason to
// stop ticking, since the next one may well work.
func TestPeriodicallyKeepsRunningAfterAFailure(t *testing.T) {
	r := newRecorder()
	ticks := make(chan struct{}, 8)
	r.onSubscribed = func(_ context.Context, s *Subscription) error {
		return s.Periodically(10*time.Millisecond, func(context.Context) error {
			select {
			case ticks <- struct{}{}:
			default:
			}
			return errFailedTick
		})
	}

	_, conn := newChannelServer(t, "ClockChannel", r, nil)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: clockIdentifier})
	waitSubscribed(t, r)

	for range 3 {
		select {
		case <-ticks:
		case <-time.After(5 * time.Second):
			t.Fatal("the timer stopped after a failure")
		}
	}
	_ = conn
}

var errFailedTick = errorString("cable test: the tick failed")

type errorString string

func (e errorString) Error() string { return string(e) }

// TestPeriodicallyStopsOnUnsubscribe is the leak that matters: a timer is a
// goroutine, and a client that comes and goes all day must not leave a trail of
// them.
func TestPeriodicallyStopsOnUnsubscribe(t *testing.T) {
	r := newRecorder()
	var ticks atomic.Int64
	r.onSubscribed = func(_ context.Context, s *Subscription) error {
		return s.Periodically(5*time.Millisecond, func(context.Context) error {
			ticks.Add(1)
			return nil
		})
	}

	_, conn := newChannelServer(t, "ClockChannel", r, nil)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: clockIdentifier})
	waitSubscribed(t, r)

	waitFor(t, "the timer to fire", func() bool { return ticks.Load() > 0 })

	writeCommand(t, conn, clientCommand{Command: commandUnsubscribe, Identifier: clockIdentifier})
	select {
	case <-r.unsubscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("Unsubscribed was never called")
	}

	// Let several intervals pass, then check the count has settled.
	time.Sleep(50 * time.Millisecond)
	stopped := ticks.Load()
	time.Sleep(50 * time.Millisecond)

	if got := ticks.Load(); got != stopped {
		t.Errorf("the timer fired %d more times after unsubscribing", got-stopped)
	}
}

// TestPeriodicTimersLeaveNoGoroutines covers the whole lifecycle: many
// subscriptions, each with a timer, all going away.
func TestPeriodicTimersLeaveNoGoroutines(t *testing.T) {
	r := newRecorder()
	r.onSubscribed = func(_ context.Context, s *Subscription) error {
		return s.Periodically(5*time.Millisecond, func(context.Context) error { return nil })
	}

	_, url := newRegisteredServer(t, "ClockChannel", r, nil)

	exercise := func() {
		for i := range 5 {
			conn := connect(t, url)
			// Two timers per connection, on two subscriptions.
			for _, identifier := range []string{clockIdentifier, `{"channel":"ClockChannel","zone":"utc"}`} {
				writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: identifier})
				waitSubscribed(t, r)
			}
			if i%2 == 0 {
				// Half leave politely, half vanish: both must release the timers.
				writeCommand(t, conn, clientCommand{Command: commandUnsubscribe, Identifier: clockIdentifier})
			}
			conn.CloseNow()
		}
	}

	exercise()
	waitForStableGoroutines(t)
	baseline := runtime.NumGoroutine()

	exercise()
	waitForStableGoroutines(t)

	if got := runtime.NumGoroutine(); got > baseline+2 {
		t.Errorf("goroutines = %d after a second round, baseline %d", got, baseline)
	}
}

// TestPeriodicallyOutlivesNothing: a timer must not survive the connection even if
// the subscription is never unsubscribed, which is the normal case for a client
// that simply disappears.
func TestPeriodicallyStopsWithTheConnection(t *testing.T) {
	r := newRecorder()
	var ticks atomic.Int64
	r.onSubscribed = func(_ context.Context, s *Subscription) error {
		return s.Periodically(5*time.Millisecond, func(context.Context) error {
			ticks.Add(1)
			return nil
		})
	}

	srv, conn := newChannelServer(t, "ClockChannel", r, nil)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: clockIdentifier})
	waitSubscribed(t, r)
	waitFor(t, "the timer to fire", func() bool { return ticks.Load() > 0 })

	conn.CloseNow()
	waitFor(t, "the connection to go", func() bool { return srv.ConnectionCount() == 0 })

	time.Sleep(50 * time.Millisecond)
	stopped := ticks.Load()
	time.Sleep(50 * time.Millisecond)

	if got := ticks.Load(); got != stopped {
		t.Errorf("the timer fired %d more times after the connection closed", got-stopped)
	}
}

const clockIdentifier = `{"channel":"ClockChannel"}`
