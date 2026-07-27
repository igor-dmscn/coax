package coax

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Periodically calls f every interval for as long as the subscription lasts, and
// stops when it ends. It is how a channel pushes without being asked: a countdown,
// a presence heartbeat, a periodic refresh.
//
// Call it from Subscribed, whose error it composes with:
//
//	func (c *ClockChannel) Subscribed(ctx context.Context) error {
//		return c.sub.Periodically(time.Second, c.tick)
//	}
//
// The first call happens one interval from now, not immediately. An error from f
// is logged and the timer keeps running: a failed tick is not a reason to stop
// ticking. The context f receives is cancelled when the subscription ends, so it
// can be passed to whatever f does.
//
// # Which goroutine f runs on
//
// Its own, unlike the other Channel methods, which all run on the connection's
// reader in command order. So f must not touch state that Perform, Subscribed or
// Unsubscribed also touch without guarding it, and it must not open or close
// streams — StreamFrom and StopStream belong to the reader goroutine. Transmit is
// safe to call from anywhere, which is what a periodic timer normally wants.
//
// ← actioncable/lib/action_cable/channel/periodic_timers.rb
func (s *Subscription) Periodically(interval time.Duration, f func(context.Context) error) error {
	if interval <= 0 {
		return fmt.Errorf("coax: Periodically needs a positive interval, got %v", interval)
	}
	if f == nil {
		return errors.New("coax: Periodically needs a function")
	}

	// Derived from the connection, so a timer cannot outlive it even if the
	// subscription is never cleaned up.
	ctx, cancel := context.WithCancel(s.conn.ctx)
	s.stopTimers = append(s.stopTimers, cancel)

	go s.runTimer(ctx, interval, f)
	return nil
}

func (s *Subscription) runTimer(ctx context.Context, interval time.Duration, f func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := f(ctx); err != nil {
				s.conn.logger.Error("coax: periodic timer failed",
					"channel", s.channel, "interval", interval, "error", err)
			}
		}
	}
}

// stopPeriodicTimers ends every timer this subscription started.
// ← actioncable/lib/action_cable/channel/periodic_timers.rb:64
func (s *Subscription) stopPeriodicTimers() {
	for _, stop := range s.stopTimers {
		stop()
	}
	s.stopTimers = nil
}

// stop releases everything a subscription holds. Every path that ends one goes
// through here, so a new kind of resource is released everywhere at once rather
// than in three places that must be kept in step.
func (s *Subscription) stop() {
	s.stopPeriodicTimers()
	s.StopAllStreams()
}
