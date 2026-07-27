package cable

import (
	"context"
	"encoding/json"
	"fmt"
)

// StreamFrom forwards everything broadcast to a broadcasting on to this
// subscription's client, as a message its received() callback gets. A
// broadcasting is just a name both sides agree on, usually built from a model:
// "room_1", "user_42_notifications".
//
// Call it from Subscribed. It blocks until the pub/sub backend has confirmed the
// subscription, so a client is only ever confirmed once it really is listening —
// no message published after the confirmation can be lost. Streaming from the
// same broadcasting twice is a no-op.
//
// ← actioncable/lib/action_cable/channel/streams.rb:78 (stream_from)
func (s *Subscription) StreamFrom(ctx context.Context, broadcasting string) error {
	if s.streams[broadcasting] != nil {
		// Divergence: Rails registers a second handler and the client then
		// receives every message twice. Two separate subscriptions on one
		// broadcasting still each get a copy, which is the behaviour that
		// matters; streaming twice within one subscription is a mistake.
		return nil
	}

	unsubscribe, err := s.conn.server.opts.PubSub.Subscribe(ctx, broadcasting, s.forward)
	if err != nil {
		return fmt.Errorf("cable: %s streaming from %q: %w", s.channel, broadcasting, err)
	}

	if s.streams == nil {
		s.streams = make(map[string]func(), 1)
	}
	s.streams[broadcasting] = unsubscribe
	return nil
}

// StopStream stops forwarding one broadcasting. Streams are stopped
// automatically when the subscription ends, so this is only for a channel that
// changes what it listens to while running.
//
// ← actioncable/lib/action_cable/channel/streams.rb:104 (stop_stream_from)
func (s *Subscription) StopStream(broadcasting string) {
	unsubscribe := s.streams[broadcasting]
	if unsubscribe == nil {
		return
	}
	unsubscribe()
	delete(s.streams, broadcasting)
}

// StopAllStreams stops forwarding everything. It runs automatically after
// Unsubscribed, and after a rejected subscription, so a channel rarely calls it.
//
// ← actioncable/lib/action_cable/channel/streams.rb:112 (stop_all_streams)
func (s *Subscription) StopAllStreams() {
	for broadcasting, unsubscribe := range s.streams {
		unsubscribe()
		delete(s.streams, broadcasting)
	}
}

// forward hands a broadcast payload to the client, addressed to this
// subscription. The payload is already JSON and is passed through untouched.
//
// It runs on the pub/sub adapter's goroutine, so it must not block: transmit
// queues on a buffered channel and drops the connection rather than waiting.
// ← actioncable/lib/action_cable/channel/streams.rb:159 (default stream handler)
func (s *Subscription) forward(payload []byte) {
	s.conn.transmitMessage(newData(s.identifier, payload))
}

// Broadcast publishes v to every subscription streaming from broadcasting, in
// this process and in every other process sharing the pub/sub backend. It is how
// the rest of an application reaches connected clients:
//
//	srv.Broadcast(ctx, "room_1", map[string]string{"body": "hello"})
//
// It returns once the backend has accepted the publish, which says nothing about
// who received it: a broadcasting with no subscribers anywhere is not an error,
// it is the normal case for a quiet room.
//
// ← actioncable/lib/action_cable/server/broadcasting.rb:33
func (s *Server) Broadcast(ctx context.Context, broadcasting string, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("cable: broadcast to %q: %w", broadcasting, err)
	}
	return s.opts.PubSub.Broadcast(ctx, broadcasting, payload)
}
