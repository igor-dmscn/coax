package coax

import (
	"context"
	"errors"
	"sync"
)

// ErrPubSubClosed is returned by a PubSub whose Close has been called.
var ErrPubSubClosed = errors.New("coax: pub/sub is closed")

// Handler receives one payload published to a broadcasting. Payloads are JSON,
// and are forwarded to clients without being re-encoded.
//
// A Handler runs on whichever goroutine the adapter delivers on, shared with
// every other subscriber of that adapter, and it may run while the adapter holds
// a lock. So it must not block, and must not call back into the PubSub that
// invoked it. The framework's own handler only queues a frame on a buffered
// channel, which cannot block.
type Handler func(payload []byte)

// PubSub carries broadcasts from whoever publishes them to whichever processes
// have subscribers for them. It is the one extension point in this package: the
// in-memory implementation reaches one process, and a backend like Redis reaches
// every process sharing it, without either changing anything above.
//
// ← actioncable/lib/action_cable/subscription_adapter/base.rb
//
// Implementations must be safe for concurrent use.
type PubSub interface {
	// Broadcast publishes payload to every subscriber of broadcasting,
	// everywhere. It must not deliver to local subscribers in place of
	// publishing: a process must receive its own broadcasts back through the
	// backend, or two processes see different orderings.
	Broadcast(ctx context.Context, broadcasting string, payload []byte) error

	// Subscribe registers h for a broadcasting and returns a function that
	// removes it again.
	//
	// It must not return until the backend has confirmed the subscription, so
	// that a caller which subscribes and then reports success cannot lose
	// messages published in between. That guarantee is what lets this package
	// skip Rails' deferred-confirmation machinery entirely; see
	// docs/go-port-plan.md section 4.
	//
	// The returned unsubscribe function must be safe to call more than once, and
	// after Close.
	Subscribe(ctx context.Context, broadcasting string, h Handler) (unsubscribe func(), err error)

	// Close releases the adapter's resources. It is safe to call more than once.
	// Broadcast and Subscribe must return ErrPubSubClosed afterwards.
	Close() error
}

// The conformance suite in cable/pubsubtest checks every rule above; a new
// adapter should run it.

// MemoryPubSub delivers broadcasts within a single process. Its zero value is
// ready to use.
//
// It is the default, because it needs no infrastructure and is what a single
// server wants. It is also the one adapter that does not scale out: with more
// than one process, a broadcast reaches only the clients connected to the
// process that published it, and nothing reports the difference. Running more
// than one process means configuring a shared backend.
//
// ← actioncable/lib/action_cable/subscription_adapter/async.rb
type MemoryPubSub struct {
	mu     sync.RWMutex
	subs   map[string]map[*subscriber]struct{}
	closed bool
}

// subscriber wraps a Handler so that identical handlers are still distinct
// subscribers: two subscriptions streaming from one broadcasting must each get a
// copy, exactly as in Rails.
type subscriber struct{ handle Handler }

// NewMemoryPubSub returns a MemoryPubSub. Naming it at the call site is the
// point: it makes single-process delivery a choice rather than a surprise.
func NewMemoryPubSub() *MemoryPubSub { return &MemoryPubSub{} }

// Broadcast delivers payload to this process's subscribers of broadcasting.
func (m *MemoryPubSub) Broadcast(ctx context.Context, broadcasting string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrPubSubClosed
	}

	// Handlers run under the read lock, which is why Handler documents that it
	// must not call back in. It keeps delivery allocation-free: snapshotting the
	// subscriber set would allocate on every broadcast.
	for s := range m.subs[broadcasting] {
		s.handle(payload)
	}
	return nil
}

// Subscribe registers h for broadcasting. There is no backend to wait for, so it
// returns once the handler is live.
func (m *MemoryPubSub) Subscribe(ctx context.Context, broadcasting string, h Handler) (func(), error) {
	if h == nil {
		return nil, errors.New("coax: Subscribe with a nil handler")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrPubSubClosed
	}

	if m.subs == nil {
		m.subs = make(map[string]map[*subscriber]struct{})
	}
	subs := m.subs[broadcasting]
	if subs == nil {
		subs = make(map[*subscriber]struct{}, 1)
		m.subs[broadcasting] = subs
	}

	s := &subscriber{handle: h}
	subs[s] = struct{}{}
	return func() { m.remove(broadcasting, s) }, nil
}

// Close drops every subscription and refuses further use.
func (m *MemoryPubSub) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.subs = nil
	return nil
}

func (m *MemoryPubSub) remove(broadcasting string, s *subscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()

	subs := m.subs[broadcasting]
	delete(subs, s)

	// Dropping the last subscriber drops the broadcasting too, or a server
	// accumulates an empty map for every room it has ever served.
	// ← actioncable/lib/action_cable/subscription_adapter/subscriber_map.rb:26
	if len(subs) == 0 {
		delete(m.subs, broadcasting)
	}
}

// broadcastingCount reports how many broadcastings have subscribers, so tests
// can prove that ending a subscription leaves nothing behind.
func (m *MemoryPubSub) broadcastingCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.subs)
}
