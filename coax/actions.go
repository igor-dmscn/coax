package coax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// ErrUnknownAction reports an action with no registered handler and no Perform to
// fall back to. Dispatch logs it distinctly, so a client sending a typo looks
// different in the log from a handler that failed.
var ErrUnknownAction = errors.New("coax: unknown action")

// Subscriber is a Channel without Perform: what a channel needs when its actions
// are registered by name rather than switched on.
type Subscriber interface {
	Subscribed(ctx context.Context) error
	Unsubscribed(ctx context.Context)
}

// Performer handles actions that no registered handler claimed. A channel may
// implement it alongside named actions as a catch-all, or instead of them — which
// is what keeps a channel that dispatches its own actions working unchanged.
type Performer interface {
	Perform(ctx context.Context, action string, data json.RawMessage) error
}

// channelType constrains a channel: the two callbacks, plus comparable so a
// factory that returns nothing can be caught. Pointer types satisfy it, which is
// what a channel almost always is.
type channelType interface {
	comparable
	Subscriber
}

// actionFunc is a handler with its channel type erased.
type actionFunc func(Subscriber, context.Context, json.RawMessage) error

// Handle registers a channel whose actions are named rather than switched on:
//
//	coax.Handle(srv, "ChatChannel", newChatChannel).
//		On("speak", (*ChatChannel).Speak).
//		On("typing", (*ChatChannel).Typing)
//
// Such a channel needs no Perform: it is Subscribed, Unsubscribed, and one method
// per action. Handle is a function rather than a method on Server because Go does
// not allow type parameters on methods, and the type parameter is what lets On
// check its handlers at compile time.
//
// Register remains the way to register a channel that dispatches its own actions.
// Both end up in the same registry, and both panic on a duplicate channel name.
//
// ← actioncable/lib/action_cable/channel/base.rb:287 (perform_action), which does
// this by reflecting over public methods
func Handle[T channelType](srv *Server, name string, factory func(*Subscription) T) *Registration[T] {
	actions := make(map[string]actionFunc)

	// The closure captures actions while it is still empty, so On can fill it in
	// afterwards and the channel registry needs to know nothing about actions.
	srv.Register(name, func(s *Subscription) Channel {
		var absent T
		inner := factory(s)
		if inner == absent {
			// Reported as no channel at all, so the registry's own nil handling
			// applies rather than a wrapper hiding it.
			return nil
		}
		return &routed{inner: inner, actions: actions}
	})

	return &Registration[T]{name: name, actions: actions}
}

// Registration collects the actions a channel handles.
//
// Call On during start-up, before serving traffic: the handlers are read by
// connection goroutines without a lock, which is the same assumption the channel
// registry itself documents.
type Registration[T channelType] struct {
	name    string
	actions map[string]actionFunc
}

// On routes one action name to one method.
//
// The handler is written as a method expression — (*ChatChannel).Speak — so a
// renamed or misspelled method is a compile error rather than an action that
// silently never fires. A message with no action arrives as "receive", so that is
// the name to register for a client's send().
//
// On panics on a duplicate action, an empty name or a nil handler, all of which are
// start-up mistakes.
func (r *Registration[T]) On(action string, h func(T, context.Context, json.RawMessage) error) *Registration[T] {
	switch {
	case action == "":
		panic("coax: On with an empty action name")
	case h == nil:
		panic("coax: On(" + action + ") with a nil handler")
	}
	if _, dup := r.actions[action]; dup {
		panic("coax: " + r.name + " already handles the action " + action)
	}

	// One interface-to-concrete assertion per call, and no reflection.
	r.actions[action] = func(c Subscriber, ctx context.Context, data json.RawMessage) error {
		return h(c.(T), ctx, data)
	}
	return r
}

// Actions lists the registered action names, sorted. Useful in a test asserting
// that a channel's surface is what it is meant to be.
func (r *Registration[T]) Actions() []string {
	return slices.Sorted(maps.Keys(r.actions))
}

// routed is the Channel the server actually holds: it forwards the lifecycle to
// the channel and turns Perform into a lookup.
type routed struct {
	inner   Subscriber
	actions map[string]actionFunc
}

func (r *routed) Subscribed(ctx context.Context) error { return r.inner.Subscribed(ctx) }
func (r *routed) Unsubscribed(ctx context.Context)     { r.inner.Unsubscribed(ctx) }

// Perform prefers a registered handler, falls back to the channel's own Perform,
// and otherwise reports that nothing handles this action — naming what does, since
// the usual cause is a typo on one side or the other.
func (r *routed) Perform(ctx context.Context, action string, data json.RawMessage) error {
	if h, ok := r.actions[action]; ok {
		return h(r.inner, ctx, data)
	}
	if p, ok := r.inner.(Performer); ok {
		return p.Perform(ctx, action, data)
	}

	registered := "none"
	if len(r.actions) > 0 {
		registered = strings.Join(slices.Sorted(maps.Keys(r.actions)), ", ")
	}
	return fmt.Errorf("%w %q; registered: %s", ErrUnknownAction, action, registered)
}
