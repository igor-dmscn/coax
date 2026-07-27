package coax

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/igor-dmscn/coax-claude-impl/ws"
)

// namedChannel has no Perform: its actions are registered by name, which is the
// whole point of Handle. It records which handler ran.
type namedChannel struct {
	sub   *Subscription
	calls chan string
}

func (c *namedChannel) Subscribed(context.Context) error { return nil }
func (c *namedChannel) Unsubscribed(context.Context)     {}

func (c *namedChannel) Speak(_ context.Context, data json.RawMessage) error {
	send(c.calls, "speak "+string(data))
	return nil
}

func (c *namedChannel) Receive(_ context.Context, data json.RawMessage) error {
	send(c.calls, "receive "+string(data))
	return nil
}

func (c *namedChannel) Fail(context.Context, json.RawMessage) error {
	return errors.New("the handler failed")
}

// mixedChannel has both named actions and a Perform, to pin which one wins.
type mixedChannel struct{ calls chan string }

func (c *mixedChannel) Subscribed(context.Context) error { return nil }
func (c *mixedChannel) Unsubscribed(context.Context)     {}

func (c *mixedChannel) Named(context.Context, json.RawMessage) error {
	send(c.calls, "named handler")
	return nil
}

func (c *mixedChannel) Perform(_ context.Context, action string, _ json.RawMessage) error {
	send(c.calls, "perform "+action)
	return nil
}

// calls is a channel of what ran, buffered and never blocking, as in channel_test.
func newCalls() chan string { return make(chan string, 8) }

func expectCall(t *testing.T, calls chan string, want string) {
	t.Helper()

	select {
	case got := <-calls:
		if got != want {
			t.Errorf("ran %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("nothing ran, want %q", want)
	}
}

// recordingLogger captures log output so a test can assert on what was reported.
func recordingLogger() (*slog.Logger, func() string) {
	var mu sync.Mutex
	var buf bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&lockedWriter{mu: &mu, w: &buf}, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	return logger, func() string {
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

const namedIdentifier = `{"channel":"NamedChannel"}`

// newNamedServer registers NamedChannel with the given actions and connects a
// client that has an active subscription to it.
func newNamedServer(t *testing.T, o *Options, register func(*Registration[*namedChannel])) (chan string, *ws.Conn) {
	t.Helper()

	calls := newCalls()
	srv, url := newTestServer(t, o)

	registration := Handle(srv, "NamedChannel", func(s *Subscription) *namedChannel {
		return &namedChannel{sub: s, calls: calls}
	})
	if register != nil {
		register(registration)
	}

	conn := connect(t, url)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: namedIdentifier})
	if got := nextNonPing(t, conn); got.Type != typeConfirmSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeConfirmSubscription)
	}
	return calls, conn
}

// TestHandleRoutesNamedActions is the feature: a channel with no Perform, and one
// method per action.
func TestHandleRoutesNamedActions(t *testing.T) {
	calls, conn := newNamedServer(t, nil, func(r *Registration[*namedChannel]) {
		r.On("speak", (*namedChannel).Speak)
	})

	writeCommand(t, conn, clientCommand{
		Command:    commandMessage,
		Identifier: namedIdentifier,
		Data:       `{"action":"speak","body":"hi"}`,
	})

	// The whole payload reaches the handler, action key included, as with Perform.
	expectCall(t, calls, `speak {"action":"speak","body":"hi"}`)
}

// TestHandleRoutesTheDefaultAction: a client's send() carries no action, which
// arrives as "receive", so that is the name to register.
func TestHandleRoutesTheDefaultAction(t *testing.T) {
	calls, conn := newNamedServer(t, nil, func(r *Registration[*namedChannel]) {
		r.On(defaultAction, (*namedChannel).Receive)
	})

	writeCommand(t, conn, clientCommand{
		Command:    commandMessage,
		Identifier: namedIdentifier,
		Data:       `{"body":"no action here"}`,
	})

	expectCall(t, calls, `receive {"body":"no action here"}`)
}

// TestNamedActionWinsOverPerform: a channel may have both, and the specific one
// takes precedence.
func TestNamedActionWinsOverPerform(t *testing.T) {
	calls := newCalls()
	srv, url := newTestServer(t, nil)

	Handle(srv, "MixedChannel", func(*Subscription) *mixedChannel {
		return &mixedChannel{calls: calls}
	}).On("named", (*mixedChannel).Named)

	const identifier = `{"channel":"MixedChannel"}`
	conn := connect(t, url)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: identifier})
	if got := nextNonPing(t, conn); got.Type != typeConfirmSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeConfirmSubscription)
	}

	writeCommand(t, conn, clientCommand{
		Command: commandMessage, Identifier: identifier, Data: `{"action":"named"}`,
	})
	expectCall(t, calls, "named handler")

	// Anything unclaimed falls through to Perform, which is what lets a channel
	// register the actions it cares about and keep a catch-all.
	writeCommand(t, conn, clientCommand{
		Command: commandMessage, Identifier: identifier, Data: `{"action":"whatever"}`,
	})
	expectCall(t, calls, "perform whatever")
}

// TestUnknownActionIsReportedDistinctly is the operational reason this exists: a
// typo has to be distinguishable from a handler that failed.
func TestUnknownActionIsReportedDistinctly(t *testing.T) {
	logger, logged := recordingLogger()

	calls, conn := newNamedServer(t, &Options{Logger: logger}, func(r *Registration[*namedChannel]) {
		r.On("speak", (*namedChannel).Speak)
		r.On("fail", (*namedChannel).Fail)
	})

	// A typo: nothing handles it, and the channel has no Perform.
	writeCommand(t, conn, clientCommand{
		Command: commandMessage, Identifier: namedIdentifier, Data: `{"action":"speaak"}`,
	})
	// A handler that ran and failed.
	writeCommand(t, conn, clientCommand{
		Command: commandMessage, Identifier: namedIdentifier, Data: `{"action":"fail"}`,
	})
	// Proof both were processed, and that the client was told nothing either way.
	writeCommand(t, conn, clientCommand{
		Command: commandMessage, Identifier: namedIdentifier, Data: `{"action":"speak"}`,
	})
	expectCall(t, calls, `speak {"action":"speak"}`)
	expectOnlyHeartbeats(t, conn)

	output := logged()
	switch {
	case !strings.Contains(output, "unknown action"):
		t.Errorf("nothing reported an unknown action:\n%s", output)
	case !strings.Contains(output, `speaak`):
		t.Errorf("the report does not name the action:\n%s", output)
	case !strings.Contains(output, "registered: fail, speak"):
		t.Errorf("the report does not list what is registered:\n%s", output)
	case !strings.Contains(output, "action failed"):
		t.Errorf("a failing handler was not reported as a failure:\n%s", output)
	}
}

// TestUnknownActionOnAChannelWithNoActions covers Handle with no On at all: every
// action is unknown, and the connection survives.
func TestUnknownActionOnAChannelWithNoActions(t *testing.T) {
	logger, logged := recordingLogger()
	_, conn := newNamedServer(t, &Options{Logger: logger}, nil)

	writeCommand(t, conn, clientCommand{
		Command: commandMessage, Identifier: namedIdentifier, Data: `{"action":"speak"}`,
	})
	expectOnlyHeartbeats(t, conn)

	if output := logged(); !strings.Contains(output, "registered: none") {
		t.Errorf("want the report to say nothing is registered:\n%s", output)
	}
}

func TestRegistrationActions(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	registration := Handle(srv, "NamedChannel", func(s *Subscription) *namedChannel {
		return &namedChannel{sub: s}
	}).
		On("speak", (*namedChannel).Speak).
		On(defaultAction, (*namedChannel).Receive)

	if got, want := strings.Join(registration.Actions(), ","), "receive,speak"; got != want {
		t.Errorf("Actions() = %q, want %q", got, want)
	}
}

func TestOnPanics(t *testing.T) {
	tests := map[string]func(*Registration[*namedChannel]){
		"empty action": func(r *Registration[*namedChannel]) {
			r.On("", (*namedChannel).Speak)
		},
		"nil handler": func(r *Registration[*namedChannel]) {
			r.On("speak", nil)
		},
		"duplicate action": func(r *Registration[*namedChannel]) {
			r.On("speak", (*namedChannel).Speak).On("speak", (*namedChannel).Receive)
		},
	}

	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			srv, _ := newTestServer(t, nil)
			registration := Handle(srv, "NamedChannel", func(s *Subscription) *namedChannel {
				return &namedChannel{sub: s}
			})

			defer func() {
				if recover() == nil {
					t.Error("On did not panic")
				}
			}()
			call(registration)
		})
	}
}

// TestHandleSharesTheRegistryWithRegister: both go into the same map, so a name
// can only be claimed once however it was claimed.
func TestHandleSharesTheRegistryWithRegister(t *testing.T) {
	tests := map[string]func(*Server){
		// Register takes a Channel, so it needs a Perform: mixedChannel has one,
		// namedChannel deliberately does not.
		"Handle after Register": func(srv *Server) {
			srv.Register("Taken", func(*Subscription) Channel { return &mixedChannel{} })
			Handle(srv, "Taken", func(s *Subscription) *namedChannel {
				return &namedChannel{sub: s}
			})
		},
		"Handle twice": func(srv *Server) {
			factory := func(s *Subscription) *namedChannel { return &namedChannel{sub: s} }
			Handle(srv, "Taken", factory)
			Handle(srv, "Taken", factory)
		},
	}

	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			srv, _ := newTestServer(t, nil)

			defer func() {
				if recover() == nil {
					t.Error("the duplicate name did not panic")
				}
			}()
			call(srv)
		})
	}
}

// TestHandleRefusesANilChannel: a factory that returns nothing must still be
// caught, rather than a wrapper hiding it until something dereferences it.
func TestHandleRefusesANilChannel(t *testing.T) {
	logger, logged := recordingLogger()
	srv, url := newTestServer(t, &Options{Logger: logger})

	Handle(srv, "NamedChannel", func(*Subscription) *namedChannel {
		return nil
	}).On("speak", (*namedChannel).Speak)

	conn := connect(t, url)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: namedIdentifier})

	// Not confirmed, not rejected: silent, as with any subscription the server
	// cannot build.
	expectOnlyHeartbeats(t, conn)
	if output := logged(); !strings.Contains(output, "factory returned nil") {
		t.Errorf("want the nil channel reported:\n%s", output)
	}
}

// TestHandledChannelKeepsTheLifecycle: Handle wraps the channel, so Subscribed and
// Unsubscribed must still reach it.
func TestHandledChannelKeepsTheLifecycle(t *testing.T) {
	lifecycle := newCalls()
	srv, url := newTestServer(t, nil)

	Handle(srv, "LifecycleChannel", func(*Subscription) *lifecycleChannel {
		return &lifecycleChannel{calls: lifecycle}
	})

	const identifier = `{"channel":"LifecycleChannel"}`
	conn := connect(t, url)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: identifier})
	expectCall(t, lifecycle, "subscribed")

	if got := nextNonPing(t, conn); got.Type != typeConfirmSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeConfirmSubscription)
	}

	writeCommand(t, conn, clientCommand{Command: commandUnsubscribe, Identifier: identifier})
	expectCall(t, lifecycle, "unsubscribed")

	if got := srv.ConnectionCount(); got != 1 {
		t.Errorf("ConnectionCount() = %d, want the connection still there", got)
	}
}

type lifecycleChannel struct{ calls chan string }

func (c *lifecycleChannel) Subscribed(context.Context) error {
	send(c.calls, "subscribed")
	return nil
}

func (c *lifecycleChannel) Unsubscribed(context.Context) { send(c.calls, "unsubscribed") }

// TestHandledChannelRejection: an error from Subscribed still rejects, since the
// wrapper only forwards it.
func TestHandledChannelRejection(t *testing.T) {
	srv, url := newTestServer(t, nil)

	Handle(srv, "RejectingChannel", func(*Subscription) *rejectingChannel {
		return &rejectingChannel{}
	}).On("speak", (*rejectingChannel).Speak)

	const identifier = `{"channel":"RejectingChannel"}`
	conn := connect(t, url)
	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: identifier})

	if got := nextNonPing(t, conn); got.Type != typeRejectSubscription {
		t.Errorf("message type = %q, want %q", got.Type, typeRejectSubscription)
	}
}

type rejectingChannel struct{}

func (c *rejectingChannel) Subscribed(context.Context) error {
	return errors.New("not for you")
}

func (c *rejectingChannel) Unsubscribed(context.Context)                 {}
func (c *rejectingChannel) Speak(context.Context, json.RawMessage) error { return nil }

// The interfaces a handled channel satisfies, asserted at compile time. A channel
// registered through Handle needs no Perform; one registered through Register does.
var (
	_ Subscriber = (*namedChannel)(nil)
	_ Performer  = (*mixedChannel)(nil)
	_ Channel    = (*routed)(nil)
)
