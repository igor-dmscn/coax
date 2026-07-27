package coax

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestInternalChannelFor(t *testing.T) {
	tests := []struct {
		name string
		ids  Identifiers
		want string
	}{
		{name: "no identifiers cannot be addressed", ids: nil, want: ""},
		{name: "empty identifiers cannot be addressed", ids: Identifiers{}, want: ""},
		{
			name: "one identifier",
			ids:  Identifiers{"current_user": "42"},
			want: "action_cable/current_user=42",
		},
		{
			// Sorted by key, so every process derives the same name from the same
			// identifiers however the map was built.
			name: "several identifiers are sorted",
			ids:  Identifiers{"tenant": "acme", "current_user": "42"},
			want: "action_cable/current_user=42:tenant=acme",
		},
		{
			// Keys are included, unlike Rails, whose values are GlobalIDs that
			// carry their model class. Without them these two would collide.
			name: "keys distinguish equal values",
			ids:  Identifiers{"room": "42"},
			want: "action_cable/room=42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := internalChannelFor(tt.ids); got != tt.want {
				t.Errorf("internalChannelFor(%v) = %q, want %q", tt.ids, got, tt.want)
			}
		})
	}
}

func TestInternalChannelIsOrderIndependent(t *testing.T) {
	// Built in different orders, so the map iteration order differs between them.
	first := Identifiers{"a": "1", "b": "2", "c": "3"}
	second := Identifiers{"c": "3", "b": "2", "a": "1"}

	if internalChannelFor(first) != internalChannelFor(second) {
		t.Errorf("%q != %q", internalChannelFor(first), internalChannelFor(second))
	}
}

// identifiedServer starts a server whose connections all carry the given
// identifiers, sharing a pub/sub backend with any other server given the same one.
func identifiedServer(t *testing.T, ps PubSub, ids Identifiers) (*Server, string) {
	t.Helper()

	return newTestServer(t, &Options{
		PubSub: ps,
		Authenticate: func(*http.Request) (Identifiers, error) {
			return ids, nil
		},
	})
}

// TestRemoteDisconnect is the point of the internal channel: the server holding a
// connection and the server asked to drop it are different processes, with no path
// between them except the pub/sub backend.
func TestRemoteDisconnect(t *testing.T) {
	tests := []struct {
		name      string
		reconnect bool
	}{
		{name: "may come back", reconnect: true},
		{name: "must stay down", reconnect: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ps := NewMemoryPubSub()
			t.Cleanup(func() { ps.Close() })

			ids := Identifiers{"current_user": "42"}
			holder, url := identifiedServer(t, ps, ids)
			other, _ := identifiedServer(t, ps, ids)

			conn := connect(t, url)
			waitFor(t, "connection to register", func() bool { return holder.ConnectionCount() == 1 })

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Asked of the server that has never seen this client.
			if err := other.Disconnect(ctx, ids, tt.reconnect); err != nil {
				t.Fatalf("Disconnect() error = %v", err)
			}

			got := nextNonPing(t, conn)
			if got.Type != typeDisconnect {
				t.Fatalf("message type = %q, want %q", got.Type, typeDisconnect)
			}
			if got.Reason != reasonRemote {
				t.Errorf("reason = %q, want %q", got.Reason, reasonRemote)
			}
			if got.Reconnect == nil || *got.Reconnect != tt.reconnect {
				t.Errorf("reconnect = %v, want %v", got.Reconnect, tt.reconnect)
			}

			waitFor(t, "connection to go", func() bool { return holder.ConnectionCount() == 0 })
		})
	}
}

func TestRemoteDisconnectIgnoresOtherIdentities(t *testing.T) {
	ps := NewMemoryPubSub()
	t.Cleanup(func() { ps.Close() })

	srv, url := identifiedServer(t, ps, Identifiers{"current_user": "42"})
	conn := connect(t, url)
	waitFor(t, "connection to register", func() bool { return srv.ConnectionCount() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// A different user, and the same user with an extra identifier: both address a
	// different connection, so neither may touch this one.
	for _, ids := range []Identifiers{
		{"current_user": "43"},
		{"current_user": "42", "tenant": "acme"},
	} {
		if err := srv.Disconnect(ctx, ids, false); err != nil {
			t.Fatalf("Disconnect(%v) error = %v", ids, err)
		}
	}

	expectOnlyHeartbeats(t, conn)
	if got := srv.ConnectionCount(); got != 1 {
		t.Errorf("ConnectionCount() = %d, want the connection still there", got)
	}
}

func TestDisconnectNeedsIdentifiers(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	if err := srv.Disconnect(context.Background(), nil, false); err == nil {
		t.Error("Disconnect(nil) error = nil, want an error rather than disconnecting everyone")
	}
}

// TestRemoteDisconnectDefaultsToReconnect covers a message published by something
// other than this package — a Rails-shaped payload, or a hand-written one — which
// may leave the field out.
func TestRemoteDisconnectDefaultsToReconnect(t *testing.T) {
	ps := NewMemoryPubSub()
	t.Cleanup(func() { ps.Close() })

	ids := Identifiers{"current_user": "42"}
	srv, url := identifiedServer(t, ps, ids)

	conn := connect(t, url)
	waitFor(t, "connection to register", func() bool { return srv.ConnectionCount() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ps.Broadcast(ctx, internalChannelFor(ids), []byte(`{"type":"disconnect"}`)); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	got := nextNonPing(t, conn)
	if got.Type != typeDisconnect {
		t.Fatalf("message type = %q, want %q", got.Type, typeDisconnect)
	}
	if got.Reconnect == nil || !*got.Reconnect {
		t.Errorf("reconnect = %v, want true when the field is absent", got.Reconnect)
	}
}

func TestUnknownInternalMessagesAreIgnored(t *testing.T) {
	ps := NewMemoryPubSub()
	t.Cleanup(func() { ps.Close() })

	ids := Identifiers{"current_user": "42"}
	srv, url := identifiedServer(t, ps, ids)

	conn := connect(t, url)
	waitFor(t, "connection to register", func() bool { return srv.ConnectionCount() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, payload := range []string{
		`{"type":"something_else"}`,
		`{}`,
		`not json`,
		`[]`,
	} {
		if err := ps.Broadcast(ctx, internalChannelFor(ids), []byte(payload)); err != nil {
			t.Fatalf("Broadcast(%s) error = %v", payload, err)
		}
	}

	expectOnlyHeartbeats(t, conn)
	if got := srv.ConnectionCount(); got != 1 {
		t.Errorf("ConnectionCount() = %d, want the connection still there", got)
	}
}

// TestConnectionsWithoutIdentifiersAreNotSubscribed: an anonymous connection has
// no address, so it must not cost a subscription either.
func TestConnectionsWithoutIdentifiersAreNotSubscribed(t *testing.T) {
	ps := NewMemoryPubSub()
	t.Cleanup(func() { ps.Close() })

	srv, url := newTestServer(t, &Options{PubSub: ps})
	connect(t, url)
	waitFor(t, "connection to register", func() bool { return srv.ConnectionCount() == 1 })

	if got := ps.broadcastingCount(); got != 0 {
		t.Errorf("broadcastingCount() = %d for a connection with no identifiers, want 0", got)
	}
}

func TestInternalChannelIsReleasedWithTheConnection(t *testing.T) {
	ps := NewMemoryPubSub()
	t.Cleanup(func() { ps.Close() })

	srv, url := identifiedServer(t, ps, Identifiers{"current_user": "42"})

	conn := connect(t, url)
	waitFor(t, "the internal channel to be subscribed", func() bool { return ps.broadcastingCount() == 1 })

	conn.CloseNow()

	waitFor(t, "the internal channel to be released", func() bool { return ps.broadcastingCount() == 0 })
	if got := srv.ConnectionCount(); got != 0 {
		t.Errorf("ConnectionCount() = %d, want 0", got)
	}
}

// TestInternalChannelPrecedesTheWelcome: a disconnect published the instant a
// client sees its welcome must reach it, which means the subscription has to be
// live before the welcome goes out.
func TestInternalChannelPrecedesTheWelcome(t *testing.T) {
	ps := NewMemoryPubSub()
	t.Cleanup(func() { ps.Close() })

	ids := Identifiers{"current_user": "42"}
	_, url := identifiedServer(t, ps, ids)

	conn := dial(t, url, nil)
	if got := readMessage(t, conn); got.Type != typeWelcome {
		t.Fatalf("first message type = %q, want %q", got.Type, typeWelcome)
	}

	// The welcome has arrived, so no wait: the subscription is either already
	// there or the ordering is wrong.
	if got := ps.broadcastingCount(); got != 1 {
		t.Fatalf("broadcastingCount() = %d when the welcome arrived, want 1", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ps.Broadcast(ctx, internalChannelFor(ids), []byte(`{"type":"disconnect"}`)); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	if got := nextNonPing(t, conn); got.Type != typeDisconnect {
		t.Errorf("message type = %q, want %q", got.Type, typeDisconnect)
	}
}

// TestABackendFailureDoesNotBreakConnections: a connection that cannot be reached
// remotely is worse than one that can, and much better than none at all.
func TestABackendFailureDoesNotBreakConnections(t *testing.T) {
	srv, url := newTestServer(t, &Options{
		PubSub: unsubscribablePubSub{NewMemoryPubSub()},
		Authenticate: func(*http.Request) (Identifiers, error) {
			return Identifiers{"current_user": "42"}, nil
		},
	})

	conn := connect(t, url)
	waitFor(t, "connection to register", func() bool { return srv.ConnectionCount() == 1 })

	// It still works, it just cannot be disconnected from elsewhere.
	if got := readMessage(t, conn); got.Type != typePing {
		t.Errorf("message type = %q, want %q", got.Type, typePing)
	}
}

// unsubscribablePubSub accepts broadcasts and refuses subscriptions, like a
// backend that is unreachable at the wrong moment.
type unsubscribablePubSub struct{ *MemoryPubSub }

func (unsubscribablePubSub) Subscribe(context.Context, string, Handler) (func(), error) {
	return nil, context.DeadlineExceeded
}

// TestRemoteDisconnectReachesLocalConnections: the message goes through the
// backend and comes back, so a process disconnects its own connections the same
// way it disconnects anyone else's. Rails behaves the same.
func TestRemoteDisconnectReachesLocalConnections(t *testing.T) {
	ids := Identifiers{"current_user": "42"}
	srv, url := newTestServer(t, &Options{
		Authenticate: func(*http.Request) (Identifiers, error) { return ids, nil },
	})

	conn := connect(t, url)
	waitFor(t, "connection to register", func() bool { return srv.ConnectionCount() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Disconnect(ctx, ids, false); err != nil {
		t.Fatalf("Disconnect() error = %v", err)
	}

	if got := nextNonPing(t, conn); got.Type != typeDisconnect {
		t.Fatalf("message type = %q, want %q", got.Type, typeDisconnect)
	}
	waitFor(t, "connection to go", func() bool { return srv.ConnectionCount() == 0 })
}

// TestRemoteDisconnectIsIdempotent: two processes may both decide to drop the same
// user at the same time.
func TestRemoteDisconnectIsIdempotent(t *testing.T) {
	ids := Identifiers{"current_user": "42"}
	srv, url := newTestServer(t, &Options{
		Authenticate: func(*http.Request) (Identifiers, error) { return ids, nil },
	})

	conn := connect(t, url)
	waitFor(t, "connection to register", func() bool { return srv.ConnectionCount() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 3 {
		if err := srv.Disconnect(ctx, ids, false); err != nil {
			t.Fatalf("Disconnect() error = %v", err)
		}
	}

	if got := nextNonPing(t, conn); got.Type != typeDisconnect {
		t.Fatalf("message type = %q, want %q", got.Type, typeDisconnect)
	}
	// Only one disconnect is sent, however many arrive.
	if _, _, err := conn.Read(ctx); err == nil {
		t.Error("a second message arrived, want the connection closed after one disconnect")
	}
}
