package redispubsub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"go-cable/cable"
	"go-cable/cable/pubsubtest"
	"go-cable/ws"
)

// The tests in this file need a real Redis. They are opt-in, because a test suite
// that requires infrastructure is a test suite people stop running:
//
//	docker run --rm -d -p 6379:6379 redis:7-alpine
//	REDIS_URL=redis://127.0.0.1:6379 go test ./cable/redispubsub/ -run Redis -v
//
// The fake server in fake_test.go covers the same logic without Redis. What only
// a real server can confirm is that the hand-rolled protocol is right: that Redis
// accepts our commands and that we understand its replies, including the ones the
// fake was written from an assumption about.
func realRedis(t *testing.T) *Options {
	t.Helper()

	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("set REDIS_URL to run the tests against a real Redis")
	}

	opts, err := ParseURL(url)
	if err != nil {
		t.Fatalf("ParseURL(%q) error = %v", url, err)
	}
	opts.Logger = testLogger(t)
	return opts
}

// prefix keeps concurrent runs of these tests from seeing each other's messages,
// since one Redis is shared by everything pointed at it.
func prefix(t *testing.T) string {
	t.Helper()
	return "go-cable-test:" + strings.ReplaceAll(t.Name(), "/", ":") + ":"
}

func TestRedisConformance(t *testing.T) {
	opts := realRedis(t)

	pubsubtest.Run(t, func(t *testing.T) cable.PubSub {
		ps := New(opts)
		t.Cleanup(func() { ps.Close() })
		return ps
	})
}

// TestRedisCrossesProcesses is the reason this adapter exists: two adapters that
// share no memory reach each other's subscribers. Two adapters in one test
// process is the same arrangement as two servers on two machines — neither has any
// path to the other except Redis.
func TestRedisCrossesProcesses(t *testing.T) {
	opts := realRedis(t)
	room := prefix(t) + "room_1"

	publisher := New(opts)
	defer publisher.Close()

	subscriber := New(opts)
	defer subscriber.Close()

	h, payloads := received()
	subscribe(t, subscriber, room, h)

	// No sleep: Subscribe returned, so Redis has confirmed, so this cannot be
	// lost. That is the guarantee the whole design rests on, here against the
	// real thing.
	broadcast(t, publisher, room, `{"body":"across processes"}`)
	expect(t, payloads, `{"body":"across processes"}`)
}

// TestRedisResubscribesAfterAKilledConnection uses Redis to break the connection,
// which is as close to a failover as a test can get without a second server:
// CLIENT KILL TYPE pubsub drops exactly the subscribe connection.
func TestRedisResubscribesAfterAKilledConnection(t *testing.T) {
	opts := realRedis(t)
	room := prefix(t) + "room_1"

	ps := New(opts)
	defer ps.Close()

	h, payloads := received()
	subscribe(t, ps, room, h)
	broadcast(t, ps, room, `before`)
	expect(t, payloads, `before`)

	// A separate connection does the killing, so it does not kill itself.
	killer := New(opts)
	defer killer.Close()
	if err := killPubSubClients(t, killer); err != nil {
		t.Fatalf("CLIENT KILL: %v", err)
	}

	// The subscription is replayed onto the new connection. Redis pub/sub drops
	// anything published during the gap, so this retries rather than publishing
	// once and hoping.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		broadcast(t, ps, room, `after`)
		select {
		case got := <-payloads:
			if got != `after` {
				t.Fatalf("received %q, want %q", got, `after`)
			}
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatal("the subscription was never restored after the connection was killed")
}

// TestRedisEndToEnd runs the whole stack: a WebSocket client, a channel streaming
// from a broadcasting, and a broadcast that arrives through Redis.
func TestRedisEndToEnd(t *testing.T) {
	opts := realRedis(t)
	room := prefix(t) + "room_1"

	ps := New(opts)
	defer ps.Close()

	srv := cable.New(&cable.Options{PubSub: ps, Logger: testLogger(t)})
	defer srv.Close()

	srv.Register("ChatChannel", func(s *cable.Subscription) cable.Channel {
		return &streamingChannel{sub: s, broadcasting: room}
	})

	hs := httptest.NewServer(srv)
	defer hs.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := ws.Dial(ctx, "ws"+strings.TrimPrefix(hs.URL, "http"), &ws.DialOptions{
		Subprotocols: []string{cable.Subprotocol},
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.CloseNow()

	const identifier = `{"channel":"ChatChannel"}`
	readFrame(t, ctx, conn) // welcome

	if err := conn.Write(ctx, ws.MessageText,
		[]byte(`{"command":"subscribe","identifier":`+quote(identifier)+`}`)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := readFrame(t, ctx, conn)["type"]; got != "confirm_subscription" {
		t.Fatalf("message type = %v, want confirm_subscription", got)
	}

	// Published the way Rails publishes: the broadcasting's name as the channel,
	// the message as JSON. Nothing about this payload came from our own encoder.
	// ← actioncable/lib/action_cable/server/broadcasting.rb:41
	const rails = `{"body":"from Rails","sent_at":1234567890}`
	publisher := New(opts)
	defer publisher.Close()
	if err := publisher.Broadcast(ctx, room, []byte(rails)); err != nil {
		t.Fatalf("Broadcast() error = %v", err)
	}

	frame := readFrame(t, ctx, conn)
	if got := frame["identifier"]; got != identifier {
		t.Errorf("identifier = %v, want %v", got, identifier)
	}

	// The payload reaches the client byte for byte: a Rails process on this Redis
	// would be understood without translation.
	message, err := json.Marshal(frame["message"])
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if string(message) != rails {
		t.Errorf("message = %s, want %s", message, rails)
	}
}

// TestRedisRemoteDisconnect is the phase-6 criterion on the real backend: two
// servers that share nothing but Redis, and a user disconnected on the one that
// has never seen them.
func TestRedisRemoteDisconnect(t *testing.T) {
	opts := realRedis(t)
	ids := cable.Identifiers{"current_user": prefix(t) + "42"}

	holderPubSub := New(opts)
	defer holderPubSub.Close()

	holder := cable.New(&cable.Options{
		PubSub: holderPubSub,
		Logger: testLogger(t),
		Authenticate: func(*http.Request) (cable.Identifiers, error) {
			return ids, nil
		},
	})
	defer holder.Close()

	hs := httptest.NewServer(holder)
	defer hs.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := ws.Dial(ctx, "ws"+strings.TrimPrefix(hs.URL, "http"), &ws.DialOptions{
		Subprotocols: []string{cable.Subprotocol},
	})
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.CloseNow()

	if got := readFrame(t, ctx, conn)["type"]; got != "welcome" {
		t.Fatalf("first message type = %v, want welcome", got)
	}

	// A second server, with its own Redis connections, holding no connections at
	// all: the only thing it shares with the first is the backend.
	otherPubSub := New(opts)
	defer otherPubSub.Close()

	other := cable.New(&cable.Options{PubSub: otherPubSub, Logger: testLogger(t)})
	defer other.Close()

	if err := other.Disconnect(ctx, ids, false); err != nil {
		t.Fatalf("Disconnect() error = %v", err)
	}

	// Heartbeats may arrive first, so read until the disconnect.
	for range 10 {
		frame := readFrame(t, ctx, conn)
		if frame["type"] == "ping" {
			continue
		}
		if frame["type"] != "disconnect" {
			t.Fatalf("message type = %v, want disconnect", frame["type"])
		}
		if frame["reason"] != "remote" {
			t.Errorf("reason = %v, want remote", frame["reason"])
		}
		if frame["reconnect"] != false {
			t.Errorf("reconnect = %v, want false", frame["reconnect"])
		}
		return
	}
	t.Fatal("the connection was never disconnected")
}

// streamingChannel streams from one fixed broadcasting.
type streamingChannel struct {
	sub          *cable.Subscription
	broadcasting string
}

func (c *streamingChannel) Subscribed(ctx context.Context) error {
	return c.sub.StreamFrom(ctx, c.broadcasting)
}

func (c *streamingChannel) Unsubscribed(context.Context) {}

func (c *streamingChannel) Perform(context.Context, string, json.RawMessage) error { return nil }

// killPubSubClients disconnects every subscribed client on the server, using the
// adapter's own publish connection to send a command it otherwise never sends.
func killPubSubClients(t *testing.T, ps *PubSub) error {
	t.Helper()

	ps.pubMu.Lock()
	defer ps.pubMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, reader, _, err := ps.publishConn(ctx)
	if err != nil {
		return err
	}

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	if _, err := conn.Write(appendCommand(nil, "CLIENT", "KILL", "TYPE", "pubsub")); err != nil {
		return err
	}

	reply, err := readValue(reader, defaultMaxReplySize)
	if err != nil {
		return err
	}
	if reply.typ == typeError {
		return &Error{Message: reply.str()}
	}
	t.Logf("CLIENT KILL TYPE pubsub killed %d client(s)", reply.num)
	return nil
}

// readFrame reads one JSON message from a WebSocket.
func readFrame(t *testing.T, ctx context.Context, conn *ws.Conn) map[string]any {
	t.Helper()

	typ, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if typ != ws.MessageText {
		t.Fatalf("message type = %v, want text", typ)
	}

	var frame map[string]any
	if err := json.Unmarshal(data, &frame); err != nil {
		t.Fatalf("Unmarshal(%s) error = %v", data, err)
	}
	return frame
}

// quote JSON-encodes a string, which is how an identifier travels: a JSON string
// containing JSON.
func quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestParseURL(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		wantAddr string
		wantUser string
		wantPass string
		wantTLS  bool
		wantErr  bool
	}{
		{name: "host and port", in: "redis://127.0.0.1:6379", wantAddr: "127.0.0.1:6379"},
		{name: "default port", in: "redis://redis.internal", wantAddr: "redis.internal:6379"},
		{name: "password only", in: "redis://:secret@localhost:6379", wantAddr: "localhost:6379", wantPass: "secret"},
		{
			name: "username and password", in: "redis://alice:secret@localhost:6379",
			wantAddr: "localhost:6379", wantUser: "alice", wantPass: "secret",
		},
		{
			// Rails' cable.yml URLs usually carry a database, which pub/sub
			// ignores: publishing on db 5 reaches subscribers on db 0.
			name: "database is accepted and ignored", in: "redis://localhost:6379/5",
			wantAddr: "localhost:6379",
		},
		{name: "rediss enables TLS", in: "rediss://localhost:6380", wantAddr: "localhost:6380", wantTLS: true},
		{name: "escaped password", in: "redis://:p%40ss@localhost:6379", wantAddr: "localhost:6379", wantPass: "p@ss"},
		{name: "wrong scheme", in: "http://localhost:6379", wantErr: true},
		{name: "no host", in: "redis://", wantErr: true},
		{name: "not a URL", in: "redis://loc alhost", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseURL(tt.in)
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("ParseURL(%q) error = %v, want an error: %v", tt.in, err, tt.wantErr)
			}
			if err != nil {
				return
			}

			if got.Address != tt.wantAddr {
				t.Errorf("Address = %q, want %q", got.Address, tt.wantAddr)
			}
			if got.Username != tt.wantUser {
				t.Errorf("Username = %q, want %q", got.Username, tt.wantUser)
			}
			if got.Password != tt.wantPass {
				t.Errorf("Password = %q, want %q", got.Password, tt.wantPass)
			}
			if gotTLS := got.TLS != nil; gotTLS != tt.wantTLS {
				t.Errorf("TLS = %v, want %v", gotTLS, tt.wantTLS)
			}
		})
	}
}

func TestOptionDefaults(t *testing.T) {
	got := Options{}.withDefaults()

	if got.Address != defaultAddress {
		t.Errorf("Address = %q, want %q", got.Address, defaultAddress)
	}
	if got.Dialer == nil || got.Logger == nil {
		t.Error("withDefaults left the dialer or logger nil")
	}

	// A read timeout at or below the ping interval would kill every idle
	// connection on schedule.
	tight := Options{PingInterval: time.Minute, ReadTimeout: time.Second}.withDefaults()
	if tight.ReadTimeout <= tight.PingInterval {
		t.Errorf("ReadTimeout = %v with a %v ping interval", tight.ReadTimeout, tight.PingInterval)
	}
}

// TestHTTPHandlerCompiles keeps the end-to-end test's imports honest when Redis is
// absent, so a broken signature is a build failure rather than a skipped test.
var _ http.Handler = (*cable.Server)(nil)
