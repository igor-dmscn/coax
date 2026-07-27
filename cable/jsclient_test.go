package cable

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// jsClientScript drives the real @rails/actioncable client against our server.
//
// The client is written for browsers, so a few globals are shimmed: it registers
// a visibilitychange listener and reads document.visibilityState. Nothing else is
// faked — the protocol logic under test is the client's own.
const jsClientScript = `
globalThis.addEventListener = () => {}
globalThis.removeEventListener = () => {}
globalThis.document = {
  visibilityState: "visible",
  head: { querySelector: () => null },
}

const { createConsumer, logger } = await import("@rails/actioncable")

logger.enabled = true

// The query parameter becomes the connection's identifier, so the two clients
// below are two different identities as far as the server is concerned.
const url = process.argv[2]
const consumer = createConsumer(url + "?client=first")
consumer.connect()

const fail = (message) => { console.error("FAIL: " + message); process.exit(1) }
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

// Wait for the socket to open.
for (let i = 0; i < 100; i++) {
  if (consumer.connection.isOpen()) break
  await sleep(100)
}
if (!consumer.connection.isOpen()) fail("the connection never opened")

const protocol = consumer.connection.getProtocol()
if (protocol !== "actioncable-v1-json") fail("negotiated subprotocol is " + protocol)

const monitor = consumer.connection.monitor
if (!monitor.isRunning()) fail("the connection monitor is not running")

// recordConnect runs on the welcome message and clears disconnectedAt, so a
// monitor that is running with no disconnect recorded means welcome arrived.
if (monitor.disconnectedAt !== undefined) fail("a disconnect was recorded")

const waitUntil = async (what, cond) => {
  for (let i = 0; i < 100; i++) {
    if (cond()) return
    await sleep(100)
  }
  fail("timed out waiting for " + what)
}

// A subscription the server confirms: connected() fires, actions round-trip.
const received = []
let connectedCount = 0

// willAttemptReconnect is the client's own reading of the reconnect field in a
// disconnect message, which is the thing that flag exists to control.
let chatWillReconnect = null

const chat = consumer.subscriptions.create({ channel: "ChatChannel", room: "1" }, {
  connected() { connectedCount++ },
  received(data) { received.push(data) },
  rejected() { fail("ChatChannel was rejected") },
  disconnected({ willAttemptReconnect }) { chatWillReconnect = willAttemptReconnect },
})

await waitUntil("ChatChannel to connect", () => connectedCount > 0)

// forget() runs on confirmation, so an empty pending list is the guarantor
// having stopped its retry loop. It retries every 500ms while a subscription is
// pending, so a stuck guarantor would still be listed a second later.
await sleep(1000)
if (consumer.subscriptions.guarantor.pendingSubscriptions.length !== 0) {
  fail("the subscription guarantor is still retrying")
}
if (connectedCount !== 1) fail("connected() fired " + connectedCount + " times")

// perform() names an action; send() does not, and defaults to receive.
chat.perform("speak", { body: "hello" })
await waitUntil("the speak reply", () => received.length > 0)
if (received[0].action !== "speak") fail("action was " + received[0].action)
if (received[0].body !== "hello") fail("body was " + received[0].body)

chat.send({ body: "plain" })
await waitUntil("the default-action reply", () => received.length > 1)
if (received[1].action !== "receive") fail("default action was " + received[1].action)
if (received[1].body !== "plain") fail("body was " + received[1].body)

// A subscription the server refuses: rejected() fires, connected() does not.
let rejected = false
consumer.subscriptions.create("RejectedChannel", {
  connected() { fail("RejectedChannel connected") },
  rejected() { rejected = true },
})

await waitUntil("RejectedChannel to be rejected", () => rejected)

// A second client in the same room: a broadcast reaches both, which is the
// difference between transmitting to one subscription and publishing to a
// broadcasting.
const otherConsumer = createConsumer(url + "?client=second")
otherConsumer.connect()

const otherReceived = []
let otherConnected = false
let otherWillReconnect = null
otherConsumer.subscriptions.create({ channel: "ChatChannel", room: "1" }, {
  connected() { otherConnected = true },
  received(data) { otherReceived.push(data) },
  rejected() { fail("the second ChatChannel was rejected") },
  disconnected({ willAttemptReconnect }) { otherWillReconnect = willAttemptReconnect },
})

await waitUntil("the second client to connect", () => otherConnected)

chat.perform("broadcast", { body: "to everyone" })
await waitUntil("both clients to receive the broadcast",
  () => received.length > 2 && otherReceived.length > 0)

for (const [who, data] of [["the publisher", received[2]], ["the other client", otherReceived[0]]]) {
  if (data.action !== "broadcast") fail(who + " got action " + data.action)
  if (data.body !== "to everyone") fail(who + " got body " + data.body)
}

// Heartbeats: stay connected across more than two intervals and confirm the
// client keeps seeing traffic. Its stale threshold is 6s, so a server that
// stopped pinging would trigger a reconnect here.
await sleep(7000)

if (!consumer.connection.isOpen()) fail("the connection did not stay open")
if (monitor.reconnectAttempts !== 0) fail("client reconnected " + monitor.reconnectAttempts + " time(s)")

const sincePing = (Date.now() - monitor.pingedAt) / 1000
if (!(sincePing < 6)) fail("last message was " + sincePing + "s ago")

// The subscription survived the heartbeats and is still usable.
chat.perform("speak", { body: "still here" })
await waitUntil("a reply after the heartbeats", () => received.length > 3)
if (received[3].body !== "still here") fail("body was " + received[3].body)

// A remote disconnect with reconnect=false: the client stays down. This is what
// the flag is for, so it is checked against the client's real reconnect logic
// rather than only against the frame on the wire.
otherConsumer.subscriptions.create({ channel: "DisconnectChannel" }, {})

await waitUntil("the second client to be disconnected", () => !otherConsumer.connection.isOpen())
if (otherConsumer.connection.monitor.isRunning()) {
  fail("the monitor is still running after reconnect=false")
}
if (otherWillReconnect !== false) fail("willAttemptReconnect was " + otherWillReconnect)

// The first client is a different identity, so it is untouched.
await sleep(500)
if (!consumer.connection.isOpen()) fail("the wrong client was disconnected")

// Graceful shutdown, the other value of the same flag: the client is told to come
// back, so it keeps its monitor running and retries.
consumer.subscriptions.create({ channel: "ShutdownChannel" }, {})

await waitUntil("the server to shut down", () => !consumer.connection.isOpen())
if (chatWillReconnect !== true) fail("willAttemptReconnect was " + chatWillReconnect)
if (!consumer.connection.monitor.isRunning()) {
  fail("the monitor stopped despite reconnect=true")
}

otherConsumer.disconnect()
consumer.disconnect()
console.log("PASS")
process.exit(0)
`

const jsClientPackageJSON = `{
  "name": "cable-jsclient-conformance",
  "private": true,
  "type": "module"
}`

// TestRailsJSClient runs the unmodified Rails JavaScript client against the
// server. It is the compatibility target, so this is the check that matters most;
// it needs npm and network access, so it is opt-in:
//
//	CABLE_JS=1 go test ./cable/ -run TestRailsJSClient -timeout 5m
func TestRailsJSClient(t *testing.T) {
	if os.Getenv("CABLE_JS") == "" {
		t.Skip("set CABLE_JS=1 to run the Rails JS client conformance test (needs npm and network)")
	}
	for _, tool := range []string{"node", "npm"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed", tool)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}
	write("package.json", jsClientPackageJSON)
	write("client.mjs", jsClientScript)

	install := exec.CommandContext(ctx, "npm", "install", "@rails/actioncable",
		"--no-audit", "--no-fund", "--loglevel=error")
	install.Dir = dir
	if out, err := install.CombinedOutput(); err != nil {
		t.Skipf("npm install failed (offline?): %v\n%s", err, out)
	}

	url := startCableServer(t)

	run := exec.CommandContext(ctx, "node", "client.mjs", url)
	run.Dir = dir
	out, err := run.CombinedOutput()
	t.Logf("client output:\n%s", out)
	if err != nil {
		t.Fatalf("the Rails JS client failed: %v", err)
	}
	if !strings.Contains(string(out), "PASS") {
		t.Error("the client did not report PASS")
	}
}

// roomChannel is what a real chat channel looks like: it streams from the room in
// its params, echoes most actions back to the one client that sent them, and
// publishes a "broadcast" action to everyone in the room. The JS client drives
// both paths itself.
type roomChannel struct {
	srv *Server
	sub *Subscription
}

func (c roomChannel) Subscribed(ctx context.Context) error {
	room, err := c.room()
	if err != nil {
		return err
	}
	return c.sub.StreamFrom(ctx, room)
}

func (roomChannel) Unsubscribed(context.Context) {}

func (c roomChannel) Perform(ctx context.Context, action string, data json.RawMessage) error {
	var payload struct {
		Body string `json:"body"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	message := map[string]string{"action": action, "body": payload.Body}

	if action != "broadcast" {
		return c.sub.Transmit(message)
	}

	room, err := c.room()
	if err != nil {
		return err
	}
	return c.srv.Broadcast(ctx, room, message)
}

func (c roomChannel) room() (string, error) {
	var params struct {
		Room string `json:"room"`
	}
	if err := json.Unmarshal(c.sub.Params(), &params); err != nil {
		return "", err
	}
	return params.Room, nil
}

// disconnectChannel disconnects its own connection the remote way: by publishing
// to the pub/sub backend, exactly as another process would. Subscribing to it is
// how the JS client asks to be thrown out.
type disconnectChannel struct {
	srv *Server
	sub *Subscription
}

func (c disconnectChannel) Subscribed(ctx context.Context) error {
	return c.srv.Disconnect(ctx, c.sub.Connection().Identifiers(), false)
}

func (disconnectChannel) Unsubscribed(context.Context) {}

func (disconnectChannel) Perform(context.Context, string, json.RawMessage) error { return nil }

// shutdownChannel shuts the whole server down when subscribed to, so the JS
// client can be shown a real graceful shutdown. Shutdown runs on its own
// goroutine: it waits for connections to finish, and this one cannot finish until
// Subscribed returns.
type shutdownChannel struct{ srv *Server }

func (c shutdownChannel) Subscribed(context.Context) error {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		c.srv.Shutdown(ctx)
	}()
	return nil
}

func (shutdownChannel) Unsubscribed(context.Context) {}

func (shutdownChannel) Perform(context.Context, string, json.RawMessage) error { return nil }

// rejectedChannel refuses every subscription.
type rejectedChannel struct{}

func (rejectedChannel) Subscribed(context.Context) error { return errors.New("not allowed") }
func (rejectedChannel) Unsubscribed(context.Context)     {}

func (rejectedChannel) Perform(context.Context, string, json.RawMessage) error {
	return errors.New("not subscribed")
}

// startCableServer serves a cable endpoint on a real port with production
// defaults, including the 3 second heartbeat the JS client expects.
func startCableServer(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}

	srv := New(&Options{
		Logger: testLogger(t),
		Authenticate: func(r *http.Request) (Identifiers, error) {
			// Whatever the client says it is: enough to give the two clients
			// distinct identities, which is what remote disconnect addresses.
			return Identifiers{"client": r.URL.Query().Get("client")}, nil
		},
	})
	t.Cleanup(func() { srv.Close() })

	srv.Register("ChatChannel", func(s *Subscription) Channel { return roomChannel{srv: srv, sub: s} })
	srv.Register("RejectedChannel", func(*Subscription) Channel { return rejectedChannel{} })
	srv.Register("DisconnectChannel", func(s *Subscription) Channel {
		return disconnectChannel{srv: srv, sub: s}
	})
	srv.Register("ShutdownChannel", func(*Subscription) Channel { return shutdownChannel{srv: srv} })

	mux := http.NewServeMux()
	mux.Handle(DefaultMountPath, srv)
	hs := &http.Server{Handler: mux}

	go hs.Serve(listener)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		hs.Shutdown(ctx)
	})

	return "ws://" + listener.Addr().String() + DefaultMountPath
}
