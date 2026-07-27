# go-cable

A Go implementation of Action Cable — real-time channels over WebSocket, wire-compatible with
the `@rails/actioncable` JavaScript client.

- **No dependencies.** `go.mod` has no `require` block. That includes the WebSocket layer and
  the Redis client, both hand-rolled.
- **Verified against the real client.** The unmodified `@rails/actioncable` npm package drives
  the test suite: connect, confirm, reject, `perform`, broadcast to two clients, and both
  values of the `reconnect` flag.
- **Autobahn 301/301** on the applicable cases, [report committed](ws/testdata/autobahn/summary.json).
- **Measured, not asserted.** 10,000 idle connections: 2 goroutines each, a 27.9ms heartbeat
  sweep of which 154µs is the framework's own work.

It began as a rebuild-to-understand of Rails' Action Cable, read from the source, and is held
to the bar of the dependencies it replaces: conformance suites, fuzzed parsers, limits checked
before allocation, and a documented reason for every divergence from the original.

## Try it

```
go run ./cmd/example      # then open http://localhost:8080
```

Two browser tabs in the same room see each other's messages. `cmd/example/main.go` is the whole
thing in one readable file, including a page that speaks the protocol with a plain `WebSocket`
and no npm.

## A server

```go
srv := cable.New(&cable.Options{
    Authenticate: func(r *http.Request) (cable.Identifiers, error) {
        user := session(r)                       // your cookie, token, whatever
        if user == "" {
            return nil, errors.New("not signed in")   // → client is told, and stays down
        }
        return cable.Identifiers{"user": user}, nil
    },
})
defer srv.Close()

srv.Register("ChatChannel", func(s *cable.Subscription) cable.Channel {
    return &ChatChannel{srv: srv, sub: s}
})

http.Handle(cable.DefaultMountPath, srv)          // "/cable"
http.ListenAndServe(":8080", nil)
```

A channel is three methods. This one joins a room, and turns an action into a broadcast:

```go
type ChatChannel struct {
    srv  *cable.Server
    sub  *cable.Subscription
    room string
}

func (c *ChatChannel) Subscribed(ctx context.Context) error {
    var params struct{ Room string `json:"room"` }
    if err := json.Unmarshal(c.sub.Params(), &params); err != nil {
        return err
    }
    if !canRead(c.sub.Connection().Identifiers()["user"], params.Room) {
        return errors.New("not a member")         // → the client's rejected() fires
    }

    c.room = "chat:" + params.Room
    return c.sub.StreamFrom(ctx, c.room)          // join the room
}

func (c *ChatChannel) Unsubscribed(context.Context) {}

func (c *ChatChannel) Perform(ctx context.Context, action string, data json.RawMessage) error {
    if action != "speak" {
        return fmt.Errorf("unknown action %q", action)
    }
    var msg struct{ Body string `json:"body"` }
    if err := json.Unmarshal(data, &msg); err != nil {
        return err
    }
    return c.srv.Broadcast(ctx, c.room, map[string]string{
        "from": c.sub.Connection().Identifiers()["user"],
        "body": msg.Body,
    })
}
```

From anywhere else — an HTTP handler, a background job — reach those clients with the same call:

```go
srv.Broadcast(ctx, "chat:1", map[string]string{"body": "the server has something to say"})
```

## A client

The Rails client works unchanged:

```js
import { createConsumer } from "@rails/actioncable"

const chat = createConsumer("/cable").subscriptions.create(
  { channel: "ChatChannel", room: "1" },
  {
    connected()    { console.log("in") },
    rejected()     { console.log("no") },
    received(data) { render(data) },
  },
)

chat.perform("speak", { body: "hello" })
```

Or a plain `WebSocket` — the protocol is six JSON message shapes, documented in
[docs/action-cable-protocol.md](docs/action-cable-protocol.md), and `cmd/example` speaks it in
40 lines of vanilla JS.

## Three words worth keeping straight

| | What it is |
|---|---|
| **Channel** | Your Go type — the code. `ChatChannel`. Like a controller. |
| **Subscription** | One client's instance of a channel, keyed by `{channel, room, …}`. One socket holds many. |
| **Broadcasting** | The string you fan out to: `"chat:1"`. What other systems call a *room*. |

"Joining a room" is a client *asking* (`subscriptions.create`) and your `Subscribed` deciding —
returning an error rejects it. A client can never join by naming something; authorization has
one place to live.

## Scaling out

The default backend delivers within one process. For more than one, hand it Redis:

```go
ps := redispubsub.New(&redispubsub.Options{Address: "localhost:6379"})
defer ps.Close()

srv := cable.New(&cable.Options{PubSub: ps})
```

Nothing else changes. Every process publishes to and subscribes from Redis independently, so
there is no server-to-server traffic and no need for sticky sessions. `redispubsub.ParseURL`
accepts `redis://` and `rediss://` URLs, including the ones in a Rails `cable.yml`.

`PubSub` is a three-method interface, so another backend is a small amount of code — and
`cable/pubsubtest` is the conformance suite both built-in adapters pass, ready to point at a
third.

## The rest of the API

```go
sub.Transmit(v)                              // to this one subscription's client
sub.StreamFrom(ctx, "chat:1")                // and StopStream, StopAllStreams
sub.Periodically(time.Second, tick)          // push without being asked
sub.Params() / sub.Identifier() / sub.ChannelName() / sub.Connection()

srv.Broadcast(ctx, "chat:1", v)
srv.Disconnect(ctx, cable.Identifiers{"user": "42"}, false)   // in every process
srv.Shutdown(ctx)                            // tell clients to reconnect, then drain
srv.ConnectionCount()
```

## What it does not do

Each of these is a decision with a workaround, not an oversight — the reasoning is in
[docs/go-port-plan.md §9](docs/go-port-plan.md).

- **`permessage-deflate`.** The gap that matters most for large repetitive broadcasts. The
  framing layer is already shaped for it.
- **Rails integration.** No cookie decryption, no `cable.yml`, no GlobalID. Compatibility is
  with the JS client and with Rails' Redis wire format, not with Rails itself.
- **Sentinel and sharded Redis pub/sub.** Resolve a master with `Options.Dialer`; ordinary
  `PUBLISH` is already cluster-wide.
- **Postgres or database-backed pub/sub.** Sketched, not built.
- **Transport fallback, acks, binary frames.** WebSocket only, fire-and-forget, JSON text — the
  same shape as Action Cable itself, and narrower than socket.io.

## Testing

```
make                # gofmt, vet, race — before every commit
make conformance    # Autobahn, the Rails JS client, real Redis  (needs Docker and npm)
make fuzz           # every parser that reads bytes off a network
make load           # 10,000 connections: memory, goroutines, sweep timing
make everything     # all of the above
```

The slow gates are opt-in through environment variables (`WS_AUTOBAHN`, `CABLE_JS`,
`REDIS_URL`, `CABLE_LOAD`) so `go test ./...` stays fast and hermetic.

## Documentation

| | |
|---|---|
| [implementation.md](docs/implementation.md) | How this code works: ownership rules, goroutine model, each layer's mechanics, divergences from Rails |
| [flows.md](docs/flows.md) | Every path traced call by call, with the goroutine marked at each step |
| [action-cable-internals.md](docs/action-cable-internals.md) | How the Ruby original works, read from the Rails source |
| [action-cable-protocol.md](docs/action-cable-protocol.md) | The wire format, exactly |
| [action-cable-glossary.md](docs/action-cable-glossary.md) | Every term, and which class owns it |
| [go-port-plan.md](docs/go-port-plan.md) | The plan this was built to, with what was decided and deferred |
| [ws/README.md](ws/README.md) | The WebSocket library on its own terms |

## Status

Complete and tested, not yet published. Before it is:

- **The module path is a placeholder.** `go-cable` needs to become
  `github.com/<you>/go-cable` — trivial now, a breaking change for importers later.
- `ws` may deserve its own module, since it depends on nothing here.

Go 1.26. Licence: not chosen yet.
