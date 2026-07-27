# coax Internals

How this implementation actually works: package graph, goroutine model, ownership rules, and
the mechanics of each layer.

The companion to [action-cable-internals](./action-cable-internals.md), which documents the
Ruby original. Where behaviour differs, §11 says so and why. Flows are traced call by call in
[flows](./flows.md); the wire format is in [protocol](./action-cable-protocol.md).

**Reference convention:** `coax/conn.go:writeLoop` means the `writeLoop` function in that
file. Symbols rather than line numbers, because line numbers rot and symbol names are
greppable. References to Rails source keep the `file.rb:line` form, pinned to the commit
named in the companion doc.

---

## 1. Layering

Three packages, one direction of dependency. Nothing points back up.

```
your application
│
├─ coax.Server                             an http.Handler, mounted at /cable
│  │
│  ├─ coax.Connection                      one per socket: identity, dispatch, queue
│  │  ├─ map[string]*Subscription           keyed by the client's raw identifier
│  │  │  └─ Subscription
│  │  │     ├─ Channel (your code)          Subscribed / Unsubscribed / Perform
│  │  │     │                              or Subscribed / Unsubscribed + named actions
│  │  │     ├─ streams map[string]func()    broadcasting → unsubscribe
│  │  │     └─ stopTimers []func()          periodic timers
│  │  └─ internal channel subscription      action_cable/<identity>
│  │
│  ├─ coax.PubSub                          the one interface: 3 methods
│  │  ├─ coax.MemoryPubSub                 default; one process
│  │  └─ redispubsub.PubSub                 RESP2 over two TCP connections
│  │
│  └─ ws.Conn                               ← the only thing coax knows about transport
│
└─ ws                                       standalone RFC 6455 library, imports no coax
   ├─ Accept / Dial                         handshake, hijack, 101
   ├─ Conn                                  read/write state machines, close bookkeeping
   ├─ frameHeader                           parse/serialise, validate
   ├─ msgReader / msgWriter                 fragmentation, streaming
   ├─ utf8Validator                         incremental, across fragments
   └─ mask                                  word-at-a-time XOR
```

`coax` touches the transport through five methods on a connection — `Read`, `Write`, `Close`,
`CloseSend`, `CloseNow` — plus `ws.Accept` to obtain one and `ws.CloseStatus` to classify why a
read ended. That is the whole contract, which is what makes `ws` publishable on its own and
would make an alternative transport a contained change.

`Close` (the full handshake, which reads) appears exactly once, in
`coax/server.go:rejectUnauthorized`: authentication runs before the reader goroutine exists, so
that is the only moment where nothing else owns reads. Everywhere else a connection ends through
`CloseSend` or `CloseNow`, which do not read.

### Where state lives

| State | Owner | Scope | Structure |
|---|---|---|---|
| Live connections | `Server.conns` | one process | `map[*Connection]struct{}`, `RWMutex` |
| Registered channels | `Server.channels` | one process | `map[string]ChannelFactory`, same mutex, written at start-up |
| Shutdown gate | `Server.stop` | one process | closed channel, `sync.Once` |
| Connection count for shutdown | `Server.live` | one process | `sync.WaitGroup` |
| Subscriptions | `Connection.subscriptions` | one connection | `map[string]*Subscription`, **no lock** (§3) |
| Outbound frames | `Connection.send` | one connection | `chan outbound`, capacity `SendBuffer` (64) |
| Internal channel unsubscribe | `Connection.stopInternalChannel` | one connection | `func()` |
| Streams | `Subscription.streams` | one subscription | `map[string]func()`, no lock |
| Periodic timers | `Subscription.stopTimers` | one subscription | `[]func()`, no lock |
| Local subscribers (memory) | `MemoryPubSub.subs` | one process | `map[string]map[*subscriber]struct{}`, `RWMutex` |
| Local subscribers (Redis) | `redispubsub.PubSub.subs` | one process | `map[string]*broadcasting`, `Mutex` |
| Redis subscribe connection | `redispubsub.PubSub.conn` | one adapter | `net.Conn`, nil while disconnected |
| Redis publish connection | `redispubsub.PubSub.pubConn` | one adapter | `net.Conn` + `bufio.Reader`, `pubMu` |
| Read/write scratch | `ws.Conn` fields | one socket | fixed arrays, preallocated (§4.5) |

Nothing is persisted. Nothing crosses a process boundary except through `PubSub`. The
connection map is per-process, which is why disconnecting a user elsewhere goes through the
pub/sub backend (§7).

### What is deliberately absent

| Rails has | We have |
|---|---|
| Worker pool, executor pool | goroutines, sized by the work rather than configured |
| `MessageBuffer` for pre-open frames | nothing — the welcome is sent before the reader starts, so there is no window |
| `SubscriptionGuarantor` state on the server | nothing — retries are the client's business |
| `Concurrent::AtomicFixnum` for deferred confirmation | nothing — `PubSub.Subscribe` blocks until acked (§6.1) |
| `TaggedLoggerProxy` | `slog.Logger.With("remote", …)` |
| `RemoteConnections` builder object | one method, `Server.Disconnect` |

---

## 2. Goroutine model

| # | Goroutine | Count | Runs | Owns |
|---|---|---|---|---|
| 1 | HTTP handler → reader | 1 per connection | `ServeHTTP` → `serve` → `readLoop`: decode, dispatch, all channel callbacks | `Connection.subscriptions`, every `Subscription`, the socket's **read** side |
| 2 | Writer | 1 per connection | `writeLoop`: drain `send`, one `ws.Write` per frame | the socket's **write** side |
| 3 | Heartbeat | 1 per server, lazy | `heartbeat`: 3s ticker → `sweep` | nothing; snapshots under `RLock` |
| 4 | Periodic timer | 1 per `Periodically` call | `runTimer`: ticker → your callback | nothing; must not touch subscription state |
| 5 | Redis reader | 1 per adapter, lazy | `run`: dial, resubscribe, `readLoop`, reconnect with backoff | the subscribe connection's read side, `dispatch` scratch |
| 6 | Redis keepalive | 1 per adapter, lazy | `keepalive`: ping ticker | nothing |
| 7 | Shutdown waiter | 1 per `Shutdown` call | `waitForConnections`: `live.Wait()` | nothing |

Two per connection, not three: the HTTP handler goroutine *is* the reader, so hijacking the
connection costs nothing extra and the handler naturally lives as long as the socket.

Measured at 10,000 idle connections: 2.0 goroutines per connection, ~40 KB RSS each with both
ends of every connection in one process (`coax/load_test.go:TestManyIdleConnections`).

### Ownership rules

These are the invariants the whole design rests on. Breaking one is how a data race gets in.

1. **One reader, one writer, per socket.** `ws.Conn` does not lock reads against reads or
   writes against the read path; it assumes at most one goroutine on each side. `coax`
   satisfies this by construction: the handler reads, `writeLoop` writes.
2. **Everything reachable from a `Subscription` belongs to the reader goroutine.** The
   subscriptions map, `streams`, `stopTimers`, and the `Channel` you wrote. This is why none
   of them is locked, and why `Subscribed`/`Unsubscribed`/`Perform` need no mutex of their own.
3. **`transmit` is the only thing safe to call from elsewhere.** It is a non-blocking send on
   a buffered channel. `Subscription.Transmit` is built on it, which is what makes it safe
   from a periodic timer or a pub/sub handler.
4. **A `Handler` must not call back into its `PubSub`.** `MemoryPubSub` delivers while holding
   its read lock, so re-entering would deadlock. The framework's own handler
   (`coax/streams.go:forward`) only queues a frame.
5. **A `PubSub` implementation must not return from `Subscribe` before the backend has
   acknowledged.** Everything in §6.1 depends on it.
6. **Nothing writes to a `ws.Conn` after `CloseNow`/`CloseSend`.** Both record a terminal
   error under `closeMu`, so later calls report the first cause instead of touching a closed
   socket.

---

## 3. The connection: two goroutines and a queue

```
      client
        │  frames
        ▼
┌────────────────────────────────────────────────────────────┐
│ reader (the HTTP handler goroutine)                        │
│   ws.Read → decodeCommand → addSubscription                │
│                            removeSubscription              │
│                            performAction                   │
│   ↳ your Channel code runs here, in command order          │
└───────────────┬────────────────────────────────────────────┘
                │ transmit(frame)          non-blocking
                ▼
        send chan outbound  cap 64
                │
┌───────────────▼────────────────────────────────────────────┐
│ writer                                                     │
│   ws.Write(ctx, MessageText, frame)  10s timeout per frame  │
│   out.final → closeSend() and return                       │
└────────────────────────────────────────────────────────────┘
                ▲
                │ transmit(frame)
    heartbeat ──┤ pub/sub handler ──┤ periodic timer
```

**Why commands are handled inline on the reader** rather than dispatched to a pool: it makes
per-connection ordering automatic and removes the need for a lock on connection state. Rails
needs its worker pool because Ruby threads are expensive; a goroutine per connection is not.
The cost is that a slow `Perform` blocks that one connection's reader — and only that one.

**The queue is bounded, and full means gone.** `coax/conn.go:enqueue`:

```go
select {
case c.send <- out:            // room: queued
case <-c.ctx.Done():           // already closing: drop silently
default:                       // full: the client is not reading
    c.logger.Warn(…)
    c.closeNow()
}
```

Rails buffers without limit, so one client that stops reading grows the process. Here it is
disconnected. `SendBuffer` (default 64) is the number of frames of slack before that verdict.

**`outbound.final` is how a disconnect message survives its own connection closing.** The
frame carrying a `disconnect` is queued with `final: true`; the writer closes the socket
*after* writing it. Without that flag, `close()` would have to close the socket itself and
would race the write it just queued — the client would lose the explanation.

**`closeNow` vs `closeSend`.** Both cancel the connection context and are idempotent through
one `sync.Once`. `closeNow` drops the socket (`ws.CloseNow`); `closeSend` sends a WebSocket
close frame first (`ws.CloseSend`, status 1000). The graceful path uses `closeSend` so a
browser reports `onclose` rather than an error; the failure paths use `closeNow` because there
is nothing worth saying.

---

## 4. The `ws` layer

A standalone RFC 6455 implementation. Conformance: Autobahn 301/301 on the cases that apply
(`ws/testdata/autobahn/summary.json`, committed), plus `FuzzFrameParse`.

### 4.1 Handshake

`ws/accept.go:Accept`, in order:

1. `verifyUpgrade` — `GET`, `Connection: Upgrade` and `Upgrade: websocket` as *tokens* (not
   whole-string comparisons, since browsers send `keep-alive, Upgrade`), `Sec-WebSocket-Version:
   13`, and a `Sec-WebSocket-Key` that base64-decodes to exactly 16 bytes. Failure → 400.
2. `verifyOrigin` unless `InsecureSkipVerify` — failure → 403.
3. `selectSubprotocol` — first offered value that the server also supports, or empty.
4. `http.NewResponseController(w).Hijack()` — HTTP/1.1 only, by nature.
5. **Clear the deadlines** the HTTP server set. Missing this is a classic: a connection meant
   to live for days inherits `ReadTimeout` and dies quietly.
6. Write the 101 by hand: `Sec-WebSocket-Accept` is `base64(sha1(key + keyGUID))`.
7. `newConn` keeps `brw.Reader` rather than making a fresh one, because a client may have
   pipelined frames immediately after the request and those bytes are already buffered.

`Dial` is the mirror image, and exists so tests drive a real client rather than a stub.

### 4.2 Frame parsing

`ws/frame.go`. `readFrameHeader` reads 2 bytes, then the extended length and mask key as
needed, into a caller-supplied 14-byte buffer — so parsing a header allocates nothing.

`frameHeader.validate` enforces, before any payload is read:

- control frames: FIN set, payload ≤ 125
- RSV bits: checked against `rsvAllowed`, an empty set today, rather than asserted zero — so
  `permessage-deflate` becomes an addition rather than a rewrite
- mask direction: client→server frames must be masked, server→client must not
- **minimal length encoding**: a 7-bit length must not be re-encoded as 16- or 64-bit
- 64-bit lengths must have the high bit clear

Size limits are applied to the *header* (`readFrameHeader` caller, then `Reader` for the
message total), so a hostile length prefix is rejected before anything is allocated.

### 4.3 Reading a message

`Reader(ctx)` returns `(MessageType, io.Reader, error)` and is the zero-allocation path;
`Read(ctx)` is `io.ReadAll` over it and allocates per message, which its doc comment says.

The loop in `Reader`: drain any abandoned previous message so framing stays correct, then read
headers until a data frame appears, handling control frames inline (§4.4). A continuation frame
with no message in progress is a protocol error.

`msgReader.Read` handles the rest: unmask in place, feed the UTF-8 validator for text, and
cross fragment boundaries via `nextFragment`, which itself must tolerate control frames
interleaved between fragments (RFC 6455 §5.4). `active` — rather than `done` — is the field,
so the zero value means "no message in progress"; the inverse spelling caused a real bug where
the first `Reader()` call tried to drain a message that never existed.

### 4.4 Control frames and the close state machine

`handleControl`:

| Frame | Action |
|---|---|
| ping | write a pong with the same payload, under `writeMu`, then keep reading |
| pong | ignore |
| close | parse the payload, echo the peer's status back, close the socket, report a `CloseError` |

`closeMu` + `closeErr` are the whole state machine: the first goroutine to record a cause wins,
and every later call reports it. Four ways in:

- `Close(code, reason)` — write close, drain until the peer's close or a 5s grace period,
  then close. **Reads**, so it must not be used while another goroutine is reading.
- `CloseSend(code, reason)` — write close, close. For the common server shape where a
  dedicated reader owns reads.
- `CloseNow()` — close, say nothing.
- `failRead(err)` — the read path found a protocol error: send the matching close code if the
  error carries one (`closeSentinel`), then tear down.

### 4.5 Zero allocation, and what that means

Preallocated on every `Conn`: the 14-byte read header buffer, the 125-byte control buffer, the
14-byte write header buffer, the `msgReader`, the `msgWriter` (4 KiB, allocated on first use),
and the UTF-8 validator. A client connection also gets a 4 KiB mask scratch buffer, because
masking must not mutate the caller's slice.

`ws/alloc_test.go` asserts **0 allocs/op** for a streaming read and write round trip via
`testing.AllocsPerRun`. It skips under `-race`, because the race runtime allocates on its own
account — verified rather than assumed, and the build-tagged `raceEnabled` constant is how.

Benchmarks on this machine: `Reader` 116ns/64B to 919ns/8KiB, `Write` 50–92ns, `mask`
8ns/64B ≈ 8 GB/s.

### 4.6 UTF-8 validation

`ws/utf8.go`. Text messages must be valid UTF-8 *and* be rejected as soon as they cannot be —
including when a rune is split across two frames. The validator keeps at most
`utf8.UTFMax` pending bytes, uses `utf8.FullRune` to tell "incomplete prefix" from "invalid",
and `utf8.DecodeRune` to reject overlong encodings, surrogates, and out-of-range values.
`done()` reports whether a message ended mid-rune.

---

## 5. The protocol layer

`coax/protocol.go` is the whole wire format and nothing else: no I/O, no state. That is what
makes it golden-testable byte for byte (`protocol_test.go`) and fuzzable
(`FuzzDecodeCommand`).

Two shapes:

```go
type clientCommand struct {         // inbound
    Command    string `json:"command"`
    Identifier string `json:"identifier"`  // JSON *string* holding JSON
    Data       string `json:"data,omitempty"`
}

type serverMessage struct {          // outbound, all six frame kinds
    Type       string          `json:"type,omitempty"`
    Identifier string          `json:"identifier,omitempty"`
    Message    json.RawMessage `json:"message,omitempty"`
    Reason     string          `json:"reason,omitempty"`
    Reconnect  *bool           `json:"reconnect,omitempty"`
}
```

Three details that are easy to get wrong and are therefore pinned by tests:

- **`Reconnect` is a `*bool`.** `omitempty` treats `false` as empty, and a disconnect message
  must be able to carry `"reconnect":false` while every other shape omits the field.
- **The identifier is echoed verbatim.** It is the map key and the client compares it as a
  string. Re-marshalling parsed params reorders keys and every reply silently misses.
  `Subscription.Params()` therefore *derives* from the stored identifier —
  `json.RawMessage(s.identifier)` — so the two cannot drift apart.
- **`message` is nested JSON, not a string.** Only `identifier` and the inbound `data` are
  double-encoded. `json.RawMessage` passes a payload through without re-encoding it, which is
  also what makes Rails interop byte-exact.

### 5a. Two ways to dispatch an action

A channel may switch on the action itself, or register a handler per action:

```go
// one Perform, one switch — everything in one place, one place to handle errors
func (c *ChatChannel) Perform(ctx context.Context, action string, data json.RawMessage) error {
	switch action { case "speak": …; case "typing": … }
}

// or named handlers, and no Perform at all
coax.Handle(srv, "ChatChannel", newChatChannel).
	On("speak", (*ChatChannel).Speak).
	On("typing", (*ChatChannel).Typing)
```

`Handle` installs a factory that wraps the channel in an unexported `routed`, which
holds the action map and implements `Perform` as a lookup. So the registry, the
`Channel` interface, `Subscription` and the dispatch path are all untouched by the
feature — the action map lives in a closure the factory captured while it was still
empty, and `On` fills it in afterwards. `Register` and a switch keep working exactly
as before, which is what `cmd/dmexample` still demonstrates.

Precedence in `routed.Perform`: a registered handler, else the channel's own
`Perform` (the `Performer` interface) as a catch-all, else `ErrUnknownAction` naming
what *is* registered. That last case is the reason to prefer `On` — a typo reads
differently in the log from a handler that failed, which a switch cannot express
because the framework never learns the action list.

Two costs, both real: `On` mutates a map that connection goroutines later read
without a lock, so it is start-up-only by documentation rather than by
construction; and the channel type is constrained `comparable` so a factory
returning nothing is still caught, which excludes a channel that is a struct value
holding a map or a slice.

---

## 6. The pub/sub layer

```go
type PubSub interface {
    Broadcast(ctx context.Context, broadcasting string, payload []byte) error
    Subscribe(ctx context.Context, broadcasting string, h Handler) (unsubscribe func(), err error)
    Close() error
}
```

The one interface in the package, because it has real alternate implementations. The contract
beyond the signatures: `Subscribe` blocks until acknowledged; `unsubscribe` is idempotent and
safe after `Close`; `Broadcast` publishes rather than delivering locally, so a process receives
its own broadcasts back through the backend; after `Close`, both return `ErrPubSubClosed`.
`coax/pubsubtest` checks all of it, and both adapters run it.

### 6.1 Why `Subscribe` blocks

Rails cannot confirm a subscription synchronously: `pubsub.subscribe` is asynchronous, so
`Channel::Base` withholds the confirmation behind a `Concurrent::AtomicFixnum` that starts at 1
and is decremented by each stream's callback (`channel/base.rb:172` and `:255`).

Making `Subscribe` block until the backend acks deletes the entire mechanism. The ordering
falls out instead: `Subscribed` calls `StreamFrom`, which returns only once Redis has
confirmed, and only then does `addSubscription` queue `confirm_subscription`. It is *impossible*
to confirm a subscription before the backend is listening. Cost: one Redis round trip on that
connection's reader, bounded by the context.

`TestConfirmationWaitsForTheBackend` proves it with a backend that withholds its ack: no
confirmation arrives while it is held, and a broadcast published immediately after it is
released still arrives.

### 6.2 `MemoryPubSub`

`map[string]map[*subscriber]struct{}` under an `RWMutex`. Two decisions worth naming:

- **Handlers are wrapped in a `*subscriber`**, so two identical handlers are still two
  subscribers. One connection subscribed twice to a room must be told twice, matching Rails.
- **Delivery iterates under the read lock** rather than snapshotting, which keeps broadcast
  allocation-free — and is why `Handler` documents that it must not call back in.

Dropping the last subscriber deletes the broadcasting, so a server does not accumulate an empty
map per room it has ever served (`SubscriberMap` does the same,
`subscription_adapter/subscriber_map.rb:33`).

It is also the one adapter that does not scale out, which is why it has a named constructor:
`NewMemoryPubSub()` at a call site is visible in review, where a silent default would not be.

### 6.3 `redispubsub`

Hand-rolled RESP2, no dependencies. The protocol is five types; the work is the operations.

**Two connections, because a subscribed Redis connection cannot be used for anything else.**
One is in subscribe mode and read by a single goroutine; the other publishes one command at a
time under `pubMu`.

**The subscription map is the source of truth, and it is replayed onto every new connection.**
This single decision removes error handling everywhere else: a failed `SUBSCRIBE` write needs
no recovery, because either the entry is in the map — and so will be replayed — or it has just
been removed from it. Only the reader declares a connection dead.

Per-broadcasting state carries the acknowledgement:

```go
type broadcasting struct {
    subs   map[*subscriber]struct{}
    active bool          // Redis confirmed on the *current* connection
    ready  chan struct{} // closed on confirmation; replaced when the connection drops
}
```

`Subscribe` adds itself, sends `SUBSCRIBE` only if it is the first subscriber, then waits on
`ready`. Replacing `ready` on disconnect is what makes a subscription that was in flight when
the connection died keep waiting and be satisfied by the *new* connection's confirmation,
rather than returning as though it had succeeded (`TestSubscribeSurvivesAReconnect`).

Three more things that are easy to get wrong:

- **A subscribed connection cannot do request/response.** Confirmations and pushes share the
  socket and are told apart only by the first array element, so acks are correlated in the read
  loop rather than awaited after a write.
- **Half-open detection.** `PING` on a timer plus a read timeout; `withDefaults` refuses a
  `ReadTimeout` at or below `PingInterval`, since that combination would kill every idle
  connection on schedule.
- **One publish retry, but only on a reused connection.** Redis closes idle clients, so the
  first publish after a quiet spell routinely lands on a dead socket. A connection just opened
  gets no second chance, and an error *reply* is never retried — the server answered.

`RESP` parsing bounds everything before allocating: bulk length against `MaxReplySize`, array
length against `maxArrayLen`, and nesting against `maxDepth` (without which `*1` repeated
recurses once per three bytes of input). `parseInt` is hand-written to avoid allocating a
string per reply and to reject anything Redis would never send.

---

## 7. The internal channel

Each identified connection subscribes to a broadcasting derived from its own identifiers, so
any process can reach it (`coax/internal.go`).

```
Identifiers{"current_user": "42", "tenant": "acme"}
   → "action_cable/current_user=42:tenant=acme"
```

Sorted by key, so every process derives the same name from the same identifiers regardless of
map iteration order. A connection with no identifiers gets no internal channel and cannot be
reached remotely — there is nothing to address it by.

`Server.Disconnect(ctx, ids, reconnect)` publishes `{"type":"disconnect","reconnect":…}` to
that name. The message goes through the backend and comes back, so a process disconnects its
own connections the same way it disconnects anyone else's. An absent `reconnect` field means
`true`, matching Rails' `message.fetch("reconnect", true)`.

Subscribed **before** the welcome, so a disconnect published the instant a client believes it
is connected cannot be missed. Bounded by `internalChannelTimeout` (5s) and non-fatal: a
connection that cannot be reached remotely is worse than one that can, and much better than no
connection at all.

---

## 8. Shutdown

Two exits, and the difference matters to clients:

| | `Close()` | `Shutdown(ctx)` |
|---|---|---|
| Client is told | nothing; the socket breaks | `disconnect`, `server_restart`, `reconnect: true` |
| Waits | no | for connections, bounded by ctx |
| WebSocket close | abrupt | status 1000 after the message |
| For | tests, no time left | rolling deploys |

The subtle part is refusing new connections. Without it a shutdown could never finish, and
`sync.WaitGroup` would see `Add` after `Wait` returned. `add()` answers under the same
`RWMutex` that `beginShutdown` takes to close the `stop` channel, so a connection arriving at
that moment is either registered before the wait begins or turned away — there is no window.
A connection turned away is told the same thing every other connection was told.

On expiry the remainder are dropped with `closeNow` and `ctx.Err()` is returned: a client that
has stopped reading cannot hold a deployment open.

---

## 9. Configuration surface

`coax.Options` — the zero value is usable and accepts every same-origin connection with no
identifiers.

| Field | Default | Notes |
|---|---|---|
| `Authenticate` | accept everyone, no identifiers | runs after the handshake, so a rejection is a frame |
| `AllowedOrigins` | none | host patterns, `path.Match`, case-insensitive; own host always allowed |
| `DisableOriginCheck` | false | any origin; only safe when the endpoint has its own credentials |
| `Logger` | `slog.Default()` | reconnects and drops are warn, lifecycle is debug |
| `PubSub` | `NewMemoryPubSub()`, owned | supplied ones are not closed by the server |
| `SendBuffer` | 64 | frames of slack before a slow client is dropped |
| `HeartbeatInterval` | 3s | a protocol constant, not a tuning knob: the JS client's stale threshold is twice it |

`ws.AcceptOptions`: `Subprotocols`, `OriginPatterns`, `InsecureSkipVerify`, `MaxFrameSize`
(1 MiB), `MaxMessageSize` (32 MiB).

`redispubsub.Options`: `Address`, `Username`, `Password`, `TLS`, `Dialer`, `Logger`,
`DialTimeout` 5s, `WriteTimeout` 5s, `PingInterval` 30s, `ReadTimeout` 90s, `MinRetryBackoff`
100ms, `MaxRetryBackoff` 5s, `MaxReplySize` 8 MiB. `ParseURL` accepts `redis://` and
`rediss://`, and accepts-and-ignores a database number, since pub/sub is not database-scoped.

---

## 10. Failure modes, as designed

| Failure | Behaviour |
|---|---|
| Not an upgrade request, or bad origin | plain `404 Page not found`, byte-identical to Rails. No frame. |
| Malformed `Sec-WebSocket-Key` | `ws` answers 400 — a deliberate divergence from Rails' 404 (§11) |
| Authentication fails | handshake **succeeds**, then `disconnect`/`unauthorized`/`reconnect:false` |
| Connection arrives during shutdown | `503 Server shutting down`, or `server_restart` if it got past the gate |
| Unknown channel, duplicate subscribe, unknown subscription, unsubscribe | logged, **nothing sent** — Rails answers an unintelligible command with silence |
| `Subscribed` returns an error | `reject_subscription`; streams and timers it started are stopped |
| `Perform` or an action handler returns an error | logged, client told nothing |
| Action with no handler and no `Perform` | logged at warn with the registered names (`ErrUnknownAction`), client told nothing |
| Periodic callback returns an error | logged, the timer keeps running |
| Send queue full | connection dropped, warn logged |
| Frame write fails or times out (10s) | connection dropped |
| Non-text frame from a client | logged, connection survives |
| Backend unreachable at subscribe time | `StreamFrom` fails → subscription rejected; the internal channel logs a warning and the connection continues |
| Redis connection lost | reconnect with backoff, replay the subscription map; messages published during the gap are lost, as in Rails |
| Broadcast to a broadcasting with no subscribers | not an error — the normal state of a quiet room |

---

## 11. Divergences from Rails

Deliberate, each with the reason. Nothing here is an accident.

| Area | Rails | Here | Why |
|---|---|---|---|
| Write buffer | unbounded | bounded, drop the client | one slow client must not grow the process |
| Rejection | `reject` inside `subscribed` | error return from `Subscribed` | Ruby's `subscribed` cannot return a value; two ways to say no is a smell |
| `unsubscribed` on rejection | called | not called | a rejected subscription never existed; its streams are still stopped |
| Deferred confirmation | atomic counter | blocking `Subscribe` | correct by construction (§6.1) |
| `stream_from` twice | delivers twice | no-op | duplicating within one subscription is a bug; two subscriptions still each get a copy |
| Internal channel name | sorted GlobalIDs | sorted `key=value` pairs | our identifiers are arbitrary strings; without keys `{"user":"42"}` and `{"room":"42"}` would disconnect each other |
| Remote disconnect interop | — | does not cross to Rails | follows from the line above. Broadcasts, which is what interop means, do |
| Failed upgrade | 404 | 404, but 400 for a malformed key | `ws` is a standalone library and answers on its own terms |
| Action dispatch | reflection on public methods | a switch in `Perform`, or handlers registered by name with `Handle`/`On` | no reflection either way; `On` takes method expressions, so a renamed method is a compile error rather than an action that never fires |
| `stream_for(model)` | GlobalID naming | absent | no GlobalID in Go; inventing a convention would be worse than composing a name |
| Logging | `TaggedLoggerProxy` | `slog` with `remote` attached | structured logging is the platform's answer |

---

## 12. What proves what

The claims this implementation makes, and the thing that would catch a regression.

| Claim | Evidence |
|---|---|
| RFC 6455 conformance | Autobahn 301/301, report committed to `ws/testdata/autobahn/` |
| Compatible with the real client | `coax/jsclient_test.go` drives unmodified `@rails/actioncable` under Node: connect, confirm, reject, perform, broadcast to two clients, both `reconnect` values |
| Zero-allocation streaming | `ws/alloc_test.go`, `testing.AllocsPerRun` == 0 |
| No parser panics on hostile input | `FuzzFrameParse`, `FuzzDecodeCommand`, `FuzzReadValue` |
| Wire format matches Rails | golden byte tests in `protocol_test.go`; `TestRedisEndToEnd` publishes Rails' exact shape and compares the delivered frame byte for byte |
| Backends behave identically | `coax/pubsubtest` run against both |
| Redis survives failover | `TestRedisResubscribesAfterAKilledConnection` uses `CLIENT KILL TYPE pubsub` |
| No goroutine leaks | `runtime.NumGoroutine` with a stabilising retry loop, in connection, timer and load tests |
| Scales to 10k connections | `TestManyIdleConnections`: sweep 27.9ms median, 154µs of it ours |

`make` runs fmt, vet and race. `make conformance` runs the three external gates (Autobahn, the
JS client, real Redis). `make everything` adds fuzzing and the load test.
