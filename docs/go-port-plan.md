# Go Port of Action Cable — Implementation Plan

Target: a Go framework that is wire-compatible with `actioncable-v1-json` — the unmodified
`@rails/actioncable` JS client works against it — with **zero runtime dependencies**,
including a hand-rolled RFC 6455 implementation and a hand-rolled Redis pubsub client.

Reference — the original: [internals](./action-cable-internals.md),
[protocol](./action-cable-protocol.md), [glossary](./action-cable-glossary.md).
What was built: [implementation](./implementation.md), [flows](./flows.md).

---

## 0. Scope (decided)

Standalone Go framework speaking `actioncable-v1-json`. Compatibility target is the Rails
**JS client**; Rails-the-app is not a dependency.

Three goals, in tension, and how they're resolved:

| Goal | Consequence |
|---|---|
| **Learning by rebuilding** | Every non-obvious function carries a cross-reference to the Rails source it mirrors (`// ← actioncable/lib/action_cable/server/socket.rb:124`). Deliberate divergences are marked as such, not silently "improved". |
| **Publishable / robust** | Hand-rolled code held to the same bar as the dep it replaces: Autobahn conformance for the WebSocket layer, fuzzing on all wire parsers, hard limits before allocation, no goroutine leaks. Idiomatic Go throughout (§1a). |
| **Horizontal scale** | A real cross-process pubsub backend (Redis) ships in v1, and `PubSub` is a 3-method interface so any other backend is ~40 LOC (§9). |

| | Decision |
|---|---|
| Auth | app-supplied `Authenticate(*http.Request) (Identifiers, error)` hook. No Rails cookie decryption. |
| Config | functional options + env. No `config/cable.yml`. |
| Identifiers | any stable string the app picks. No `to_gid_param` format. |
| Dependencies | **none at runtime.** `go.mod` has no `require` block. Docker (Autobahn) and Node (JS-client conformance) are test-time tools, not deps. |
| `ws/` package | a **standalone, independently usable WebSocket library** that happens to live in this repo — public API designed on its own merits, not shaped by Action Cable's needs (§1b). |
| Backends | `PubSub` is the extension point. In-memory + Redis in v1; Postgres and a Solid-Cable-style DB poller are follow-ups (§9), and a reusable conformance harness makes third-party adapters provable. |
| `permessage-deflate` | follow-up (§9). A **known** gap, not an oversight — but the framing layer must leave room for extension negotiation rather than assuming RSV bits are always zero. |

### 1a. Go conventions this holds itself to

Not a style essay — the specific things that get reviewed:

- Package names are short, lowercase, no stutter: `ws.Conn`, `ws.Accept`, `coax.Server` — never
  `ws.WSConn` or `coax.CableServer`.
- `context.Context` is the first parameter of anything that blocks or does I/O.
- Accept interfaces, return concrete types. `PubSub` is the one interface we define, because it
  has real alternate implementations — no single-implementation interfaces anywhere else.
- Errors: wrapped with `%w`, sentinel `var ErrX = errors.New(...)` only for conditions callers
  actually branch on (`ws.ErrClosed`), typed errors only where callers need fields
  (`ws.CloseError{Code, Reason}`).
- Every exported identifier has a doc comment starting with its own name. Each package has a
  `doc.go` when the overview outgrows one paragraph.
- Zero values useful where cheap; `Options` structs with sane zero defaults over long parameter
  lists; functional options only where genuinely open-ended.
- Table-driven tests, `t.Run` subtests, `testing.TB` helpers with `t.Helper()`, `t.Cleanup` over
  defer-in-test.
- No `any` where a concrete type or generic works. `json.RawMessage` for pass-through payloads.
- `go vet` clean, `gofmt`/`gofumpt` clean, exported API vetted with `go doc` before each phase
  closes.
- Nothing exported that isn't needed by a caller — export is a promise.

### 1b. `ws` as a standalone library

You want it usable by other projects, which changes three things versus my earlier
`internal/ws` sketch:

- **Not `internal/`, and not under `coax/`.** Import path is `<module>/ws`, a peer of `coax/`,
  so it reads as a library rather than a detail. Go compiles per package, so importing
  `<module>/ws` pulls in none of the coax framework — a single module already gives us
  "independently usable" at zero tooling cost. Promoting it to its own module or repo later is a
  mechanical move; doing it now buys nothing and costs `go work` friction.
- **API designed on its own merits**, and it should look like what Go WebSocket libraries have
  converged on, because that shape is right: `ws.Accept(w, r, *ws.AcceptOptions)`,
  `ws.Dial(ctx, url, *ws.DialOptions)`, `conn.Read(ctx) (MessageType, []byte, error)`,
  `conn.Write(ctx, MessageType, []byte)`, `conn.Close(code, reason)`, plus
  `conn.Reader/Writer` for streaming. Action Cable's needs get met *through* that API, never by
  bending it.
- **Its own README, `doc.go`, runnable examples, and the committed Autobahn report** as evidence.
  A WebSocket library without a conformance report is not a credible publish.

> Module path: **set** — `github.com/igor-dmscn/coax-claude-impl`. The framework package is `coax`, a peer of
> `ws/`, so the layered structure stays visible: `coax` imports `ws`, never the reverse.

### 1c. Zero-allocation discipline (`ws`)

Matching `coder/websocket`'s "zero alloc reads and writes". First, what that claim actually
covers — from their source, `Read` is `io.ReadAll(c.Reader(ctx))`, which allocates per message.
The zero-alloc property belongs to the **streaming** API; a method that returns `[]byte` cannot
have it, because the bytes must live somewhere. They achieve it with pooled buffers
(`internal/bpool`) plus masking assembly (`mask_amd64.s`, `mask_arm64.s`).

**API consequence, designed in from Phase 1:** the streaming pair is primary and zero-alloc, the
`[]byte` pair is a documented convenience built on it.

```go
// zero-alloc path (steady state, established conn)
func (c *Conn) Reader(ctx context.Context) (MessageType, io.Reader, error)
func (c *Conn) Writer(ctx context.Context, typ MessageType) (io.WriteCloser, error)

// convenience; allocates per message, and says so in its doc comment
func (c *Conn) Read(ctx context.Context) (MessageType, []byte, error)
func (c *Conn) Write(ctx context.Context, typ MessageType, p []byte) error
```

What "zero alloc" requires concretely:

- All per-connection scratch preallocated as `Conn` fields: read/write buffers, 14-byte frame
  header, 4-byte mask key, 125-byte close-frame scratch. Never locals in the hot path.
- `bufio.Reader`/`Writer` sized once per conn, `sync.Pool`'d across conns so idle memory stays sane.
- Masking XOR **in place, word-at-a-time** (8 bytes/iteration) in pure Go. No `unsafe`, no assembly
  in v1 — asm is a measured follow-up (§9.4) with the pure-Go version kept as the reference and a
  differential test between them.
- Streaming UTF-8 validation carrying state across fragments — no re-buffering.
- No `string([]byte)` / `[]byte(string)` in hot paths. Handshake header parsing works on string
  slices (slicing a string doesn't allocate) with `strings.EqualFold`, not `strings.Split`.
- No closures capturing per-message state, no `any` boxing — both heap-allocate.
- Hot-path errors are preallocated sentinels; `fmt.Errorf` only off the hot path.

**Proof, as a regression gate rather than a number someone eyeballs** — asserted in a *test*:

```go
allocs := testing.AllocsPerRun(100, func() { echoOneMessage(t, conn) })
if allocs != 0 { t.Fatalf("got %v allocs/op, want 0", allocs) }
```

Plus `-benchmem` benchmarks for the absolute numbers, and `go build -gcflags=-m` review whenever
something escapes unexpectedly.

**Honest scope limit:** this is a property of `ws`, not of the whole coax path. `encoding/json`
allocates, so `coax` will allocate per message regardless. The related win available there is
different in kind: **marshal a broadcast once and share the `[]byte` across all N subscribers**
(Rails does the same — `Broadcaster` encodes once), and write per-connection frames through a
`json.Encoder` over a pooled `bytes.Buffer`. Stated so "zero alloc" isn't read as end-to-end.

Rails-interop-over-shared-Redis stays as a *test fixture* (cheapest proof the wire format is
right), not a supported deployment.

---

## 1. What the port deletes

The port is mostly subtraction. Six Ruby classes exist to work around thread cost and Rack's
synchronous contract; Go's runtime and `net/http` make them unnecessary.

| Ruby | Go | Why it goes |
|---|---|---|
| `Server::StreamEventLoop` (nio4r selector) | — | goroutine scheduler is the event loop |
| `Socket::Stream` (hijack IO, write buffer) | `chan []byte` + writer goroutine | our `ws.Conn` owns the socket |
| `Socket::ClientSocket` (ready-state machine) | `ws.Conn` state | folded into our own WS layer |
| `Socket::MessageBuffer` | — | run auth *before* the read loop starts; nothing to buffer |
| `Server::Worker` (thread pool + `:work` callbacks) | goroutine per connection | goroutines are cheap |
| `Server::ThreadedExecutor` (`streamer` pool) | goroutines + `time.Ticker` | ditto |
| deferred-confirmation counter (`AtomicFixnum`) | synchronous `Subscribe() error` | blocking subscribe *is* the ack — §4 |
| `Server::Base#restart` / reloader hook | `context` cancellation | no code reloading in Go |
| `TaggedLoggerProxy` | `*slog.Logger` + `With()` | stdlib already does per-value loggers |
| Rack hijack / `[-1,{},[]]` / `async.callback` | `http.Hijacker` | same trick, one layer down |
| `ActiveSupport::Notifications` | `slog` now; explicit hooks if a consumer appears | YAGNI |

Two Ruby behaviors are deliberately **not** reproduced, because they're flaws:

1. **Unbounded write queue** (`Socket::Stream@write_buffer`) → bounded `send` channel; a client
   that can't keep up is closed. Divergence on purpose.
2. **No per-connection message ordering** (each frame is an independent worker task) → we
   handle commands inline on the reader goroutine, so ordering is guaranteed. Stricter, so no
   client can notice.

Rough size: ~1,300 LOC core + ~700 WebSocket + ~250 Redis. Ruby's actioncable `lib/` is ~2,600.

---

## 2. Package layout

```
ws/                        standalone WebSocket library (§1b) — no coax imports
  doc.go  accept.go  dial.go  conn.go  frame.go  mask.go  close.go  utf8.go
  example_test.go  frame_fuzz_test.go  README.md
  testdata/autobahn/       committed conformance report

coax/                      the Action Cable framework — imports ws/
  server.go        Server: http.Handler, registry, heartbeat, shutdown
  conn.go          Connection: reader/writer goroutines, transmit, close
  protocol.go      frame types, message-type constants, encode/decode
  channel.go       Channel interface, registry, Subscription, action dispatch
  streams.go       StreamFrom/StopStream, default handler, Server.Broadcast
  timers.go        Periodically, and the one place a subscription is released
  pubsub.go        PubSub interface + in-memory implementation
  internal.go      internal channel + remote disconnect
  options.go       Options
  ↳ built as planned, except that Subscription lives in channel.go rather than its
    own file, and timers.go was not foreseen

coax/pubsubtest/          reusable conformance harness: pubsubtest.Run(t, factory)
coax/redispubsub/         hand-rolled RESP2 adapter
cmd/example/               demo server driven by the conformance tests
```

Dependency direction is one-way: `coax` imports `ws`, never the reverse. `pubsubtest` exists so
a third-party adapter — ours or someone else's — can prove itself against the same suite the
built-in ones pass. It's the tests we're writing anyway, parameterized over a constructor.

---

## 3. Concurrency model

Two goroutines per connection:

```
reader  ── ws.Read ──► decode ──► dispatch command inline ──► (may push to send)
writer  ── <-send ──► ws.Write                          [sole writer; RFC requires it]
```

Process-wide: one `time.Ticker(3s)` heartbeat sweeping a registry snapshot; one goroutine per
pubsub adapter draining the backend; one per periodic-timer subscription.

Consequences:

- Commands handled inline on the reader ⇒ the per-connection `subscriptions` map needs **no
  mutex**. Only the server registry and the pubsub subscriber map are shared; both get a
  `sync.RWMutex`.
- The writer goroutine is the only WS writer. `transmit` blocks on a bounded channel, never on
  the socket.
- `send chan []byte` cap ~64. Full ⇒ close the connection, log it.
- Stream handlers run on the pubsub goroutine. The default handler only marshals + non-blocking
  sends, so it cannot block. Custom handlers must not block — documented, and marked:
  `// ponytail: handlers run on the pubsub goroutine; add a per-conn dispatch goroutine if a real handler needs to block`

---

## 4. The one non-obvious simplification: deferred confirmation

Rails withholds `confirm_subscription` behind an atomic counter because `pubsub.subscribe` is
async (internals §5). Make `PubSub.Subscribe` **block until the backend acks** and the whole
mechanism disappears:

```go
// Shipped in Phase 4. The topic parameter is named `broadcasting`, Rails' word for
// it, because `channel` already means the Channel interface in this package.
type PubSub interface {
    Broadcast(ctx context.Context, broadcasting string, payload []byte) error
    Subscribe(ctx context.Context, broadcasting string, h Handler) (unsubscribe func(), err error)
    Close() error
}
```

`Subscribe` returning nil means the backend has confirmed. So `Subscribed()` → `StreamFrom()`
blocks → then confirm. Correct ordering, zero counters, and it is *impossible* to confirm
before the backend is listening. Cost: one Redis round-trip blocks that connection's reader,
bounded by the context deadline.

**Built and tested** (`TestConfirmationWaitsForTheBackend`): a `PubSub` whose `Subscribe` blocks
until released produces *no* confirmation while it is held, then the confirmation, then a
broadcast published immediately after arrives. The counter, the atomic and the deferred callback
Rails needs for this do not exist here.

---

## 5. Public API sketch

```go
type Authenticator func(*http.Request) (Identifiers, error)
type Identifiers map[string]string   // {"current_user": "1"}

// A channel instance == one subscription.
type Channel interface {
    Subscribed(context.Context) error
    Unsubscribed(context.Context)
    Perform(ctx context.Context, action string, data json.RawMessage) error
}

srv.Register("ChatChannel", func(s *coax.Subscription) coax.Channel {
    return &ChatChannel{sub: s}
})

s.StreamFrom(ctx, "room_1")   // blocking-acked
s.Transmit(payload)           // {"identifier":…,"message":payload}
s.Params() / s.Identifier() / s.ChannelName() / s.Connection().Identifiers()

srv.Broadcast(ctx, "room_1", payload)
srv.RemoteConnections(Identifiers{"current_user": "1"}).Disconnect(ctx, false)
```

**Action dispatch**: one `Perform` with a `switch` in the channel. No reflection, no map
allocation, boring at 3am. `action == ""` dispatches to `receive`, per protocol.
`// ponytail: switch dispatch; swap for map[string]func or reflection only past ~10 actions`

**Rejection is the error return, not a second mechanism** (built, Phase 3). The sketch had both
`Subscribed() error` and `s.Reject()`; two ways to say no is a smell, and Rails only has the one
because Ruby's `subscribed` cannot return a value. A non-nil error from `Subscribed` sends
`reject_subscription`; there is no `Reject()`. Also no `ErrRejected` sentinel — nothing branches
on it, it would only pick a log level.

**Identifier discipline** — the single easiest thing to get wrong. Store the client's raw
string and echo *that*; never re-marshal parsed params, since Go emits a different key order
and every reply would miss:

```go
type Subscription struct {
    identifier string // raw, byte-for-byte from the client — the map key
}

// The identifier *is* the params: a JSON object as a string. No second field.
func (s *Subscription) Params() json.RawMessage { return json.RawMessage(s.identifier) }
```

---

## 6. Phases

Each phase ends runnable and independently verifiable.

**Status:** Phase 0 ✅ · Phase 1 ✅ (Autobahn 301/301, 0 allocs/op) · Phase 2 ✅ (real
`@rails/actioncable` client verified) · Phase 3 ✅ (JS client `connected()`, `rejected()`,
`perform`, guarantor forgets) · Phase 4 ✅ (two JS clients, one broadcast, both received) ·
Phase 5 ✅ (real Redis 7 + 8, CLIENT KILL recovery, 2.1M fuzz execs) ·
Phase 6 ✅ (JS client honours both reconnect values; remote disconnect across two servers on real
Redis) · Phase 7 ✅ (timers leak-free; 10k connections measured) — **all phases done**.

Everything the plan set out is built and verified. What was added beyond it: `coax/pubsubtest`
(§2 listed it, no phase owned it), `cmd/example`, a `Makefile` for the slow gates, and
`ws.CloseSend`. What was left out is in §9, each with its workaround.

**Deviations from the phase-5 plan, both deliberate:**

- **`coax/pubsubtest` was built here, not left for later.** A conformance suite is worth
  writing when the second implementation appears, and the Redis adapter passing the exact suite
  the in-memory one passes is stronger evidence than any adapter-specific test.
- **No Rails process in the interop test.** Rails' Redis contract is `PUBLISH <broadcasting>
  <json>`, which a raw publish reproduces exactly; booting Rails would test the `redis` gem, not
  us. `TestRedisEndToEnd` publishes that shape and asserts the client's frame byte for byte.
  Running against a real Rails app is a follow-up (§9.5) if it ever earns itself.

### Phase 0 — protocol types
The 3 inbound commands, 6 outbound frame shapes, `INTERNAL` constants copied from
`lib/action_cable.rb:61`.
→ **verify:** golden tests round-tripping every frame in [protocol.md §2–3](./action-cable-protocol.md),
including double-encoded `identifier`/`data` and the absent-`action` case.

### Phase 1 — the `ws` library (RFC 6455)
Shipped as a standalone library per §1b, so it gets its own API review, README, `doc.go`, and
runnable examples — not just enough surface for `coax` to work.

Handshake (`base64(sha1(key+GUID))`, header validation, subprotocol selection, `http.Hijacker`,
hand-written 101). Framing: FIN/RSV/opcode, MASK, 7/16/64-bit length, XOR unmask, unmasked server
frames. Fragmentation with interleaved control frames. Auto-pong. Close handshake with 2-byte BE
code. Streaming UTF-8 validation across fragment boundaries. Hard `MaxFrameSize`/`MaxMessageSize`
enforced **before allocating**. Read/write deadlines. `Dial` for a real client, not a test stub.

RSV bits are *validated against a negotiated-extension set* (empty for now) rather than asserted
zero, and `AcceptOptions` reserves a place for extension negotiation — retrofitting that into a
framer that hardcodes "RSV must be 0" is invasive, and deflate is a live follow-up (§9).

→ **verify:** (a) `go test -fuzz=FuzzFrameParse` — no panic, no allocation blowup on malformed
headers; (b) **Autobahn**: `docker run crossbario/autobahn-testsuite` fuzzingclient against our
server *and* fuzzingserver against our `Dial` — every non-extension case `OK`/`NON-STRICT`,
deflate cases `UNIMPLEMENTED`, report committed to `ws/testdata/autobahn/`; (c) table-driven unit
tests for every close-code and UTF-8 rejection path; (d) **`testing.AllocsPerRun` asserting 0
allocs/op** for a `Reader`/`Writer` echo round-trip, per §1c, plus `-benchmem` numbers recorded;
(e) `go doc ./ws` reads like a library someone else could pick up.

### Phase 2 — connection lifecycle
`http.Handler`: upgrade, origin check, non-upgrade/bad-origin ⇒ `404 text/plain "Page not
found"`; `Authenticator`; failure ⇒ `{"type":"disconnect","reason":"unauthorized","reconnect":false}`
then close 1000. Reader+writer goroutines, `welcome`, registry, 3s `ping`.
→ **verify:** Go client asserting the exact sequence; **plus the real `@rails/actioncable`**
under Node (global `WebSocket` since Node 22) logging `welcome` + two pings. Assert the 404
body/content-type byte-exactly, and that auth failure is a *frame*, not an HTTP status.

### Phase 3 — subscriptions, channels, actions
Command dispatch; channel registry; `confirm_subscription`/`reject_subscription`; `Perform`
with `action` defaulting to `receive`; duplicate subscribe logged+ignored; unknown
channel/command/subscription logged and **silent to the client**.
→ **verify:** JS client gets `connected()`; a rejecting channel fires `rejected()`;
`perform("speak", …)` reaches the handler; the `SubscriptionGuarantor` stops retrying (no
repeat after 1s).

### Phase 4 — pubsub interface + in-memory + streams
Interface (§4), in-memory impl with first/last-subscriber transitions,
`StreamFrom`/`StreamFor`/`StopStream`/`StopAllStreams`, `Server.Broadcast`, auto-stop on
unsubscribe/close.
→ **verify:** two JS clients on one broadcasting both receive; after `unsubscribe()` the
ex-subscriber gets nothing and the subscriber map dropped the key (no leak); one connection with
two subscriptions on the same broadcasting receives it twice — matching Rails.

### Phase 5 — Redis adapter (hand-rolled RESP2)
`redispubsub`: two conns (one publish, one subscribe), RESP2 writer/reader, `PUBLISH`/
`SUBSCRIBE`/`UNSUBSCRIBE`, `Receive` as the subscribe ack, reconnect with backoff +
resubscribe-from-map, read deadlines, optional TLS (`crypto/tls`) and `AUTH`, bounded reply
buffer.
→ **verify:** `FuzzRESPParse`; integration test gated on `REDIS_URL` (skips here — no local
Redis) covering two Go processes on one Redis; **Rails-interop fixture**: `ActionCable.server.broadcast`
from a real Rails process reaches a client on the Go server and vice versa. Kill Redis
mid-test, assert resubscription.

### Phase 6 — internal channel, remote disconnect, shutdown
Per-connection subscribe to `action_cable/<connection_identifier>`; `{"type":"disconnect"}` ⇒
close with reason `remote`; `RemoteConnections().Disconnect()`; graceful shutdown sending
`server_restart` (`reconnect: true`) to all, then draining under a deadline.
→ **verify:** two Go processes on one Redis — disconnecting a user on A closes their socket on
B. `SIGTERM` ⇒ every JS client logs `server_restart` and reconnects.

**Done, with one substitution.** Both criteria are covered:
`TestRedisRemoteDisconnect` runs two `coax.Server`s with separate Redis connections, sharing
nothing else, and disconnects a user on the one that never held them. For the second, the JS test
asserts through the client's own `disconnected({willAttemptReconnect})` callback — `false` after a
remote disconnect, `true` after a shutdown — rather than sending an actual `SIGTERM`, which would
end the test process. The client's log shows both: `Reason: remote` → `ConnectionMonitor stopped`,
`Reason: server_restart` → monitor still running.

Also needed a small addition to `ws`: `CloseSend`. `Close` completes the close handshake, which
reads, so it cannot be called while the reader goroutine owns reads — leaving only `CloseNow`,
which tells the peer nothing and surfaces in the browser as an `onerror`. `CloseSend` writes the
close frame and closes without waiting for the answer. This is a real gap in a library that
documents single-reader semantics, not a concession to coax (§1b), and Autobahn stayed 301/301.

### Phase 7 — periodic timers + hardening
`Periodically` as a ticker goroutine bound to the subscription context.
→ **verify:** transmits N times in N intervals, stops on unsubscribe with **zero leaked
goroutines** (`runtime.NumGoroutine` with a retry loop — no `goleak` dep). 10k idle connections:
record RSS and confirm a heartbeat sweep stays under ~100ms. That number is the reason this port
exists, so measure it.

**Measured** (12-core machine, `make load`), 10,000 idle connections:

| | |
|---|---|
| RSS | +388 MiB, ~40 KB per connection — *both ends* live in this process, so halve it for a server |
| Goroutines | 2.0 per connection |
| Heartbeat sweep | median **27.9ms**, well inside the 100ms budget |
| — of which our bookkeeping | **154µs (1%)**; `BenchmarkHeartbeatSweep` puts it at ~240ns per connection |
| Dropping all 10k | 155ms |

The other 99% of the sweep is the delivery it sets off: the runtime hands the CPU to each writer
goroutine it readies, so the sweeping goroutine pays for the socket writes. The pings genuinely go
out in that window — it is not overhead. Still linear at 25,000 connections (87.8ms, 3.5µs each);
above ~28,000 the *test* runs out of ephemeral ports, since both ends are on loopback.

The budget is expressed per connection (10µs) rather than as a flat 100ms, so it keeps its meaning
at other sizes.

---

## 7. Cross-cutting checks

- `go test -race ./...` every phase — the registry and subscriber map are the only places a
  data race can hide.
- `go vet ./...` clean; no `//nolint`-style suppressions.
- Fuzz targets on **every** parser that touches the network: WS frames, RESP replies, inbound
  JSON commands.
- Goroutine-count assertion in connection teardown tests.
- `make conformance` — Autobahn + the Node JS-client script against `cmd/example`, so protocol
  regressions surface without a browser.

## 8. Known open questions

1. Raw (non-JSON) stream payloads: Rails supports `stream_from ..., coder: nil`. Plan assumes
   JSON everywhere; add a raw variant if a use case appears.
2. `addSubProtocol` interop: the Rails JS client offers extra subprotocols but only accepts
   `actioncable-v1-json` back (`connection.js:9`). Whatever a client offers, we must answer
   `actioncable-v1-json` — explicit test in Phase 2.
3. Autobahn's `permessage-deflate` cases will report unimplemented. Confirm that's acceptable
   for a v1 release, or promote deflate out of the follow-ups (§9.1) into a phase.

**Answered:**
1. **Still open, still deferred.** No use case appeared; §9.4 records it with its workaround.
2. **Closed in Phase 2.** `TestSubprotocolAlwaysActionCableV1JSON` answers
   `actioncable-v1-json` to four different offers.
3. **Closed: excluded, not failed.** The deflate cases (12.x, 13.x) are excluded from the
   Autobahn run rather than reported as failures, since we never negotiate the extension — a
   client asking for it simply gets a connection without it. The committed report is 301/301 on
   the cases that apply. Deflate stays a follow-up (§9.1); the framing layer already validates
   RSV bits against a negotiated set, so it lands as an addition.

---

## 9. Follow-ups (explicitly not v1)

Ordered by what I'd reach for first. Each is additive — none requires reworking v1.

### 9.1 `permessage-deflate` (RFC 7692) — `ws`
The one gap that would bite a real deployment: repetitive JSON broadcasts compress 5–10×.
Irrelevant for 30-byte pings, which is why it's not v1. `compress/flate` is stdlib, so the work is
extension negotiation (`Sec-WebSocket-Extensions`, `client_max_window_bits`,
`server_no_context_takeover`), per-connection flate reader/writer reuse, and setting/honouring
RSV1. Phase 1 leaves the negotiation hook and validates RSV against a negotiated set precisely so
this lands as an addition rather than a rewrite. ~250 LOC + the Autobahn deflate cases going green.

### 9.2 Postgres `LISTEN/NOTIFY` adapter
Direct port of `subscription_adapter/postgresql.rb`. Needs a dedicated session-level connection
(LISTEN is session state), and must reproduce the **63-byte** identifier rule: PG silently
truncates identifiers past `NAMEDATALEN-1`, so a longer channel name must be SHA1-hashed or
notifications miss their subscribers. Known constraints to document, not fix: ~8000-byte `NOTIFY`
payload cap, one extra long-lived connection per process, and **incompatible with pgbouncer in
transaction-pooling mode**. ~200 LOC on top of a driver.

### 9.3 DB-polling adapter (Solid-Cable-shaped, built in Go)
Same goal as Solid Cable — multi-process fan-out with no Redis — but our own implementation. The
elegant part: take a **`*sql.DB` from the caller**. `database/sql` is stdlib, so our module stays
zero-dep and the driver is the user's existing dependency (pgx/stdlib, lib/pq,
go-sql-driver/mysql, or pure-Go modernc.org/sqlite). This is *easier* than the Redis adapter —
no wire protocol at all.

```
broadcast  INSERT INTO cable_messages (channel, payload) VALUES (…)
poller     SELECT id, channel, payload FROM cable_messages
             WHERE id > :cursor ORDER BY id LIMIT :batch      every ~100ms
trimmer    DELETE FROM cable_messages WHERE created_at < now() - :retention
boot       cursor := COALESCE(MAX(id), 0)   -- no history replay; online-only, like Rails
```

`Subscribe` becomes purely local — the poller fetches everything and dispatches by channel — so
there's no remote ack and the blocking-ack contract (§4) is satisfied trivially.

**The subtle bug to get right:** autoincrement ids are assigned before commit, so with concurrent
inserts a row with id 5 can become visible *after* id 6. A poller that has advanced its cursor
past 6 never sees 5 — a silently lost broadcast. Mitigation is an overlap window plus id dedupe
(re-read the last N ids / last few hundred ms and drop ones already dispatched), not a bare
`id > cursor`. Worth writing the failing test first.

Trade-offs to document: ~poll-interval latency (100ms, vs sub-ms for Redis); write amplification
(an INSERT and eventually a DELETE per broadcast, so WAL/vacuum pressure); poor fit past a few
hundred broadcasts/sec. In exchange: SQLite-viable, single-service deployments. ~200 LOC + ~40 for
a 3-query dialect struct (placeholder style, autoincrement type, now()) with Postgres/MySQL/SQLite
presets.

### 9.4 Channel/stream sugar skipped in Phase 4
Deliberate omissions, each with a working substitute today:

- **`StreamFor(model)`** — Rails builds the broadcasting name from a GlobalID
  (`chat_channel:Z2lkOi8v…`). There is no GlobalID in Go, so any equivalent would be an invented
  naming convention. Callers compose their own name and pass it to `StreamFrom`.
- **Custom stream handlers** (`stream_from(b) { |msg| … }`) and **coders** — the only handler is
  the default one, forwarding the payload verbatim to the client. Add if a use case shows up; it
  is a second `StreamFrom` variant, not a redesign.
- **Periodic timers** (`periodically`) — Phase 7.

### 9.5 Redis gaps, named rather than hidden
The adapter covers what pub/sub needs and stops there. Each of these is a real gap with a
workaround today:

- **Sentinel discovery** — resolve the current master with `Options.Dialer`. Doing it properly
  means speaking `SENTINEL get-master-addr-by-name` and subscribing to `+switch-master`, which is
  a second protocol's worth of work.
- **Cluster sharded pub/sub** (`SSUBSCRIBE`) — not needed: ordinary `PUBLISH` is broadcast
  cluster-wide, so a cluster behaves as one server. Sharded pub/sub is the opt-in scaling variant.
- **Interop test against a real Rails process** — see the phase-5 note. Cheap once a Rails app is
  around; proves nothing new about our code.
- **A go-redis-backed adapter** as an alternative, in its own module so the core `go.mod` stays
  empty. ~80 LOC against `PubSub`, provable with `pubsubtest`. The escape hatch if the
  hand-rolled client ever disappoints.

### 9.6 Smaller items
- **Masking in assembly** (amd64/arm64), the way `coder/websocket` does it. Only if a benchmark
  says the pure-Go word-wise mask is the bottleneck. The pure-Go path stays as the reference
  implementation, with a differential test asserting the two agree on random inputs.
- Rails-compat package (`coax/railscompat`): cookie decryption, `cable.yml`, `to_gid_param` —
  only if a Rails app ever needs to sit in front of this.
- Explicit metrics/tracing hooks, once there's a consumer. `slog` until then.
- Promote `ws` to its own module or repo if it earns independent versioning.
