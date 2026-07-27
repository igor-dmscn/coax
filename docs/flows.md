# coax Data Flows

Every significant path through the implementation, traced call by call: what calls what, on
which goroutine, and what happens at each step.

Companion to [implementation](./implementation.md), which describes the structures these flows
move through. `file.go:symbol` names a function rather than a line.

Goroutines are marked in the traces:

| Mark | Goroutine |
|---|---|
| **R** | the connection's reader — the HTTP handler goroutine |
| **W** | the connection's writer |
| **H** | the server's heartbeat |
| **T** | a periodic timer |
| **P** | a pub/sub adapter's reader |
| **C** | whichever goroutine the caller is on (an HTTP handler, a job, `main`) |

---

## 1. Opening a connection

```
GET /cable  Upgrade: websocket  Sec-WebSocket-Version: 13
            Sec-WebSocket-Protocol: actioncable-v1-json, actioncable-unsupported
```

**R** throughout. The handler never returns until the connection ends.

1. `coax/server.go:ServeHTTP`
   - `isUpgrade(r)` — `GET` plus `websocket` as a token in `Upgrade`. Not an upgrade →
     `writePageNotFound` and done. Rails' exact bytes: `text/plain; charset=utf-8`,
     `Page not found`, no trailing newline.
   - `originAllowed(r)` — Action Cable's rules: no `Origin` header is allowed (only browsers
     send one), the server's own host is allowed, then `AllowedOrigins` patterns. Refused →
     the same 404, no WebSocket, no explanation.
   - `stopping()` — shutting down → `503 Server shutting down`. Without this a shutdown could
     never finish (§8).
2. `ws.Accept(w, r, opts)` with `InsecureSkipVerify: true`, because origin was already checked
   with coax's rules rather than `ws`'s.
   - `verifyUpgrade` → `selectSubprotocol` → `Hijack()` → clear deadlines → write the 101 →
     `newConn`. Full detail in [implementation §4.1](./implementation.md#41-handshake).
   - The answer is always `actioncable-v1-json`: it is the only value the JS client accepts
     back, whatever it offered (`TestSubprotocolAlwaysActionCableV1JSON`).
3. `coax/server.go:serve`
   - `opts.Authenticate(r)` — **your code**. Cookies, headers, query. Returns `Identifiers`.
     - Error → `rejectUnauthorized(sock)`: write `{"type":"disconnect","reason":"unauthorized",
       "reconnect":false}` directly to the socket, then close. **The handshake already
       succeeded**, so the client learns this in a frame, not an HTTP status.
   - `newConnection(...)` — context derived from the request's, `send` channel sized
     `SendBuffer`, empty subscriptions map.
   - `defer c.shutdown()` — registered first, so it runs last.
   - `go c.writeLoop()` — **W** starts; from here it owns the socket's write side.
   - `c.subscribeToInternalChannel()` — before the welcome, so a remote disconnect published
     the instant the client believes it is connected cannot be missed. 5s bound; a failure is
     a warning, not fatal.
   - `c.transmitMessage(newWelcome())` → `{"type":"welcome"}`.
   - `s.add(c)` — registers, `live.Add(1)`. Returns false if shutdown has begun, in which case
     the connection is told `server_restart` and waited for.
   - `defer s.remove(c)`, `s.startHeartbeat()` — the ticker starts on first use, so a server
     that never accepts a connection runs no goroutine.
   - `c.readLoop()` — blocks here for the life of the connection.

**Why the welcome precedes registration:** a connection must never be heartbeated before it
has been told it is usable. Rails orders it the same way (`connection/base.rb:91`).

### The client's half

```
new WebSocket(url, ["actioncable-v1-json"])
  → onopen        checks the negotiated subprotocol; anything else → close, no reconnect
  → "welcome"     monitor.recordConnect(); subscriptions.reload() → resubscribe everything
  → "ping" ×N     every 3s; two missed beats (6s) → reconnect
```

The server keeps no subscription state across sockets. Reconnection is entirely the client
replaying what it holds — which is why there is no `MessageBuffer` equivalent here: the
welcome is sent before the reader starts, so no command can arrive too early.

---

## 2. A client command arrives

```
{"command":"subscribe","identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}"}
```

**R** throughout — which is what makes per-connection ordering automatic and channel state
lock-free.

```
ws.Conn.Read ──► readLoop ──► handleMessage ──► decodeCommand ──┬─► addSubscription
                                                                ├─► removeSubscription
                                                                └─► performAction
```

1. `coax/conn.go:readLoop` — `c.sock.Read(c.ctx)`. An error ends the connection (§7.1). A
   non-text frame is logged and skipped: Action Cable is JSON over text frames.
2. `coax/conn.go:handleMessage` → `coax/protocol.go:decodeCommand`, which validates the command
   verb and requires a non-empty identifier. Failure → logged, **nothing sent**, connection
   continues. Rails answers an unintelligible command with silence, and
   `TestMalformedCommandDoesNotCloseTheConnection` pins that by reading the next heartbeat.
3. Dispatch on the verb. No `default` case: `decodeCommand` already rejected anything else.

### 2.1 `subscribe`

`coax/channel.go:addSubscription`:

```
already in the map?          → log debug, return          (the client's guarantor retries; expected traffic)
decodeIdentifier(identifier) → channel name, or error     → log, return
server.channelFactory(name)  → nil?                       → log "channel not found", return
factory(sub)                 → nil?                       → log, return
sub.impl.Subscribed(c.ctx)   → error?                     → sub.stop(); send reject_subscription; return
store in c.subscriptions[identifier]
send confirm_subscription
```

Four ways to fail and three of them are **silent to the client** — it is left waiting, exactly
as in Rails (`connection/subscriptions.rb:123`). Only an error from `Subscribed` produces
`reject_subscription`, which is what fires the client's `rejected()`.

`sub.stop()` on the rejection path stops streams *and* timers: a channel may well have opened
one before deciding to reject, and those would otherwise run forever with no subscription to
serve.

The subscription is stored *after* `Subscribed` returns, so `confirm_subscription` cannot be
queued before whatever `Subscribed` set up is live. Rails stores first and removes on
rejection; the difference is invisible on the wire.

### 2.2 `unsubscribe`

`coax/channel.go:removeSubscription`:

```
not in the map? → log "unable to find subscription", return
delete from the map
sub.impl.Unsubscribed(c.ctx)
sub.stop()                     → streams and timers
```

Nothing is sent back: the client considers itself unsubscribed the moment it asks. Cleanup
runs *after* `Unsubscribed`, matching Rails' callback order
(`channel/streams.rb:81`, `on_unsubscribe :stop_all_streams`), so a channel can still say
goodbye over its streams.

### 2.3 `message`

`coax/channel.go:performAction`:

```
c.subscriptions[cmd.Identifier] → missing? → log, return   (silent to the client)
decodeAction(cmd.Data)          → action, payload
  no "action" key or empty      → "receive"
sub.impl.Perform(c.ctx, action, payload)
  error                         → log, client told nothing
```

The whole payload is handed over, `action` key included, matching Rails' behaviour of passing
the entire decoded hash to the channel method.

---

## 3. Subscribing with a stream, and why confirmation waits

The ordering the design exists for. **R**, except where marked.

```
R  addSubscription
R   └─ Subscribed(ctx)                       your code
R       └─ sub.StreamFrom(ctx, "chat:1")
R           └─ PubSub.Subscribe(ctx, "chat:1", sub.forward)
                  ├─ memory: registers, returns                        (immediate)
                  └─ redis:  sends SUBSCRIBE, waits for the ack        ← blocks here
R           ◄─ returns nil: the backend is listening
R       ◄─ returns nil
R   store in the map
R   transmit confirm_subscription  ──────────► W ──► client
```

Because `Subscribe` cannot return before the backend has acknowledged, a broadcast published
in the instant after the client sees `confirm_subscription` cannot be lost. Rails needs an
atomic counter and a deferred callback for the same guarantee
([implementation §6.1](./implementation.md#61-why-subscribe-blocks)).

`StreamFrom` records the returned `unsubscribe` in `s.streams[broadcasting]`. Streaming from
the same broadcasting twice within one subscription is a no-op — Rails would register a second
handler and deliver everything twice.

If `StreamFrom` fails, the error propagates out of `Subscribed` and the subscription is
rejected. Better than confirming a client that would then silently receive nothing.

---

## 4. Broadcast reaching clients

```
C  srv.Broadcast(ctx, "chat:1", payload)
C   └─ json.Marshal(payload)
C   └─ PubSub.Broadcast(ctx, "chat:1", bytes)
```

### 4.1 In-memory

```
C  MemoryPubSub.Broadcast
C   └─ RLock; for each subscriber of "chat:1":
C        └─ sub.forward(payload)              ← still on the caller's goroutine
C             └─ transmitMessage(newData(identifier, payload))
C                  └─ enqueue ──► send ──► W ──► ws.Write ──► client
```

Delivery happens inline on the publishing goroutine, under the adapter's read lock. Safe
because `forward` only queues a frame — and the reason `Handler` documents that it must not
call back into the `PubSub`.

### 4.2 Redis

```
C  redispubsub.Broadcast
C   └─ pubMu; publishConn(ctx) → dial if needed
C   └─ appendPublish(buf, "chat:1", payload)  → *3 $7 PUBLISH …
C   └─ conn.Write; read the :N reply
C  ◄─ returns; the payload has left this process

   ─ ─ ─ Redis fans out to every subscribed process, this one included ─ ─ ─

P  readLoop → readValue → *3 ["message", "chat:1", payload]
P   └─ handleReply → dispatch("chat:1", payload, scratch)
P        └─ Lock; copy subscribers into scratch; Unlock
P        └─ for each: sub.forward(payload)
P             └─ enqueue ──► send ──► W ──► ws.Write ──► client
```

The publishing process receives its own broadcast back through Redis rather than short-circuiting
locally. That is deliberate — two processes otherwise see different orderings.

`dispatch` copies the subscriber set into a scratch slice and releases the lock before calling
handlers, so one connection's queueing cannot stall a `Subscribe` on another. The scratch slice
is reused because only **P** ever dispatches, so it costs no allocation per message.

**The payload is never re-encoded.** It arrives as bytes and goes into `message` as
`json.RawMessage`, which is what makes a Rails-published payload byte-identical when it reaches
the client (`TestRedisEndToEnd`).

---

## 5. Channel action producing a broadcast

The complete round trip for a chat message, four goroutines deep.

```
R  client → {"command":"message","identifier":…,"data":"{\"action\":\"speak\",\"body\":\"hi\"}"}
R  performAction → Perform(ctx, "speak", data)          your code
R   └─ srv.Broadcast(ctx, "chat:1", msg)
R        └─ (memory) inline fan-out, or (redis) PUBLISH  ← blocks the reader briefly
   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
P  message pushed back → dispatch → forward, once per subscriber
W  every subscriber's writer sends {"identifier":…,"message":{…}}
   → each client's received() fires, sender included
```

The sender's own copy arrives the same way as everyone else's. To reply to just the one client
instead, `Subscription.Transmit(v)` skips the backend entirely.

---

## 6. Heartbeat

```
H  heartbeat: ticker every HeartbeatInterval (3s)
H   └─ newPing(now).encode()                    once per tick
H   └─ sweep(frame, dst)
H        └─ snapshot(dst)                       RLock, copy into a reused slice
H        └─ for each connection: transmit(frame)
```

One encode per tick, **shared by every connection**. Safe specifically because server frames
are written unmasked, so `ws` never touches the buffer — in-place masking would make this
sharing a data race.

`transmit` is a non-blocking send, so the sweep does no I/O and a slow client cannot slow it
down. At 10,000 connections the sweep's own work is 154µs; the rest of the 27.9ms wall time is
the delivery it sets off, because the runtime hands the CPU to each writer it readies
(`BenchmarkHeartbeatSweep` isolates it at ~240ns per connection).

Client side: `staleThreshold` is 6s, twice the interval. Two missed beats and it reconnects —
which is why `HeartbeatInterval` is a protocol constant rather than a tuning knob.

---

## 7. Closing

Five ways a connection ends. All of them converge on `shutdown()`.

### 7.1 The client goes away

```
R  ws.Read returns an error (close frame, EOF, or reset)
R   └─ logClosed(err)          1000/1001/1005/1006 → debug; anything else → info
R   └─ readLoop returns → serve returns
R   └─ deferred, in order:  s.remove(c) → live.Done()
R                           c.shutdown()
R                             ├─ closeNow()                    cancel ctx, drop socket
R                             ├─ unsubscribeAll()              Unsubscribed + stop() each
R                             └─ unsubscribeFromInternalChannel()
W  ctx cancelled → writeLoop returns
```

`unsubscribeAll` runs with a fresh 10s context, not the connection's: that one is already
cancelled, and releasing a subscription may still need to talk to a backend.

### 7.2 The write path fails

```
W  ws.Write returns an error (or the 10s per-frame timeout expires)
W   └─ log debug; closeNow()      ← cancels ctx and drops the socket, which unblocks R
R  ws.Read returns → 7.1 from here
```

### 7.3 The send queue fills

```
X  enqueue: select has no room and ctx is live → default branch
X   └─ log warn "send buffer full, dropping connection"
X   └─ closeNow() → 7.1
```

The deliberate divergence from Rails, which would buffer indefinitely. `X` is whichever
goroutine was transmitting — the heartbeat, a pub/sub handler, a timer, or the reader itself.

### 7.4 The server closes one connection, with an explanation

Used by remote disconnect (§7.5) and shutdown (§8).

```
X  c.close(reason, reconnect)                    once, via closingOnce
X   └─ newDisconnect(reason, reconnect).encode()
X   └─ enqueue({frame, final: true})
W  writes the frame
W   └─ out.final → closeSend()
W        └─ cancel ctx; ws.CloseSend(1000, "")   close frame, then the socket
R  ws.Read returns → 7.1
```

`final` is the whole mechanism: the writer tears the connection down *after* the bytes are on
the socket. Closing from `close()` directly would race the write it just queued and the client
would lose the explanation.

The close frame matters too — a browser reports `onclose` rather than an error event. That is
why `ws.CloseSend` exists: `ws.Close` would complete the close handshake, which *reads*, and
the reader goroutine owns reads.

### 7.5 Remote disconnect, from another process

```
C  (elsewhere) srv.Disconnect(ctx, Identifiers{"current_user":"42"}, false)
C   └─ internalChannelFor(ids) → "action_cable/current_user=42"
C   └─ PubSub.Broadcast(ctx, that name, {"type":"disconnect","reconnect":false})

   ─ ─ ─ through the backend, to every process ─ ─ ─

P  handleInternalMessage(payload)
P   └─ type != "disconnect" → log debug, return
P   └─ reconnect = message.Reconnect == nil || *Reconnect      absent means true
P   └─ log info "removing connection"
P   └─ c.close("remote", reconnect) → 7.4
```

`closingOnce` makes this idempotent: two processes may both decide to drop the same user, and
the client still gets exactly one disconnect (`TestRemoteDisconnectIsIdempotent`).

---

## 8. Graceful shutdown

```
C  srv.Shutdown(ctx)
C   └─ beginShutdown()               Lock; close(stop) once
C        ↳ heartbeat's select sees stop → H returns
C        ↳ ServeHTTP now answers 503; add() now returns false
C   └─ for each connection: c.close("server_restart", true) → 7.4 on each
C   └─ waitForConnections(ctx)
C        ├─ ConnectionCount() == 0 → return immediately
C        └─ else wait on live.Wait() in a goroutine, racing ctx
C   └─ ctx expired?
C        └─ log warn; closeNow() on the remainder; return ctx.Err()
C   └─ closePubSub()                 only if the server created it
```

The lock in `beginShutdown` is load-bearing. `add()` takes the same `RWMutex`, so a connection
accepted at that moment is either registered before the wait begins — and therefore waited for
— or refused. Without it, `live.Add` could happen after `live.Wait` returned.

Client side of the same flow, from the real JS client's log:

```
Disconnecting. Reason: server_restart      reconnect: true  → monitor keeps running, retries
Disconnecting. Reason: remote              reconnect: false → ConnectionMonitor stopped
```

`Close()` is the abrupt sibling: `beginShutdown`, `closeNow` on everything, close the pub/sub.
No message, no waiting.

---

## 9. Periodic timers

```
R  Subscribed(ctx) → sub.Periodically(interval, f)
R   └─ validate interval > 0 and f != nil     → error rejects the subscription
R   └─ ctx, cancel := WithCancel(conn.ctx)     ← derived from the *connection*
R   └─ append cancel to s.stopTimers
R   └─ go runTimer(ctx, interval, f)          T starts

T  ticker every interval:
T   └─ f(ctx)                                  your code, on its own goroutine
T        └─ error → log, keep ticking
```

Two ways it stops, and both are covered:

- `sub.stop()` on unsubscribe, rejection, or connection teardown → each `cancel()` → **T**
  returns.
- The connection's context being cancelled, since the timer's context descends from it. A timer
  cannot outlive its connection even if the subscription is somehow never cleaned up.

The first tick is one interval away, not immediate — matching Rails, and avoiding a surprise
for a channel that has just transmitted its initial state.

**This is the one callback not on R.** `Transmit` is safe from it; `StreamFrom` and
`StopStream` are not, because they mutate the subscription's own map.

---

## 10. Redis: connection loss and recovery

```
P  run: loop
P   ├─ dial(ctx) → TLS? → AUTH? → bufio.Reader
P   │    failure → log warn, sleep backoff (100ms → 5s, doubling), retry
P   ├─ resubscribe(conn)
P   │    └─ Lock; p.conn = conn; snapshot the broadcasting names; Unlock
P   │    └─ SUBSCRIBE each                    ← the map replayed in full
P   ├─ readLoop(conn, reader)
P   │    ├─ SetReadDeadline(now + ReadTimeout) before every read
P   │    ├─ readValue → handleReply
P   │    │    ["message", ch, payload] → dispatch (§4.2)
P   │    │    ["subscribe", ch, n]     → confirm(ch): active = true, close(ready)
P   │    │    ["unsubscribe"|"pong"]   → ignore
P   │    │    +PONG / -ERR             → ignore / log
P   │    └─ any read error → return
P   └─ dropConn(conn)
P        └─ close the socket; p.conn = nil
P        └─ for each broadcasting that was active: active = false, ready = a fresh channel
P   ↳ log warn "subscribe connection lost" — every gap is visible
P   ↳ sleep backoff, loop

K  keepalive: PING every PingInterval (30s) on whatever the current connection is
```

Three properties fall out of this shape:

- **A dropped `SUBSCRIBE` write needs no error handling.** The map is the source of truth and
  `resubscribe` replays it. `send` logs write failures at debug and returns.
- **A subscribe in flight when the connection dies keeps waiting.** `dropConn` replaces
  `ready` only for broadcastings that were *active*; an unconfirmed one keeps the channel its
  callers are already blocked on, and the replay confirms it.
- **Half-open connections are detected.** No bytes at all within `ReadTimeout` ends the
  connection, and `keepalive` guarantees there would have been bytes.

Messages published while this process is reconnecting are lost. Redis pub/sub is fire and
forget and so is this; Rails has the same property. The warn log is the only trace.

---

## 11. Byte level: reading one message

```
ws.Conn.Reader(ctx)
 ├─ err()                        already closed? report the recorded cause
 ├─ applyReadDeadline(ctx)       ctx's deadline onto the socket; zero clears it
 ├─ reader.active?               abandoned previous message → drain() so framing stays correct
 └─ loop:
     readFrameHeader()
      ├─ readFrameHeader(br, buf)  2 bytes, then extended length, then mask key
      ├─ h.validate(masked, rsv)   control FIN + ≤125, RSV set, mask direction,
      │                            minimal length encoding, 64-bit high bit
      └─ payloadLen > maxFrame  → failRead(1009 message too big)
     control frame?
      ├─ ping  → write pong (same payload), continue the loop
      ├─ pong  → continue
      └─ close → parseClosePayload → echo the status → finishPeerClose → report CloseError
     opContinuation with no message → failRead(protocol error)
     payloadLen > maxMessage       → failRead(1009)
     validator.reset(); reader.reset(typ, h)
     return the reader

msgReader.Read(p)
 ├─ remaining == 0 → nextFragment(): read the next header, tolerate interleaved control
 │                   frames, require opContinuation, enforce maxMessage on the total
 ├─ read min(len(p), remaining) bytes
 ├─ masked → mask(p, key, pos) in place, carrying pos across calls
 ├─ text → validator.write(p); invalid → failRead(1007)
 └─ fin and remaining == 0 → validator.done()? else failRead(1007); return io.EOF
```

Every limit is checked against a header before the payload is read, so no length prefix can
make the connection allocate.

## 12. Byte level: writing one message

```
ws.Conn.Write(ctx, typ, p)          one frame, whatever the size
 ├─ err(); applyWriteDeadline(ctx)
 ├─ writeMu.Lock()
 ├─ writeFrameLocked(opcode(typ), p, fin: true)
 │    ├─ appendFrameHeader(writeHeaderBuf, h)   into a preallocated array
 │    ├─ bw.Write(header)
 │    ├─ client? copy into maskScratch and mask it — never mutate the caller's slice
 │    ├─ bw.Write(payload)
 │    └─ bw.Flush()
 └─ writeMu.Unlock()

ws.Conn.Writer(ctx, typ)            streaming, fragmented
 └─ msgWriter over a 4 KiB buffer
     Write(p)  fills the buffer; full → flushFragment(fin: false)
     Close()   flushFragment(fin: true)
     flushFragment takes writeMu per fragment and releases it in between
```

**`writeMu` is per frame, not per message.** That is what lets an automatic pong or a close
frame go out between the fragments of a slow message — which RFC 6455 §5.4 permits, and which
also fixed a self-deadlock: an echo server whose read path needed to send a close frame would
otherwise block on a lock its own goroutine held.
