# Action Cable Internals

How Action Cable actually works: object graph, threading model, storage, and every
significant flow, traced through the source.

Source: `rails` @ `3947087c3` (8.2.0.alpha). Paths relative to the rails checkout root.
Companion docs: [glossary](./action-cable-glossary.md), [wire protocol](./action-cable-protocol.md).

---

## 1. Layering

Action Cable is a Rack app that never returns a normal response. It hijacks the TCP socket
out from under the web server and manages it itself.

```
Puma/Falcon (accepts HTTP, runs Rack)
│
└─ ActionCable::Server::Base#call(env)                 ← singleton Rack app at /cable
   │   health check? → config.health_check_application
   │   setup_heartbeat_timer
   └─ ActionCable::Server::Socket.new(server, env).process
      │
      ├─ Socket::WebSocket            thin nil-guarding wrapper
      │  └─ Socket::ClientSocket      ready-state machine + websocket-driver events
      │     ├─ ::WebSocket::Driver    (gem) handshake + RFC6455 framing
      │     └─ Socket::Stream         rack.hijack IO, write buffer
      │        └─ StreamEventLoop     nio4r selector thread (shared, per server)
      │
      ├─ Socket::MessageBuffer        holds frames until the connection is open
      │
      └─ ApplicationCable::Connection < ActionCable::Connection::Base
         ├─ Connection::Subscriptions            { identifier => Channel instance }
         │  └─ ChatChannel (a Channel::Base instance, one per subscription)
         │     └─ Channel::Streams  { broadcasting => handler lambda } → pubsub
         └─ Connection::InternalChannel          pubsub sub on action_cable/<gid>
```

The split at the `Socket` / `Connection` boundary is deliberate and recent (Rails 8.1):
`Socket` owns bytes, framing, and encoding; `Connection` owns identity and dispatch. The
connection reaches the socket only through `transmit`, `close`, `perform_work`, `request`,
`env`, `protocol`. That's the whole contract — which is what makes an alternative transport
(or an alternative server) possible without touching user channels.

### Where state lives

| State | Owner | Scope | Structure |
|---|---|---|---|
| Live connections | `Server::Connections#connections_map` | one server process | `Hash{connection.object_id => connection}` |
| Channel subscriptions | `Connection::Subscriptions@subscriptions` | one connection | `Hash{identifier_string => Channel instance}` |
| Streams | `Channel::Streams#streams` | one channel instance | `Hash{broadcasting => handler}` |
| Stream callbacks | `SubscriptionAdapter::SubscriberMap@subscribers` | one server process | `Hash{broadcasting => [callbacks]}`, `Mutex`-guarded |
| Pending client writes | `Socket::Stream@write_buffer` | one socket | `Queue` + `@write_head` partial slice |
| Buffered inbound frames | `Socket::MessageBuffer` | one socket | `Array`, drained once at open |
| Pending client subscribes | `SubscriptionGuarantor` (JS) | one consumer | array, retried every 500ms |

Nothing is persisted. Nothing is shared between processes except through pubsub. The
connections map is per-process — hence `RemoteConnections` for anything cross-process
(§7.3).

Note `connections_map` is keyed by `object_id` and mutated from worker threads, so all
readers iterate a `values` snapshot (`server/connections.rb:18`) — mutating a Hash
mid-iteration would raise.

---

## 2. Threading model

Five kinds of thread, and the discipline is: *do as little as possible on the first two.*

| # | Thread | Count | Runs |
|---|---|---|---|
| 1 | Web server thread | Puma's pool | `Server::Base#call` → build socket, hijack IO, return `[-1, {}, []]`. Never touched again for this socket. |
| 2 | `StreamEventLoop` | 1 per server | nio4r `select` loop: read bytes → driver, flush queued writes, detect EOF. No app code. |
| 3 | Worker pool | `worker_pool_size`, default 4 | **All application code**: `connect`/`disconnect`, `subscribed`/`unsubscribed`, actions, stream handlers, periodic timer bodies. |
| 4 | Executor (`streamer`) | `executor_pool_size`, default 10 | pubsub `subscribe`/`unsubscribe`, broadcast fan-out (`SubscriberMap::Async#invoke_callback`), heartbeat tick, timers. |
| 5 | Adapter listener | 1 per adapter | Redis `SUBSCRIBE` loop / PG `wait_for_notify` loop, on its own dedicated backend connection. |

Consequences worth internalizing:

- **Sizing.** Every worker thread that touches Active Record checks out its own DB
  connection. `5 servers × 5 Puma workers × 8 worker threads = 200 DB connections`
  (`server/base.rb:110-125`). The DB pool must be ≥ worker pool size or workers block each
  other.
- **Stream handlers never run on the event loop.** `stream_from` wraps every handler so it
  is re-posted to the worker pool (`channel/streams.rb:163-169`). A slow handler can starve
  the worker pool, but can't stall the selector.
- **Ordering.** Per-connection message order is preserved only as far as the worker pool
  permits: each inbound frame is posted as an independent task
  (`socket.rb:75-77`), so two frames from the same client can execute concurrently on
  different workers. Action Cable does not serialize per-connection work.
- **All lazy singletons are `Monitor`-guarded** on the server
  (`server/base.rb:102-138`) because they're first touched from arbitrary threads.
- **Worker tasks carry their connection in a thread-local** (`thread_mattr_accessor
  :connection`, `worker.rb:15`) so the `:work` callbacks can find the right logger.

### The `:work` callback chain

Everything the worker pool runs goes through `run_callbacks :work` (`worker.rb:40`), which
by the time Rails has booted looks like:

1. **Rails executor wrap** (`engine.rb:80-89`) — reloader/AR integration, and a
   `stopping?` check that discards tasks queued before a `halt` so a restarting server
   doesn't run work with unloaded classes.
2. **Active Record log tagging** (`worker/active_record_connection_management.rb`) — tags
   AR's logger with the connection's tags for the duration.
3. The actual `receiver.send(method, *args)`, with `rescue Exception` → log + optional
   `receiver.handle_exception`.

Channel `subscribe`/`unsubscribe` callbacks get their own executor wrap
(`engine.rb:96-102`).

---

## 3. Flow: opening a connection

```
GET /cable  Upgrade: websocket  Sec-WebSocket-Protocol: actioncable-v1-json
```

1. `Server::Base#call` (`server/base.rb:61`)
   - `PATH_INFO == config.health_check_path` → hand to `health_check_application`, done.
   - `setup_heartbeat_timer` — idempotent; the 3s timer is created on first request, not at
     boot.
   - `Socket.new(self, env).process`.
2. `Socket#initialize` (`server/socket.rb:18`) builds, eagerly: the tagged logger, the
   `WebSocket` wrapper (which builds `ClientSocket` → `WebSocket::Driver.rack` → `Stream`
   *only if* `WebSocket::Driver.websocket?(env)`), the `MessageBuffer`, and the user
   connection via `config.connection_class.call.new(server, self)`. So your
   `ApplicationCable::Connection` object exists before the handshake completes — but
   `connect` has not run.
3. `Socket#process` (`server/socket.rb:32`) gates on two things:
   - `websocket.possible?` — was this a valid upgrade request?
   - `server.allow_request_origin?(env)` — origin check (`server/base.rb:154`):
     `disable_request_forgery_protection` short-circuits true; else
     `allow_same_origin_as_host` compares `HTTP_ORIGIN` to
     `"#{proto}://#{request.host_with_port}"`; else `allowed_request_origins` matched with
     `===` (so strings, regexps, and lambdas all work).

   Failure → close if alive, log, return a plain `404 Page not found`. **No `disconnect`
   message is sent** and the `invalid_request` disconnect reason goes unused.
4. Success → `websocket.rack_response` → `ClientSocket#start_driver`
   (`socket/client_socket.rb:58`):
   - `@stream.hijack_rack_socket` → calls `env["rack.hijack"]`, takes the raw IO, and
     registers it with the event loop for reads (`stream_event_loop.rb:19`).
   - If `env["async.callback"]` exists (async-Rack servers), calls it with
     `[101, {}, @stream]`.
   - `@driver.start` — writes the 101 handshake bytes through `Stream#write`.
   - Returns `[-1, {}, []]`. Status `-1` is the Rack convention for "I hijacked this;
     don't touch it."
5. The driver fires `:open` → `ClientSocket#open` → `ready_state = OPEN` →
   `Socket#on_open` → `send_async :handle_open` — **hop to the worker pool.**
6. `Socket#handle_open` (`server/socket.rb:124`), on a worker:
   - `@protocol = websocket.protocol` (the negotiated subprotocol).
   - `@connection.handle_open` → `Connection::Base#handle_open`
     (`connection/base.rb:91`):
     - `connect` — **your** authentication code. Raising
       `Authorization::UnauthorizedError` (via `reject_unauthorized_connection`) is caught
       right here and turned into `close(reason: "unauthorized", reconnect: false)`.
     - `subscribe_to_internal_channel` — only if `connection_identifier` is present
       (i.e. only if you declared `identified_by` and assigned it).
     - `send_welcome_message` → `{"type":"welcome"}`.
   - `message_buffer.process!` — replay anything the client sent before this point.
   - `server.add_connection(@connection)` — *now* it's in the map, and thus heartbeated.

The buffer at step 6 exists because the client sends `subscribe` immediately on
`onopen`, which can easily beat `connect` finishing on a busy worker pool. Frames that
arrive first are queued in `MessageBuffer` (`socket/message_buffer.rb`) and drained in
order once the connection is live. Non-String frames (binary) are dropped with an error
log.

### Client side of the same flow

`createConsumer()` → `new Consumer(url)` builds `Subscriptions` and `Connection` but does
**not** open a socket. The socket opens on the first `subscriptions.create(...)` (which
calls `ensureActiveConnection`) or an explicit `consumer.connect()`.

`Connection#open` (`connection.js:31`) sends `[...INTERNAL.protocols,
...consumer.subprotocols]` as requested subprotocols and starts the `ConnectionMonitor`.
On `onopen` the client checks `isProtocolSupported()` — the negotiated protocol must be in
`protocols` minus its last element, i.e. exactly `actioncable-v1-json`. Anything else (a
server that answered `actioncable-unsupported`) → `close({allowReconnect: false})`.

On `{"type":"welcome"}` the client calls `monitor.recordConnect()` and
`subscriptions.reload()` — resubscribing everything it knows about. That's the entire
reconnect story: the server keeps no subscription state across sockets, the client replays
it.

---

## 4. Flow: inbound message → channel action

```
frame → event loop → driver → MessageBuffer → worker → Connection → Subscriptions → Channel
```

1. Event loop reads bytes → `Stream#receive` → `ClientSocket#parse` → driver → `:message`
   → `Socket#on_message` → `message_buffer.append`.
2. Buffer is `processing?` → `Socket#receive` → `send_async :dispatch_websocket_message`.
   **Worker hop.**
3. `Socket#dispatch_websocket_message` (`server/socket.rb:79`): bail with an error log if
   the socket died meanwhile; else `@connection.handle_incoming decode(frame)` —
   `ActiveSupport::JSON.decode`. Wrapped in `rescue Exception` → log; a malformed frame can
   never kill the connection.
4. `Connection::Base#handle_channel_command` (aliased `handle_incoming`,
   `connection/base.rb:106`): runs the `:command` callbacks
   (`before_command`/`around_command`/`after_command` — the hook for setting `Current`
   attributes per command) around `subscriptions.execute_command(payload)`, with
   `rescue_from` support via `ActiveSupport::Rescuable`.
5. `Connection::Subscriptions#execute_command` (`connection/subscriptions.rb:58`)
   dispatches on `data["command"]`:

   **`subscribe`** → `#add`
   - Blank identifier → `MalformedCommandError`; already-present identifier →
     `AlreadySubscribedError`.
   - `subscription_from_identifier`: JSON-decode the identifier, take `channel`,
     `safe_constantize`, and require `ActionCable::Channel::Base > klass`. This is the only
     guard against instantiating arbitrary constants from client input — note it accepts
     *any* channel subclass in the app, so authorization belongs in `subscribed`.
   - Store under the raw identifier string, then `subscription.subscribe_to_channel`.
   - Unknown channel → `ChannelNotFound`.

   **`unsubscribe`** → `#remove` → `unsubscribe_from_channel` (runs the `:unsubscribe`
   callbacks, which include `stop_all_streams`) and delete from the hash.

   **`message`** → `#perform_action`: find by identifier (`UnknownSubscription` if gone),
   JSON-decode `data["data"]` (the inner, double-encoded payload) and
   `subscription.perform_action`.

   All of these raise; nothing between here and step 3's rescue handles them, so a bad
   command produces `Could not handle incoming message: … [Error]` in the log and the
   client hears nothing. If you want the client informed, use `rescue_from` on the
   connection.
6. `Channel::Base#perform_action` (`channel/base.rb:184`):
   - `extract_action` — `data["action"]` or `:receive`.
   - `processable_action?` — must be in `self.class.action_methods` (and the subscription
     must not be rejected).
   - Instrument `perform_action.action_cable`, then `dispatch_action`: `method(action).arity
     == 1 ? public_send(action, data) : public_send(action)`. So both `def speak(data)` and
     `def away` work. Note `arity == 1` exactly — a splat or optional arg (`arity == -1`)
     is called with **no** arguments.
   - `rescue Exception` → `rescue_with_handler(e) || raise` (channel-level `rescue_from`).

---

## 5. Flow: subscription confirmation and its deferral

The problem: if the server confirms a subscription before Redis has acknowledged
`SUBSCRIBE`, messages broadcast in that window are silently lost while the client believes
it is listening.

The mechanism (`channel/base.rb:172, 199-207, 253-265`) is a
`Concurrent::AtomicFixnum` starting at **1**:

```
new(...)                                   counter = 1   (owed: subscribe_to_channel)
subscribe_to_channel
  run_callbacks :subscribe
    subscribed
      stream_from "room_1"                 counter = 2   defer_subscription_confirmation!
      stream_from "room_2"                 counter = 3
  ensure_confirmation_sent                 counter = 2   → still > 0, send nothing
                    ← pubsub ack "room_1"  counter = 1   → still > 0, send nothing
                    ← pubsub ack "room_2"  counter = 0   → transmit confirm_subscription
```

Each `stream_from` increments before calling `pubsub.subscribe(broadcasting, handler,
success_callback)`; each `success_callback` decrements via `ensure_confirmation_sent`. The
confirmation goes out exactly when the count reaches zero, guarded by
`@subscription_confirmation_sent` so it's sent once.

A channel with no streams confirms immediately: 1 → 0 at `ensure_confirmation_sent`.

**Rejection** short-circuits all of it (`channel/base.rb:204`): if `reject` was called,
`subscribed` is skipped when the rejection came from a `before_subscribe` callback,
`reject_subscription` removes the instance from `Connection::Subscriptions` and transmits
`reject_subscription`. `ensure_confirmation_sent` returns early for rejected subscriptions,
so no confirmation can follow.

Client side: `Subscriptions#subscribe` hands the subscription to the
`SubscriptionGuarantor`, which re-sends the `subscribe` command every 500ms until
`confirmSubscription` (on `confirm_subscription`) or `reject` (on `reject_subscription`)
calls `guarantor.forget`. This is why the server must raise/ignore
`AlreadySubscribedError` benignly — duplicate subscribes are normal traffic.

---

## 6. Flow: broadcast → client

```
ActionCable.server.broadcast("room_1", {...})
│
├─ Broadcaster#broadcast (server/broadcasting.rb:52)
│    debug log; instrument "broadcast.action_cable"
│    payload = coder.encode(message)                    ← JSON, once, here
│    pubsub.broadcast("room_1", payload)
│
├─ adapter: ChannelPrefix prepends "prefix:" if configured
│    Redis      → PUBLISH room_1 payload                (own pooled connection)
│    PostgreSQL → NOTIFY "room_1", 'payload'            (AR pool, escaped)
│    Inline     → straight into the local SubscriberMap
│    Async      → local SubscriberMap, callbacks on the executor
│
└─ every server process (including the publisher):
     listener thread receives the message
     SubscriberMap#broadcast("room_1", msg)             ← Mutex, dup the callback list
       for each callback: executor.post { callback.call(msg) }   (Async map)
         ↓ that callback is Channel::Streams' wrapper:
       connection.perform_work(handler, :call, msg)     ← worker pool hop
         ↓ handler = default_stream_handler:
       coder.decode(msg) → Channel#transmit(data, via:)
         ↓
       connection.transmit(identifier:, message: data)
       Socket#transmit → coder.encode → websocket.transmit → driver.text → Stream#write
```

Two hops (executor → worker) before any user code runs, and the payload is JSON-decoded
then re-encoded on the way out. The source notes that as a known optimization opportunity
(`channel/streams.rb:188-190`): when both coders are JSON the proxy could pass bytes
through untouched.

Handler variants (`channel/streams.rb:163-210`):

- `stream_from "x"` → decode JSON, transmit to client, `via: "streamed from x"`.
- `stream_from "x", coder: nil` with a block → block receives the **raw** payload string.
- `stream_from "x", coder: ActiveSupport::JSON` with a block → block receives the decoded
  object and decides whether to `transmit`.

`stop_stream_from` / `stop_all_streams` unsubscribe the handler from pubsub; when the last
subscriber for a broadcasting goes away, `SubscriberMap#remove_channel` issues the actual
`UNSUBSCRIBE`/`UNLISTEN`.

### Adapter listener details

**Redis** (`subscription_adapter/redis.rb`). Two separate connections: a pooled one for
`PUBLISH`, and a dedicated pubsub one owned by `Listener`'s thread. The loop
(`redis.rb:106`) calls `next_event(60)` and handles `subscribe` (pop and run the pending
success callback → this is what decrements the deferral counter), `message` (fan out), and
`unsubscribe` (break the loop only when the count hits 0). Two subtleties, both commented
in-source:

- A permanent subscription to `_action_cable_internal` keeps the count above zero so the
  loop survives the last real channel going away.
- `ensure_listener_running` drops a dead thread before memoizing, otherwise an exhausted
  reconnect budget would leave `@thread` set-but-dead and every later `subscribe` would
  queue forever.

On `ConnectionError` it resets, sleeps per `reconnect_attempts` (integer or array of
backoff seconds), and `resubscribe`s every channel currently in the map.

**PostgreSQL** (`subscription_adapter/postgresql.rb`). Uses a *new* connection created
outside the AR pool (`new_connection`, deliberately not `checkout`, to avoid pinning) and
sets `application_name` to the adapter identifier. A `Queue` carries
`:listen`/`:unlisten`/`:shutdown` commands into the loop, which alternates draining the
queue with `wait_for_notify(1)`. Channel names longer than 63 **bytes** are replaced by
their SHA1 hex, because Postgres silently truncates identifiers and the truncated name
wouldn't match what `wait_for_notify` reports back. Payload size is bounded by Postgres's
`NOTIFY` limit (~8000 bytes).

**Async / Inline** are single-process only: `Inline` runs callbacks on the caller's thread,
`Async` posts them to the executor. Defaults for dev/test; broadcasts from another process
(a Sidekiq worker, a console) are invisible to the server.

---

## 7. Flow: closing

### 7.1 Client goes away / clean close

Event loop gets EOF or `nil` from `read_nonblock` → `Stream#close` → `clean_rack_hijack`
(deregister + close IO) → `ClientSocket#client_gone` → `finalize_close` →
`Socket#on_close` → `send_async :handle_close`. On a worker
(`server/socket.rb:133`):

1. log "Finished …"
2. `server.remove_connection(@connection)` — out of the map, no more heartbeats
3. `@connection.handle_close` → `subscriptions.unsubscribe_from_all` (each channel's
   `:unsubscribe` callbacks → `stop_all_streams`, `stop_periodic_timers`, your
   `unsubscribed`), `unsubscribe_from_internal_channel`, then your `disconnect`.

A driver-initiated close (`:close` event) goes through `begin_close` → shutdown the stream
→ `finalize_close`, landing in the same place.

### 7.2 Server closes one connection

`Connection::Base#close(reason:, reconnect:)` (`connection/base.rb:121`) transmits
`{"type":"disconnect","reason":…,"reconnect":…}` — `rescue nil`, because the socket may
already be gone — then `socket.close` → `driver.close` (code 1000). Close codes are
validated: only `1000` or `3000..4999` (`socket/client_socket.rb:95`).

The client's `disconnect` handler honors `reconnect`: `close({allowReconnect: reconnect})`,
and `allowReconnect: false` stops the `ConnectionMonitor` entirely, so the client stays
down until something calls `connect()`.

### 7.3 Remote disconnect (cross-process)

```ruby
ActionCable.server.remote_connections.where(current_user: user).disconnect(reconnect: false)
```

`RemoteConnection` reuses `Connection::Identification` + `Connection::InternalChannel` to
compute the same `action_cable/<gid>` name the real connection subscribed to, then
`server.broadcast internal_channel, {type: "disconnect", reconnect: …}`
(`remote_connections.rb:59`). Every server process running that user's connections receives
it on the internal channel and calls
`close(reason: "remote", reconnect: …)` (`connection/internal_channel.rb:36`).

`where` validates that you passed *every* declared identifier
(`valid_identifiers?`) — a partial match raises `InvalidIdentifiersError`, since the gid is
a join of all of them.

This is also the reason `identified_by` matters beyond convenience: no identifier means no
internal channel subscription (`internal_channel.rb:20`) and therefore no way to reach the
connection remotely.

### 7.4 Server restart / code reload

`app.reloader.before_class_unload { ActionCable.server.restart }` (`engine.rb:91`).
`Server::Base#restart` (`server/base.rb:73`):

1. `each_connection { |c| c.close(reason: "server_restart") }` — clients get
   `reconnect: true` and come back.
2. Under the monitor: clear `connections_map` (because `remove_connection` normally runs on
   the worker pool, which is about to be halted, so entries would leak), shut down the
   heartbeat timer, `worker_pool.halt` (queued-but-unstarted work is discarded), shut down
   the executor and the pubsub adapter.

Everything is lazily recreated on the next request. The `stopping?` check inside the work
callback (`engine.rb:84`) catches tasks that got a thread just as the halt landed.

---

## 8. Flow: heartbeat and client-side reconnection

**Server.** `setup_heartbeat_timer` (`server/connections.rb:38`), first call only, creates
`executor.timer(3)` which posts `each_connection(&:beat)` onto the executor. `beat`
transmits `{"type":"ping","message":<unix seconds>}` (`connection/base.rb:142`). Purpose is
stated plainly in-source: WebSocket implementations disagree about when a connection is
stale, and Action Cable wants it to *never* be considered stale, so it keeps bytes moving.

**Client.** `ConnectionMonitor` (`connection_monitor.js`):

- `recordMessage()` on **every** inbound message, not just pings — any traffic proves
  liveness. `refreshedAt = pingedAt || startedAt`.
- `staleThreshold = 6` (`BEAT_INTERVAL * 2`: two missed pings).
- Poll timer: `staleThreshold * 1000 * (1.15 ** min(attempts,10)) * (1 + jitter)`, where
  `jitter` is `rand()` on the first attempt and `0.15 * rand()` afterwards. So the first
  check lands 6–12s out and later ones back off toward ~24s+ — jitter on attempt 0 is what
  spreads a thundering herd after a server restart.
- `reconnectIfStale`: if stale, increment attempts and `connection.reopen()` — unless
  `disconnectedRecently()` (a disconnect within the last 6s), which avoids fighting an
  in-flight reconnect.
- `visibilityDidChange`: on tab focus, after 200ms, reopen if stale or not open. This is
  the path that matters most in practice — mobile/background tabs get their timers
  throttled and come back long-stale.
- `reopen()` closes then reopens after `Connection.reopenDelay = 500`ms.

`Connection#close` deliberately skips closing a socket in `connecting` state, working
around a Safari 15.1+ bug (`connection.js:48`).

---

## 9. Writes and backpressure

`Socket::Stream#write` (`socket/stream.rb:36`) is the only place bytes reach the client:

1. If `env["stream.send"]` exists (server-provided async writer), delegate and return.
2. `try_lock` — non-blocking. If another thread holds the write lock, skip straight to
   queueing (step 4) rather than blocking a worker.
3. With the lock, and only if nothing is already queued: `write_nonblock(data, exception:
   false)`.
   - full write → done, return bytesize.
   - `:wait_writable` → fall through to queueing.
   - partial write → stash the remainder in `@write_head`, register write interest, return.
4. Queue the data and `event_loop.writes_pending(io)` → the selector adds `:rw` interest.
5. The event loop later calls `flush_write_buffer`, which drains `@write_head` + the queue
   until `:wait_writable`, then drops back to `:r` interest when empty
   (`stream_event_loop.rb:87-91`).

`EOFError`/`ECONNRESET` during a write → `client_gone`, i.e. a failed heartbeat is what
reaps a half-open connection. There is **no bound on `@write_buffer`**: a client that
doesn't read while the server broadcasts heavily grows that queue without limit. That's the
practical backpressure risk in Action Cable.

The event loop thread is spawned lazily on first `attach` and re-spawned if it died
(`spawn`, guarded by `@spawn_mutex`); `wakeup` either spawns it or pokes the selector.
Cross-thread work is handed in through a `@todo` Queue of lambdas, so registration always
happens on the selector thread.

---

## 10. Periodic timers

`periodically :transmit_progress, every: 5.seconds` (`channel/periodic_timers.rb`).
Registered per channel *class*; started `after_subscribe`, stopped `after_unsubscribe`.
Each timer is an `executor.timer(every)` whose body does
`connection.perform_work(callback, :call)` — so the tick fires on the executor and the body
runs on the worker pool, `instance_exec`'d in the channel instance so `transmit` and
instance variables work. Timers are per subscription instance: 1,000 subscribers with a 5s
timer is 1,000 timers and 200 worker tasks/second.

---

## 11. Configuration surface

`ActionCable::Configuration` (`lib/action_cable/configuration.rb`), reachable as
`ActionCable.server.config`, populated from `config.action_cable.*` by
`engine.rb:48-65`.

| Setting | Default | Notes |
|---|---|---|
| `connection_class` | `-> { ActionCable::Connection::Base }` | engine rewrites it to prefer `ApplicationCable::Connection`; a lambda so reloading picks up the new constant |
| `worker_pool_size` | 4 | application threads |
| `executor_pool_size` | 10 | framework threads |
| `disable_request_forgery_protection` | false | skips origin checks entirely |
| `allowed_request_origins` | dev: `/https?:\/\/localhost:\d+/` | matched with `===` |
| `allow_same_origin_as_host` | true | compares against `request.host_with_port`, so it works behind a proxy that sets `X-Forwarded-Host` |
| `cable` | from `config/cable.yml` | `adapter`, `channel_prefix`, `id`, adapter-specific keys |
| `log_tags` | `[]` | strings/symbols (camelized) or callables taking the request |
| `filter_parameters` | app's + `[]` | applied to action argument logging |
| `mount_path` | `/cable` | `nil` skips route mounting (standalone server) |
| `url` | — | what `action_cable_meta_tag` advertises |
| `health_check_path` / `health_check_application` | nil / `Rails::HealthController` | plain HTTP probe |
| `pubsub_adapter` | derived from `cable[:adapter]`, default `redis` | resolution + friendly LoadError messages in `configuration.rb:41` |

Adapter resolution deliberately re-raises `LoadError` with guidance distinguishing "you
misspelled the adapter / missed the gem" from "the adapter's own dependency is missing".

---

## 12. Instrumentation and logging

`ActiveSupport::Notifications` events:

| Event | Emitted by |
|---|---|
| `broadcast.action_cable` | `Broadcaster#broadcast` — payload `broadcasting`, `message`, `coder` |
| `perform_action.action_cable` | `Channel#perform_action` — `channel_class`, `action`, `data` |
| `transmit.action_cable` | `Channel#transmit` — `channel_class`, `data`, `via` |
| `transmit_subscription_confirmation.action_cable` | `Channel#transmit_subscription_confirmation` |
| `transmit_subscription_rejection.action_cable` | `Channel#transmit_subscription_rejection` |

Logging is per-connection through `TaggedLoggerProxy`, built in `Socket#initialize` with a
lazily-evaluated request (`server/base.rb:148`). Tags are fixed for the connection's
lifetime, plus whatever `logger.add_tags` adds from `connect`. Action argument logging runs
through `ActiveSupport::ParameterFilter` (`channel/base.rb:318`).

Both `Socket` and `Connection::Base` override `instance_variables_to_inspect` to `[]`
(`ActiveSupport::InspectBackport`) so an accidental `inspect` in a log line can't dump the
whole object graph — or a session cookie.

---

## 13. Deployment shapes

- **In-app** — mounted at `config.action_cable.mount_path` inside the main Rack app. Cable
  connections occupy Puma threads for their entire lifetime (one hijacked socket each), so
  a chatty app can exhaust the thread pool.
- **Standalone** — `mount_path = nil` in the app, plus a separate `cable/config.ru`
  running `ActionCable.server`. Needs `config.action_cable.url` (for
  `action_cable_meta_tag`), `allowed_request_origins`, and a shared cookie domain if you
  authenticate from cookies. `health_check_path` exists for exactly this setup.

Either way, pubsub is what ties processes together: N cable servers all `SUBSCRIBE` to the
broadcastings their local subscribers care about, and any process in the deployment can
`broadcast`.

---

## 14. Failure modes, as designed

| Failure | Behavior |
|---|---|
| Bad upgrade request / bad origin | plain `404`, no `disconnect` frame |
| `reject_unauthorized_connection` | `disconnect` with `unauthorized`, `reconnect: false` — client stays down |
| Unparseable frame / unknown command / unknown channel | logged server-side, **client is not told**; connection survives |
| Exception in a channel action | `rescue_from` on the channel, else logged by the worker |
| Exception in `connect` (not Unauthorized) | escapes to `Worker#invoke`'s rescue → logged; the socket stays open but never gets `welcome` |
| Redis connection lost | listener retries per `reconnect_attempts`, then gives up and logs; a later `subscribe` respawns the thread |
| Client vanishes silently | next 3s heartbeat write fails → `client_gone` → full close path |
| Broadcast with no subscribers | dropped; no persistence, no replay |
| Code reload | all connections closed with `server_restart`, clients reconnect and resubscribe |
