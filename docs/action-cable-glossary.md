# Action Cable Glossary

Terms as Action Cable itself uses them, with the class/file that owns each concept.

Source: `rails` @ `3947087c3` (8.2.0.alpha). Paths are relative to the rails checkout,
e.g. `actioncable/lib/action_cable/server/base.rb`.

> Note on versions: 8.1+ split the old monolithic `ActionCable::Connection::Base` into
> `ActionCable::Server::Socket` (transport) + `ActionCable::Connection::Base` (application).
> Docs and blog posts written before that show `Connection::Base` doing the WebSocket work.

---

## Core vocabulary

**Cable**
The whole subsystem, and by convention the mount path (`/cable`). Not a class.

**Server** — `ActionCable::Server::Base` (`lib/action_cable/server/base.rb`)
The singleton Rack application, reachable as `ActionCable.server`. Owns the shared
machinery: pubsub adapter, worker pool, executor, event loop, heartbeat timer, and the
map of live connections. Mounted into the app's routes by the engine
(`lib/action_cable/engine.rb:67`).

**Socket** — `ActionCable::Server::Socket` (`lib/action_cable/server/socket.rb`)
One per accepted WebSocket request. All transport-level concerns: handshake, framing,
JSON encode/decode, buffering, closing, and dispatching work onto the worker pool. Knows
nothing about your app. Holds exactly one **connection**.

**Connection** — `ActionCable::Connection::Base`, subclassed as `ApplicationCable::Connection`
The application-level peer of a socket. One per socket, and the parent of every channel
subscription made over that socket. Responsible only for authentication/authorization
(`connect`, `reject_unauthorized_connection`) and identification. Long-lived: minutes to
days.

**Consumer** — `ActionCable.Consumer` (`app/javascript/action_cable/consumer.js`)
The client-side counterpart of a connection: one WebSocket per consumer, created with
`createConsumer()`. Also the gateway to creating subscriptions. One consumer per browser
tab is the norm.

**Channel** — `ActionCable::Channel::Base`, subclassed as e.g. `ChatChannel`
A logical unit of work, roughly "a controller that can also push". Public instance
methods declared on the subclass become remotely callable **actions**. A channel *class*
is code; a channel *instance* is a subscription (see below).

**Subscription (server side)** — a `Channel::Base` instance
Created when a consumer subscribes, kept in `Connection::Subscriptions` keyed by
**identifier**, destroyed when the consumer unsubscribes or the socket closes. Because it
lives as long as the connection, instance variables persist across actions — and go stale
if you cache records in them (`lib/action_cable/channel/base.rb:20-29`).

**Subscription (client side)** — `ActionCable.Subscription` (`app/javascript/action_cable/subscription.js`)
The JS object returned by `consumer.subscriptions.create(...)`. Carries the callbacks
(`initialized`, `connected`, `disconnected`, `rejected`, `received`) and `perform`.

**Subscriber**
Informal term for the consumer/client on the far end of a subscription. Guide-level word,
not a class.

**Identifier**
The JSON string that names one subscription, e.g. `{"channel":"ChatChannel","room":"1"}`.
Built client-side by `JSON.stringify(params)` and used *verbatim as a hash key* on the
server (`lib/action_cable/connection/subscriptions.rb:69`). It appears in every message
belonging to that subscription. Distinct from *connection identifier*.

**Connection identifier** — `Connection::Identification#connection_identifier`
A single string derived from all `identified_by` values, GlobalID-ified and joined with
`:` (e.g. `gid://app/User/1`). Used to build the connection's internal channel name, which
is what makes remote disconnects possible.

**`identified_by`** — `Connection::Identification` (`lib/action_cable/connection/identification.rb:21`)
Class macro on the connection declaring which attributes identify it (`current_user`,
`current_account`, …). Each one gets an `attr_accessor` on the connection *and* an
auto-generated delegator on every channel instance
(`lib/action_cable/channel/base.rb:279`).

**Action**
A remotely-invocable public method on a channel. Computed by
`Channel::Base.action_methods`: public instance methods of the subclass minus those
inherited from `Channel::Base` (`lib/action_cable/channel/base.rb:128`). Dispatched by
name from `{"command":"message", ...}` payloads; a missing `action` key defaults to
`receive`.

**Command**
One of the three verbs a client can send: `subscribe`, `unsubscribe`, `message`
(`lib/action_cable/connection/subscriptions.rb:58`). Everything the client does travels as
one of these three.

**Stream** — `Channel::Streams` (`lib/action_cable/channel/streams.rb`)
A subscription's registration on a pubsub queue: `stream_from "room_1"` /
`stream_for @room`. Streams are how broadcasts reach a client. Purely online — a message
broadcast while you weren't streaming is gone.
(Do not confuse with `Server::Socket::Stream`, the byte-level IO wrapper.)

**Broadcasting**
The *name* of a pubsub queue, i.e. the string you broadcast to and stream from
(`"room_1"`, `"comments:Z2lkOi8v…"`). `Channel.broadcasting_for(model)` derives one from
the channel name plus GlobalID (`lib/action_cable/channel/broadcasting.rb:24`). Also used
as a verb, for the act of publishing.

**Broadcast** — `Server::Broadcasting#broadcast` (`lib/action_cable/server/broadcasting.rb:33`)
Publish a message to a broadcasting: JSON-encode, hand to the pubsub adapter. Fire and
forget, from anywhere in the app (`ActionCable.server.broadcast`, `Channel.broadcast_to`).

**Broadcaster** — `Server::Broadcasting::Broadcaster`
A reusable object bound to one broadcasting + coder, returned by `broadcaster_for`.

**Pub/Sub**
The message bus between processes. Action Cable is a pubsub consumer, not a queue: no
persistence, no replay, no delivery guarantees.

**Subscription adapter** — `ActionCable::SubscriptionAdapter::Base` and subclasses
The pluggable pubsub backend, chosen by `config/cable.yml`. Ships with `redis`,
`postgresql`, `async`, `inline`, `test`. Interface is four methods: `broadcast`,
`subscribe`, `unsubscribe`, `shutdown` (`lib/action_cable/subscription_adapter/base.rb`).

**`SubscriberMap`** — `lib/action_cable/subscription_adapter/subscriber_map.rb`
In-process registry of `broadcasting => [callbacks]`, shared by every adapter. Tracks when
a broadcasting gains its first subscriber (→ `add_channel`, i.e. `SUBSCRIBE`/`LISTEN`) and
loses its last (→ `remove_channel`). `SubscriberMap::Async` posts the same work to the
executor instead of running it inline.

**Listener** — `Redis::Listener`, `PostgreSQL::Listener`
A `SubscriberMap::Async` subclass owning a dedicated thread and its own backend
connection, running the blocking `SUBSCRIBE`/`LISTEN` loop and fanning received messages
back into the map.

**Channel prefix** — `SubscriptionAdapter::ChannelPrefix`
`channel_prefix:` from `cable.yml`, prepended to every broadcasting name so several apps
can share one Redis/Postgres instance.

**Coder**
Anything with `encode`/`decode`; defaults to `ActiveSupport::JSON`. There are two
independent coders: the pubsub payload coder (`broadcast(..., coder:)`, `stream_from(...,
coder:)`) and the socket's wire coder (`Socket#initialize(coder:)`).

**Internal channel** — `Connection::InternalChannel` (`lib/action_cable/connection/internal_channel.rb`)
A per-connection pubsub subscription on `action_cable/<connection_identifier>`, used for
out-of-band control messages. Today it carries exactly one: `{"type":"disconnect"}`. This
is the mechanism behind `RemoteConnections`.

**Remote connections** — `ActionCable::RemoteConnections` (`lib/action_cable/remote_connections.rb`)
`ActionCable.server.remote_connections.where(current_user: u).disconnect` — finds
connections *across all servers* by identifier and disconnects them, by broadcasting to
their internal channel. The only way to reach a connection you don't hold in memory.

**Worker pool** — `ActionCable::Server::Worker` (`lib/action_cable/server/worker.rb`)
`Concurrent::ThreadPoolExecutor`, default max 4 (`config.action_cable.worker_pool_size`).
Runs *application* code: connection callbacks, channel actions, stream handlers, periodic
timers. Each task is wrapped in `run_callbacks :work` — which is where the Rails executor
wrap and Active Record connection/log tagging hook in.

**Executor** — `ActionCable::Server::ThreadedExecutor` (`lib/action_cable/server/base.rb:10`)
A second, larger pool (default max 10, `executor_pool_size`) named `streamer`, plus a
`timer` factory. Runs *framework* async work: pubsub subscribe/unsubscribe, broadcast
fan-out, heartbeat, periodic timers.

**Event loop** — `ActionCable::Server::StreamEventLoop` (`lib/action_cable/server/stream_event_loop.rb`)
One nio4r-selector thread per server, multiplexing reads and buffered writes over every
hijacked client socket. Does no application work.

**Heartbeat / beat** — `Server::Connections::BEAT_INTERVAL = 3`
A `{"type":"ping"}` sent to every connection on this server every 3 seconds, so
intermediaries and clients can tell a live connection from a dead one
(`lib/action_cable/server/connections.rb:38`).

**Stale connection** — client-side, `ConnectionMonitor.staleThreshold = 6`
No message received for >6s (two missed beats) ⇒ the client assumes the socket is dead and
reopens it with jittered exponential backoff
(`app/javascript/action_cable/connection_monitor.js`).

**Connection monitor** — `ActionCable.ConnectionMonitor`
Client-side watchdog: records message/connect/disconnect times, polls for staleness,
reconnects, and reopens on `visibilitychange` when a backgrounded tab returns.

**Subscription guarantor** — `ActionCable.SubscriptionGuarantor`
Client-side re-sender: repeats the `subscribe` command every 500ms until a
`confirm_subscription` (or `reject_subscription`) arrives
(`app/javascript/action_cable/subscription_guarantor.js`).

**Subprotocol / protocol** — `ActionCable::INTERNAL[:protocols]`
`["actioncable-v1-json", "actioncable-unsupported"]`, negotiated via
`Sec-WebSocket-Protocol`. A client that ends up on anything outside its supported list
closes the socket without reconnecting.

**Deferred subscription confirmation**
`confirm_subscription` is withheld until every `stream_from` in `subscribed` has been
acknowledged by the pubsub backend, so a client is never told "you're subscribed" before
the server can actually receive for it. Implemented as an atomic counter starting at 1
(`lib/action_cable/channel/base.rb:172, 253-265`).

**Rejection** — `Channel::Base#reject`
Refusing a subscription from inside `subscribed` (or a `before_subscribe` callback). Sends
`reject_subscription` and drops the channel instance; the client's `rejected()` callback
fires and it stops retrying.

**Tagged logger proxy** — `Server::TaggedLoggerProxy`
Per-connection log tags that survive for the connection's whole life, which
`ActiveSupport::TaggedLogging` (reset per request) cannot do. Tags come from
`config.action_cable.log_tags`.

**`ActionCable::INTERNAL`** — `lib/action_cable.rb:61`
The frozen hash of protocol constants: message types, disconnect reasons, default mount
path, subprotocol list. Mirrored verbatim in JS at
`app/javascript/action_cable/internal.js` — the two must stay in sync.

**Health check** — `config.health_check_path` / `health_check_application`
A plain HTTP path served by the cable Rack app before any WebSocket handling, so load
balancers can probe a standalone cable server (`lib/action_cable/server/base.rb:62`).

**`action_cable_meta_tag`** — `Helpers::ActionCableHelper`
Renders `<meta name="action-cable-url" content="…">`; `createConsumer()` reads it when
called with no URL.

---

## Message types (wire-level `type` field)

| Constant | Wire value | Direction | Meaning |
|---|---|---|---|
| `welcome` | `welcome` | S→C | Connection accepted; client may (re)subscribe |
| `disconnect` | `disconnect` | S→C | Server is closing; carries `reason` + `reconnect` |
| `ping` | `ping` | S→C | Heartbeat, `message` = unix seconds |
| `confirmation` | `confirm_subscription` | S→C | Subscription accepted (per identifier) |
| `rejection` | `reject_subscription` | S→C | Subscription refused (per identifier) |

Anything with an `identifier` and no recognized `type` is application data → the client's
`received(message)`.

## Disconnect reasons

| Reason | Sent when |
|---|---|
| `unauthorized` | `reject_unauthorized_connection` raised during `connect`; `reconnect: false` |
| `server_restart` | `Server#restart` (code reload / shutdown); `reconnect: true` |
| `remote` | `RemoteConnections#disconnect`; `reconnect:` per caller (default `true`) |
| `invalid_request` | **defined but never sent** in current code — a failed upgrade gets a plain `404` instead |

---

## Test-side vocabulary

**`ActionCable::TestHelper`** — swaps in the `test` adapter and provides
`assert_broadcasts` / `assert_broadcast_on` / `assert_no_broadcasts`.

**`SubscriptionAdapter::Test`** — an `Async` adapter that also records every broadcast.

**`Connection::TestCase`** + **`TestSocket`** / **`TestServer`** / **`TestTimer`** —
unit-test a connection with no real socket: `TestSocket` collects `transmissions` and runs
`perform_work` inline; `TestServer` is its own pubsub and executor and can `advance_time`.

**`Channel::TestCase`** — `subscribe` / `unsubscribe` / `perform` against one channel, with
`assert_has_stream`, `assert_broadcast_on`, `transmissions`.
