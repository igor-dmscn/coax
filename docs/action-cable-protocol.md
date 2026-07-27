# Action Cable Wire Protocol (`actioncable-v1-json`)

Exact bytes on the wire, plus the sequences they appear in. This is the contract an
alternative client or server must satisfy.

Source: `rails` @ `3947087c3` (8.2.0.alpha). Constants live in
`actioncable/lib/action_cable.rb:61` and are mirrored in
`actioncable/app/javascript/action_cable/internal.js` — the two files must stay identical.

---

## 1. Handshake

Client request (standard RFC 6455 upgrade):

```http
GET /cable HTTP/1.1
Host: example.com
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Version: 13
Sec-WebSocket-Key: <base64>
Sec-WebSocket-Protocol: actioncable-v1-json, actioncable-unsupported
Origin: https://example.com
Cookie: _session=...
```

Server offers `["actioncable-v1-json", "actioncable-unsupported"]`; `websocket-driver`
selects the first mutual entry. Response:

```http
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Accept: <base64>
Sec-WebSocket-Protocol: actioncable-v1-json
```

Rejections:

- Not a WebSocket upgrade, or origin not allowed → **`404` with
  `Content-Type: text/plain; charset=utf-8` and body `Page not found`**. No WebSocket, no
  `disconnect` frame (`server/socket.rb:145`).
- Authentication is *not* part of the handshake. The upgrade succeeds first; `connect` runs
  afterwards on a worker and, if it rejects, the server sends a `disconnect` frame over the
  now-open socket and closes it.

Origin rules (`server/base.rb:154`), in order: `disable_request_forgery_protection` → allow
everything; `allow_same_origin_as_host` (default true) → allow when `Origin` equals
`"#{scheme}://#{request.host_with_port}"`; otherwise `allowed_request_origins` matched with
`===` (String, Regexp, or callable).

Client-side check on `onopen`: the negotiated subprotocol must be `actioncable-v1-json`
(`INTERNAL.protocols` minus its last element). Anything else → close with
`allowReconnect: false`. Note that custom subprotocols added via
`consumer.addSubProtocol(...)` are sent in the request but do **not** widen this check — the
client only accepts `actioncable-v1-json` back.

All frames are **text** frames containing one JSON object. Binary frames sent by a client
are dropped with an error log (`socket/message_buffer.rb:38`). Encoding is
`ActiveSupport::JSON` on both sides.

---

## 2. Client → server

Exactly three commands. `identifier` is always a **JSON string containing JSON**.

### `subscribe`

```json
{"command":"subscribe","identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}"}
```

- `identifier` must be present and must not already be subscribed on this connection.
- The decoded object's `channel` key names a Ruby constant that must resolve to a subclass
  of `ActionCable::Channel::Base`. Every other key becomes `params` on the channel.
- Answered with `confirm_subscription` or `reject_subscription`. Nothing is sent on error
  (unknown channel, duplicate, malformed) — only a server-side log.
- Duplicate subscribes are normal traffic: the client's `SubscriptionGuarantor` re-sends
  every 500ms until confirmed.

### `unsubscribe`

```json
{"command":"unsubscribe","identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}"}
```

No acknowledgement. Unknown identifiers are silently ignored.

### `message`

```json
{"command":"message",
 "identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}",
 "data":"{\"action\":\"speak\",\"message\":\"hello\"}"}
```

- `data` is a JSON **string**, decoded separately (double encoding is part of the
  protocol, on both `identifier` and `data`).
- `action` names a public method on the channel subclass. **Omitting `action` dispatches to
  `receive`.**
- The method receives the whole decoded `data` hash (including `"action"`) if its arity is
  exactly 1; otherwise it's called with no arguments.
- No acknowledgement, ever. Replies are ordinary channel transmissions.

---

## 3. Server → client

### `welcome`

```json
{"type":"welcome"}
```

Sent once `connect` has succeeded. The client treats it as "the connection is usable" —
resets the reconnect counter and (re)sends `subscribe` for every subscription it holds. A
reconnecting client therefore rebuilds all its subscriptions itself; the server keeps no
cross-socket state.

### `ping`

```json
{"type":"ping","message":1753560000}
```

Every 3 seconds to every connection on the server (`BEAT_INTERVAL`). `message` is
`Time.now.to_i`. Clients do not reply. Any inbound frame — not just this one — refreshes
the client's staleness clock.

### `confirm_subscription`

```json
{"identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}","type":"confirm_subscription"}
```

Sent once per successful subscription, and *only after* every `stream_from` issued during
`subscribed` has been acknowledged by the pubsub backend. Fires the client's
`connected({reconnected: bool})`.

### `reject_subscription`

```json
{"identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}","type":"reject_subscription"}
```

The subscription was refused (`reject`). The client drops it and fires `rejected()` — no
retry.

### `disconnect`

```json
{"type":"disconnect","reason":"unauthorized","reconnect":false}
```

| `reason` | When | `reconnect` |
|---|---|---|
| `unauthorized` | `reject_unauthorized_connection` during `connect` | `false` |
| `server_restart` | code reload / graceful shutdown | `true` |
| `remote` | `RemoteConnections#disconnect` | caller's choice, default `true` |
| `invalid_request` | *never sent* — defined in `INTERNAL` but unused; bad upgrades get a `404` | — |

`reconnect: false` stops the client's `ConnectionMonitor`; it will not come back on its own.
The frame is best-effort (`rescue nil`) and is immediately followed by a close.

### Application data

```json
{"identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}","message":{"body":"hello"}}
```

Anything with an `identifier` and no recognized `type`. `message` is arbitrary JSON. The
client routes it to `received(message)` on **every** subscription whose identifier matches
byte-for-byte.

### Close codes

`ClientSocket#close` accepts only `1000` or `3000..4999` (`socket/client_socket.rb:95`);
Action Cable itself always uses the default `1000`. A socket that dies without a close frame
reports `["", 1006]`.

---

## 4. Sequences

### Connect and subscribe

```
C→S  HTTP upgrade (Sec-WebSocket-Protocol: actioncable-v1-json)
S→C  101 Switching Protocols
                                    ← server: on_open → worker → connect
C→S  {"command":"subscribe","identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}"}
                                    ← may arrive before connect finishes; buffered
S→C  {"type":"welcome"}
                                    ← client: reload() → resends subscribe (harmless dup)
                                    ← server: subscribed → stream_from → await pubsub ack
S→C  {"identifier":"…","type":"confirm_subscription"}
S→C  {"type":"ping","message":1753560000}          every 3s from here on
```

### Action → broadcast → delivery

```
C→S  {"command":"message","identifier":"…","data":"{\"action\":\"speak\",\"message\":\"hi\"}"}
                                    ← ChatChannel#speak(data) → ActionCable.server.broadcast
                                    ← pubsub → every subscribed process → each streaming channel
S→C  {"identifier":"…","message":{"body":"hi","user":"igor"}}      to every subscriber
```

Note the sender receives its own broadcast the same way as everyone else. There is no echo
suppression.

### Rejected subscription

```
C→S  {"command":"subscribe","identifier":"{\"channel\":\"AdminChannel\"}"}
S→C  {"identifier":"{\"channel\":\"AdminChannel\"}","type":"reject_subscription"}
                                    ← client: rejected(), stops guarantor, keeps socket
```

### Unauthorized connection

```
C→S  HTTP upgrade
S→C  101 Switching Protocols
S→C  {"type":"disconnect","reason":"unauthorized","reconnect":false}
S→C  close 1000
                                    ← client: monitor stopped, no reconnect
```

### Reconnect after a dropped socket

```
     (no frames for >6s)
                                    ← client: connectionIsStale() → reopen()
C→S  HTTP upgrade
S→C  101, {"type":"welcome"}
C→S  subscribe × N                  ← every subscription replayed by the client
S→C  confirm_subscription × N       ← connected({reconnected: true}) on each
```

---

## 5. Timing constants

| Constant | Value | Where |
|---|---|---|
| Heartbeat interval | 3s | `Server::Connections::BEAT_INTERVAL` |
| Client stale threshold | 6s | `ConnectionMonitor.staleThreshold` (2 × beat) |
| Reconnect backoff rate | 0.15 | `ConnectionMonitor.reconnectionBackoffRate` |
| Backoff formula | `6s × 1.15^min(attempts,10) × (1 + jitter)`, `jitter = rand()` on attempt 0 else `0.15 × rand()` | `getPollInterval` |
| Reopen delay | 500ms | `Connection.reopenDelay` |
| Subscribe retry | 500ms | `SubscriptionGuarantor.retrySubscribing` |
| Visibility-change recheck | 200ms | `ConnectionMonitor.visibilityDidChange` |

---

## 6. Notes for a reimplementation

Behaviors that are easy to get wrong because they're implicit in the Ruby:

1. **`identifier` is an opaque string key, compared byte-for-byte.** The server never
   canonicalizes it. `{"channel":"C","room":"1"}` and `{"room":"1","channel":"C"}` are two
   different subscriptions. Whatever a client's JSON serializer emits for key order is what
   the server keys on — so echo the client's exact string back in every response rather
   than re-serializing your parsed form.
2. **Double encoding is mandatory**, on both `identifier` and the `message` command's
   `data`. Sending them as nested objects rather than strings breaks the Rails client and
   the Rails server.
3. **The upgrade always succeeds before auth.** Failure is reported as a `disconnect`
   frame + close, not as an HTTP status. Only a malformed upgrade or a bad origin yields
   `404`.
4. **Confirmation must be deferred until the pubsub backend acknowledges the
   subscription**, or you will drop messages published in the gap while the client believes
   it is listening. Count outstanding stream subscriptions; confirm at zero.
5. **A confirmation is required or the client never stops.** The guarantor re-sends
   `subscribe` every 500ms forever. Duplicate subscribes must be handled benignly
   (Rails logs and ignores them).
6. **Errors are silent to the client** for unknown commands, unknown channels, unparseable
   frames, and unknown subscriptions. If you invent error frames, the Rails JS client will
   route them to `received` (no `type` match) or ignore them.
7. **Heartbeats must actually be written**, since a failed write is how a half-open socket
   is detected. Don't optimize the ping away when the connection has been quiet.
8. **Any inbound frame refreshes the client's staleness timer**, so a busy connection needs
   no pings at all. Conversely, going quiet for >6s guarantees a client-side reconnect.
9. **Messages are fanned out per subscription, not per connection.** One connection with
   two subscriptions streaming the same broadcasting receives the payload twice, once per
   identifier.
10. **Per-connection message ordering is not guaranteed** in Rails: each frame is dispatched
    as an independent worker-pool task. A reimplementation that serializes per connection is
    *stricter* than Rails, which is safe; one that reorders across connections is fine too.
11. **`action` is optional** and defaults to `receive`. An arity-1 handler gets the whole
    `data` hash including the `action` key.
12. **`welcome` is the client's signal to (re)subscribe everything.** Send it exactly once
    per socket, after auth, before anything else.
