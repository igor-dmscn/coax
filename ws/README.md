# ws

A minimal WebSocket implementation (RFC 6455) for Go servers and clients.

- **No dependencies.** Standard library only.
- **Autobahn-verified.** 301 of 301 applicable cases pass — 298 `OK`, 3
  `INFORMATIONAL`. See [`testdata/autobahn/summary.json`](testdata/autobahn/summary.json).
- **Zero allocations** on the streaming read and write paths, asserted in tests
  rather than claimed in a README.

It lives in the coax repository because that is what it was written for, but
it imports nothing from it and is usable on its own.

## Install

```
go get github.com/igor-dmscn/coax-claude-impl/ws
```

## Server

```go
func handler(w http.ResponseWriter, r *http.Request) {
	conn, err := ws.Accept(w, r, &ws.AcceptOptions{
		Subprotocols: []string{"chat"},
	})
	if err != nil {
		return // Accept already wrote an error response
	}
	defer conn.CloseNow()

	for {
		typ, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		if err := conn.Write(r.Context(), typ, data); err != nil {
			return
		}
	}
}
```

`Accept` hijacks the connection, so the handler must not write to the
`ResponseWriter` afterwards and must not return until it is done with the
connection.

## Client

```go
conn, err := ws.Dial(ctx, "ws://localhost:8080/echo", nil)
```

## The allocation-free path

`Read` and `Write` take and return `[]byte`, which means `Read` must allocate —
the bytes have to live somewhere. `Reader` and `Writer` stream through
per-connection buffers instead and read straight into a slice you own:

```go
typ, r, err := conn.Reader(ctx)   // io.Reader over one message
w, err := conn.Writer(ctx, typ)   // io.WriteCloser for one message
```

Measured on this machine (`go test -bench`), with the framing path isolated from
a peer goroutine:

```
BenchmarkReader/64B      118.0 ns/op    542 MB/s   0 B/op   0 allocs/op
BenchmarkReader/512B     176.7 ns/op   2898 MB/s   0 B/op   0 allocs/op
BenchmarkReader/8192B    870.6 ns/op   9409 MB/s   0 B/op   0 allocs/op
BenchmarkWrite/64B        48.3 ns/op   1325 MB/s   0 B/op   0 allocs/op
BenchmarkWrite/512B       62.9 ns/op   8145 MB/s   0 B/op   0 allocs/op
BenchmarkWrite/8192B     101.2 ns/op  80971 MB/s   0 B/op   0 allocs/op
BenchmarkMask/8192B      627.7 ns/op  13051 MB/s   0 B/op   0 allocs/op
```

`TestReaderIsAllocationFree` and friends assert `0 allocs/op` via
`testing.AllocsPerRun`, so a regression fails the build. They skip under `-race`,
because the race runtime allocates on its own.

Masking is pure Go, eight bytes at a time. At ~13 GB/s it is not the bottleneck,
which is why there is no assembly here.

## Design decisions

**Context deadlines, not cancellation.** Read and write apply `ctx`'s deadline to
the connection and return immediately if `ctx` is already cancelled, but they do
not poll `ctx` while blocked. Polling would need either a per-connection watchdog
goroutine or a per-call registration, and both cost allocations on the hot path.
To interrupt a blocked call, use `CloseNow` from another goroutine — which is the
natural way to shut a connection down anyway. This is the one place where the API
is deliberately weaker than the alternatives.

**Limits are checked before allocating.** A frame header declaring more than
`MaxFrameSize` is rejected without reading its payload, so an attacker cannot
provoke a large allocation with a 64-bit length field.

**Control frames lock per frame, not per message.** A streaming writer takes the
write lock for each fragment and releases it in between, so an automatic pong or
a close frame never waits for a slow message to finish. RFC 6455 §5.4 permits
control frames between fragments, so this matches the spec rather than merely
being convenient.

**Strict framing.** Non-minimal length encodings, reserved opcodes, reserved bits
without a negotiated extension, unmasked client frames, masked server frames, and
fragmented control frames are all rejected with status 1002. Text messages are
validated as UTF-8 incrementally, across fragment boundaries, so a rune split
between two frames is handled and a message ending mid-rune is rejected with
1007.

## Not implemented

- **permessage-deflate** (RFC 7692). The gap that matters most for large
  repetitive payloads. `AcceptOptions` and the RSV validation are already shaped
  so it can be added without a rewrite.
- **Sending pings.** Received pings are answered automatically, which is what the
  spec requires; there is no API to originate one, because a useful version needs
  pong matching.
- **HTTP/2.** The handshake hijacks an HTTP/1.1 connection.

## Testing

```
go test ./ws/                                      # unit and integration
go test -race ./ws/                                # race detector
go test -run=XXX -fuzz=FuzzFrameParse ./ws/        # fuzz the header parser
WS_AUTOBAHN=1 go test ./ws/ -run TestAutobahn -timeout 15m   # conformance, needs Docker
```
