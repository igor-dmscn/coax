// Package ws is a minimal, dependency-free WebSocket implementation (RFC 6455)
// for servers and clients.
//
// It passes the Autobahn test suite for every case outside the compression
// extension; see testdata/autobahn/summary.json for the recorded result.
//
// # Server
//
// Accept upgrades an HTTP request. It hijacks the underlying connection, so the
// handler must not write to the ResponseWriter afterwards and must not return
// until it is finished with the connection.
//
//	func handler(w http.ResponseWriter, r *http.Request) {
//		conn, err := ws.Accept(w, r, &ws.AcceptOptions{
//			Subprotocols: []string{"chat"},
//		})
//		if err != nil {
//			return // Accept already wrote an error response
//		}
//		defer conn.CloseNow()
//
//		for {
//			typ, data, err := conn.Read(r.Context())
//			if err != nil {
//				return
//			}
//			if err := conn.Write(r.Context(), typ, data); err != nil {
//				return
//			}
//		}
//	}
//
// # Client
//
// Dial connects to a ws:// or wss:// URL.
//
//	conn, err := ws.Dial(ctx, "ws://localhost:8080/chat", nil)
//
// # Concurrency
//
// A Conn supports one concurrent reader and one concurrent writer. Reads and
// writes may proceed at the same time as each other, but the read methods must
// not be called from two goroutines at once, and neither must the write methods.
// Control frames are handled internally: a ping is answered automatically, even
// while a fragmented message is being read.
//
// # Allocations
//
// Reader and Writer are the allocation-free path: they stream through
// per-connection buffers and read straight into the caller's slice. Read and
// Write are conveniences on top and allocate per message, because a method that
// returns a []byte has to put the bytes somewhere. On a server connection,
// Write also allocates nothing, since server frames are not masked and the
// payload is written through untouched.
//
// # Context handling
//
// The read and write methods apply ctx's deadline to the underlying connection
// and return immediately if ctx is already cancelled, but they do not poll ctx
// while blocked — that keeps the hot path free of allocations and of a
// per-connection watchdog goroutine. To interrupt a blocked call, use CloseNow
// from another goroutine, which is also the natural way to shut a connection
// down.
//
// # Limits
//
// AcceptOptions and DialOptions cap both a single frame and a whole message.
// A frame declaring more than the limit is rejected from its header, before any
// payload is read, so an oversized declaration cannot cause a large allocation.
//
// # Not implemented
//
// The permessage-deflate extension (RFC 7692) is not implemented, and neither is
// sending pings. Frames with reserved bits set are rejected, as they must be
// without a negotiated extension. HTTP/2 is not supported: the WebSocket
// handshake here relies on hijacking an HTTP/1.1 connection.
package ws
