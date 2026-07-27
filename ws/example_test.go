package ws_test

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"time"

	"go-cable/ws"
)

// ExampleAccept shows an echo server. Accept hijacks the connection, so the
// handler owns it until it returns.
func ExampleAccept() {
	http.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		conn, err := ws.Accept(w, r, &ws.AcceptOptions{
			OriginPatterns: []string{"*.example.com"},
		})
		if err != nil {
			return // Accept has already written an error response
		}
		defer conn.CloseNow()

		for {
			typ, data, err := conn.Read(r.Context())
			if err != nil {
				if ws.CloseStatus(err) == ws.StatusNormalClosure {
					return
				}
				log.Printf("read: %v", err)
				return
			}
			if err := conn.Write(r.Context(), typ, data); err != nil {
				return
			}
		}
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}

// ExampleDial shows a client exchanging one message and closing cleanly.
func ExampleDial() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := ws.Dial(ctx, "ws://localhost:8080/echo", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.CloseNow()

	if err := conn.Write(ctx, ws.MessageText, []byte("hello")); err != nil {
		log.Fatal(err)
	}
	_, data, err := conn.Read(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("echoed: %s", data)

	if err := conn.Close(ws.StatusNormalClosure, ""); err != nil {
		log.Fatal(err)
	}
}

// ExampleConn_Reader shows the allocation-free path: the message is streamed
// through a buffer the caller owns and reuses.
func ExampleConn_Reader() {
	var conn *ws.Conn // from Accept or Dial
	ctx := context.Background()
	buf := make([]byte, 4096)

	for {
		typ, r, err := conn.Reader(ctx)
		if err != nil {
			return
		}

		w, err := conn.Writer(ctx, typ)
		if err != nil {
			return
		}
		for {
			n, err := r.Read(buf)
			if n > 0 {
				if _, werr := w.Write(buf[:n]); werr != nil {
					return
				}
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return
			}
		}
		if err := w.Close(); err != nil {
			return
		}
	}
}
