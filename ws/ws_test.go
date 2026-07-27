package ws

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// dialTestServer starts an HTTP test server running handler and dials it.
func dialTestServer(t *testing.T, handler http.HandlerFunc, opts *DialOptions) *Conn {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	conn, err := Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), opts)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

// echoHandler accepts a connection and echoes every message back until the peer
// closes. Errors are reported through errc so the test can inspect them.
func echoHandler(t *testing.T, opts *AcceptOptions, errc chan<- error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := Accept(w, r, opts)
		if err != nil {
			if errc != nil {
				errc <- err
			}
			return
		}
		defer conn.CloseNow()

		ctx := r.Context()
		for {
			typ, reader, err := conn.Reader(ctx)
			if err != nil {
				if errc != nil {
					errc <- err
				}
				return
			}

			writer, err := conn.Writer(ctx, typ)
			if err != nil {
				if errc != nil {
					errc <- err
				}
				return
			}
			if _, err := io.Copy(writer, reader); err != nil {
				writer.Close()
				if errc != nil {
					errc <- err
				}
				return
			}
			if err := writer.Close(); err != nil {
				if errc != nil {
					errc <- err
				}
				return
			}
		}
	}
}

func TestEchoRoundTrip(t *testing.T) {
	conn := dialTestServer(t, echoHandler(t, nil, nil), nil)
	ctx := context.Background()

	sizes := []int{0, 1, 125, 126, 127, 8192, 70000} // spans all three length encodings
	for _, size := range sizes {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte('a' + i%26)
		}

		if err := conn.Write(ctx, MessageBinary, payload); err != nil {
			t.Fatalf("Write(%d bytes) error = %v", size, err)
		}
		typ, got, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("Read() after %d bytes error = %v", size, err)
		}
		if typ != MessageBinary {
			t.Errorf("type = %v, want %v", typ, MessageBinary)
		}
		if string(got) != string(payload) {
			t.Errorf("echo of %d bytes did not match", size)
		}
	}
}

func TestTextRoundTripWithMultibyteRunes(t *testing.T) {
	conn := dialTestServer(t, echoHandler(t, nil, nil), nil)
	ctx := context.Background()

	const text = "héllo → 世界 🎉"
	if err := conn.Write(ctx, MessageText, []byte(text)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	typ, got, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if typ != MessageText {
		t.Errorf("type = %v, want %v", typ, MessageText)
	}
	if string(got) != text {
		t.Errorf("got %q, want %q", got, text)
	}
}

// TestFragmentedMessage drives the streaming writer past its internal buffer so
// the message goes out as continuation fragments, and checks it reassembles.
func TestFragmentedMessage(t *testing.T) {
	conn := dialTestServer(t, echoHandler(t, nil, nil), nil)
	ctx := context.Background()

	chunk := strings.Repeat("x", streamWriteBuffer/2)
	writer, err := conn.Writer(ctx, MessageText)
	if err != nil {
		t.Fatalf("Writer() error = %v", err)
	}
	for range 5 { // 2.5 buffers worth, so several fragments
		if _, err := io.WriteString(writer, chunk); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	_, got, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if want := strings.Repeat(chunk, 5); string(got) != want {
		t.Errorf("got %d bytes, want %d", len(got), len(want))
	}
}

func TestCloseHandshake(t *testing.T) {
	serverErr := make(chan error, 1)
	conn := dialTestServer(t, echoHandler(t, nil, serverErr), nil)

	if err := conn.Close(StatusNormalClosure, "done"); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// The server's reader must report the close with the status we sent.
	select {
	case err := <-serverErr:
		if got := CloseStatus(err); got != StatusNormalClosure {
			t.Errorf("server CloseStatus = %v, want %v", got, StatusNormalClosure)
		}
		var ce CloseError
		if errors.As(err, &ce) && ce.Reason != "done" {
			t.Errorf("server reason = %q, want %q", ce.Reason, "done")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not report the close")
	}

	// Operations after close report the terminal error rather than blocking.
	if _, _, err := conn.Read(context.Background()); err == nil {
		t.Error("Read() after Close() = nil error, want the close error")
	}
}

// TestCloseSend covers the close a server with a dedicated reader goroutine can
// actually perform: the peer still learns the status, which is the whole
// difference from CloseNow.
func TestCloseSend(t *testing.T) {
	serverErr := make(chan error, 1)
	conn := dialTestServer(t, echoHandler(t, nil, serverErr), nil)

	if err := conn.CloseSend(StatusGoingAway, "restarting"); err != nil {
		t.Fatalf("CloseSend() error = %v", err)
	}

	select {
	case err := <-serverErr:
		if got := CloseStatus(err); got != StatusGoingAway {
			t.Errorf("peer CloseStatus = %v, want %v", got, StatusGoingAway)
		}
		var ce CloseError
		if errors.As(err, &ce) && ce.Reason != "restarting" {
			t.Errorf("peer reason = %q, want %q", ce.Reason, "restarting")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the peer did not report the close")
	}

	// Repeated calls, and a later CloseNow, report the first cause rather than
	// closing twice: teardown paths overlap.
	if got := CloseStatus(conn.CloseSend(StatusNormalClosure, "")); got != StatusGoingAway {
		t.Errorf("second CloseSend() = %v, want the first cause", got)
	}
	if got := CloseStatus(conn.CloseNow()); got != StatusGoingAway {
		t.Errorf("CloseNow() after CloseSend() = %v, want the first cause", got)
	}
	if _, _, err := conn.Read(context.Background()); err == nil {
		t.Error("Read() after CloseSend() = nil error, want the close error")
	}
}

func TestCloseReasonTooLong(t *testing.T) {
	conn := dialTestServer(t, echoHandler(t, nil, nil), nil)

	err := conn.Close(StatusNormalClosure, strings.Repeat("x", maxCloseReason+1))
	if err == nil {
		t.Fatal("Close() with an oversized reason = nil error, want a failure")
	}
	if !strings.Contains(err.Error(), "close reason") {
		t.Errorf("Close() error = %v, want it to mention the reason length", err)
	}
}

func TestSubprotocolNegotiation(t *testing.T) {
	tests := []struct {
		name     string
		server   []string
		client   []string
		want     string
		wantFail bool
	}{
		{
			name:   "first server preference wins",
			server: []string{"a", "b"},
			client: []string{"b", "a"},
			want:   "a",
		},
		{
			name:   "only mutual protocol",
			server: []string{"a", "b"},
			client: []string{"b"},
			want:   "b",
		},
		{
			name:   "no overlap negotiates nothing",
			server: []string{"a"},
			client: []string{"z"},
			want:   "",
		},
		{
			name:   "client offers none",
			server: []string{"a"},
			client: nil,
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := dialTestServer(t,
				echoHandler(t, &AcceptOptions{Subprotocols: tt.server}, nil),
				&DialOptions{Subprotocols: tt.client})

			if got := conn.Subprotocol(); got != tt.want {
				t.Errorf("Subprotocol() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAcceptRejectsBadHandshake(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		headers    map[string]string
		wantStatus int
	}{
		{
			name:       "not a GET",
			method:     http.MethodPost,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing upgrade",
			headers:    map[string]string{"Connection": "keep-alive"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "wrong version",
			headers:    map[string]string{"Sec-WebSocket-Version": "8"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "key is not 16 base64 bytes",
			headers:    map[string]string{"Sec-WebSocket-Key": "tooshort"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "cross origin",
			headers:    map[string]string{"Origin": "http://evil.example"},
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(echoHandler(t, nil, nil))
			defer srv.Close()

			method := tt.method
			if method == "" {
				method = http.MethodGet
			}
			req, err := http.NewRequest(method, srv.URL, nil)
			if err != nil {
				t.Fatalf("NewRequest() error = %v", err)
			}
			// A valid handshake, then broken by the case's override.
			req.Header.Set("Connection", "Upgrade")
			req.Header.Set("Upgrade", "websocket")
			req.Header.Set("Sec-WebSocket-Version", "13")
			req.Header.Set("Sec-WebSocket-Key", "AQIDBAUGBwgJCgsMDQ4PEA==")
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Do() error = %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}
}

func TestOriginPatterns(t *testing.T) {
	srv := httptest.NewServer(echoHandler(t, &AcceptOptions{
		OriginPatterns: []string{"*.allowed.example"},
	}, nil))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx := context.Background()

	t.Run("matching pattern", func(t *testing.T) {
		conn, err := Dial(ctx, url, &DialOptions{
			Header: http.Header{"Origin": []string{"http://app.allowed.example"}},
		})
		if err != nil {
			t.Fatalf("Dial() error = %v", err)
		}
		conn.CloseNow()
	})

	t.Run("non-matching origin", func(t *testing.T) {
		if _, err := Dial(ctx, url, &DialOptions{
			Header: http.Header{"Origin": []string{"http://other.example"}},
		}); err == nil {
			t.Error("Dial() from a disallowed origin succeeded")
		}
	})
}

func TestMaxMessageSize(t *testing.T) {
	serverErr := make(chan error, 1)
	conn := dialTestServer(t, echoHandler(t, &AcceptOptions{MaxMessageSize: 1024}, serverErr), nil)
	ctx := context.Background()

	if err := conn.Write(ctx, MessageBinary, make([]byte, 4096)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// The server rejects it and closes with 1009.
	if _, _, err := conn.Read(ctx); CloseStatus(err) != StatusMessageTooBig {
		t.Errorf("client close status = %v (err %v), want %v", CloseStatus(err), err, StatusMessageTooBig)
	}

	select {
	case err := <-serverErr:
		if !strings.Contains(err.Error(), "exceeds limit") {
			t.Errorf("server error = %v, want a size limit failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not report the oversized message")
	}
}

func TestWriteAfterCloseFails(t *testing.T) {
	conn := dialTestServer(t, echoHandler(t, nil, nil), nil)

	if err := conn.Close(StatusNormalClosure, ""); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := conn.Write(context.Background(), MessageText, []byte("late")); err == nil {
		t.Error("Write() after Close() = nil error, want the close error")
	}
}

func TestReadWithCancelledContext(t *testing.T) {
	conn := dialTestServer(t, echoHandler(t, nil, nil), nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := conn.Read(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Read() with a cancelled context = %v, want context.Canceled", err)
	}
}

func TestReadDeadlineFromContext(t *testing.T) {
	// A server that accepts and then never sends anything.
	conn := dialTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		c, err := Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		<-r.Context().Done()
	}, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("Read() = nil error, want a timeout")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Read() blocked for %v, want it to honour the context deadline", elapsed)
	}
}

// TestPingIsAnsweredAutomatically checks that a ping arriving while the peer
// waits for data is answered without application involvement.
func TestPingIsAnsweredAutomatically(t *testing.T) {
	conn := dialTestServer(t, echoHandler(t, nil, nil), nil)
	ctx := context.Background()

	// Send a ping followed by a text message. The pong must come back first,
	// and Reader must skip it and surface only the echoed message.
	conn.writeMu.Lock()
	if err := conn.writeFrameLocked(opPing, []byte("hi"), true); err != nil {
		conn.writeMu.Unlock()
		t.Fatalf("writing ping: %v", err)
	}
	conn.writeMu.Unlock()

	if err := conn.Write(ctx, MessageText, []byte("data")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	typ, got, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if typ != MessageText || string(got) != "data" {
		t.Errorf("Read() = (%v, %q), want (text, %q)", typ, got, "data")
	}
}
