package ws

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// keyGUID is the fixed value concatenated with Sec-WebSocket-Key to derive the
// accept token (RFC 6455 §4.2.2).
const keyGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// AcceptOptions configures a server handshake. The zero value is usable: it
// accepts same-origin requests, negotiates no subprotocol, and applies the
// default size limits.
type AcceptOptions struct {
	// Subprotocols the server supports, most preferred first. The first one the
	// client also offers is selected.
	Subprotocols []string

	// OriginPatterns are host patterns permitted in the Origin header, matched
	// case-insensitively and supporting path.Match wildcards, e.g.
	// "*.example.com". The request host itself is always allowed. A request
	// without an Origin header is allowed, since only browsers send one.
	OriginPatterns []string

	// InsecureSkipVerify disables Origin checking entirely. Doing so lets any
	// site on the internet open a connection with the user's cookies, so it is
	// appropriate only when the endpoint requires its own credentials.
	InsecureSkipVerify bool

	// MaxFrameSize and MaxMessageSize cap an individual frame and a whole
	// message. Zero applies the defaults of 1 MiB and 32 MiB. A frame declaring
	// more than the limit is rejected from its header, before any payload is
	// read or buffered.
	MaxFrameSize   int64
	MaxMessageSize int64
}

// Accept completes a WebSocket handshake on an HTTP request and returns the
// connection.
//
// Accept hijacks the underlying network connection, so w must not be written to
// afterwards and the handler must not return until the connection is finished
// with. On failure Accept writes an error response and returns; the caller
// should not write one of its own.
//
//	conn, err := ws.Accept(w, r, &ws.AcceptOptions{Subprotocols: []string{"chat"}})
//	if err != nil {
//	    return
//	}
//	defer conn.CloseNow()
func Accept(w http.ResponseWriter, r *http.Request, opts *AcceptOptions) (*Conn, error) {
	if opts == nil {
		opts = &AcceptOptions{}
	}

	if err := verifyUpgrade(r); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, err
	}
	if !opts.InsecureSkipVerify {
		if err := verifyOrigin(r, opts.OriginPatterns); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return nil, err
		}
	}

	subprotocol := selectSubprotocol(r, opts.Subprotocols)

	rwc, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		err = fmt.Errorf("ws: hijack failed (HTTP/1.1 only): %w", err)
		http.Error(w, "websocket upgrade unsupported", http.StatusInternalServerError)
		return nil, err
	}

	// The HTTP server's read and write timeouts would otherwise apply to a
	// connection that is meant to live indefinitely.
	if err := rwc.SetDeadline(time.Time{}); err != nil {
		rwc.Close()
		return nil, fmt.Errorf("ws: clearing deadlines: %w", err)
	}

	if err := writeHandshakeResponse(brw, r.Header.Get("Sec-WebSocket-Key"), subprotocol); err != nil {
		rwc.Close()
		return nil, err
	}

	// brw's reader may already hold bytes the client pipelined after the
	// request, so it must be carried into the connection rather than replaced.
	return newConn(rwc, brw.Reader, brw.Writer, false, subprotocol, opts.MaxFrameSize, opts.MaxMessageSize), nil
}

func verifyUpgrade(r *http.Request) error {
	if r.Method != http.MethodGet {
		return fmt.Errorf("ws: handshake must be GET, got %s", r.Method)
	}
	if v := r.Header.Get("Sec-WebSocket-Version"); v != "13" {
		return fmt.Errorf("ws: unsupported Sec-WebSocket-Version %q, need 13", v)
	}
	if !headerHasToken(r.Header, "Connection", "upgrade") {
		return fmt.Errorf("ws: Connection header %q does not contain upgrade", r.Header.Get("Connection"))
	}
	if !headerHasToken(r.Header, "Upgrade", "websocket") {
		return fmt.Errorf("ws: Upgrade header %q is not websocket", r.Header.Get("Upgrade"))
	}

	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return fmt.Errorf("ws: missing Sec-WebSocket-Key")
	}
	// RFC 6455 §4.1: the key is a base64-encoded 16-byte nonce.
	if decoded, err := base64.StdEncoding.DecodeString(key); err != nil || len(decoded) != 16 {
		return fmt.Errorf("ws: Sec-WebSocket-Key %q is not a base64 16-byte value", key)
	}
	return nil
}

// verifyOrigin allows requests with no Origin, requests from the same host, and
// requests matching a configured pattern.
func verifyOrigin(r *http.Request, patterns []string) error {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return nil
	}

	u, err := url.Parse(origin)
	if err != nil {
		return fmt.Errorf("ws: cannot parse Origin %q: %w", origin, err)
	}

	host := strings.ToLower(u.Host)
	if host == strings.ToLower(r.Host) {
		return nil
	}
	for _, pattern := range patterns {
		if ok, err := path.Match(strings.ToLower(pattern), host); err == nil && ok {
			return nil
		}
	}
	return fmt.Errorf("ws: request Origin %q is not allowed", origin)
}

// selectSubprotocol returns the first server-preferred subprotocol the client
// also offered, or "" when there is no overlap.
func selectSubprotocol(r *http.Request, supported []string) string {
	for _, want := range supported {
		if headerHasToken(r.Header, "Sec-WebSocket-Protocol", want) {
			return want
		}
	}
	return ""
}

func writeHandshakeResponse(brw *bufio.ReadWriter, key, subprotocol string) error {
	brw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	brw.WriteString("Upgrade: websocket\r\n")
	brw.WriteString("Connection: Upgrade\r\n")
	brw.WriteString("Sec-WebSocket-Accept: ")
	brw.WriteString(secWebSocketAccept(key))
	brw.WriteString("\r\n")
	if subprotocol != "" {
		brw.WriteString("Sec-WebSocket-Protocol: ")
		brw.WriteString(subprotocol)
		brw.WriteString("\r\n")
	}
	brw.WriteString("\r\n")

	if err := brw.Flush(); err != nil {
		return fmt.Errorf("ws: writing handshake response: %w", err)
	}
	return nil
}

// secWebSocketAccept derives the Sec-WebSocket-Accept value for a client key.
func secWebSocketAccept(key string) string {
	h := sha1.New()
	h.Write([]byte(key))
	h.Write([]byte(keyGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// headerHasToken reports whether a comma-separated header contains a token,
// compared case-insensitively.
func headerHasToken(h http.Header, name, token string) bool {
	for _, value := range h.Values(name) {
		for value != "" {
			var candidate string
			candidate, value, _ = strings.Cut(value, ",")
			if strings.EqualFold(strings.TrimSpace(candidate), token) {
				return true
			}
		}
	}
	return false
}
