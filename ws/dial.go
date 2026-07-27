package ws

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DialOptions configures a client handshake. The zero value is usable.
type DialOptions struct {
	// Subprotocols to offer, most preferred first.
	Subprotocols []string

	// Header carries additional request headers, e.g. Origin or Cookie. The
	// handshake headers themselves may not be set here.
	Header http.Header

	// TLSConfig is used for wss:// connections.
	TLSConfig *tls.Config

	// MaxFrameSize and MaxMessageSize cap an individual frame and a whole
	// message. Zero applies the defaults.
	MaxFrameSize   int64
	MaxMessageSize int64
}

// Dial opens a WebSocket connection to a ws:// or wss:// URL.
//
// It performs the handshake over a plain net.Dialer rather than an http.Client,
// because the connection must be taken over wholesale once the server answers
// 101.
func Dial(ctx context.Context, rawURL string, opts *DialOptions) (*Conn, error) {
	if opts == nil {
		opts = &DialOptions{}
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("ws: parsing %q: %w", rawURL, err)
	}

	var useTLS bool
	switch u.Scheme {
	case "ws":
	case "wss":
		useTLS = true
	default:
		return nil, fmt.Errorf("ws: unsupported scheme %q, want ws or wss", u.Scheme)
	}

	addr := u.Host
	if u.Port() == "" {
		if useTLS {
			addr = net.JoinHostPort(u.Hostname(), "443")
		} else {
			addr = net.JoinHostPort(u.Hostname(), "80")
		}
	}

	var d net.Dialer
	rwc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ws: dialing %s: %w", addr, err)
	}

	if useTLS {
		tlsConn := tls.Client(rwc, tlsConfigFor(opts.TLSConfig, u.Hostname()))
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			rwc.Close()
			return nil, fmt.Errorf("ws: TLS handshake with %s: %w", addr, err)
		}
		rwc = tlsConn
	}

	conn, err := clientHandshake(ctx, rwc, u, opts)
	if err != nil {
		rwc.Close()
		return nil, err
	}
	return conn, nil
}

func tlsConfigFor(base *tls.Config, host string) *tls.Config {
	if base == nil {
		return &tls.Config{ServerName: host}
	}
	if base.ServerName != "" {
		return base
	}
	cfg := base.Clone()
	cfg.ServerName = host
	return cfg
}

func clientHandshake(ctx context.Context, rwc net.Conn, u *url.URL, opts *DialOptions) (*Conn, error) {
	if deadline, ok := ctx.Deadline(); ok {
		if err := rwc.SetDeadline(deadline); err != nil {
			return nil, err
		}
	}

	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("ws: generating handshake key: %w", err)
	}
	key := base64.StdEncoding.EncodeToString(nonce[:])

	httpURL := *u
	if u.Scheme == "wss" {
		httpURL.Scheme = "https"
	} else {
		httpURL.Scheme = "http"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("ws: building handshake request: %w", err)
	}
	for name, values := range opts.Header {
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", key)
	if len(opts.Subprotocols) > 0 {
		req.Header.Set("Sec-WebSocket-Protocol", strings.Join(opts.Subprotocols, ", "))
	}

	bw := bufio.NewWriter(rwc)
	if err := req.Write(bw); err != nil {
		return nil, fmt.Errorf("ws: writing handshake request: %w", err)
	}
	if err := bw.Flush(); err != nil {
		return nil, fmt.Errorf("ws: flushing handshake request: %w", err)
	}

	// The reader is reused for the connection: the server may have written
	// frames immediately after its handshake response.
	br := bufio.NewReader(rwc)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		return nil, fmt.Errorf("ws: reading handshake response: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		return nil, fmt.Errorf("ws: handshake failed with status %s", resp.Status)
	}
	if !headerHasToken(resp.Header, "Upgrade", "websocket") {
		return nil, fmt.Errorf("ws: response Upgrade header is %q", resp.Header.Get("Upgrade"))
	}
	if !headerHasToken(resp.Header, "Connection", "upgrade") {
		return nil, fmt.Errorf("ws: response Connection header is %q", resp.Header.Get("Connection"))
	}
	if got, want := resp.Header.Get("Sec-WebSocket-Accept"), secWebSocketAccept(key); got != want {
		return nil, fmt.Errorf("ws: Sec-WebSocket-Accept is %q, want %q", got, want)
	}

	subprotocol := resp.Header.Get("Sec-WebSocket-Protocol")
	if subprotocol != "" && !containsFold(opts.Subprotocols, subprotocol) {
		return nil, fmt.Errorf("ws: server chose subprotocol %q which was not offered", subprotocol)
	}

	if err := rwc.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	return newConn(rwc, br, bufio.NewWriter(rwc), true, subprotocol, opts.MaxFrameSize, opts.MaxMessageSize), nil
}

func containsFold(haystack []string, needle string) bool {
	for _, v := range haystack {
		if strings.EqualFold(v, needle) {
			return true
		}
	}
	return false
}

// newMaskKey fills key with cryptographically random bytes. Every client frame
// needs a fresh key (RFC 6455 §5.3).
func newMaskKey(key *[4]byte) error {
	if _, err := rand.Read(key[:]); err != nil {
		return fmt.Errorf("ws: generating mask key: %w", err)
	}
	return nil
}
