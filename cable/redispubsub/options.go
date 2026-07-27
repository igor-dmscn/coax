package redispubsub

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"time"
)

// Defaults. The timing ones are deliberately conservative: a pub/sub connection
// is idle most of the time, and the failure it must detect is a connection that
// looks alive but is not.
const (
	defaultAddress = "127.0.0.1:6379"

	defaultDialTimeout  = 5 * time.Second
	defaultWriteTimeout = 5 * time.Second

	// defaultPingInterval keeps the subscribe connection warm. Redis closes idle
	// clients when its own `timeout` is set, and a NAT or load balancer will drop
	// a silent connection without telling either end.
	defaultPingInterval = 30 * time.Second

	// defaultReadTimeout must be comfortably more than one ping interval, or a
	// quiet room looks like a dead connection. It is the half-open detector: no
	// bytes at all within this window means the connection is gone.
	defaultReadTimeout = 3 * defaultPingInterval

	defaultMinRetryBackoff = 100 * time.Millisecond
	defaultMaxRetryBackoff = 5 * time.Second

	// defaultMaxReplySize bounds one bulk string, so a broadcast payload has a
	// ceiling before it is allocated.
	defaultMaxReplySize = 8 << 20
)

// Options configures a Redis-backed PubSub. The zero value connects to
// 127.0.0.1:6379 with no authentication.
type Options struct {
	// Address is the Redis server's host:port. Empty means 127.0.0.1:6379.
	Address string

	// Username and Password authenticate the connection. A password alone sends
	// the legacy single-argument AUTH; both together send the Redis 6 ACL form.
	Username string
	Password string

	// TLS, when set, wraps both connections. ServerName is filled in from
	// Address when it is empty. Use rediss:// with ParseURL to get a default
	// configuration.
	TLS *tls.Config

	// Dialer opens a connection. When nil, a net.Dialer with DialTimeout is
	// used. Set it for Unix sockets, a proxy, or Sentinel-style address
	// discovery, which this adapter does not do itself.
	Dialer func(ctx context.Context, address string) (net.Conn, error)

	// Logger receives connection and protocol events. Reconnects are logged at
	// warn, since a silent reconnect is a silent gap in delivery. When nil,
	// slog.Default is used.
	Logger *slog.Logger

	// DialTimeout bounds establishing a connection, including the TLS handshake
	// and authentication.
	DialTimeout time.Duration

	// WriteTimeout bounds sending one command.
	WriteTimeout time.Duration

	// ReadTimeout is how long the subscribe connection may be completely silent
	// before it is considered dead. It must exceed PingInterval; a smaller value
	// is raised to three times it.
	ReadTimeout time.Duration

	// PingInterval is how often PING is sent on the subscribe connection.
	PingInterval time.Duration

	// MinRetryBackoff and MaxRetryBackoff bound the wait between reconnection
	// attempts, which doubles from the minimum up to the maximum.
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	// MaxReplySize bounds a single reply payload. A larger one fails the
	// connection rather than being allocated.
	MaxReplySize int
}

// ParseURL turns a Redis URL into Options:
//
//	redis://localhost:6379
//	redis://:secret@redis.internal:6379
//	redis://alice:secret@redis.internal:6379/0
//	rediss://redis.internal:6380          (TLS)
//
// A database number is accepted and ignored: Redis pub/sub is not scoped to a
// database, so subscribers on db 0 receive what was published on db 5. Rails'
// cable.yml URLs therefore work unchanged, including the database they usually
// carry.
func ParseURL(raw string) (*Options, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("redispubsub: parsing %q: %w", raw, err)
	}

	var opts Options
	switch u.Scheme {
	case "redis":
	case "rediss":
		opts.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	default:
		return nil, fmt.Errorf("redispubsub: unsupported URL scheme %q, want redis or rediss", u.Scheme)
	}

	if u.Host == "" {
		return nil, fmt.Errorf("redispubsub: %q has no host", raw)
	}
	opts.Address = u.Host
	if u.Port() == "" {
		opts.Address = net.JoinHostPort(u.Hostname(), "6379")
	}

	if u.User != nil {
		opts.Username = u.User.Username()
		opts.Password, _ = u.User.Password()
	}

	// Anything beyond a database number is a mistake worth reporting rather than
	// ignoring, since it would silently not do what it looks like.
	if path := strings.Trim(u.Path, "/"); path != "" {
		if strings.ContainsAny(path, "/") {
			return nil, fmt.Errorf("redispubsub: %q has an unexpected path", raw)
		}
	}

	return &opts, nil
}

// withDefaults returns a copy with zero fields filled in.
func (o Options) withDefaults() Options {
	if o.Address == "" {
		o.Address = defaultAddress
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = defaultDialTimeout
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = defaultWriteTimeout
	}
	if o.PingInterval <= 0 {
		o.PingInterval = defaultPingInterval
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = defaultReadTimeout
	}
	if o.ReadTimeout <= o.PingInterval {
		// A read timeout below the ping interval would kill every idle
		// connection on schedule.
		o.ReadTimeout = 3 * o.PingInterval
	}
	if o.MinRetryBackoff <= 0 {
		o.MinRetryBackoff = defaultMinRetryBackoff
	}
	if o.MaxRetryBackoff < o.MinRetryBackoff {
		o.MaxRetryBackoff = max(defaultMaxRetryBackoff, o.MinRetryBackoff)
	}
	if o.MaxReplySize <= 0 {
		o.MaxReplySize = defaultMaxReplySize
	}
	if o.Dialer == nil {
		dialer := &net.Dialer{Timeout: o.DialTimeout}
		o.Dialer = func(ctx context.Context, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", address)
		}
	}
	return o
}

// tlsConfig returns the configuration to use for a connection, filling in the
// server name from the address without modifying the caller's config.
func (o Options) tlsConfig() *tls.Config {
	if o.TLS == nil {
		return nil
	}
	if o.TLS.ServerName != "" || o.TLS.InsecureSkipVerify {
		return o.TLS
	}

	host, _, err := net.SplitHostPort(o.Address)
	if err != nil {
		return o.TLS
	}
	cfg := o.TLS.Clone()
	cfg.ServerName = host
	return cfg
}
