package cable

import (
	"log/slog"
	"net/http"
	"time"
)

// heartbeatInterval is how often a ping is sent to every connection.
// The Rails JS client treats a connection with no traffic for twice this long as
// stale and reconnects, so it is a protocol constant rather than a tuning knob.
// ← actioncable/lib/action_cable/server/connections.rb:14 (BEAT_INTERVAL)
const heartbeatInterval = 3 * time.Second

// defaultSendBuffer is how many outbound messages may be queued per connection
// before it is considered too slow to keep.
const defaultSendBuffer = 64

// Identifiers describe who a connection belongs to, for example
// {"current_user": "42"}. They are set by an Authenticator and are visible to
// every channel on the connection.
//
// ← actioncable/lib/action_cable/connection/identification.rb (identified_by)
type Identifiers map[string]string

// Authenticator decides whether a connection may proceed, returning the
// identifiers to attach to it. Returning an error rejects the connection: the
// client is sent a disconnect message with reason "unauthorized" and
// reconnect=false, so it stays down instead of retrying.
//
// It runs after the WebSocket handshake has completed, so a rejection is
// reported in a WebSocket frame rather than an HTTP status. Cookies, headers and
// query parameters are all available on the request.
type Authenticator func(*http.Request) (Identifiers, error)

// Options configures a Server. The zero value is usable: it accepts every
// same-origin connection with no identifiers and logs to slog's default.
type Options struct {
	// Authenticate is called once per connection after the handshake. When nil,
	// every connection is accepted with no identifiers.
	Authenticate Authenticator

	// AllowedOrigins are the origins permitted to connect, as host patterns
	// matched case-insensitively with optional wildcards, e.g.
	// "*.example.com". The server's own host is always allowed, so a
	// same-origin browser connection needs no configuration.
	//
	// ← actioncable/lib/action_cable/server/base.rb:154 (allow_request_origin?)
	AllowedOrigins []string

	// DisableOriginCheck accepts connections from any origin. Doing so lets any
	// website open a connection carrying the user's cookies, so it is only
	// appropriate when the endpoint authenticates with its own credentials.
	DisableOriginCheck bool

	// Logger receives connection lifecycle and error events. When nil,
	// slog.Default is used.
	Logger *slog.Logger

	// PubSub carries broadcasts to the processes that have subscribers for them.
	// When nil, a MemoryPubSub is created and closed with the server.
	//
	// That default is right for a single process and wrong for more than one:
	// in-memory delivery never leaves the process that published, so half the
	// clients silently miss every broadcast. Running more than one process means
	// setting a shared backend here.
	PubSub PubSub

	// SendBuffer is how many outbound messages may be queued for one connection
	// before it is dropped as too slow. Zero applies defaultSendBuffer.
	//
	// Unlike Rails, whose write queue is unbounded, a client that cannot keep up
	// is disconnected rather than allowed to consume memory without limit.
	SendBuffer int

	// HeartbeatInterval overrides how often pings are sent. Zero applies
	// heartbeatInterval, which is what the Rails JS client expects; changing it
	// is mainly useful in tests.
	HeartbeatInterval time.Duration
}

// withDefaults returns a copy with zero fields filled in.
func (o Options) withDefaults() Options {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.SendBuffer <= 0 {
		o.SendBuffer = defaultSendBuffer
	}
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = heartbeatInterval
	}
	if o.Authenticate == nil {
		o.Authenticate = func(*http.Request) (Identifiers, error) { return nil, nil }
	}
	return o
}
