// Package redispubsub implements cable.PubSub on Redis pub/sub, so that
// broadcasts reach clients connected to every process sharing one Redis.
//
//	ps := redispubsub.New(&redispubsub.Options{Address: "localhost:6379"})
//	defer ps.Close()
//
//	srv := cable.New(&cable.Options{PubSub: ps})
//
// It speaks RESP2 directly and has no dependencies. The protocol is five types
// wide (see resp.go) and pub/sub uses four commands, so a client library would be
// a large dependency for a small need. What it does not do, deliberately:
// Sentinel discovery and Redis Cluster's sharded pub/sub (SSUBSCRIBE). Ordinary
// PUBLISH is broadcast across a cluster, so a cluster works as one server; for
// Sentinel, resolve the address with Options.Dialer.
//
// # Wire compatibility with Rails
//
// The format is Rails' own: PUBLISH to a channel named after the broadcasting,
// with the message JSON as the payload. A Rails process and a Go process on one
// Redis reach each other's clients with no translation.
// ← actioncable/lib/action_cable/subscription_adapter/redis.rb
//
// # Delivery guarantees
//
// Redis pub/sub is fire and forget, and so is this: a broadcast published while a
// process is reconnecting is not delivered to that process's clients, and nothing
// reports the gap. Rails has the same property. Reconnections are logged at warn
// so the gap is at least visible. Clients that need to catch up have to replay
// from a database, which is an application concern rather than a transport one.
package redispubsub

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"go-cable/cable"
)

// PubSub is a Redis-backed cable.PubSub.
//
// It holds two connections, because a subscribed Redis connection may not be used
// for anything else: one is in subscribe mode and read by a single goroutine, the
// other publishes and is used one command at a time.
//
// ← actioncable/lib/action_cable/subscription_adapter/redis.rb:29
type PubSub struct {
	opts Options

	ctx    context.Context
	cancel context.CancelFunc

	startOnce sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup

	// mu guards the subscription map and the current subscribe connection. It is
	// never held across a socket write.
	mu     sync.Mutex
	subs   map[string]*broadcasting
	conn   net.Conn
	closed bool

	// writeMu serializes writes to the subscribe connection.
	writeMu  sync.Mutex
	writeBuf []byte

	// pubMu serializes use of the publish connection, which is a plain
	// request/response socket.
	pubMu   sync.Mutex
	pubConn net.Conn
	pubRead *bufio.Reader
	pubBuf  []byte
}

// broadcasting is one Redis channel: the local handlers listening to it, and
// whether Redis has confirmed the subscription on the current connection.
//
// ← actioncable/lib/action_cable/subscription_adapter/subscriber_map.rb
type broadcasting struct {
	subs map[*subscriber]struct{}

	// active is true once a subscribe confirmation has arrived. It goes back to
	// false when the connection drops, and ready is replaced, so that a
	// Subscribe arriving mid-reconnect waits for the new confirmation rather than
	// believing an old one.
	active bool
	ready  chan struct{}
}

// subscriber wraps a handler so that identical handlers are distinct subscribers.
type subscriber struct{ handle cable.Handler }

var _ cable.PubSub = (*PubSub)(nil)

// New returns a PubSub. opts may be nil. It does not connect: the subscribe
// connection is opened on the first Subscribe and the publish connection on the
// first Broadcast, so a process that never uses one never opens it.
func New(opts *Options) *PubSub {
	if opts == nil {
		opts = &Options{}
	}
	p := &PubSub{opts: opts.withDefaults()}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.subs = make(map[string]*broadcasting)
	return p
}

// Subscribe registers h for a broadcasting, blocking until Redis has confirmed
// the subscription so that a caller can rely on nothing being missed afterwards.
//
// While the connection is down it waits for the reconnection rather than failing,
// bounded by ctx: the subscription is recorded first, and every recorded
// subscription is replayed onto the new connection.
func (p *PubSub) Subscribe(ctx context.Context, name string, h cable.Handler) (func(), error) {
	if h == nil {
		return nil, errors.New("redispubsub: Subscribe with a nil handler")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s := &subscriber{handle: h}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, cable.ErrPubSubClosed
	}
	// Started while holding the lock that Close also takes, so a Subscribe
	// racing a Close either starts the goroutines before Close waits for them,
	// or sees closed and never starts them at all.
	p.start()

	b := p.subs[name]
	if b == nil {
		b = &broadcasting{subs: make(map[*subscriber]struct{}, 1), ready: make(chan struct{})}
		p.subs[name] = b
	}
	b.subs[s] = struct{}{}
	first, active, ready, conn := len(b.subs) == 1, b.active, b.ready, p.conn
	p.mu.Unlock()

	unsubscribe := func() { p.removeSubscriber(name, s) }

	// Already confirmed: one Redis subscription serves every local subscriber.
	// ← actioncable/lib/action_cable/subscription_adapter/subscriber_map.rb:12
	if active {
		return unsubscribe, nil
	}

	// Only the first subscriber sends SUBSCRIBE. A later one arriving before the
	// confirmation waits for the same one.
	if first && conn != nil {
		p.send(conn, "SUBSCRIBE", name)
	}

	select {
	case <-ready:
		return unsubscribe, nil
	case <-ctx.Done():
		unsubscribe()
		return nil, fmt.Errorf("redispubsub: subscribing to %q: %w", name, ctx.Err())
	case <-p.ctx.Done():
		unsubscribe()
		return nil, cable.ErrPubSubClosed
	}
}

// Broadcast publishes a payload to a broadcasting.
func (p *PubSub) Broadcast(ctx context.Context, name string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.isClosed() {
		return cable.ErrPubSubClosed
	}

	p.pubMu.Lock()
	defer p.pubMu.Unlock()

	// Two attempts, because Redis closes idle clients when its `timeout` is set
	// and a load balancer will drop a quiet connection: the first publish after a
	// silent spell can land on a socket the server has already forgotten. A
	// connection we just opened gets no second chance, so a real outage fails
	// immediately instead of being retried twice.
	for {
		conn, reader, fresh, err := p.publishConn(ctx)
		if err != nil {
			return err
		}

		err = p.publish(ctx, conn, reader, name, payload)
		if err == nil {
			return nil
		}

		p.dropPublishConn()

		var redisErr *Error
		if fresh || errors.As(err, &redisErr) {
			return err
		}
		p.opts.Logger.Debug("redispubsub: retrying publish on a new connection", "error", err)
	}
}

// Close stops the connections and releases everything. It is safe to call more
// than once, and safe to call while subscriptions are still live: the unsubscribe
// functions handed out earlier remain callable and become no-ops.
func (p *PubSub) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		conn := p.conn
		p.conn = nil
		p.mu.Unlock()

		// Cancelling first releases anything waiting on a confirmation; closing
		// the connection unblocks the reader.
		p.cancel()
		if conn != nil {
			conn.Close()
		}

		p.pubMu.Lock()
		p.dropPublishConn()
		p.pubMu.Unlock()

		p.wg.Wait()
	})
	return nil
}

func (p *PubSub) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// start launches the subscribe connection's goroutines, once. The caller holds mu.
func (p *PubSub) start() {
	p.startOnce.Do(func() {
		p.wg.Add(2)
		go p.run()
		go p.keepalive()
	})
}

// run keeps a subscribe connection up for as long as the adapter is open,
// reconnecting with exponential backoff.
func (p *PubSub) run() {
	defer p.wg.Done()

	backoff := p.opts.MinRetryBackoff
	for {
		if p.ctx.Err() != nil {
			return
		}

		conn, reader, err := p.dial(p.ctx)
		if err != nil {
			p.opts.Logger.Warn("redispubsub: cannot connect", "address", p.opts.Address,
				"error", err, "retrying_in", backoff)
			if !p.sleep(backoff) {
				return
			}
			backoff = min(backoff*2, p.opts.MaxRetryBackoff)
			continue
		}
		backoff = p.opts.MinRetryBackoff

		p.resubscribe(conn)
		err = p.readLoop(conn, reader)
		p.dropConn(conn)

		if p.ctx.Err() != nil {
			return
		}
		// Every disconnection is a gap in delivery, so it is worth a warning even
		// though it is recovered automatically.
		p.opts.Logger.Warn("redispubsub: subscribe connection lost", "error", err)
		if !p.sleep(backoff) {
			return
		}
	}
}

// resubscribe installs the current subscription map on a new connection, and is
// the reason a dropped SUBSCRIBE write needs no error handling anywhere: the map
// is the source of truth and it is replayed in full.
func (p *PubSub) resubscribe(conn net.Conn) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		return
	}
	p.conn = conn
	names := make([]string, 0, len(p.subs))
	for name := range p.subs {
		names = append(names, name)
	}
	p.mu.Unlock()

	if len(names) == 0 {
		return
	}
	p.opts.Logger.Debug("redispubsub: resubscribing", "broadcastings", len(names))
	for _, name := range names {
		p.send(conn, "SUBSCRIBE", name)
	}
}

// readLoop reads pushes and confirmations until the connection fails.
func (p *PubSub) readLoop(conn net.Conn, reader *bufio.Reader) error {
	// Reused across messages: only this goroutine dispatches, so one scratch
	// slice serves every delivery without allocating.
	var scratch []*subscriber

	for {
		if err := conn.SetReadDeadline(time.Now().Add(p.opts.ReadTimeout)); err != nil {
			return err
		}

		reply, err := readValue(reader, p.opts.MaxReplySize)
		if err != nil {
			return err
		}
		scratch = p.handleReply(reply, scratch)
	}
}

// handleReply routes one reply from the subscribe connection.
//
// Confirmations and pushes share the connection and are told apart only by their
// first element, which is why a subscribed connection cannot be used for ordinary
// request/response: there is nothing to correlate a reply with a command.
func (p *PubSub) handleReply(reply value, scratch []*subscriber) []*subscriber {
	if reply.typ == typeError {
		p.opts.Logger.Error("redispubsub: error reply on the subscribe connection", "error", reply.str())
		return scratch
	}
	// PING outside subscribe mode answers +PONG rather than an array.
	if reply.isString() {
		return scratch
	}
	if reply.typ != typeArray || len(reply.arr) < 2 || !reply.arr[0].isString() {
		p.opts.Logger.Debug("redispubsub: ignoring unexpected reply", "type", string(reply.typ))
		return scratch
	}

	switch kind := reply.arr[0].str(); kind {
	case "message":
		if len(reply.arr) < 3 {
			return scratch
		}
		return p.dispatch(reply.arr[1].str(), reply.arr[2].text, scratch)

	case "subscribe":
		p.confirm(reply.arr[1].str())

	case "unsubscribe", "pong":
		// Nothing to do: unsubscribing is fire and forget, and a pong only had
		// to arrive to prove the connection is alive.

	default:
		// Pattern subscriptions are not used, so pmessage and friends are noise.
		p.opts.Logger.Debug("redispubsub: ignoring reply", "kind", kind)
	}
	return scratch
}

// dispatch delivers a payload to the local subscribers of a broadcasting.
func (p *PubSub) dispatch(name string, payload []byte, scratch []*subscriber) []*subscriber {
	scratch = scratch[:0]

	p.mu.Lock()
	if b := p.subs[name]; b != nil {
		for s := range b.subs {
			scratch = append(scratch, s)
		}
	}
	p.mu.Unlock()

	// Handlers run outside the lock: one of them queueing a frame must not be
	// able to stall a Subscribe on another connection.
	for _, s := range scratch {
		s.handle(payload)
	}
	return scratch
}

// confirm marks a broadcasting live and releases everyone waiting for it.
func (p *PubSub) confirm(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	b := p.subs[name]
	if b == nil || b.active {
		// A duplicate confirmation happens when a subscribe races a reconnect's
		// replay. Harmless.
		return
	}
	b.active = true
	close(b.ready)
}

// removeSubscriber drops one handler, and the Redis subscription with it when it
// was the last one.
func (p *PubSub) removeSubscriber(name string, s *subscriber) {
	p.mu.Lock()
	b := p.subs[name]
	if b == nil {
		p.mu.Unlock()
		return
	}
	delete(b.subs, s)
	if len(b.subs) > 0 {
		p.mu.Unlock()
		return
	}
	delete(p.subs, name)
	conn := p.conn
	p.mu.Unlock()

	// Sent whether or not the subscription was confirmed: commands on one
	// connection are ordered, so an UNSUBSCRIBE behind an unconfirmed SUBSCRIBE
	// still ends up unsubscribed.
	if conn != nil {
		p.send(conn, "UNSUBSCRIBE", name)
	}
}

// keepalive pings the subscribe connection so that a connection which has died
// without a TCP reset is noticed within a read timeout rather than never.
func (p *PubSub) keepalive() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.opts.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.mu.Lock()
			conn := p.conn
			p.mu.Unlock()

			if conn != nil {
				p.send(conn, "PING")
			}
		}
	}
}

// send writes one command to the subscribe connection.
//
// Failures are logged and dropped on purpose. The reader owns deciding that a
// connection is dead, and whatever was being sent is either already in the
// subscription map — and so replayed on the next connection — or has just been
// removed from it.
func (p *PubSub) send(conn net.Conn, args ...string) {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	p.writeBuf = appendCommand(p.writeBuf[:0], args...)

	if err := conn.SetWriteDeadline(time.Now().Add(p.opts.WriteTimeout)); err != nil {
		p.opts.Logger.Debug("redispubsub: setting a write deadline", "error", err)
		return
	}
	if _, err := conn.Write(p.writeBuf); err != nil {
		p.opts.Logger.Debug("redispubsub: sending a command", "command", args[0], "error", err)
	}
}

// dropConn closes a subscribe connection and marks every subscription unconfirmed
// so that later subscribers wait for the next connection's confirmation.
func (p *PubSub) dropConn(conn net.Conn) {
	conn.Close()

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == conn {
		p.conn = nil
	}
	for _, b := range p.subs {
		if b.active {
			b.active = false
			b.ready = make(chan struct{})
		}
		// An unconfirmed broadcasting keeps its channel: callers are already
		// waiting on it and the replay will confirm it.
	}
}

// sleep waits, reporting false if the adapter closed first.
func (p *PubSub) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-p.ctx.Done():
		return false
	}
}

// publishConn returns the publish connection, opening it if needed, and reports
// whether it was just opened.
func (p *PubSub) publishConn(ctx context.Context) (net.Conn, *bufio.Reader, bool, error) {
	if p.pubConn != nil {
		return p.pubConn, p.pubRead, false, nil
	}

	conn, reader, err := p.dial(ctx)
	if err != nil {
		return nil, nil, false, err
	}
	p.pubConn, p.pubRead = conn, reader
	return conn, reader, true, nil
}

// dropPublishConn closes the publish connection so the next Broadcast opens a
// fresh one. The caller holds pubMu.
func (p *PubSub) dropPublishConn() {
	if p.pubConn != nil {
		p.pubConn.Close()
		p.pubConn, p.pubRead = nil, nil
	}
}

// publish sends one PUBLISH and reads its reply, which is the number of
// subscribers that received it — informational only, since a broadcasting with no
// subscribers anywhere is the normal state of a quiet room.
func (p *PubSub) publish(ctx context.Context, conn net.Conn, reader *bufio.Reader, name string, payload []byte) error {
	p.pubBuf = appendPublish(p.pubBuf[:0], name, payload)

	if err := conn.SetWriteDeadline(p.deadline(ctx)); err != nil {
		return err
	}
	if _, err := conn.Write(p.pubBuf); err != nil {
		return fmt.Errorf("redispubsub: publishing to %q: %w", name, err)
	}

	if err := conn.SetReadDeadline(p.deadline(ctx)); err != nil {
		return err
	}
	reply, err := readValue(reader, p.opts.MaxReplySize)
	if err != nil {
		return fmt.Errorf("redispubsub: reading the reply to PUBLISH %q: %w", name, err)
	}

	switch reply.typ {
	case typeInteger:
		return nil
	case typeError:
		return &Error{Message: reply.str()}
	default:
		return fmt.Errorf("%w: PUBLISH answered with type %q", errProtocol, string(reply.typ))
	}
}

// deadline turns a context into a socket deadline, falling back to the configured
// timeout when the context has none.
func (p *PubSub) deadline(ctx context.Context) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return d
	}
	return time.Now().Add(p.opts.WriteTimeout)
}

// dial opens a connection and authenticates it.
func (p *PubSub) dial(ctx context.Context) (net.Conn, *bufio.Reader, error) {
	ctx, cancel := context.WithTimeout(ctx, p.opts.DialTimeout)
	defer cancel()

	conn, err := p.opts.Dialer(ctx, p.opts.Address)
	if err != nil {
		return nil, nil, fmt.Errorf("redispubsub: dialing %s: %w", p.opts.Address, err)
	}

	if cfg := p.opts.tlsConfig(); cfg != nil {
		tlsConn := tls.Client(conn, cfg)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("redispubsub: TLS handshake with %s: %w", p.opts.Address, err)
		}
		conn = tlsConn
	}

	reader := bufio.NewReader(conn)
	if err := p.authenticate(ctx, conn, reader); err != nil {
		conn.Close()
		return nil, nil, err
	}

	p.opts.Logger.Debug("redispubsub: connected", "address", p.opts.Address)
	return conn, reader, nil
}

// authenticate sends AUTH when a password is configured. It is a plain
// request/response exchange, before the connection enters subscribe mode.
func (p *PubSub) authenticate(ctx context.Context, conn net.Conn, reader *bufio.Reader) error {
	if p.opts.Password == "" {
		return nil
	}

	// The two-argument form is Redis 6 ACL; the one-argument form is the legacy
	// shared password.
	var buf []byte
	if p.opts.Username != "" {
		buf = appendCommand(nil, "AUTH", p.opts.Username, p.opts.Password)
	} else {
		buf = appendCommand(nil, "AUTH", p.opts.Password)
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(p.opts.DialTimeout)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("redispubsub: sending AUTH: %w", err)
	}

	reply, err := readValue(reader, p.opts.MaxReplySize)
	if err != nil {
		return fmt.Errorf("redispubsub: reading the reply to AUTH: %w", err)
	}
	if reply.typ == typeError {
		return &Error{Message: reply.str()}
	}
	return nil
}
