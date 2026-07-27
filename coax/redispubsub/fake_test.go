package redispubsub

import (
	"bufio"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeRedis is a RESP2 server that understands the handful of commands this
// adapter sends. It exists so that the parts most likely to be wrong — waiting
// for a confirmation, replaying subscriptions after a reconnect, retrying a
// stale publish — are tested without Redis or Docker. The tests against a real
// Redis then confirm the same behaviour on the real thing.
type fakeRedis struct {
	t        *testing.T
	listener net.Listener
	password string

	// holdSubscribe, when non-nil, delays every subscribe confirmation until it
	// is closed, so a test can observe what happens while a subscription is
	// pending.
	holdSubscribe chan struct{}

	// failPublish, when set, is the error reply PUBLISH answers with.
	failPublish string

	mu       sync.Mutex
	conns    map[*fakeConn]struct{}
	commands []string // every command received, as "SUBSCRIBE room_1"
}

// fakeConn is one client connection and what it has subscribed to.
type fakeConn struct {
	conn     net.Conn
	writer   *bufio.Writer
	writeMu  sync.Mutex
	channels map[string]struct{}
}

func newFakeRedis(t *testing.T) *fakeRedis {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}

	f := &fakeRedis{t: t, listener: listener, conns: make(map[*fakeConn]struct{})}
	t.Cleanup(f.close)

	go f.serve()
	return f
}

func (f *fakeRedis) address() string { return f.listener.Addr().String() }

// options returns adapter options pointing at this server, with timings short
// enough that a test does not wait on them.
func (f *fakeRedis) options() *Options {
	return &Options{
		Address:         f.address(),
		Logger:          testLogger(f.t),
		PingInterval:    20 * time.Millisecond,
		ReadTimeout:     2 * time.Second,
		MinRetryBackoff: 5 * time.Millisecond,
		MaxRetryBackoff: 20 * time.Millisecond,
	}
}

func (f *fakeRedis) serve() {
	for {
		conn, err := f.listener.Accept()
		if err != nil {
			return
		}

		c := &fakeConn{
			conn:     conn,
			writer:   bufio.NewWriter(conn),
			channels: make(map[string]struct{}),
		}

		f.mu.Lock()
		f.conns[c] = struct{}{}
		f.mu.Unlock()

		go f.handle(c)
	}
}

func (f *fakeRedis) handle(c *fakeConn) {
	defer func() {
		c.conn.Close()
		f.mu.Lock()
		delete(f.conns, c)
		f.mu.Unlock()
	}()

	reader := bufio.NewReader(c.conn)
	for {
		args, err := readCommand(reader)
		if err != nil {
			return
		}

		f.mu.Lock()
		f.commands = append(f.commands, strings.Join(args, " "))
		f.mu.Unlock()

		if err := f.reply(c, args); err != nil {
			return
		}
	}
}

func (f *fakeRedis) reply(c *fakeConn, args []string) error {
	switch strings.ToUpper(args[0]) {
	case "AUTH":
		if f.password != "" && args[len(args)-1] != f.password {
			return c.write(appendError(nil, "WRONGPASS invalid username-password pair"))
		}
		return c.write([]byte("+OK\r\n"))

	case "PING":
		// Subscribe mode answers with an array; this is close enough for the
		// adapter, which ignores both forms.
		return c.write([]byte("+PONG\r\n"))

	case "SUBSCRIBE":
		if f.holdSubscribe != nil {
			<-f.holdSubscribe
		}
		return c.write(appendSubscriptionReply(nil, "subscribe", args[1], f.subscribeTo(c, args[1])))

	case "UNSUBSCRIBE":
		return c.write(appendSubscriptionReply(nil, "unsubscribe", args[1], f.unsubscribeFrom(c, args[1])))

	case "PUBLISH":
		if f.failPublish != "" {
			return c.write(appendError(nil, f.failPublish))
		}
		return c.write(appendInteger(nil, int64(f.push(args[1], []byte(args[2])))))

	default:
		return c.write(appendError(nil, "ERR unknown command '"+args[0]+"'"))
	}
}

// subscribeTo and unsubscribeFrom take the server's lock because push reads
// another connection's channel set: within one server, connections are not
// independent.
func (f *fakeRedis) subscribeTo(c *fakeConn, channel string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	c.channels[channel] = struct{}{}
	return len(c.channels)
}

func (f *fakeRedis) unsubscribeFrom(c *fakeConn, channel string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(c.channels, channel)
	return len(c.channels)
}

// push delivers a message to every connection subscribed to a channel, the way
// Redis itself would, and reports how many received it.
func (f *fakeRedis) push(channel string, payload []byte) int {
	f.mu.Lock()
	targets := make([]*fakeConn, 0, len(f.conns))
	for c := range f.conns {
		if _, ok := c.channels[channel]; ok {
			targets = append(targets, c)
		}
	}
	f.mu.Unlock()

	frame := appendMessage(nil, channel, payload)
	for _, c := range targets {
		if err := c.write(frame); err != nil {
			f.t.Logf("fakeRedis: pushing to a client: %v", err)
		}
	}
	return len(targets)
}

// killConns drops every open connection, which is what a Redis restart, a
// failover, or CLIENT KILL looks like from the adapter's side.
func (f *fakeRedis) killConns() {
	f.mu.Lock()
	conns := make([]*fakeConn, 0, len(f.conns))
	for c := range f.conns {
		conns = append(conns, c)
	}
	f.mu.Unlock()

	for _, c := range conns {
		c.conn.Close()
	}
}

// stopListening refuses new connections while leaving existing ones alone, so a
// test can watch the adapter fail to reconnect.
func (f *fakeRedis) stopListening() { f.listener.Close() }

func (f *fakeRedis) close() {
	f.listener.Close()
	f.killConns()
}

// commandsSeen returns every command received so far.
func (f *fakeRedis) commandsSeen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.commands...)
}

// countCommand counts how many times a command was received.
func (f *fakeRedis) countCommand(command string) int {
	n := 0
	for _, seen := range f.commandsSeen() {
		if seen == command {
			n++
		}
	}
	return n
}

func (f *fakeRedis) connectionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.conns)
}

// waitForCommand waits until a command has been received at least n times.
func (f *fakeRedis) waitForCommand(command string, n int) {
	f.t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f.countCommand(command) >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	f.t.Fatalf("timed out waiting for %q %d time(s); saw %v", command, n, f.commandsSeen())
}

func (c *fakeConn) write(b []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if _, err := c.writer.Write(b); err != nil {
		return err
	}
	return c.writer.Flush()
}

// readCommand reads one RESP array of bulk strings, which is how clients send
// commands. It reuses the adapter's own parser: a bug there would be caught by
// the RESP tests rather than silently accommodated here.
func readCommand(reader *bufio.Reader) ([]string, error) {
	reply, err := readValue(reader, defaultMaxReplySize)
	if err != nil {
		return nil, err
	}
	if reply.typ != typeArray || len(reply.arr) == 0 {
		return nil, errors.New("fakeRedis: not a command")
	}

	args := make([]string, len(reply.arr))
	for i, arg := range reply.arr {
		args[i] = arg.str()
	}
	return args, nil
}

func appendError(buf []byte, message string) []byte {
	buf = append(buf, '-')
	buf = append(buf, message...)
	return append(buf, '\r', '\n')
}

func appendInteger(buf []byte, n int64) []byte {
	buf = append(buf, ':')
	buf = strconv.AppendInt(buf, n, 10)
	return append(buf, '\r', '\n')
}

// appendSubscriptionReply builds the three-element confirmation Redis sends for
// SUBSCRIBE and UNSUBSCRIBE.
func appendSubscriptionReply(buf []byte, kind, channel string, count int) []byte {
	buf = appendHeader(buf, typeArray, 3)
	buf = appendBulkString(buf, kind)
	buf = appendBulkString(buf, channel)
	return appendInteger(buf, int64(count))
}

// appendMessage builds the three-element push Redis sends for a published
// message.
func appendMessage(buf []byte, channel string, payload []byte) []byte {
	buf = appendHeader(buf, typeArray, 3)
	buf = appendBulkString(buf, "message")
	buf = appendBulkString(buf, channel)
	buf = appendHeader(buf, typeBulkString, len(payload))
	buf = append(buf, payload...)
	return append(buf, '\r', '\n')
}
