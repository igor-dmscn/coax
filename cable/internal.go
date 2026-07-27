package cable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// internalChannelPrefix namespaces the framework's own pub/sub traffic away from
// application broadcastings. Rails' prefix, so the names are recognisable in
// redis-cli MONITOR.
// ← actioncable/lib/action_cable/connection/internal_channel.rb:11
const internalChannelPrefix = "action_cable/"

// internalChannelTimeout bounds subscribing to the internal channel. It only
// matters when the backend is unreachable: rather than holding a client's welcome
// indefinitely, the connection proceeds without the ability to be disconnected
// remotely, and says so in the log.
const internalChannelTimeout = 5 * time.Second

// internalMessage is the framework's own server-to-server message. Only one type
// exists.
//
// Reconnect is a *bool because absent means true: Rails reads it with a default
// rather than requiring it.
// ← actioncable/lib/action_cable/connection/internal_channel.rb:31
type internalMessage struct {
	Type      string `json:"type"`
	Reconnect *bool  `json:"reconnect,omitempty"`
}

// internalChannelFor names the broadcasting that reaches a connection's identity
// from anywhere. Every process derives the same name from the same identifiers,
// which is what makes disconnecting a user possible without knowing which process
// holds them.
//
// A connection with no identifiers gets no internal channel, and cannot be
// reached remotely: there is nothing to address it by.
//
// Divergence: Rails joins the sorted GlobalIDs of the identified_by values, which
// carry their model class. Identifiers here are arbitrary strings, so the keys are
// included — otherwise Identifiers{"user": "42"} and Identifiers{"room": "42"}
// would share an internal channel and disconnect each other. The consequence is
// that remote disconnect does not reach a Rails process's connections, and
// vice versa; broadcasts, which is what interoperability is actually about, do.
// ← actioncable/lib/action_cable/connection/identification.rb:26 (connection_gid)
func internalChannelFor(ids Identifiers) string {
	if len(ids) == 0 {
		return ""
	}

	var name strings.Builder
	name.WriteString(internalChannelPrefix)
	for i, key := range slices.Sorted(maps.Keys(ids)) {
		if i > 0 {
			name.WriteByte(':')
		}
		name.WriteString(key)
		name.WriteByte('=')
		name.WriteString(ids[key])
	}
	return name.String()
}

// Disconnect closes every connection belonging to these identifiers, in this
// process and in every other process sharing the pub/sub backend. It is how an
// application drops a user it has just banned, logged out, or deleted:
//
//	srv.Disconnect(ctx, cable.Identifiers{"current_user": "42"}, false)
//
// reconnect false tells the client to stay down; true lets it come back, which is
// what a rebalance or a forced re-authentication wants.
//
// The identifiers must match a connection's whole set, not a subset: they are
// combined into one address, so a connection identified by two things is not
// reachable by one of them. Passing none is an error rather than a way to
// disconnect everybody, since that is far more likely to be a bug than an
// intention.
//
// It returns once the backend has accepted the message, which says nothing about
// how many connections matched — possibly none, in a process that has never seen
// that user.
//
// ← actioncable/lib/action_cable/remote_connections.rb
func (s *Server) Disconnect(ctx context.Context, ids Identifiers, reconnect bool) error {
	channel := internalChannelFor(ids)
	if channel == "" {
		return errors.New("cable: Disconnect needs at least one identifier")
	}

	payload, err := json.Marshal(internalMessage{Type: typeDisconnect, Reconnect: &reconnect})
	if err != nil {
		return fmt.Errorf("cable: encoding a remote disconnect: %w", err)
	}
	return s.opts.PubSub.Broadcast(ctx, channel, payload)
}

// subscribeToInternalChannel makes this connection reachable from other
// processes. A failure is logged rather than fatal: a connection that works but
// cannot be disconnected remotely is better than no connection at all.
//
// It runs before the welcome, so that a disconnect published immediately after a
// client believes it is connected cannot be missed.
// ← actioncable/lib/action_cable/connection/base.rb:88 (handle_open)
func (c *Connection) subscribeToInternalChannel() {
	channel := internalChannelFor(c.identifiers)
	if channel == "" {
		return
	}

	ctx, cancel := context.WithTimeout(c.ctx, internalChannelTimeout)
	defer cancel()

	unsubscribe, err := c.server.opts.PubSub.Subscribe(ctx, channel, c.handleInternalMessage)
	if err != nil {
		c.logger.Warn("cable: connection cannot be reached remotely", "channel", channel, "error", err)
		return
	}

	c.stopInternalChannel = unsubscribe
	c.logger.Debug("cable: registered connection", "channel", channel)
}

// unsubscribeFromInternalChannel is called once the connection is finished.
func (c *Connection) unsubscribeFromInternalChannel() {
	if c.stopInternalChannel != nil {
		c.stopInternalChannel()
		c.stopInternalChannel = nil
	}
}

// handleInternalMessage acts on a message from another process. It runs on the
// pub/sub goroutine, so it only queues work.
// ← actioncable/lib/action_cable/connection/internal_channel.rb:29
func (c *Connection) handleInternalMessage(payload []byte) {
	var m internalMessage
	if err := json.Unmarshal(payload, &m); err != nil {
		c.logger.Error("cable: could not handle an internal message", "error", err)
		return
	}

	if m.Type != typeDisconnect {
		c.logger.Debug("cable: ignoring an internal message", "type", m.Type)
		return
	}

	// Absent means reconnect: only an explicit false keeps a client down.
	reconnect := m.Reconnect == nil || *m.Reconnect
	c.logger.Info("cable: removing connection", "reconnect", reconnect)
	c.close(reasonRemote, reconnect)
}
