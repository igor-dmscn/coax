package coax

import (
	"context"
	"encoding/json"
	"fmt"
)

// Channel is the server-side behaviour of one subscription. A fresh Channel is
// built for every subscription, so it may hold per-subscription state without
// locking: all three methods are called on the connection's reader goroutine,
// one at a time, in the order the client's commands arrived.
//
// ← actioncable/lib/action_cable/channel/base.rb
type Channel interface {
	// Subscribed is called once, when the client subscribes. Returning an error
	// rejects the subscription: the client is sent reject_subscription, fires its
	// rejected() callback and stops retrying. Unsubscribed is not called for a
	// rejected subscription.
	//
	// This is where streams are opened. It runs before the subscription is
	// confirmed, so a client is never told it is listening before it is.
	Subscribed(ctx context.Context) error

	// Unsubscribed is called once when the subscription ends, whether the client
	// unsubscribed or the connection closed. It cannot report failure because
	// there is no longer anyone to report it to.
	Unsubscribed(ctx context.Context)

	// Perform handles a message command. action is the value of the payload's
	// "action" key, or "receive" when it has none; data is the whole payload,
	// including that key. An error is logged and nothing is sent to the client,
	// matching Rails.
	Perform(ctx context.Context, action string, data json.RawMessage) error
}

// ChannelFactory builds the Channel for a new subscription. It is called once
// per subscribe command, before Subscribed.
type ChannelFactory func(*Subscription) Channel

// Register makes a channel available under the name clients use in the "channel"
// key of a subscription identifier, for example "ChatChannel". A subscription
// naming an unregistered channel is logged and ignored, exactly as in Rails,
// where the client is left waiting rather than told.
//
// Register panics on a duplicate name or a nil factory, both of which are
// programmer errors at start-up. Register before serving traffic.
//
// ← actioncable/lib/action_cable/connection/subscriptions.rb:22 (safe_constantize)
func (s *Server) Register(name string, factory ChannelFactory) {
	if name == "" {
		panic("coax: Register with an empty channel name")
	}
	if factory == nil {
		panic("coax: Register(" + name + ") with a nil factory")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.channels[name]; dup {
		panic("coax: channel already registered: " + name)
	}
	s.channels[name] = factory
}

// channelFactory looks up a registered channel, returning nil when there is none.
func (s *Server) channelFactory(name string) ChannelFactory {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.channels[name]
}

// Subscription is one client's subscription to one channel: the pairing of a
// connection with an identifier. It is what a Channel is given to talk back
// through.
//
// ← actioncable/lib/action_cable/channel/base.rb
type Subscription struct {
	conn *Connection

	// identifier is the raw string the client sent, kept byte for byte because
	// it is both the map key and what every reply must echo. Re-marshalling the
	// parsed params would reorder keys and the client would not match the reply
	// to its subscription.
	identifier string
	channel    string
	impl       Channel

	// streams maps a broadcasting to the function that stops listening to it,
	// and stopTimers ends the periodic timers. Both are touched only from the
	// reader goroutine, like the subscriptions map itself.
	streams    map[string]func()
	stopTimers []func()
}

// Identifier returns the subscription's identifier: the raw JSON string the
// client sent, which is also the key it matches replies on.
func (s *Subscription) Identifier() string { return s.identifier }

// ChannelName returns the registered name this subscription was built from.
func (s *Subscription) ChannelName() string { return s.channel }

// Params returns the subscription identifier decoded as JSON, which is where a
// channel finds what the client asked for:
//
//	var params struct{ Room string `json:"room"` }
//	json.Unmarshal(sub.Params(), &params)
//
// It always includes the "channel" key. The returned bytes must not be modified.
func (s *Subscription) Params() json.RawMessage { return json.RawMessage(s.identifier) }

// Connection returns the connection this subscription belongs to, for its
// identifiers and originating request.
func (s *Subscription) Connection() *Connection { return s.conn }

// Transmit sends a message to this subscription's client, encoding v as the
// payload the client's received() callback gets.
//
// The error reports a failure to encode v, and nothing else: queueing is
// asynchronous, so a client that has gone away or stopped reading is dropped by
// the connection rather than reported here.
//
// ← actioncable/lib/action_cable/channel/base.rb:240
func (s *Subscription) Transmit(v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("coax: transmit to %s: %w", s.channel, err)
	}
	s.conn.transmitMessage(newData(s.identifier, payload))
	return nil
}

// addSubscription handles a subscribe command.
// ← actioncable/lib/action_cable/connection/subscriptions.rb:15 (add)
func (c *Connection) addSubscription(identifier string) {
	if _, dup := c.subscriptions[identifier]; dup {
		// Rails returns silently. The client resubscribes on reconnect and its
		// SubscriptionGuarantor retries until confirmed, so duplicates are
		// expected traffic rather than a fault.
		c.logger.Debug("coax: already subscribed", "identifier", identifier)
		return
	}

	params, err := decodeIdentifier(identifier)
	if err != nil {
		c.logger.Error("coax: could not handle subscribe command", "error", err)
		return
	}

	factory := c.server.channelFactory(params.Channel)
	if factory == nil {
		c.logger.Error("coax: subscription channel not found", "channel", params.Channel)
		return
	}

	sub := &Subscription{conn: c, identifier: identifier, channel: params.Channel}
	sub.impl = factory(sub)
	if sub.impl == nil {
		c.logger.Error("coax: channel factory returned nil", "channel", params.Channel)
		return
	}

	if err := sub.impl.Subscribed(c.ctx); err != nil {
		// Divergence: Rails adds the subscription first and calls unsubscribed
		// while removing it again on rejection. Here a rejected subscription
		// never existed, so Unsubscribed is not called for one. Whatever it
		// started is still stopped: a channel may well have opened a stream or a
		// timer before deciding to reject, and those would otherwise run forever.
		sub.stop()
		c.logger.Info("coax: subscription rejected", "channel", params.Channel, "error", err)
		c.transmitMessage(newRejectSubscription(identifier))
		return
	}

	c.subscriptions[identifier] = sub
	c.transmitMessage(newConfirmSubscription(identifier))
	c.logger.Debug("coax: subscription confirmed", "channel", params.Channel)
}

// removeSubscription handles an unsubscribe command. Nothing is sent back: the
// client considers itself unsubscribed the moment it asks.
// ← actioncable/lib/action_cable/connection/subscriptions.rb:31 (remove)
func (c *Connection) removeSubscription(identifier string) {
	sub, ok := c.subscriptions[identifier]
	if !ok {
		c.logger.Error("coax: unable to find subscription", "identifier", identifier)
		return
	}

	delete(c.subscriptions, identifier)

	// Streams and timers stop after Unsubscribed, matching Rails' callback order,
	// so a channel can still say goodbye over them.
	// ← actioncable/lib/action_cable/channel/streams.rb:74 (on_unsubscribe)
	sub.impl.Unsubscribed(c.ctx)
	sub.stop()
	c.logger.Debug("coax: unsubscribed", "channel", sub.channel)
}

// performAction handles a message command by dispatching it to its subscription.
// ← actioncable/lib/action_cable/connection/subscriptions.rb:44 (perform_action)
func (c *Connection) performAction(cmd clientCommand) {
	sub, ok := c.subscriptions[cmd.Identifier]
	if !ok {
		c.logger.Error("coax: unable to find subscription", "identifier", cmd.Identifier)
		return
	}

	action, data, err := decodeAction(cmd.Data)
	if err != nil {
		c.logger.Error("coax: could not handle message command", "error", err, "channel", sub.channel)
		return
	}

	if err := sub.impl.Perform(c.ctx, action, data); err != nil {
		c.logger.Error("coax: action failed", "channel", sub.channel, "action", action, "error", err)
	}
}

// unsubscribeAll ends every subscription, for when the connection itself is
// going away.
// ← actioncable/lib/action_cable/connection/subscriptions.rb:51 (unsubscribe_from_all)
func (c *Connection) unsubscribeAll() {
	if len(c.subscriptions) == 0 {
		return
	}

	// A fresh context: the connection's own is already cancelled by the time
	// teardown runs, and cleaning up a subscription may still need to talk to a
	// backend.
	ctx, cancel := shortContext()
	defer cancel()

	for identifier, sub := range c.subscriptions {
		delete(c.subscriptions, identifier)
		sub.impl.Unsubscribed(ctx)
		sub.stop()
	}
}
