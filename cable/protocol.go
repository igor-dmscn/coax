// Package cable implements a server for the Action Cable protocol
// (actioncable-v1-json), wire-compatible with the @rails/actioncable
// JavaScript client.
//
// The protocol is small: clients send one of three commands (subscribe,
// unsubscribe, message) and servers reply with typed control messages plus
// application data addressed to a subscription identifier. See
// docs/action-cable-protocol.md for the full wire reference.
package cable

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Protocol constants, mirroring ActionCable::INTERNAL.
// ← actioncable/lib/action_cable.rb:61
const (
	// Subprotocol is the WebSocket subprotocol a client must negotiate. It is
	// the only protocol the Rails JS client will accept in a handshake
	// response, so a server must always answer with this value.
	// ← actioncable/app/javascript/action_cable/connection.js:9
	Subprotocol = "actioncable-v1-json"

	// DefaultMountPath is where the Rails JS client looks when created without
	// an explicit URL and no action-cable-url meta tag is present.
	DefaultMountPath = "/cable"

	// subprotocolUnsupported is offered alongside Subprotocol during the
	// handshake purely as a graceful-degradation path: a client that can only
	// negotiate this value completes the handshake and then closes itself
	// without reconnecting, rather than failing the upgrade outright.
	subprotocolUnsupported = "actioncable-unsupported"
)

// Message types carried in the "type" field of a server message.
const (
	typeWelcome             = "welcome"
	typeDisconnect          = "disconnect"
	typePing                = "ping"
	typeConfirmSubscription = "confirm_subscription"
	typeRejectSubscription  = "reject_subscription"
)

// Command verbs a client may send.
// ← actioncable/lib/action_cable/connection/subscriptions.rb:58
const (
	commandSubscribe   = "subscribe"
	commandUnsubscribe = "unsubscribe"
	commandMessage     = "message"
)

// defaultAction is dispatched when a message command carries no action name.
// ← actioncable/lib/action_cable/channel/base.rb:287 (data["action"].presence || :receive)
const defaultAction = "receive"

// Reasons sent in a disconnect message.
const (
	reasonUnauthorized  = "unauthorized"
	reasonServerRestart = "server_restart"
	reasonRemote        = "remote"

	// reasonInvalidRequest is part of the protocol constants but is never sent:
	// Rails answers a failed upgrade with a plain 404 instead. Defined for
	// completeness so the constant set matches ActionCable::INTERNAL.
	reasonInvalidRequest = "invalid_request"
)

// Errors reported when a client command cannot be understood. Rails logs each
// of these server-side and sends nothing back to the client, so callers are
// expected to log and continue rather than to reply.
// ← actioncable/lib/action_cable/connection/subscriptions.rb:15-51
var (
	errMalformedCommand  = errors.New("cable: malformed command")
	errUnknownCommand    = errors.New("cable: unknown command")
	errMissingIdentifier = errors.New("cable: missing identifier")
	errMissingChannel    = errors.New("cable: identifier has no channel")
)

// clientCommand is a message received from a client.
//
// Identifier and Data are JSON strings *containing* JSON — the double encoding
// is part of the protocol, not an accident. Identifier is kept as the raw
// string a client sent and is echoed back verbatim, because the server keys
// subscriptions on it byte-for-byte: re-marshalling parsed params would emit a
// different key order and every reply would miss its subscription.
type clientCommand struct {
	Command    string `json:"command"`
	Identifier string `json:"identifier"`
	Data       string `json:"data,omitempty"`
}

// serverMessage is a message sent to a client. One struct covers all six frame
// shapes; encoding/json cannot express a sum type, and six near-identical
// structs would buy nothing.
//
// Reconnect is a *bool rather than a bool because `omitempty` treats false as
// empty: a disconnect message must be able to carry "reconnect":false, while
// every other shape must omit the field entirely.
type serverMessage struct {
	Type       string          `json:"type,omitempty"`
	Identifier string          `json:"identifier,omitempty"`
	Message    json.RawMessage `json:"message,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	Reconnect  *bool           `json:"reconnect,omitempty"`
}

// identifierParams is the decoded form of a subscription identifier. Only the
// channel name is needed by the framework; everything else in the object is
// the channel's params and is passed through as raw JSON.
type identifierParams struct {
	Channel string `json:"channel"`
}

// newWelcome builds the message that tells a client its connection is usable.
// It is the client's signal to (re)subscribe everything it holds, so it must be
// sent exactly once per socket, after authentication.
// ← actioncable/lib/action_cable/connection/base.rb:160
func newWelcome() serverMessage {
	return serverMessage{Type: typeWelcome}
}

// newPing builds a heartbeat. The payload is whole unix seconds, matching
// Rails' Time.now.to_i.
// ← actioncable/lib/action_cable/connection/base.rb:142
func newPing(now time.Time) serverMessage {
	return serverMessage{
		Type:    typePing,
		Message: strconv.AppendInt(make([]byte, 0, 20), now.Unix(), 10),
	}
}

// newDisconnect builds a disconnect notice. A client that receives
// reconnect=false stops its connection monitor and will not come back on its
// own.
// ← actioncable/lib/action_cable/connection/base.rb:121
func newDisconnect(reason string, reconnect bool) serverMessage {
	return serverMessage{Type: typeDisconnect, Reason: reason, Reconnect: &reconnect}
}

// newConfirmSubscription acknowledges a subscription. It must not be sent until
// every stream the channel opened is live on the pubsub backend, or messages
// published in the gap are lost while the client believes it is listening.
// ← actioncable/lib/action_cable/channel/base.rb:322
func newConfirmSubscription(identifier string) serverMessage {
	return serverMessage{Type: typeConfirmSubscription, Identifier: identifier}
}

// newRejectSubscription refuses a subscription. The client fires its rejected()
// callback and stops retrying.
// ← actioncable/lib/action_cable/channel/base.rb:338
func newRejectSubscription(identifier string) serverMessage {
	return serverMessage{Type: typeRejectSubscription, Identifier: identifier}
}

// newData builds an application message for one subscription. It carries no
// type, which is what makes a client route it to received().
// ← actioncable/lib/action_cable/channel/base.rb:240
func newData(identifier string, payload json.RawMessage) serverMessage {
	return serverMessage{Identifier: identifier, Message: payload}
}

// encode renders a message as a WebSocket text payload.
//
// Go's encoder escapes <, > and & as <, > and &, which matches
// ActiveSupport's default (escape_html_entities_in_json), so payloads are byte
// -comparable with Rails for these characters.
func (m serverMessage) encode() ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("cable: encode %q message: %w", m.Type, err)
	}
	return b, nil
}

// decodeCommand parses and validates a message received from a client.
func decodeCommand(b []byte) (clientCommand, error) {
	var c clientCommand
	if err := json.Unmarshal(b, &c); err != nil {
		return clientCommand{}, fmt.Errorf("%w: %w", errMalformedCommand, err)
	}

	switch c.Command {
	case commandSubscribe, commandUnsubscribe, commandMessage:
	default:
		return clientCommand{}, fmt.Errorf("%w: %q", errUnknownCommand, c.Command)
	}

	if c.Identifier == "" {
		return clientCommand{}, fmt.Errorf("%w: %s command", errMissingIdentifier, c.Command)
	}

	return c, nil
}

// decodeIdentifier extracts the channel name from a subscription identifier.
// The identifier string itself remains the key; this is only used to find the
// channel to construct.
// ← actioncable/lib/action_cable/connection/subscriptions.rb:121
func decodeIdentifier(identifier string) (identifierParams, error) {
	var p identifierParams
	if err := json.Unmarshal([]byte(identifier), &p); err != nil {
		return identifierParams{}, fmt.Errorf("%w: %w", errMalformedCommand, err)
	}
	if p.Channel == "" {
		return identifierParams{}, fmt.Errorf("%w: %q", errMissingChannel, identifier)
	}
	return p, nil
}

// decodeAction extracts the action name from the inner data payload of a
// message command, returning the payload itself for the channel to handle.
//
// An absent or empty action dispatches to "receive". The payload is returned
// whole, including its action key, matching Rails' behaviour of handing the
// entire decoded hash to the channel method.
func decodeAction(data string) (action string, payload json.RawMessage, err error) {
	if data == "" {
		return "", nil, fmt.Errorf("%w: message command has no data", errMalformedCommand)
	}

	var envelope struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal([]byte(data), &envelope); err != nil {
		return "", nil, fmt.Errorf("%w: %w", errMalformedCommand, err)
	}

	if envelope.Action == "" {
		return defaultAction, json.RawMessage(data), nil
	}
	return envelope.Action, json.RawMessage(data), nil
}
