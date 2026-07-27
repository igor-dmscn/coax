package coax

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// chatIdentifier is a realistic subscription identifier: a JSON object encoded
// as a string, exactly as the JS client's JSON.stringify(params) produces it.
const chatIdentifier = `{"channel":"ChatChannel","room":"1"}`

// TestServerMessageGolden pins the exact bytes of every server message shape
// against docs/action-cable-protocol.md §3.
//
// Key order within an object is ours (struct field order), not Rails'; JSON
// objects are unordered and the JS client destructures by name. What must match
// Rails byte-for-byte is the *identifier value*, which is covered by
// TestIdentifierRoundTripsVerbatim.
func TestServerMessageGolden(t *testing.T) {
	tests := []struct {
		name string
		msg  serverMessage
		want string
	}{
		{
			name: "welcome",
			msg:  newWelcome(),
			want: `{"type":"welcome"}`,
		},
		{
			name: "ping",
			msg:  newPing(time.Unix(1753560000, 0)),
			want: `{"type":"ping","message":1753560000}`,
		},
		{
			name: "ping truncates sub-second precision",
			msg:  newPing(time.Unix(1753560000, 999_999_999)),
			want: `{"type":"ping","message":1753560000}`,
		},
		{
			name: "disconnect unauthorized without reconnect",
			msg:  newDisconnect(reasonUnauthorized, false),
			want: `{"type":"disconnect","reason":"unauthorized","reconnect":false}`,
		},
		{
			name: "disconnect server_restart with reconnect",
			msg:  newDisconnect(reasonServerRestart, true),
			want: `{"type":"disconnect","reason":"server_restart","reconnect":true}`,
		},
		{
			name: "disconnect remote",
			msg:  newDisconnect(reasonRemote, true),
			want: `{"type":"disconnect","reason":"remote","reconnect":true}`,
		},
		{
			name: "confirm subscription",
			msg:  newConfirmSubscription(chatIdentifier),
			want: `{"type":"confirm_subscription","identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}"}`,
		},
		{
			name: "reject subscription",
			msg:  newRejectSubscription(chatIdentifier),
			want: `{"type":"reject_subscription","identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}"}`,
		},
		{
			name: "application data carries no type",
			msg:  newData(chatIdentifier, json.RawMessage(`{"body":"hello"}`)),
			want: `{"identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}","message":{"body":"hello"}}`,
		},
		{
			name: "application data passes through any JSON",
			msg:  newData(chatIdentifier, json.RawMessage(`[1,null,true]`)),
			want: `{"identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}","message":[1,null,true]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.msg.encode()
			if err != nil {
				t.Fatalf("encode() error = %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("encode() =\n  %s\nwant\n  %s", got, tt.want)
			}
		})
	}
}

// TestEncodeEscapesHTML pins the HTML-escaping behaviour asserted in encode's
// doc comment. ActiveSupport escapes these characters by default too
// (escape_html_entities_in_json), so payloads stay byte-comparable with Rails.
func TestEncodeEscapesHTML(t *testing.T) {
	const text = "a < b & c > d"
	msg := newData(chatIdentifier, json.RawMessage(`{"body":"a < b & c > d"}`))

	got, err := msg.encode()
	if err != nil {
		t.Fatalf("encode() error = %v", err)
	}

	// No HTML-significant character survives unescaped...
	if i := strings.IndexAny(string(got), "<>&"); i >= 0 {
		t.Errorf("encode() = %s\nwant < > & escaped, found %q at byte %d", got, got[i], i)
	}

	// ...and the escaping is lossless.
	var decoded serverMessage
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	var payload struct{ Body string }
	if err := json.Unmarshal(decoded.Message, &payload); err != nil {
		t.Fatalf("Unmarshal(message) error = %v", err)
	}
	if payload.Body != text {
		t.Errorf("body = %q, want %q", payload.Body, text)
	}
}

// TestIdentifierRoundTripsVerbatim guards the invariant that breaks everything
// if violated: the identifier is an opaque string key compared byte-for-byte,
// so two objects differing only in key order are two distinct subscriptions and
// the exact bytes a client sent must come back out.
func TestIdentifierRoundTripsVerbatim(t *testing.T) {
	identifiers := []string{
		`{"channel":"ChatChannel","room":"1"}`,
		`{"room":"1","channel":"ChatChannel"}`, // same object, different key order
		`{"channel":"ChatChannel"}`,
		`{"channel":"C","nested":{"a":[1,2]},"unicode":"héllo → 世界"}`,
		`{"channel":"C","spaces": "kept as sent" }`,
	}

	for _, identifier := range identifiers {
		t.Run(identifier, func(t *testing.T) {
			encoded, err := newConfirmSubscription(identifier).encode()
			if err != nil {
				t.Fatalf("encode() error = %v", err)
			}

			var decoded serverMessage
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if decoded.Identifier != identifier {
				t.Errorf("identifier round-trip =\n  %s\nwant\n  %s", decoded.Identifier, identifier)
			}
		})
	}
}

func TestDecodeCommand(t *testing.T) {
	tests := []struct {
		name           string
		raw            string
		wantCommand    string
		wantIdentifier string
		wantData       string
	}{
		{
			name:           "subscribe",
			raw:            `{"command":"subscribe","identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}"}`,
			wantCommand:    commandSubscribe,
			wantIdentifier: chatIdentifier,
		},
		{
			name:           "unsubscribe",
			raw:            `{"command":"unsubscribe","identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}"}`,
			wantCommand:    commandUnsubscribe,
			wantIdentifier: chatIdentifier,
		},
		{
			name:           "message carries double-encoded data",
			raw:            `{"command":"message","identifier":"{\"channel\":\"ChatChannel\",\"room\":\"1\"}","data":"{\"action\":\"speak\",\"message\":\"hi\"}"}`,
			wantCommand:    commandMessage,
			wantIdentifier: chatIdentifier,
			wantData:       `{"action":"speak","message":"hi"}`,
		},
		{
			name:           "unknown fields are ignored",
			raw:            `{"command":"subscribe","identifier":"{\"channel\":\"C\"}","future_field":42}`,
			wantCommand:    commandSubscribe,
			wantIdentifier: `{"channel":"C"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeCommand([]byte(tt.raw))
			if err != nil {
				t.Fatalf("decodeCommand() error = %v", err)
			}
			if got.Command != tt.wantCommand {
				t.Errorf("Command = %q, want %q", got.Command, tt.wantCommand)
			}
			if got.Identifier != tt.wantIdentifier {
				t.Errorf("Identifier = %q, want %q", got.Identifier, tt.wantIdentifier)
			}
			if got.Data != tt.wantData {
				t.Errorf("Data = %q, want %q", got.Data, tt.wantData)
			}
		})
	}
}

func TestDecodeCommandErrors(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want error
	}{
		{
			name: "not JSON",
			raw:  `not json at all`,
			want: errMalformedCommand,
		},
		{
			name: "JSON but not an object",
			raw:  `[1,2,3]`,
			want: errMalformedCommand,
		},
		{
			name: "empty frame",
			raw:  ``,
			want: errMalformedCommand,
		},
		{
			name: "unknown command",
			raw:  `{"command":"destroy","identifier":"{\"channel\":\"C\"}"}`,
			want: errUnknownCommand,
		},
		{
			name: "absent command",
			raw:  `{"identifier":"{\"channel\":\"C\"}"}`,
			want: errUnknownCommand,
		},
		{
			name: "subscribe without identifier",
			raw:  `{"command":"subscribe"}`,
			want: errMissingIdentifier,
		},
		{
			name: "unsubscribe with empty identifier",
			raw:  `{"command":"unsubscribe","identifier":""}`,
			want: errMissingIdentifier,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeCommand([]byte(tt.raw))
			if !errors.Is(err, tt.want) {
				t.Errorf("decodeCommand() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestDecodeIdentifier(t *testing.T) {
	t.Run("extracts channel", func(t *testing.T) {
		got, err := decodeIdentifier(chatIdentifier)
		if err != nil {
			t.Fatalf("decodeIdentifier() error = %v", err)
		}
		if got.Channel != "ChatChannel" {
			t.Errorf("Channel = %q, want %q", got.Channel, "ChatChannel")
		}
	})

	errorCases := []struct {
		name string
		raw  string
		want error
	}{
		{"not JSON", `ChatChannel`, errMalformedCommand},
		{"no channel key", `{"room":"1"}`, errMissingChannel},
		{"empty channel", `{"channel":""}`, errMissingChannel},
	}
	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeIdentifier(tt.raw); !errors.Is(err, tt.want) {
				t.Errorf("decodeIdentifier() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestDecodeAction(t *testing.T) {
	tests := []struct {
		name        string
		data        string
		wantAction  string
		wantPayload string
	}{
		{
			name:        "named action",
			data:        `{"action":"speak","message":"hi"}`,
			wantAction:  "speak",
			wantPayload: `{"action":"speak","message":"hi"}`,
		},
		{
			name:        "absent action defaults to receive",
			data:        `{"message":"hi"}`,
			wantAction:  defaultAction,
			wantPayload: `{"message":"hi"}`,
		},
		{
			name:        "empty action defaults to receive",
			data:        `{"action":"","message":"hi"}`,
			wantAction:  defaultAction,
			wantPayload: `{"action":"","message":"hi"}`,
		},
		{
			name:        "no-argument action",
			data:        `{"action":"away"}`,
			wantAction:  "away",
			wantPayload: `{"action":"away"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action, payload, err := decodeAction(tt.data)
			if err != nil {
				t.Fatalf("decodeAction() error = %v", err)
			}
			if action != tt.wantAction {
				t.Errorf("action = %q, want %q", action, tt.wantAction)
			}
			// The payload keeps its action key: Rails hands the whole decoded
			// hash to the channel method.
			if string(payload) != tt.wantPayload {
				t.Errorf("payload = %s, want %s", payload, tt.wantPayload)
			}
		})
	}

	errorCases := []struct {
		name string
		data string
	}{
		{"absent data", ``},
		{"malformed data", `{"action":`},
	}
	for _, tt := range errorCases {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := decodeAction(tt.data); !errors.Is(err, errMalformedCommand) {
				t.Errorf("decodeAction() error = %v, want %v", err, errMalformedCommand)
			}
		})
	}
}

// FuzzDecodeCommand asserts the inbound parser never panics and that a
// successful decode always satisfies the invariants the dispatcher relies on.
func FuzzDecodeCommand(f *testing.F) {
	f.Add(`{"command":"subscribe","identifier":"{\"channel\":\"C\"}"}`)
	f.Add(`{"command":"message","identifier":"{\"channel\":\"C\"}","data":"{\"action\":\"a\"}"}`)
	f.Add(`{"command":"unsubscribe","identifier":""}`)
	f.Add(`{}`)
	f.Add(``)
	f.Add(`{"command":1}`)

	f.Fuzz(func(t *testing.T, raw string) {
		cmd, err := decodeCommand([]byte(raw))
		if err != nil {
			return
		}
		switch cmd.Command {
		case commandSubscribe, commandUnsubscribe, commandMessage:
		default:
			t.Fatalf("decoded unknown command %q from %q", cmd.Command, raw)
		}
		if cmd.Identifier == "" {
			t.Fatalf("decoded empty identifier from %q", raw)
		}
	})
}
