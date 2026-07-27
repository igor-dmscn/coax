package cable

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"go-cable/ws"
)

// recorder collects channel callbacks so a test can assert on them, and lets a
// test steer what the channel does.
type recorder struct {
	subscribed   chan *Subscription
	unsubscribed chan string
	performed    chan performed

	// reject, when set, is returned by Subscribed. Set before connecting.
	reject error
	// onSubscribed, when set, runs instead of returning reject.
	onSubscribed func(context.Context, *Subscription) error
	// onPerform, when set, runs after a call is recorded.
	onPerform func(*Subscription, string, json.RawMessage) error
}

type performed struct {
	action string
	data   json.RawMessage
}

// send records an event without ever blocking. A test double that can block the
// code it observes is worse than one that drops: a test that stops draining would
// hang a connection's reader goroutine, and the failure would look like a leak in
// the library rather than in the test.
func send[T any](events chan T, event T) {
	select {
	case events <- event:
	default:
	}
}

func newRecorder() *recorder {
	return &recorder{
		subscribed:   make(chan *Subscription, 8),
		unsubscribed: make(chan string, 8),
		performed:    make(chan performed, 8),
	}
}

func (r *recorder) factory() ChannelFactory {
	return func(s *Subscription) Channel { return &recorded{r: r, sub: s} }
}

type recorded struct {
	r   *recorder
	sub *Subscription
}

func (c *recorded) Subscribed(ctx context.Context) error {
	send(c.r.subscribed, c.sub)
	if c.r.onSubscribed != nil {
		return c.r.onSubscribed(ctx, c.sub)
	}
	return c.r.reject
}

func (c *recorded) Unsubscribed(context.Context) {
	send(c.r.unsubscribed, c.sub.Identifier())
}

func (c *recorded) Perform(_ context.Context, action string, data json.RawMessage) error {
	send(c.r.performed, performed{action: action, data: data})
	if c.r.onPerform != nil {
		return c.r.onPerform(c.sub, action, data)
	}
	return nil
}

// newRegisteredServer starts a server with one registered channel.
func newRegisteredServer(t *testing.T, name string, r *recorder, o *Options) (*Server, string) {
	t.Helper()

	srv, url := newTestServer(t, o)
	srv.Register(name, r.factory())
	return srv, url
}

// connect dials a client and consumes its welcome.
func connect(t *testing.T, url string) *ws.Conn {
	t.Helper()

	conn := dial(t, url, nil)
	if got := readMessage(t, conn); got.Type != typeWelcome {
		t.Fatalf("first message type = %q, want %q", got.Type, typeWelcome)
	}
	return conn
}

// newChannelServer starts a server with one registered channel and a connected
// client that has already received its welcome.
func newChannelServer(t *testing.T, name string, r *recorder, o *Options) (*Server, *ws.Conn) {
	t.Helper()

	srv, url := newRegisteredServer(t, name, r, o)
	return srv, connect(t, url)
}

// writeCommand sends one client command.
func writeCommand(t *testing.T, conn *ws.Conn, cmd clientCommand) {
	t.Helper()

	frame, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Marshal(%+v) error = %v", cmd, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, ws.MessageText, frame); err != nil {
		t.Fatalf("Write(%s) error = %v", frame, err)
	}
}

// nextNonPing reads until a message that is not a heartbeat, since the ticker
// runs throughout these tests.
func nextNonPing(t *testing.T, conn *ws.Conn) serverMessage {
	t.Helper()

	for range 100 {
		if got := readMessage(t, conn); got.Type != typePing {
			return got
		}
	}
	t.Fatal("only heartbeats arrived")
	return serverMessage{}
}

// expectOnlyHeartbeats asserts the server said nothing back, which is how Rails
// answers a command it cannot make sense of.
func expectOnlyHeartbeats(t *testing.T, conn *ws.Conn) {
	t.Helper()

	for range 3 {
		if got := readMessage(t, conn); got.Type != typePing {
			t.Fatalf("server replied with %q, want silence", got.Type)
		}
	}
}

func waitSubscribed(t *testing.T, r *recorder) *Subscription {
	t.Helper()

	select {
	case sub := <-r.subscribed:
		return sub
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribed was never called")
		return nil
	}
}

func TestSubscribeIsConfirmed(t *testing.T) {
	r := newRecorder()
	_, conn := newChannelServer(t, "ChatChannel", r, nil)

	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: chatIdentifier})

	sub := waitSubscribed(t, r)
	if got := sub.ChannelName(); got != "ChatChannel" {
		t.Errorf("ChannelName() = %q, want %q", got, "ChatChannel")
	}

	got := nextNonPing(t, conn)
	if got.Type != typeConfirmSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeConfirmSubscription)
	}
	if got.Identifier != chatIdentifier {
		t.Errorf("identifier = %q, want %q", got.Identifier, chatIdentifier)
	}
}

// TestIdentifierIsEchoedVerbatim covers the mistake that breaks everything
// quietly: the client matches replies against the exact string it sent, so the
// server must echo those bytes and never re-encode the parsed params.
func TestIdentifierIsEchoedVerbatim(t *testing.T) {
	// Same object, awkward spelling: reversed key order and extra whitespace.
	const identifier = `{ "room" : "1", "channel":"ChatChannel" }`

	r := newRecorder()
	_, conn := newChannelServer(t, "ChatChannel", r, nil)

	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: identifier})

	sub := waitSubscribed(t, r)
	if got := sub.Identifier(); got != identifier {
		t.Errorf("Identifier() = %q, want %q", got, identifier)
	}

	if got := nextNonPing(t, conn); got.Identifier != identifier {
		t.Errorf("confirmation identifier = %q, want %q", got.Identifier, identifier)
	}
}

func TestSubscriptionParamsAndConnection(t *testing.T) {
	r := newRecorder()
	_, conn := newChannelServer(t, "ChatChannel", r, &Options{
		Authenticate: func(*http.Request) (Identifiers, error) {
			return Identifiers{"current_user": "42"}, nil
		},
	})

	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: chatIdentifier})
	sub := waitSubscribed(t, r)

	var params struct {
		Channel string `json:"channel"`
		Room    string `json:"room"`
	}
	if err := json.Unmarshal(sub.Params(), &params); err != nil {
		t.Fatalf("Unmarshal(Params()) error = %v", err)
	}
	if params.Room != "1" || params.Channel != "ChatChannel" {
		t.Errorf("params = %+v, want room 1 on ChatChannel", params)
	}

	if got := sub.Connection().Identifiers()["current_user"]; got != "42" {
		t.Errorf("current_user = %q, want %q", got, "42")
	}
}

// TestSubscribeIsRejected checks the whole rejection contract: the frame the
// client needs to fire rejected(), and that no Unsubscribed follows for a
// subscription that never existed.
func TestSubscribeIsRejected(t *testing.T) {
	r := newRecorder()
	r.reject = errors.New("not your room")
	_, conn := newChannelServer(t, "ChatChannel", r, nil)

	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: chatIdentifier})
	waitSubscribed(t, r)

	got := nextNonPing(t, conn)
	if got.Type != typeRejectSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeRejectSubscription)
	}
	if got.Identifier != chatIdentifier {
		t.Errorf("identifier = %q, want %q", got.Identifier, chatIdentifier)
	}

	// A rejected subscription is not registered, so a message for it finds nothing.
	writeCommand(t, conn, clientCommand{
		Command:    commandMessage,
		Identifier: chatIdentifier,
		Data:       `{"action":"speak"}`,
	})
	expectOnlyHeartbeats(t, conn)

	select {
	case id := <-r.unsubscribed:
		t.Errorf("Unsubscribed called for rejected subscription %q", id)
	default:
	}
}

func TestUnknownChannelIsSilent(t *testing.T) {
	r := newRecorder()
	_, conn := newChannelServer(t, "ChatChannel", r, nil)

	writeCommand(t, conn, clientCommand{
		Command:    commandSubscribe,
		Identifier: `{"channel":"NoSuchChannel"}`,
	})

	expectOnlyHeartbeats(t, conn)
}

func TestDuplicateSubscribeIsIgnored(t *testing.T) {
	r := newRecorder()
	_, conn := newChannelServer(t, "ChatChannel", r, nil)

	cmd := clientCommand{Command: commandSubscribe, Identifier: chatIdentifier}
	writeCommand(t, conn, cmd)
	waitSubscribed(t, r)
	if got := nextNonPing(t, conn); got.Type != typeConfirmSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeConfirmSubscription)
	}

	// The second subscribe is dropped: no second channel is built, and Rails
	// sends no second confirmation either.
	writeCommand(t, conn, cmd)
	expectOnlyHeartbeats(t, conn)

	select {
	case <-r.subscribed:
		t.Error("Subscribed called twice for one identifier")
	default:
	}
}

func TestPerformDispatchesAction(t *testing.T) {
	tests := []struct {
		name       string
		data       string
		wantAction string
	}{
		{name: "named action", data: `{"action":"speak","body":"hi"}`, wantAction: "speak"},
		{name: "no action defaults to receive", data: `{"body":"hi"}`, wantAction: defaultAction},
		{name: "empty action defaults to receive", data: `{"action":"","body":"hi"}`, wantAction: defaultAction},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRecorder()
			_, conn := newChannelServer(t, "ChatChannel", r, nil)

			writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: chatIdentifier})
			waitSubscribed(t, r)

			writeCommand(t, conn, clientCommand{
				Command:    commandMessage,
				Identifier: chatIdentifier,
				Data:       tt.data,
			})

			select {
			case got := <-r.performed:
				if got.action != tt.wantAction {
					t.Errorf("action = %q, want %q", got.action, tt.wantAction)
				}
				// The whole payload is handed over, action key included.
				if string(got.data) != tt.data {
					t.Errorf("data = %s, want %s", got.data, tt.data)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Perform was never called")
			}
		})
	}
}

// TestTransmitReachesTheClient closes the loop: a client action produces a
// message addressed back to its subscription.
func TestTransmitReachesTheClient(t *testing.T) {
	r := newRecorder()
	r.onPerform = func(s *Subscription, action string, data json.RawMessage) error {
		return s.Transmit(map[string]any{"echoed": action})
	}
	_, conn := newChannelServer(t, "ChatChannel", r, nil)

	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: chatIdentifier})
	waitSubscribed(t, r)
	if got := nextNonPing(t, conn); got.Type != typeConfirmSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeConfirmSubscription)
	}

	writeCommand(t, conn, clientCommand{
		Command:    commandMessage,
		Identifier: chatIdentifier,
		Data:       `{"action":"speak"}`,
	})

	got := nextNonPing(t, conn)
	if got.Type != "" {
		t.Errorf("type = %q, want empty so the client routes it to received()", got.Type)
	}
	if got.Identifier != chatIdentifier {
		t.Errorf("identifier = %q, want %q", got.Identifier, chatIdentifier)
	}
	if want := `{"echoed":"speak"}`; string(got.Message) != want {
		t.Errorf("message = %s, want %s", got.Message, want)
	}
}

func TestPerformOnUnknownSubscriptionIsSilent(t *testing.T) {
	r := newRecorder()
	_, conn := newChannelServer(t, "ChatChannel", r, nil)

	writeCommand(t, conn, clientCommand{
		Command:    commandMessage,
		Identifier: chatIdentifier,
		Data:       `{"action":"speak"}`,
	})

	expectOnlyHeartbeats(t, conn)
}

func TestUnsubscribe(t *testing.T) {
	r := newRecorder()
	_, conn := newChannelServer(t, "ChatChannel", r, nil)

	writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: chatIdentifier})
	waitSubscribed(t, r)
	if got := nextNonPing(t, conn); got.Type != typeConfirmSubscription {
		t.Fatalf("message type = %q, want %q", got.Type, typeConfirmSubscription)
	}

	writeCommand(t, conn, clientCommand{Command: commandUnsubscribe, Identifier: chatIdentifier})

	select {
	case id := <-r.unsubscribed:
		if id != chatIdentifier {
			t.Errorf("unsubscribed identifier = %q, want %q", id, chatIdentifier)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Unsubscribed was never called")
	}

	// Unsubscribing is not acknowledged, and the subscription is gone.
	expectOnlyHeartbeats(t, conn)
}

// TestClosingUnsubscribesEverything is what keeps a backend from leaking
// subscriptions when a client vanishes without unsubscribing.
func TestClosingUnsubscribesEverything(t *testing.T) {
	r := newRecorder()
	_, conn := newChannelServer(t, "ChatChannel", r, nil)

	identifiers := []string{
		`{"channel":"ChatChannel","room":"1"}`,
		`{"channel":"ChatChannel","room":"2"}`,
	}
	for _, identifier := range identifiers {
		writeCommand(t, conn, clientCommand{Command: commandSubscribe, Identifier: identifier})
		waitSubscribed(t, r)
	}

	conn.CloseNow()

	seen := map[string]bool{}
	for range identifiers {
		select {
		case id := <-r.unsubscribed:
			seen[id] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d subscriptions were unsubscribed", len(seen), len(identifiers))
		}
	}
	for _, identifier := range identifiers {
		if !seen[identifier] {
			t.Errorf("%s was not unsubscribed", identifier)
		}
	}
}

func TestRegisterPanics(t *testing.T) {
	tests := []struct {
		name string
		call func(*Server)
	}{
		{name: "empty name", call: func(s *Server) { s.Register("", func(*Subscription) Channel { return nil }) }},
		{name: "nil factory", call: func(s *Server) { s.Register("ChatChannel", nil) }},
		{name: "duplicate name", call: func(s *Server) {
			f := func(*Subscription) Channel { return nil }
			s.Register("ChatChannel", f)
			s.Register("ChatChannel", f)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := New(quietOptions(nil))
			defer srv.Close()

			defer func() {
				if recover() == nil {
					t.Error("Register did not panic")
				}
			}()
			tt.call(srv)
		})
	}
}
