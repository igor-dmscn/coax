// Command dmexample is a private 1:1 messaging server: the pattern for direct
// messages, where every subscription has to be authorised against stored state
// rather than accepted on the strength of its params.
//
//	go run ./cmd/dmexample
//	open "http://localhost:8081/?me=alice&with=bob"
//	open "http://localhost:8081/?me=bob&with=alice"
//
// carol has blocked dave, so /?me=carol&with=dave demonstrates a rejection.
//
// Presence — who is in the conversation — is announced rather than looked up, and
// expires if it stops being announced. See the presence section below for why that
// is the only way to get it right over pub/sub.
//
// Two broadcastings per user, carrying different things on purpose:
//
//	dm:<a>:<b>   the conversation itself — messages and typing, for whoever has it open
//	user:<x>     one inbox per user — notifications, for unread badges when it is not
//
// Sending to both is what lets a client show a badge for a conversation it is not
// looking at without receiving every message twice. The payloads differ, so a
// client can tell them apart.
//
// The security property is one rule: a broadcasting name is derived from the
// *authenticated* identity, never from what the client asked for. A client cannot
// subscribe to somebody else's inbox because the name is not theirs to choose.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/igor-dmscn/coax-claude-impl/coax"
	"github.com/igor-dmscn/coax-claude-impl/coax/redispubsub"
)

func main() {
	addr := flag.String("addr", ":8081", "address to listen on")
	verbose := flag.Bool("v", false, "log at debug level")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	pubsub, err := backend(logger)
	if err != nil {
		logger.Error("cannot set up the pub/sub backend", "error", err)
		os.Exit(1)
	}

	st := newStore()
	srv := coax.New(&coax.Options{
		Logger:       logger,
		PubSub:       pubsub,
		Authenticate: authenticate,
	})

	srv.Register("InboxChannel", func(s *coax.Subscription) coax.Channel {
		return &inboxChannel{sub: s}
	})
	srv.Register("ThreadChannel", func(s *coax.Subscription) coax.Channel {
		return &threadChannel{server: srv, store: st, sub: s}
	})

	mux := http.NewServeMux()
	mux.Handle(coax.DefaultMountPath, srv)
	mux.HandleFunc("/", index)

	server := &http.Server{Addr: *addr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		stop()

		logger.Info("shutting down")
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdown); err != nil {
			logger.Warn("coax did not shut down cleanly", "error", err)
		}
		if err := server.Shutdown(shutdown); err != nil {
			logger.Warn("the HTTP server did not shut down cleanly", "error", err)
		}
	}()

	logger.Info("listening", "addr", *addr)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func backend(logger *slog.Logger) (coax.PubSub, error) {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		logger.Info("using the in-memory backend: broadcasts reach this process only")
		return coax.NewMemoryPubSub(), nil
	}

	opts, err := redispubsub.ParseURL(url)
	if err != nil {
		return nil, err
	}
	opts.Logger = logger
	logger.Info("using Redis", "address", opts.Address)
	return redispubsub.New(opts), nil
}

// authenticate stands in for a session cookie or a bearer token. Whatever it
// returns is the identity every broadcasting name below is derived from, which is
// why it is the only place a user's name may come from.
func authenticate(r *http.Request) (coax.Identifiers, error) {
	name := strings.TrimSpace(r.URL.Query().Get("user"))
	if name == "" {
		return nil, errors.New("no user")
	}
	return coax.Identifiers{"user": name}, nil
}

// ---------------------------------------------------------------------------
// Application state. A real app has a database; the shape of the questions it
// gets asked is the same.

type message struct {
	Conversation string `json:"conversation"`
	From         string `json:"from"`
	Body         string `json:"body"`
	At           string `json:"at"`
}

type store struct {
	mu       sync.Mutex
	messages map[string][]message
	blocked  map[string][]string // who each user refuses to hear from
}

func newStore() *store {
	return &store{
		messages: make(map[string][]message),
		// Seeded so the rejection path is visible: carol will not talk to dave.
		blocked: map[string][]string{"carol": {"dave"}},
	}
}

func (s *store) append(m message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages[m.Conversation] = append(s.messages[m.Conversation], m)
}

func (s *store) history(conversation string) []message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.messages[conversation])
}

// blocks reports whether either side refuses the other. Checked on subscribe,
// which is the whole point of doing authorisation there: the answer depends on
// stored state, not on anything the client sent.
func (s *store) blocks(a, b string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Contains(s.blocked[a], b) || slices.Contains(s.blocked[b], a)
}

// conversation names the broadcasting for a pair of users. Sorted, so both sides
// derive the same name without having to agree on who started it.
func conversation(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return "dm:" + a + ":" + b
}

func inbox(user string) string { return "user:" + user }

// ---------------------------------------------------------------------------
// Presence: who is in this conversation right now.
//
// The one thing pub/sub cannot deliver on its own, because presence is *state*
// and pub/sub carries *events*. There is no roster to read: each participant
// announces itself, and everyone builds their own table from what they hear.
//
//	join   on subscribe, so the others learn immediately
//	here   every presenceInterval, and in reply to someone else's join
//	leave  on unsubscribe — best effort only
//
// The heartbeat is what makes it correct, and `leave` is only what makes it
// quick. A leave never arrives when a process is killed, or when the backend is
// unreachable at that moment — so anyone not heard from within presenceExpiry is
// dropped regardless. Without that, a crashed server leaves its users online
// forever.
//
// This works across processes with no shared state, which is what makes it worth
// showing: two servers on one Redis need nothing but the pub/sub they already
// have. A production system at scale would keep the roster in Redis instead and
// pay for the round trips.
const (
	presenceInterval = 5 * time.Second

	// Two missed announcements plus slack. Sent to the client, which does the
	// expiring, so the number lives in one place.
	presenceExpiry = presenceInterval*2 + presenceInterval/2
)

// procID distinguishes this process, so that presence identifiers minted by two
// servers sharing one Redis cannot collide.
var procID = func() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "p"
	}
	return hex.EncodeToString(b[:])
}()

var presenceCounter atomic.Int64

// nextPresenceID identifies one *subscription*, not one user. A user with three
// tabs open is three of these, which is what lets a client drop the tab that left
// while keeping the user online.
func nextPresenceID() string {
	return procID + "-" + strconv.FormatInt(presenceCounter.Add(1), 10)
}

// ---------------------------------------------------------------------------

// inboxChannel delivers notifications for a user wherever they are connected,
// including tabs and devices with no conversation open. It takes no params: the
// only thing it could take is a user name, and trusting one from the client is
// exactly the bug this design avoids.
type inboxChannel struct{ sub *coax.Subscription }

func (c *inboxChannel) Subscribed(ctx context.Context) error {
	me := c.sub.Connection().Identifiers()["user"]
	return c.sub.StreamFrom(ctx, inbox(me))
}

func (c *inboxChannel) Unsubscribed(context.Context) {}

func (c *inboxChannel) Perform(context.Context, string, json.RawMessage) error {
	return errors.New("the inbox is read-only")
}

// threadChannel is one open conversation: messages, typing, and the history the
// client needs to render it.
type threadChannel struct {
	server *coax.Server
	store  *store
	sub    *coax.Subscription

	me, peer, name string

	// presenceID identifies this subscription among a user's tabs and devices.
	presenceID string
}

func (c *threadChannel) Subscribed(ctx context.Context) error {
	var params struct {
		With string `json:"with"`
	}
	if err := json.Unmarshal(c.sub.Params(), &params); err != nil {
		return err
	}

	c.me = c.sub.Connection().Identifiers()["user"]
	c.peer = strings.TrimSpace(params.With)

	switch {
	case c.peer == "":
		return errors.New("no peer")
	case c.peer == c.me:
		return errors.New("cannot message yourself")
	case c.store.blocks(c.me, c.peer):
		// The client learns only that it was rejected, which is the right amount
		// to tell it.
		return fmt.Errorf("%s and %s cannot exchange messages", c.me, c.peer)
	}

	c.name = conversation(c.me, c.peer)
	c.presenceID = nextPresenceID()

	if err := c.sub.StreamFrom(ctx, c.name); err != nil {
		return err
	}

	// Sent after the stream is live, so nothing published in between is missed.
	// A larger app would page this over HTTP instead; the ordering requirement is
	// the same either way. presenceEvery travels with it so the client expires
	// stale participants on the server's schedule rather than a guess.
	if err := c.sub.Transmit(map[string]any{
		"kind":          "history",
		"messages":      c.store.history(c.name),
		"me":            c.me,
		"presenceID":    c.presenceID,
		"presenceEvery": presenceInterval.Milliseconds(),
		"presenceFor":   presenceExpiry.Milliseconds(),
	}); err != nil {
		return err
	}

	if err := c.announce(ctx, "join"); err != nil {
		return err
	}

	// Announce on a schedule for as long as the subscription lasts. This is the
	// part that makes presence recover by itself: nothing has to be told about a
	// crash, it just stops being announced.
	return c.sub.Periodically(presenceInterval, func(ctx context.Context) error {
		return c.announce(ctx, "here")
	})
}

// Unsubscribed announces the departure. Best effort: this never runs when the
// process is killed, and the broadcast is dropped if the backend is unreachable,
// which is exactly why the client also expires what it stops hearing about.
//
// The context is usable here even during connection teardown, because
// unsubscribeAll hands one that is not already cancelled.
func (c *threadChannel) Unsubscribed(ctx context.Context) {
	if c.name == "" {
		return // rejected before it ever streamed
	}
	if err := c.announce(ctx, "leave"); err != nil {
		// Nothing to do about it: the expiry covers this case.
		return
	}
}

// announce tells the conversation about this subscription.
func (c *threadChannel) announce(ctx context.Context, event string) error {
	return c.server.Broadcast(ctx, c.name, map[string]any{
		"kind":  "presence",
		"event": event,
		"user":  c.me,
		"id":    c.presenceID,
	})
}

func (c *threadChannel) Perform(ctx context.Context, action string, data json.RawMessage) error {
	switch action {
	case "speak":
		return c.speak(ctx, data)
	case "typing":
		// Not persisted, and not sent to the inboxes: a typing indicator is only
		// interesting to someone looking at the conversation.
		return c.server.Broadcast(ctx, c.name, map[string]any{"kind": "typing", "from": c.me})
	case "here":
		// Someone announced a join, and this is the reply so they do not have to
		// wait a whole interval to see us.
		//
		// The client asks for this rather than the server answering by itself,
		// because a channel does not observe its own streams: StreamFrom delivers
		// to the socket, not back into the channel. Custom stream handlers would
		// change that and are a deliberate omission (go-port-plan.md section 9.4).
		return c.announce(ctx, "here")
	default:
		return fmt.Errorf("unknown action %q", action)
	}
}

func (c *threadChannel) speak(ctx context.Context, data json.RawMessage) error {
	var payload struct {
		Body string `json:"body"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	if strings.TrimSpace(payload.Body) == "" {
		return errors.New("empty message")
	}

	m := message{
		Conversation: c.name,
		From:         c.me,
		Body:         payload.Body,
		At:           time.Now().Format(time.TimeOnly),
	}

	// Persisted first. Pub/sub is fire and forget: a peer who is offline, or
	// reconnecting, never sees this broadcast, and the stored copy is the only
	// reason the conversation survives that.
	c.store.append(m)

	// The conversation gets the message.
	if err := c.server.Broadcast(ctx, c.name, map[string]any{"kind": "message", "message": m}); err != nil {
		return err
	}

	// Both inboxes get a notification — the peer's for their badge, and the
	// sender's own so their other tabs and devices know something was sent. A
	// different shape from the message above, so a client with the conversation
	// open does not render it twice.
	notice := map[string]any{
		"kind":         "notice",
		"conversation": c.name,
		"from":         c.me,
		"preview":      preview(m.Body),
		"at":           m.At,
	}
	for _, user := range []string{c.peer, c.me} {
		if err := c.server.Broadcast(ctx, inbox(user), notice); err != nil {
			return err
		}
	}
	return nil
}

func preview(body string) string {
	const max = 40
	if len(body) <= max {
		return body
	}
	return body[:max] + "…"
}

func index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, page)
}

const page = `<!doctype html>
<title>coax — direct messages</title>
<style>
 body { font: 14px/1.5 system-ui, sans-serif; max-width: 44rem; margin: 2rem auto; padding: 0 1rem }
 .row { display: flex; gap: .5rem; align-items: center; margin-bottom: .75rem }
 #thread { border: 1px solid #ccc; padding: .5rem; height: 14rem; overflow-y: auto }
 #notices { border: 1px dashed #ccc; padding: .5rem; height: 6rem; overflow-y: auto; color: #666; margin-top: .75rem }
 input, button { font: inherit; padding: .3rem }
 .meta { color: #888 }
 .mine { color: #06c }
 h2 { font-size: 1rem; margin: 1rem 0 .25rem }
</style>
<h1>coax — direct messages</h1>
<p class="meta">Open two windows: <a href="/?me=alice&amp;with=bob">alice ↔ bob</a> ·
<a href="/?me=bob&amp;with=alice">bob ↔ alice</a> ·
<a href="/?me=carol&amp;with=dave">carol ↔ dave (blocked)</a></p>
<div class="row">
  I am <input id="me" value="alice" size="8">
  talking to <input id="peer" value="bob" size="8">
  <button id="connect">connect</button>
  <span id="typing" class="meta"></span>
</div>
<h2>conversation <span id="roster" class="meta"></span></h2>
<div id="thread"></div>
<div class="row" style="margin-top:.75rem">
  <input id="body" size="40" placeholder="private message" autocomplete="off">
  <button id="send">send</button>
</div>
<h2>inbox notifications <span class="meta">(arrive whether or not the thread is open)</span></h2>
<div id="notices"></div>
<script>
const add = (where, html, cls = "") => {
  const el = document.getElementById(where)
  el.insertAdjacentHTML("beforeend", "<div class='" + cls + "'>" + html + "</div>")
  el.scrollTop = el.scrollHeight
}
const escape = (s) => s.replace(/[<>&]/g, (c) => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;" }[c]))

let socket, inbox, thread, me, myPresenceID, typingTimer
let presenceFor = 12500

// Who is in the conversation, keyed by subscription rather than by user: one
// person with two tabs is two entries, so closing one does not take them offline.
const present = new Map()

document.getElementById("connect").onclick = () => {
  me = document.getElementById("me").value
  const peer = document.getElementById("peer").value

  if (socket) socket.close()
  // Both panes, or a previous user's notifications linger and look like this
  // user received them.
  document.getElementById("thread").innerHTML = ""
  document.getElementById("notices").innerHTML = ""
  present.clear()
  renderRoster()
  socket = new WebSocket("ws://" + location.host + "/cable?user=" + encodeURIComponent(me),
                         ["actioncable-v1-json"])

  // Two subscriptions on one socket. The inbox takes no params — the server
  // derives it from who we are — while the thread names the other person.
  inbox  = JSON.stringify({ channel: "InboxChannel" })
  thread = JSON.stringify({ channel: "ThreadChannel", with: peer })

  socket.onopen  = () => add("thread", "connected as " + escape(me), "meta")
  socket.onclose = (e) => add("thread", "closed (" + e.code + ")", "meta")

  socket.onmessage = ({ data }) => {
    const frame = JSON.parse(data)

    switch (frame.type) {
      case "welcome":
        for (const identifier of [inbox, thread]) {
          socket.send(JSON.stringify({ command: "subscribe", identifier }))
        }
        return
      case "ping":
        return
      case "confirm_subscription":
        add("thread", "subscribed to " + JSON.parse(frame.identifier).channel, "meta")
        return
      case "reject_subscription":
        add("thread", "rejected: " + JSON.parse(frame.identifier).channel +
                      " — you two cannot exchange messages", "meta")
        return
      case "disconnect":
        add("thread", "disconnected: " + frame.reason, "meta")
        return
    }

    const payload = frame.message
    if (frame.identifier === thread) {
      if (payload.kind === "history") {
        myPresenceID = payload.presenceID
        presenceFor = payload.presenceFor
        payload.messages.forEach(render)
      } else if (payload.kind === "presence") {
        onPresence(payload)
      } else if (payload.kind === "message") {
        render(payload.message)
      } else if (payload.kind === "typing" && payload.from !== me) {
        showTyping(payload.from)
      }
    } else if (frame.identifier === inbox && payload.kind === "notice") {
      add("notices", escape(payload.at + "  " + payload.from + ": " + payload.preview))
    }
  }
}

const onPresence = ({ event, user, id }) => {
  if (event === "leave") {
    present.delete(id)
  } else {
    present.set(id, { user, seen: Date.now() })

    // Answer a join so the newcomer sees us now rather than in a few seconds.
    if (event === "join" && id !== myPresenceID) {
      socket.send(JSON.stringify({
        command: "message",
        identifier: thread,
        data: JSON.stringify({ action: "here" }),
      }))
    }
  }
  prune()
  renderRoster()
}

// prune drops anyone we have stopped hearing from. Called on every presence
// message as well as on a timer, because a background tab has its timers throttled
// to about once a minute by the browser — but its WebSocket messages still arrive
// on time. Our own announcement comes back to us through the broadcast, so there
// is always traffic to prune on for as long as we are subscribed.
const prune = () => {
  const cutoff = Date.now() - presenceFor
  let changed = false
  for (const [id, p] of present) {
    if (p.seen < cutoff) { present.delete(id); changed = true }
  }
  return changed
}

const renderRoster = () => {
  const users = [...new Set([...present.values()].map((p) => p.user))].sort()
  document.getElementById("roster").textContent =
    users.length ? "— here: " + users.join(", ") : "— nobody here"
}

// The fallback, for a conversation with no traffic at all. Neither a killed
// process nor a dropped socket sends a leave, so expiry is the only thing that
// removes them.
setInterval(() => { if (prune()) renderRoster() }, 1000)

// Timers are throttled in a hidden tab, so bring both displays up to date as soon
// as it is looked at.
document.addEventListener("visibilitychange", () => { prune(); renderRoster() })

const render = (m) =>
  add("thread", "<b>" + escape(m.from) + ":</b> " + escape(m.body) +
                " <span class='meta'>" + m.at + "</span>", m.from === me ? "mine" : "")

const showTyping = (who) => {
  document.getElementById("typing").textContent = who + " is typing…"
  clearTimeout(typingTimer)
  typingTimer = setTimeout(() => { document.getElementById("typing").textContent = "" }, 1500)
}

const send = () => {
  const input = document.getElementById("body")
  if (!socket || !input.value) return

  socket.send(JSON.stringify({
    command: "message",
    identifier: thread,
    data: JSON.stringify({ action: "speak", body: input.value }),
  }))
  input.value = ""
}

// The two participants come from the URL, so a demo is two links rather than
// two people typing the same names into two windows.
const query = new URL(location).searchParams
document.getElementById("me").value = query.get("me") || "alice"
document.getElementById("peer").value = query.get("with") || "bob"

document.getElementById("send").onclick = send
// Throttled rather than sent per keystroke: the indicator lasts longer than the
// gap between announcements, so once a second is enough.
let typingSentAt = 0

document.getElementById("body").onkeydown = (e) => {
  if (e.key === "Enter") return send()
  if (!socket || Date.now() - typingSentAt < 1000) return
  typingSentAt = Date.now()

  socket.send(JSON.stringify({
    command: "message",
    identifier: thread,
    data: JSON.stringify({ action: "typing" }),
  }))
}
document.getElementById("connect").click()
</script>
`
