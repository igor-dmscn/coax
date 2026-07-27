// Command example is a chat server built on the coax package, small enough to
// read in one sitting and complete enough to use:
//
//	go run ./cmd/example
//	open http://localhost:8080
//
// With a Redis backend, so that two of them reach each other's clients:
//
//	REDIS_URL=redis://localhost:6379 go run ./cmd/example -addr :8080
//	REDIS_URL=redis://localhost:6379 go run ./cmd/example -addr :8081
//
// The page it serves speaks the protocol with a plain WebSocket rather than the
// Rails client, so it runs with no npm and no network. The Rails client works
// against it unchanged; that is what cable/jsclient_test.go checks.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/igor-dmscn/coax-claude-impl/coax"
	"github.com/igor-dmscn/coax-claude-impl/coax/redispubsub"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
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

	srv := coax.New(&coax.Options{
		Logger:       logger,
		PubSub:       pubsub,
		Authenticate: authenticate,
	})

	srv.Register("ChatChannel", func(s *coax.Subscription) coax.Channel {
		return &chatChannel{server: srv, sub: s}
	})
	srv.Register("ClockChannel", func(s *coax.Subscription) coax.Channel {
		return &clockChannel{sub: s}
	})

	mux := http.NewServeMux()
	mux.Handle(coax.DefaultMountPath, srv)
	mux.HandleFunc("/", index)

	server := &http.Server{Addr: *addr, Handler: mux}

	// Ctrl-C or SIGTERM: tell every client the server is restarting so it
	// reconnects — to another process, during a rolling deploy — then stop.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		stop() // a second signal kills it rather than being swallowed

		logger.Info("shutting down")
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdown); err != nil {
			logger.Warn("cable did not shut down cleanly", "error", err)
		}
		if err := server.Shutdown(shutdown); err != nil {
			logger.Warn("the HTTP server did not shut down cleanly", "error", err)
		}
	}()

	logger.Info("listening", "addr", *addr, "cable", coax.DefaultMountPath)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

// backend uses Redis when REDIS_URL is set and memory otherwise. Swapping them is
// the whole point of the PubSub interface: nothing else in this file changes.
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

// authenticate identifies a connection from a query parameter, which is a stand-in
// for whatever a real application uses — a session cookie, a bearer token, a
// signed URL. Returning an error rejects the connection with a disconnect message
// telling the client not to come back.
func authenticate(r *http.Request) (coax.Identifiers, error) {
	name := r.URL.Query().Get("user")
	if name == "" {
		return nil, errors.New("no user")
	}
	return coax.Identifiers{"user": name}, nil
}

// chatChannel streams a room and turns a spoken message into a broadcast, so every
// client in that room hears it — including those on other processes when Redis is
// the backend.
type chatChannel struct {
	server *coax.Server
	sub    *coax.Subscription
	room   string
}

func (c *chatChannel) Subscribed(ctx context.Context) error {
	var params struct {
		Room string `json:"room"`
	}
	if err := json.Unmarshal(c.sub.Params(), &params); err != nil {
		return err
	}
	if params.Room == "" {
		return errors.New("no room")
	}
	c.room = "chat:" + params.Room

	// Blocks until the backend confirms, so the subscription is only confirmed to
	// the client once it really is listening.
	return c.sub.StreamFrom(ctx, c.room)
}

func (c *chatChannel) Unsubscribed(context.Context) {}

func (c *chatChannel) Perform(ctx context.Context, action string, data json.RawMessage) error {
	switch action {
	case "speak":
		var payload struct {
			Body string `json:"body"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return err
		}
		return c.server.Broadcast(ctx, c.room, map[string]string{
			"kind": "message",
			"from": c.me(),
			"body": payload.Body,
		})

	case "typing":
		// Broadcast and forgotten. Nothing is stored, and nobody who joins in a
		// second's time needs to know: a typing indicator is only true for about
		// as long as it takes to arrive.
		//
		// It goes to the whole room, the sender included, because a broadcast has
		// no way to exclude anyone — the client ignores its own.
		return c.server.Broadcast(ctx, c.room, map[string]string{
			"kind": "typing",
			"from": c.me(),
		})

	default:
		return fmt.Errorf("unknown action %q", action)
	}
}

func (c *chatChannel) me() string {
	return c.sub.Connection().Identifiers()["user"]
}

// clockChannel pushes without being asked, which is what periodic timers are for.
type clockChannel struct{ sub *coax.Subscription }

func (c *clockChannel) Subscribed(context.Context) error {
	return c.sub.Periodically(time.Second, func(context.Context) error {
		return c.sub.Transmit(map[string]string{"now": time.Now().Format(time.TimeOnly)})
	})
}

func (c *clockChannel) Unsubscribed(context.Context) {}

func (c *clockChannel) Perform(context.Context, string, json.RawMessage) error {
	return errors.New("the clock takes no actions")
}

func index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, page)
}

// page speaks actioncable-v1-json with a plain WebSocket, so the demo needs
// nothing installed. Note the two things the protocol insists on: the subprotocol
// in the handshake, and identifier being a JSON *string* holding JSON, echoed back
// exactly as it was sent.
const page = `<!doctype html>
<title>coax example</title>
<style>
 body { font: 14px/1.5 system-ui, sans-serif; max-width: 42rem; margin: 2rem auto; padding: 0 1rem }
 #log { border: 1px solid #ccc; padding: .5rem; height: 16rem; overflow-y: auto; white-space: pre-wrap }
 input, button { font: inherit; padding: .3rem }
 .meta { color: #888 }
</style>
<h1>coax</h1>
<p>User <input id="user" value="alice" size="8"> in room <input id="room" value="1" size="4">
<button id="connect">connect</button> <span id="clock" class="meta"></span>
<p id="typing" class="meta">&nbsp;</p>
<div id="log"></div>
<p><input id="body" size="40" placeholder="say something" autocomplete="off"> <button id="send">send</button>
<script>
const log = (line, cls = "") => {
  const el = document.getElementById("log")
  el.insertAdjacentHTML("beforeend", "<div class='" + cls + "'>" + line + "</div>")
  el.scrollTop = el.scrollHeight
}

let socket, chat, clock, me, typingSentAt = 0

// Who is typing, and when we last heard so. A room has any number of them, unlike
// a 1:1 conversation, so this is a table rather than a flag.
const typers = new Map()

// Sent at most this often while typing, and shown for this long after the last
// one heard. The gap between them is the slack that keeps an indicator from
// flickering between keystrokes.
const typingEvery = 1000
const typingFor = 3000

document.getElementById("connect").onclick = () => {
  const user = document.getElementById("user").value
  const room = document.getElementById("room").value
  me = user
  typers.clear()
  renderTyping()

  if (socket) socket.close()
  socket = new WebSocket("ws://" + location.host + "/cable?user=" + encodeURIComponent(user),
                         ["actioncable-v1-json"])

  // The identifier is a JSON string containing JSON, and the server echoes the
  // exact bytes back, so keep the one you sent and compare against it.
  chat = JSON.stringify({ channel: "ChatChannel", room: room })
  clock = JSON.stringify({ channel: "ClockChannel" })

  socket.onopen = () => log("connected as " + user, "meta")
  socket.onclose = (e) => log("closed (" + e.code + ")", "meta")

  socket.onmessage = ({ data }) => {
    const message = JSON.parse(data)

    switch (message.type) {
      case "welcome":
        // Subscribe only after the welcome: before it, the server is not ready.
        for (const identifier of [chat, clock]) {
          socket.send(JSON.stringify({ command: "subscribe", identifier }))
        }
        return
      case "ping":
        return // the heartbeat: seeing it is the point
      case "confirm_subscription":
        log("subscribed to " + JSON.parse(message.identifier).channel, "meta")
        return
      case "reject_subscription":
        log("rejected: " + message.identifier, "meta")
        return
      case "disconnect":
        log("disconnected by the server: " + message.reason +
            (message.reconnect ? " (will reconnect)" : " (staying down)"), "meta")
        return
    }

    // No type means application data addressed to one subscription.
    if (message.identifier === clock) {
      document.getElementById("clock").textContent = message.message.now
    } else if (message.identifier === chat) {
      const payload = message.message
      if (payload.kind === "typing") {
        if (payload.from !== me) {
          typers.set(payload.from, Date.now())
          renderTyping()
        }
      } else {
        // Someone spoke, so they have stopped typing.
        typers.delete(payload.from)
        renderTyping()
        log("<b>" + payload.from + ":</b> " + payload.body)
      }
    }
  }
}

// renderTyping drops anyone who has gone quiet and describes the rest. Called on
// every typing message as well as on a timer, because a background tab has its
// timers throttled to about once a minute while its WebSocket messages still
// arrive on time.
const renderTyping = () => {
  const cutoff = Date.now() - typingFor
  for (const [who, at] of typers) {
    if (at < cutoff) typers.delete(who)
  }

  const names = [...typers.keys()].sort()
  document.getElementById("typing").innerHTML =
    names.length === 0 ? "&nbsp;" :
    names.length === 1 ? names[0] + " is typing…" :
    names.length === 2 ? names[0] + " and " + names[1] + " are typing…" :
                         names.length + " people are typing…"
}

setInterval(renderTyping, 500)

// A hidden tab has its timers throttled to about once a minute, so its indicator
// goes stale — which nobody can see, by definition. This makes it right again the
// moment the tab is looked at, rather than up to a minute later.
document.addEventListener("visibilitychange", renderTyping)

const send = () => {
  const input = document.getElementById("body")
  if (!socket || !input.value) return

  socket.send(JSON.stringify({
    command: "message",
    identifier: chat,
    data: JSON.stringify({ action: "speak", body: input.value }),
  }))
  input.value = ""
  typingSentAt = 0
}

// Throttled rather than sent per keystroke: the indicator lasts three seconds, so
// announcing once a second is enough, and a room of twenty people typing is then
// twenty messages a second instead of hundreds.
const announceTyping = () => {
  if (!socket || Date.now() - typingSentAt < typingEvery) return
  typingSentAt = Date.now()

  socket.send(JSON.stringify({
    command: "message",
    identifier: chat,
    data: JSON.stringify({ action: "typing" }),
  }))
}

document.getElementById("send").onclick = send
document.getElementById("body").onkeydown = (e) => {
  if (e.key === "Enter") return send()
  announceTyping()
}
document.getElementById("connect").click()
</script>
`
