package coax_test

// The Go snippets from README.md, verbatim, so the most-copied code in the
// repository cannot drift away from the API. Nothing here runs; it only has to
// compile. If a signature changes, this file breaks before a reader does.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/igor-dmscn/coax-claude-impl/coax"
	"github.com/igor-dmscn/coax-claude-impl/coax/redispubsub"
)

func session(*http.Request) string   { return "" }
func canRead(user, room string) bool { return true }
func tick(context.Context) error     { return nil }

func readmeServer() {
	srv := coax.New(&coax.Options{
		Authenticate: func(r *http.Request) (coax.Identifiers, error) {
			user := session(r)
			if user == "" {
				return nil, errors.New("not signed in")
			}
			return coax.Identifiers{"user": user}, nil
		},
	})
	defer srv.Close()

	srv.Register("ChatChannel", func(s *coax.Subscription) coax.Channel {
		return &ChatChannel{srv: srv, sub: s}
	})

	http.Handle(coax.DefaultMountPath, srv)
	http.ListenAndServe(":8080", nil)
}

type ChatChannel struct {
	srv  *coax.Server
	sub  *coax.Subscription
	room string
}

func (c *ChatChannel) Subscribed(ctx context.Context) error {
	var params struct {
		Room string `json:"room"`
	}
	if err := json.Unmarshal(c.sub.Params(), &params); err != nil {
		return err
	}
	if !canRead(c.sub.Connection().Identifiers()["user"], params.Room) {
		return errors.New("not a member")
	}

	c.room = "chat:" + params.Room
	return c.sub.StreamFrom(ctx, c.room)
}

func (c *ChatChannel) Unsubscribed(context.Context) {}

func (c *ChatChannel) Perform(ctx context.Context, action string, data json.RawMessage) error {
	if action != "speak" {
		return fmt.Errorf("unknown action %q", action)
	}
	var msg struct {
		Body string `json:"body"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}
	return c.srv.Broadcast(ctx, c.room, map[string]string{
		"from": c.sub.Connection().Identifiers()["user"],
		"body": msg.Body,
	})
}

func readmeRest(ctx context.Context, srv *coax.Server, sub *coax.Subscription, v any) {
	srv.Broadcast(ctx, "chat:1", map[string]string{"body": "the server has something to say"})

	sub.Transmit(v)
	sub.StreamFrom(ctx, "chat:1")
	sub.Periodically(time.Second, tick)
	_, _, _, _ = sub.Params(), sub.Identifier(), sub.ChannelName(), sub.Connection()

	srv.Disconnect(ctx, coax.Identifiers{"user": "42"}, false)
	srv.Shutdown(ctx)
	srv.ConnectionCount()
}

func readmeRedis() {
	ps := redispubsub.New(&redispubsub.Options{Address: "localhost:6379"})
	defer ps.Close()

	srv := coax.New(&coax.Options{PubSub: ps})
	_ = srv
}
