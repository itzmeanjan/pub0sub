package hub

import (
	"context"

	"github.com/itzmeanjan/pubsub"
)

// StartHub - Starts underlying pub/sub hub, this is the instance
// to be used for communication from connection managers
func StartHub(ctx context.Context) *pubsub.PubSub {
	hub := pubsub.New(ctx)
	if !hub.IsAlive() {
		return nil
	}
	return hub
}
