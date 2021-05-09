package pub0sub

import (
	"context"

	"github.com/itzmeanjan/pubsub"
)

// StartHub - Starts underlying pub/sub hub, this is the instance
// to be used for communication from connection managers
func StartHub(ctx context.Context) *pubsub.PubSub {
	hub := pubsub.New(ctx)
	return hub
}
