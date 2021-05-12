package hub

import (
	"context"
	"sync"

	"github.com/itzmeanjan/pubsub"
)

// Hub - Abstraction between message publishers & subscribers,
// works as a multiplexer ( or router )
type Hub struct {
	indexLock    *sync.RWMutex
	index        uint64
	subLock      *sync.RWMutex
	subscribers  map[string]map[uint64]bool
	queueLock    *sync.RWMutex
	pendingQueue []*pubsub.Message
}

// nextId - Generates next subscriber id [ concurrrent-safe ]
func (h *Hub) nextId() uint64 {
	h.indexLock.Lock()
	defer h.indexLock.Unlock()

	id := h.index
	h.index++

	return id
}

// StartHub - Starts underlying pub/sub hub, this is the instance
// to be used for communication from connection managers
func StartHub(ctx context.Context) *pubsub.PubSub {
	hub := pubsub.New(ctx)
	if !hub.IsAlive() {
		return nil
	}
	return hub
}
