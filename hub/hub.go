package hub

import (
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

// queued - Manager to check whether it has anything to act on
func (h *Hub) queued() bool {
	h.queueLock.RLock()
	defer h.queueLock.RUnlock()

	return len(h.pendingQueue) != 0
}

// next - Next queued message for manager to act on, if any
func (h *Hub) next() *pubsub.Message {
	if !h.queued() {
		return nil
	}

	h.queueLock.Lock()
	defer h.queueLock.Unlock()

	msg := h.pendingQueue[0]

	len := len(h.pendingQueue)
	copy(h.pendingQueue[:], h.pendingQueue[1:])
	h.pendingQueue[len-1] = nil
	h.pendingQueue = h.pendingQueue[:len-1]

	return msg
}

// Subscribe - Client sends subscription request with a non-empty list
// of topics it's interested in
func (h *Hub) Subscribe(topics ...string) {
	if len(topics) == 0 {
		return
	}

	id := h.nextId()

	h.subLock.Lock()
	defer h.subLock.Unlock()

	for i := 0; i < len(topics); i++ {
		subs, ok := h.subscribers[topics[i]]
		if !ok {
			subs = make(map[uint64]bool)
		}

		subs[id] = true
		h.subscribers[topics[i]] = subs
	}
}

// addSubscription - Subscriber showing intent of receiving messages
// from a non-empty set of topics [ on-the-fly i.e. after subscriber has been registered ]
func (h *Hub) addSubscription(subId uint64, topics ...string) {
	if len(topics) == 0 {
		return
	}

	h.subLock.Lock()
	defer h.subLock.Unlock()

	for i := 0; i < len(topics); i++ {
		subs, ok := h.subscribers[topics[i]]
		if !ok {
			subs = make(map[uint64]bool)
		}

		subs[subId] = true
		h.subscribers[topics[i]] = subs
	}
}

// unsubscribe - Subscriber shows intent of not receiving messages
// from non-empty set of topics
func (h *Hub) unsubscribe(subId uint64, topics ...string) {
	if len(topics) == 0 {
		return
	}

	h.subLock.Lock()
	defer h.subLock.Unlock()

	for i := 0; i < len(topics); i++ {
		subs, ok := h.subscribers[topics[i]]
		if !ok {
			continue
		}

		delete(subs, subId)
	}
}

// Publish - Publisher to invoke when needed, will queue message
// & to be acted on soon
func (h *Hub) Publish(msg *pubsub.Message) {
	if len(msg.Topics) == 0 {
		return
	}

	h.queueLock.Lock()
	defer h.queueLock.Unlock()

	h.pendingQueue = append(h.pendingQueue, msg)
}
