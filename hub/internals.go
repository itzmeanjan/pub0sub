package hub

import (
	"net"
	"sync/atomic"

	"github.com/itzmeanjan/pub0sub/ops"
)

// nextId - Generates next subscriber id [ concurrrent-safe ]
func (h *Hub) nextId() uint64 {
	return atomic.AddUint64(&h.index, 1)
}

// next - Next queued message to act on, if any
func (h *Hub) next() *ops.Msg {
	h.queueLock.Lock()
	defer h.queueLock.Unlock()

	if len(h.pendingQueue) == 0 {
		return nil
	}

	msg := h.pendingQueue[0]

	len := len(h.pendingQueue)
	copy(h.pendingQueue[:], h.pendingQueue[1:])
	h.pendingQueue[len-1] = nil
	h.pendingQueue = h.pendingQueue[:len-1]

	return msg
}

// keepRevSubs - Populate map where we can keep track of which subscriber
// is subscriber to which topics, so that they can be evicted easily, when needed
func (h *Hub) keepRevSubs(subId uint64, topic string) {
	h.revLock.Lock()
	defer h.revLock.Unlock()

	revSubs, ok := h.revSubscribers[subId]
	if !ok {
		revSubs := make(map[string]bool)
		revSubs[topic] = true
		h.revSubscribers[subId] = revSubs
		return
	}

	revSubs[topic] = true
}

// removeRevSubs - If not required, remove entry from map
func (h *Hub) removeRevSubs(subId uint64, topic string) {
	h.revLock.Lock()
	defer h.revLock.Unlock()

	revSubs, ok := h.revSubscribers[subId]
	if !ok {
		return
	}

	delete(revSubs, topic)
	if len(revSubs) == 0 {
		delete(h.revSubscribers, subId)
	}
}

// topicSubscribe - Subscribe client to topics
func (h *Hub) topicSubscribe(subId uint64, conn net.Conn, topics ...string) uint32 {
	var count uint32

	h.subLock.Lock()
	defer h.subLock.Unlock()

	for i := 0; i < len(topics); i++ {
		subs, ok := h.subscribers[topics[i]]
		if !ok {
			subs = make(map[uint64]net.Conn)
			subs[subId] = conn
			h.subscribers[topics[i]] = subs

			count++
			h.keepRevSubs(subId, topics[i])
			continue
		}

		if _, ok := subs[subId]; ok {
			continue
		}

		subs[subId] = conn
		count++
		h.keepRevSubs(subId, topics[i])
	}

	return count
}

// subscribe - Client sends subscription request with a non-empty list
// of topics it's interested in, for very first time, which is why
// one unique id to be generated
func (h *Hub) subscribe(conn net.Conn, topics ...string) (uint64, uint32) {
	if len(topics) == 0 {
		return 0, 0
	}

	id := h.nextId()
	return id, h.topicSubscribe(id, conn, topics...)
}

// addSubscription - Subscriber showing intent of receiving messages
// from a non-empty set of topics [ on-the-fly i.e. after subscriber has been registered ]
func (h *Hub) addSubscription(subId uint64, conn net.Conn, topics ...string) uint32 {
	if len(topics) == 0 {
		return 0
	}

	return h.topicSubscribe(subId, conn, topics...)
}

// Unsubscribe - Subscriber shows intent of not receiving messages
// from non-empty set of topics
func (h *Hub) unsubscribe(subId uint64, topics ...string) uint32 {
	if len(topics) == 0 {
		return 0
	}

	h.subLock.Lock()
	defer h.subLock.Unlock()

	var count uint32
	for i := 0; i < len(topics); i++ {
		subs, ok := h.subscribers[topics[i]]
		if !ok {
			continue
		}

		if _, ok := subs[subId]; ok {
			delete(subs, subId)
			count++
			h.removeRevSubs(subId, topics[i])

			if len(subs) == 0 {
				delete(h.subscribers, topics[i])
			}
		}
	}
	return count
}

// publish - Message publish request to be enqueued
// for some worker to process, while this function will
// calcalate how many clients will receive this message
// & respond back
func (h *Hub) publish(msg *ops.Msg) uint32 {
	if len(msg.Topics) == 0 {
		return 0
	}

	h.queueLock.Lock()
	h.pendingQueue = append(h.pendingQueue, msg)
	h.queueLock.Unlock()

	// try to notify, otherwise don't
	if len(h.ping) < cap(h.ping) {
		h.ping <- struct{}{}
	}

	h.subLock.RLock()
	defer h.subLock.RUnlock()

	var count uint32
	for i := 0; i < len(msg.Topics); i++ {
		subs, ok := h.subscribers[msg.Topics[i]]
		if !ok {
			continue
		}

		count += uint32(len(subs))
	}
	return count
}
