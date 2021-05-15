package hub

import (
	"context"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itzmeanjan/pub0sub/ops"
)

// Hub - Abstraction between message publishers & subscribers,
// works as a multiplexer ( or router )
type Hub struct {
	index        uint64
	subLock      *sync.RWMutex
	subscribers  map[string]map[uint64]net.Conn
	queueLock    *sync.RWMutex
	pendingQueue []*ops.Msg
	ping         chan struct{}
}

// publish - Actually writes message, along with opcode
// to network connection
func (h *Hub) publish(op *ops.OP, msg *ops.Msg) {
	h.subLock.RLock()
	defer h.subLock.RUnlock()

	pushMsg := ops.PushedMessage{Data: msg.Data}
	for i := 0; i < len(msg.Topics); i++ {
		subs, ok := h.subscribers[msg.Topics[i]]
		if !ok {
			continue
		}

		pushMsg.Topic = msg.Topics[i]
		for _, conn := range subs {
			// handle error
			op.WriteTo(conn)
			pushMsg.WriteTo(conn)
		}
	}
}

// Process - Listens for new message ready to published & works on publishing
// it to all topic subscribers
func (h *Hub) Process(ctx context.Context, running chan struct{}) {
	close(running)

	for {
		select {
		case <-ctx.Done():
			return

		case <-h.ping:
			msg := h.Next()
			op := ops.MSG_PUSH
			h.publish(&op, msg)

		case <-time.After(time.Duration(100) * time.Millisecond):
			started := time.Now()
			op := ops.MSG_PUSH

			for msg := h.Next(); msg != nil; {
				if time.Since(started) > time.Duration(100)*time.Millisecond {
					break
				}

				h.publish(&op, msg)
			}

		}
	}
}

// Listen - Hub listens for TCP connections, accepts those & spawns
// new go routine for handling each of those
func (h *Hub) Listen(ctx context.Context, addr string, done chan bool) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("[pub0sub] Error : %s\n", err.Error())

		done <- false
		return
	}

	defer func() {
		if err := lis.Close(); err != nil {
			log.Printf("[pub0sub] Error : %s\n", err.Error())
		}
	}()

	done <- true

	for {
		select {
		case <-ctx.Done():
			return

		default:
			conn, err := lis.Accept()
			if err != nil {
				log.Printf("[pub0sub] Error : %s\n", err.Error())
				break
			}

			func(conn net.Conn) {
				go h.handleTCPConnection(ctx, conn)
			}(conn)

		}
	}
}

// handleTCPConnection - Each publisher, subscriber connection is handled
// in this method, as seperate go routine
func (h *Hub) handleTCPConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("[pub0sub] Error : %s\n", err.Error())
		}
	}()

STOP:
	for {
		select {
		case <-ctx.Done():
			return

		default:
			op := new(ops.OP)
			_, err := op.ReadFrom(conn)
			if err != nil {
				break STOP
			}

			switch *op {
			case ops.PUB_REQ:
				msg := new(ops.Msg)
				if _, err := msg.ReadFrom(conn); err != nil {
					break STOP
				}

				subCount := h.Publish(msg)
				rOp := ops.PUB_RESP
				if _, err := rOp.WriteTo(conn); err != nil {
					break STOP
				}

				pResp := ops.CountResponse(subCount)
				if _, err := pResp.WriteTo(conn); err != nil {
					break STOP
				}

			case ops.NEW_SUB_REQ:
				msg := new(ops.NewSubscriptionRequest)
				if _, err := msg.ReadFrom(conn); err != nil {
					break STOP
				}

				subId, topicCount := h.Subscribe(conn, msg.Topics...)
				rOp := ops.NEW_SUB_RESP
				if _, err := rOp.WriteTo(conn); err != nil {
					break STOP
				}

				sResp := ops.NewSubResponse{Id: subId, TopicCount: topicCount}
				if _, err := sResp.WriteTo(conn); err != nil {
					break STOP
				}

			case ops.ADD_SUB_REQ:
				msg := new(ops.AddSubscriptionRequest)
				if _, err := msg.ReadFrom(conn); err != nil {
					break STOP
				}

				topicCount := h.AddSubscription(msg.Id, conn, msg.Topics...)
				rOp := ops.ADD_SUB_RESP
				if _, err := rOp.WriteTo(conn); err != nil {
					break STOP
				}

				pResp := ops.CountResponse(topicCount)
				if _, err := pResp.WriteTo(conn); err != nil {
					break STOP
				}

			case ops.UNSUB_REQ:
				msg := new(ops.UnsubcriptionRequest)
				if _, err := msg.ReadFrom(conn); err != nil {
					break STOP
				}

				topicCount := h.Unsubscribe(msg.Id, msg.Topics...)
				rOp := ops.UNSUB_RESP
				if _, err := rOp.WriteTo(conn); err != nil {
					break STOP
				}

				pResp := ops.CountResponse(topicCount)
				if _, err := pResp.WriteTo(conn); err != nil {
					break STOP
				}

			case ops.UNSUPPORTED:
				break STOP

			}

		}
	}

}

// nextId - Generates next subscriber id [ concurrrent-safe ]
func (h *Hub) nextId() uint64 {
	id := atomic.LoadUint64(&h.index)
	atomic.AddUint64(&h.index, 1)

	return id
}

// queued - Manager to check whether it has anything to act on
func (h *Hub) Queued() bool {
	h.queueLock.RLock()
	defer h.queueLock.RUnlock()

	return len(h.pendingQueue) != 0
}

// next - Next queued message to act on, if any
func (h *Hub) Next() *ops.Msg {
	if !h.Queued() {
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
			continue
		}

		if _, ok := subs[subId]; ok {
			continue
		}

		subs[subId] = conn
		count++
	}

	return count
}

// Subscribe - Client sends subscription request with a non-empty list
// of topics it's interested in, for very first time, which is why
// one unique id to be generated
func (h *Hub) Subscribe(conn net.Conn, topics ...string) (uint64, uint32) {
	if len(topics) == 0 {
		return 0, 0
	}

	id := h.nextId()
	return id, h.topicSubscribe(id, conn, topics...)
}

// AddSubscription - Subscriber showing intent of receiving messages
// from a non-empty set of topics [ on-the-fly i.e. after subscriber has been registered ]
func (h *Hub) AddSubscription(subId uint64, conn net.Conn, topics ...string) uint32 {
	if len(topics) == 0 {
		return 0
	}

	return h.topicSubscribe(subId, conn, topics...)
}

// Unsubscribe - Subscriber shows intent of not receiving messages
// from non-empty set of topics
func (h *Hub) Unsubscribe(subId uint64, topics ...string) uint32 {
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

			if len(subs) == 0 {
				delete(h.subscribers, topics[i])
			}
		}
	}
	return count
}

// Publish - Message publish request to be enqueued
// for some worker to process, while this function will
// calcalate how many clients will receive this message
// & respond back
func (h *Hub) Publish(msg *ops.Msg) uint32 {
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
