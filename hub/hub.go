package hub

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/xtaci/gaio"
)

type enqueuedRead struct {
	yes bool
	buf []byte
}

// Hub - Abstraction between message publishers & subscribers,
// works as a multiplexer ( or router )
type Hub struct {
	watcher                    *gaio.Watcher
	pendingPublishers          map[net.Conn]bool
	pendingNewSubscribers      map[net.Conn]bool
	pendingExistingSubscribers map[net.Conn]bool
	pendingUnsubscribers       map[net.Conn]bool
	enqueuedRead               map[net.Conn]*enqueuedRead
	connectedSubscribers       map[net.Conn]uint64
	index                      uint64
	subLock                    *sync.RWMutex
	subscribers                map[string]map[uint64]net.Conn
	revLock                    *sync.RWMutex
	revSubscribers             map[uint64]map[string]bool
	queueLock                  *sync.RWMutex
	pendingQueue               []*ops.Msg
	ping                       chan struct{}
	evict                      chan uint64
}

// New - Creates a new instance of hub, ready to be used
func New(ctx context.Context, addr string, cap uint64) (*Hub, error) {
	watcher, err := gaio.NewWatcher()
	if err != nil {
		return nil, err
	}

	hub := Hub{
		watcher:                    watcher,
		pendingPublishers:          make(map[net.Conn]bool),
		pendingNewSubscribers:      make(map[net.Conn]bool),
		pendingExistingSubscribers: make(map[net.Conn]bool),
		pendingUnsubscribers:       make(map[net.Conn]bool),
		enqueuedRead:               make(map[net.Conn]*enqueuedRead),
		connectedSubscribers:       make(map[net.Conn]uint64),
		index:                      0,
		subLock:                    &sync.RWMutex{},
		subscribers:                make(map[string]map[uint64]net.Conn),
		revLock:                    &sync.RWMutex{},
		revSubscribers:             make(map[uint64]map[string]bool),
		queueLock:                  &sync.RWMutex{},
		pendingQueue:               make([]*ops.Msg, 0, cap),
		ping:                       make(chan struct{}, cap),
		evict:                      make(chan uint64, cap),
	}

	var (
		runListener = make(chan bool)
		runWatcher  = make(chan struct{})
		runProc     = make(chan struct{})
		runEvict    = make(chan struct{})
	)

	go hub.listen(ctx, addr, runListener)
	if !<-runListener {
		return nil, errors.New("failed to start listener")
	}

	go hub.watch(ctx, runWatcher)
	go hub.process(ctx, runProc)
	go hub.evictSubscribers(ctx, runEvict)
	<-runWatcher
	<-runProc
	<-runEvict

	return &hub, nil
}

func (h *Hub) listen(ctx context.Context, addr string, done chan bool) {
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
				return
			}

			buf := make([]byte, 5)
			h.enqueuedRead[conn] = &enqueuedRead{yes: true, buf: buf}
			h.watcher.Read(ctx, conn, buf)
		}
	}
}

func (h *Hub) watch(ctx context.Context, done chan struct{}) {
	close(done)

	for {
		select {
		case <-ctx.Done():
			return

		default:
			results, err := h.watcher.WaitIO()
			if err != nil {
				log.Printf("[pub0sub] Error : %s\n", err.Error())
				return
			}

			for i := 0; i < len(results); i++ {

				switch results[i].Operation {
				case gaio.OpRead:
					if err := h.handleRead(ctx, results[i]); err != nil {
						log.Printf("[pub0sub] Error : %s\n", err.Error())

						if id, ok := h.connectedSubscribers[results[i].Conn]; ok {
							h.evict <- id
							delete(h.connectedSubscribers, results[i].Conn)
						}
						if err := h.watcher.Free(results[i].Conn); err != nil {
							log.Printf("[pub0sub] Error : %s\n", err.Error())
						}
					}

				case gaio.OpWrite:
					if err := h.handleWrite(ctx, results[i]); err != nil {
						log.Printf("[pub0sub] Error : %s\n", err.Error())

						if id, ok := h.connectedSubscribers[results[i].Conn]; ok {
							h.evict <- id
							delete(h.connectedSubscribers, results[i].Conn)
						}
						if err := h.watcher.Free(results[i].Conn); err != nil {
							log.Printf("[pub0sub] Error : %s\n", err.Error())
						}
					}
				}

			}
		}
	}
}

func (h *Hub) handleRead(ctx context.Context, result gaio.OpResult) error {
	if result.Error != nil {
		return result.Error
	}

	if result.Size == 0 {
		return errors.New("read zero bytes")
	}

	data := result.Buffer[:result.Size]

	{
		if _, ok := h.pendingPublishers[result.Conn]; ok {
			iStream := bytes.NewReader(data[:])

			msg := new(ops.Msg)
			if _, err := msg.ReadFrom(iStream); err != nil {
				return err
			}

			subCount := h.publish(msg)
			oStream := new(bytes.Buffer)

			rOp := ops.PUB_RESP
			if _, err := rOp.WriteTo(oStream); err != nil {
				return err
			}

			cResp := ops.CountResponse(subCount)
			if _, err := cResp.WriteTo(oStream); err != nil {
				return err
			}

			delete(h.pendingPublishers, result.Conn)
			return h.watcher.Write(ctx, result.Conn, oStream.Bytes())
		}
	}

	{
		if _, ok := h.pendingNewSubscribers[result.Conn]; ok {
			iStream := bytes.NewReader(data[:])

			msg := new(ops.NewSubscriptionRequest)
			if _, err := msg.ReadFrom(iStream); err != nil {
				return err
			}

			subId, topicCount := h.subscribe(result.Conn, msg.Topics...)

			// keeping track of active subscriber, so that
			// when need can run eviction routine targeting
			// this subscriber ( unique id )
			h.connectedSubscribers[result.Conn] = subId

			oStream := new(bytes.Buffer)

			rOp := ops.NEW_SUB_RESP
			if _, err := rOp.WriteTo(oStream); err != nil {
				return err
			}

			sResp := ops.NewSubResponse{Id: subId, TopicCount: topicCount}
			if _, err := sResp.WriteTo(oStream); err != nil {
				return err
			}

			delete(h.pendingNewSubscribers, result.Conn)
			return h.watcher.Write(ctx, result.Conn, oStream.Bytes())
		}
	}

	{
		if _, ok := h.pendingExistingSubscribers[result.Conn]; ok {
			iStream := bytes.NewReader(data)

			msg := new(ops.AddSubscriptionRequest)
			if _, err := msg.ReadFrom(iStream); err != nil {
				return err
			}

			topicCount := h.addSubscription(msg.Id, result.Conn, msg.Topics...)
			oStream := new(bytes.Buffer)

			rOp := ops.ADD_SUB_RESP
			if _, err := rOp.WriteTo(oStream); err != nil {
				return err
			}

			pResp := ops.CountResponse(topicCount)
			if _, err := pResp.WriteTo(oStream); err != nil {
				return err
			}

			delete(h.pendingExistingSubscribers, result.Conn)
			return h.watcher.Write(ctx, result.Conn, oStream.Bytes())
		}
	}

	{
		if _, ok := h.pendingUnsubscribers[result.Conn]; ok {
			iStream := bytes.NewReader(data)

			msg := new(ops.UnsubcriptionRequest)
			if _, err := msg.ReadFrom(iStream); err != nil {
				return err
			}

			topicCount := h.unsubscribe(msg.Id, msg.Topics...)
			oStream := new(bytes.Buffer)

			rOp := ops.UNSUB_RESP
			if _, err := rOp.WriteTo(oStream); err != nil {
				return err
			}

			pResp := ops.CountResponse(topicCount)
			if _, err := pResp.WriteTo(oStream); err != nil {
				return err
			}

			delete(h.pendingUnsubscribers, result.Conn)
			return h.watcher.Write(ctx, result.Conn, oStream.Bytes())
		}
	}

	switch op := ops.OP(data[0]); op {
	case ops.PUB_REQ, ops.NEW_SUB_REQ, ops.ADD_SUB_REQ, ops.UNSUB_REQ:
		payloadSize := bytes.NewReader(data[1:])

		var size uint32
		if err := binary.Read(payloadSize, binary.BigEndian, &size); err != nil {
			return err
		}

		if op == ops.PUB_REQ {
			h.pendingPublishers[result.Conn] = true
		}

		if op == ops.NEW_SUB_REQ {
			h.pendingNewSubscribers[result.Conn] = true
		}

		if op == ops.ADD_SUB_REQ {
			h.pendingExistingSubscribers[result.Conn] = true
		}

		if op == ops.UNSUB_REQ {
			h.pendingUnsubscribers[result.Conn] = true
		}

		if enqueued, ok := h.enqueuedRead[result.Conn]; ok && enqueued.yes {
			enqueued.yes = false
			return h.watcher.Read(ctx, result.Conn, enqueued.buf)
		}

		return errors.New("illegal envelope read completion event")

	default:
		// non-defined behaviour as of now
	}

	return nil
}

func (h *Hub) handleWrite(ctx context.Context, result gaio.OpResult) error {
	if result.Error != nil {
		return result.Error
	}

	if enqueued, ok := h.enqueuedRead[result.Conn]; ok && !enqueued.yes {
		enqueued.yes = true
		return h.watcher.Read(ctx, result.Conn, enqueued.buf)
	}

	return nil
}

// Actually writes message, along with opcode
// to network connection
func (h *Hub) writeMessage(ctx context.Context, op *ops.OP, msg *ops.Msg) {
	h.subLock.RLock()
	defer h.subLock.RUnlock()

	pushMsg := ops.PushedMessage{Data: msg.Data}
	for i := 0; i < len(msg.Topics); i++ {
		subs, ok := h.subscribers[msg.Topics[i]]
		if !ok {
			continue
		}

		pushMsg.Topic = msg.Topics[i]

		buf := new(bytes.Buffer)
		if _, err := op.WriteTo(buf); err != nil {
			continue
		}
		if _, err := pushMsg.WriteTo(buf); err != nil {
			continue
		}

		for _, conn := range subs {
			if err := h.watcher.Write(ctx, conn, buf.Bytes()); err != nil {
				log.Printf("[pub0sub] Error : %s\n", err.Error())
				continue
			}
		}
	}
}

// Process - Listens for new message ready to published & works on publishing
// it to all topic subscribers
func (h *Hub) process(ctx context.Context, running chan struct{}) {
	close(running)

	op := ops.MSG_PUSH
	for {
		select {
		case <-ctx.Done():
			return

		case <-h.ping:
			msg := h.next()
			h.writeMessage(ctx, &op, msg)

		case <-time.After(time.Duration(100) * time.Millisecond):
			started := time.Now()

			for msg := h.next(); msg != nil; {
				if time.Since(started) > time.Duration(100)*time.Millisecond {
					break
				}

				h.writeMessage(ctx, &op, msg)
			}

		}
	}
}

// As soon as it's determined peer is not anymore
// connected & it didn't follow graceful tear down ( didn't unsubscribe from topics )
// its entry from subscription table to be evicted
func (h *Hub) evictSubscribers(ctx context.Context, running chan struct{}) {
	close(running)

	for {
		select {
		case <-ctx.Done():
			return

		case id := <-h.evict:
			h.revLock.Lock()
			revSubs, ok := h.revSubscribers[id]
			if ok {
				for topic := range revSubs {
					h.subLock.Lock()
					subs, ok := h.subscribers[topic]
					if ok {
						delete(subs, id)
						if len(subs) == 0 {
							delete(h.subscribers, topic)
						}
					}
					h.subLock.Unlock()
				}

				delete(h.revSubscribers, id)
			}
			h.revLock.Unlock()
		}
	}
}

// nextId - Generates next subscriber id [ concurrrent-safe ]
func (h *Hub) nextId() uint64 {
	return atomic.AddUint64(&h.index, 1)
}

// queued - Manager to check whether it has anything to act on
func (h *Hub) queued() bool {
	h.queueLock.RLock()
	defer h.queueLock.RUnlock()

	return len(h.pendingQueue) != 0
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
