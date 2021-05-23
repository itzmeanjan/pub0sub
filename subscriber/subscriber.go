package subscriber

import (
	"bytes"
	"context"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itzmeanjan/pub0sub/ops"
)

// Subscriber - Abstraction layer at which subscriber interacts, while
// all underlying networking details are kept hidden
type Subscriber struct {
	connected    uint64
	id           uint64
	conn         net.Conn
	topicLock    *sync.RWMutex
	topics       map[string]bool
	bufferLock   *sync.RWMutex
	buffer       []*ops.PushedMessage
	newSubChan   chan chan newSubscriptionResponse
	subUnsubChan chan chan uint32
	ping         chan struct{}
}

type newSubscriptionResponse struct {
	id         uint64
	topicCount uint32
}

// New - First time subscribing to a non-empty set of topics, for first time subscription
// use this function & then keep using obtained subscriber handle for further communication
// with HUB
func New(ctx context.Context, proto, addr string, cap uint64, topics ...string) (*Subscriber, error) {
	if len(topics) == 0 {
		return nil, ops.ErrEmptyTopicSet
	}

	d := net.Dialer{
		Timeout:  time.Duration(10) * time.Second,
		Deadline: time.Now().Add(time.Duration(20) * time.Second),
	}

	conn, err := d.DialContext(ctx, proto, addr)
	if err != nil {
		return nil, err
	}

	sub := Subscriber{
		conn:         conn,
		topicLock:    &sync.RWMutex{},
		topics:       make(map[string]bool),
		bufferLock:   &sync.RWMutex{},
		buffer:       make([]*ops.PushedMessage, 0, cap),
		newSubChan:   make(chan chan newSubscriptionResponse),
		subUnsubChan: make(chan chan uint32, 1),
		ping:         make(chan struct{}, cap),
	}

	for _, topic := range topics {
		sub.topics[topic] = true
	}

	running := make(chan struct{})
	go sub.listen(ctx, running)
	<-running

	var buf = new(bytes.Buffer)

	// writes envelope
	sReq := ops.NewSubscriptionRequest{Topics: topics}
	if _, err := sReq.WriteEnvelope(buf); err != nil {
		return nil, err
	}
	if _, err := sub.conn.Write(buf.Bytes()); err != nil {
		return nil, err
	}

	buf.Reset()

	// writes message body
	if _, err := sReq.WriteTo(buf); err != nil {
		return nil, err
	}
	if _, err := sub.conn.Write(buf.Bytes()); err != nil {
		return nil, err
	}

	defer func() {
		buf.Reset()
	}()

	resChan := make(chan newSubscriptionResponse)
	sub.newSubChan <- resChan
	res := <-resChan

	sub.id = res.id
	return &sub, nil
}

// listen - Keeps waiting for new message arrival from HUB & buffers them
// so that subscriber can pull it from queue
func (s *Subscriber) listen(ctx context.Context, running chan struct{}) {
	atomic.AddUint64(&s.connected, 1)
	close(running)

	defer func() {
		if !s.Connected() {
			return
		}

		atomic.AddUint64(&s.connected, ^uint64(0))
		if err := s.conn.Close(); err != nil {
			log.Printf("[pub0sub] Error : %s\n", err.Error())
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		default:
			op := new(ops.OP)
			if _, err := op.ReadFrom(s.conn); err != nil {
				if nErr, ok := err.(net.Error); ok && !nErr.Temporary() {
					return
				}
				break
			}

			switch *op {
			case ops.NEW_SUB_RESP:
				sResp := new(ops.NewSubResponse)
				if _, err := sResp.ReadFrom(s.conn); err != nil {
					if nErr, ok := err.(net.Error); ok && !nErr.Temporary() {
						return
					}
				}

				resp := <-s.newSubChan
				resp <- newSubscriptionResponse{id: sResp.Id, topicCount: sResp.TopicCount}

			case ops.ADD_SUB_RESP:
				aResp := new(ops.CountResponse)
				if _, err := aResp.ReadFrom(s.conn); err != nil {
					if nErr, ok := err.(net.Error); ok && !nErr.Temporary() {
						return
					}
				}

				if len(s.subUnsubChan) != 0 {
					resp := <-s.subUnsubChan
					resp <- uint32(*aResp)
				}

			case ops.UNSUB_RESP:
				aResp := new(ops.CountResponse)
				if _, err := aResp.ReadFrom(s.conn); err != nil {
					if nErr, ok := err.(net.Error); ok && !nErr.Temporary() {
						return
					}
				}

				if len(s.subUnsubChan) != 0 {
					resp := <-s.subUnsubChan
					resp <- uint32(*aResp)
				}

			case ops.MSG_PUSH:
				msg := new(ops.PushedMessage)
				if _, err := msg.ReadFrom(s.conn); err != nil {
					if nErr, ok := err.(net.Error); ok && !nErr.Temporary() {
						return
					}
				}

				s.bufferLock.Lock()
				s.buffer = append(s.buffer, msg)
				s.bufferLock.Unlock()

				// notify if possible
				if len(s.ping) < cap(s.ping) {
					s.ping <- struct{}{}
				}

			default:
				return

			}

		}
	}
}

// AddSubscription - After a subscriber has been registered ( knows its ID ),
// more topics can be subscribed to
func (s *Subscriber) AddSubscription(topics ...string) (uint32, error) {
	if len(topics) == 0 {
		return 0, ops.ErrEmptyTopicSet
	}

	if !s.Connected() {
		return 0, ops.ErrConnectionTerminated
	}

	s.topicLock.Lock()
	for i := 0; i < len(topics); i++ {
		if _, ok := s.topics[topics[i]]; ok {
			continue
		}
		s.topics[topics[i]] = true
	}
	s.topicLock.Unlock()

	// two parts of message to be written
	// in two steps using this buffer ( into stream )
	//
	// <envelope> + <body>
	var buf = new(bytes.Buffer)

	// writing envelope
	sReq := ops.AddSubscriptionRequest{Id: s.id, Topics: topics}
	if _, err := sReq.WriteEnvelope(buf); err != nil {
		return 0, err
	}
	if _, err := s.conn.Write(buf.Bytes()); err != nil {
		return 0, err
	}

	buf.Reset()

	// Writing message body into stream
	if _, err := sReq.WriteTo(buf); err != nil {
		return 0, err
	}
	if _, err := s.conn.Write(buf.Bytes()); err != nil {
		return 0, err
	}

	defer func() {
		buf.Reset()
	}()

	resChan := make(chan uint32)
	s.subUnsubChan <- resChan

	return <-resChan, nil
}

// Unsubscribe - Unsubscribe from a non-empty set of topics, no
// message to be received from those anymore
func (s *Subscriber) Unsubscribe(topics ...string) (uint32, error) {
	if len(topics) == 0 {
		return 0, ops.ErrEmptyTopicSet
	}

	if !s.Connected() {
		return 0, ops.ErrConnectionTerminated
	}

	s.topicLock.Lock()
	for i := 0; i < len(topics); i++ {
		if _, ok := s.topics[topics[i]]; !ok {
			continue
		}
		delete(s.topics, topics[i])
	}
	s.topicLock.Unlock()

	uReq := ops.UnsubcriptionRequest{Id: s.id, Topics: topics}
	if _, err := uReq.WriteEnvelope(s.conn); err != nil {
		return 0, err
	}
	if _, err := uReq.WriteTo(s.conn); err != nil {
		return 0, err
	}

	resChan := make(chan uint32)
	s.subUnsubChan <- resChan

	return <-resChan, nil
}

// UnsubscribeAll - Client not interested in receiving any messages
// from any of currently subscribed topics
func (s *Subscriber) UnsubscribeAll() (uint32, error) {
	if !s.Connected() {
		return 0, ops.ErrConnectionTerminated
	}

	s.topicLock.Lock()
	if len(s.topics) == 0 {
		return 0, ops.ErrEmptyTopicSet
	}
	topics := make([]string, 0, len(s.topics))
	var unsubCount uint64
	for topic := range s.topics {
		topics = append(topics, topic)
		delete(s.topics, topic)
		unsubCount++
	}
	s.topicLock.Unlock()

	uReq := ops.UnsubcriptionRequest{Id: s.id, Topics: topics}
	if _, err := uReq.WriteEnvelope(s.conn); err != nil {
		return 0, err
	}
	if _, err := uReq.WriteTo(s.conn); err != nil {
		return 0, err
	}

	resChan := make(chan uint32)
	s.subUnsubChan <- resChan

	return <-resChan, nil
}

// Watch - Watch if new message has arrived in mailbox
//
// Note: If subscriber is slow & more messages come in, some
// notifications may be missed ( not sent to be clear )
func (s *Subscriber) Watch() chan struct{} {
	return s.ping
}

// Queued - Checks existance of any consumable message in buffer
func (s *Subscriber) Queued() bool {
	s.bufferLock.RLock()
	defer s.bufferLock.RUnlock()

	return len(s.buffer) != 0
}

// Next - Pulls out oldest queued message from buffer
func (s *Subscriber) Next() *ops.PushedMessage {
	s.bufferLock.Lock()
	defer s.bufferLock.Unlock()

	if len(s.buffer) == 0 {
		return nil
	}

	msg := s.buffer[0]

	len := len(s.buffer)
	copy(s.buffer[:], s.buffer[1:])
	s.buffer[len-1] = nil
	s.buffer = s.buffer[:len-1]

	return msg
}

// Connected - Concurrent safe check for connection aliveness with HUB
func (s *Subscriber) Connected() bool {
	return atomic.LoadUint64(&s.connected) == 1
}

// Disconnect - Disconnects subscriber by closing network connection
//
// Invoking this method also unblocks read attempt in `listen`-er go routine
// if that's blocked
func (s *Subscriber) Disconnect() error {
	if !s.Connected() {
		return ops.ErrConnectionTerminated
	}
	atomic.AddUint64(&s.connected, ^uint64(0))
	return s.conn.Close()
}
