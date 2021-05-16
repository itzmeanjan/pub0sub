package subscriber

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/itzmeanjan/pub0sub/ops"
)

// Subscriber - Abstraction layer at which subscriber interacts, while
// all underlying networking details are kept hidden
type Subscriber struct {
	id         uint64
	conn       net.Conn
	topicLock  *sync.RWMutex
	topics     map[string]bool
	bufferLock *sync.RWMutex
	buffer     []*ops.PushedMessage
}

// New - First time subscribing to a non-empty set of topics, for first time subscription
// use this function & then keep using obtained subscriber handle for further communication
// with HUB
func New(ctx context.Context, proto, addr string, cap uint64, topics ...string) (*Subscriber, error) {
	if len(topics) == 0 {
		return nil, errors.New("non-empty topic set required")
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
		conn:       conn,
		topicLock:  &sync.RWMutex{},
		topics:     make(map[string]bool),
		bufferLock: &sync.RWMutex{},
		buffer:     make([]*ops.PushedMessage, 0, cap),
	}

	running := make(chan struct{})
	go sub.listen(ctx, running)
	<-running

	op := ops.NEW_SUB_REQ
	if _, err := op.WriteTo(sub.conn); err != nil {
		return nil, err
	}
	sReq := ops.NewSubscriptionRequest{Topics: topics}
	if _, err := sReq.WriteTo(sub.conn); err != nil {
		return nil, err
	}

	return &sub, nil
}

// listen - Keeps waiting for new message arrival from HUB & buffers them
// so that subscriber can pull it from queue
func (s *Subscriber) listen(ctx context.Context, running chan struct{}) {
	close(running)
	defer func() {
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
			}

			switch *op {
			case ops.NEW_SUB_RESP:
				sResp := new(ops.NewSubResponse)
				if _, err := sResp.ReadFrom(s.conn); err != nil {
					if nErr, ok := err.(net.Error); ok && !nErr.Temporary() {
						return
					}
				}

				s.id = sResp.Id

			case ops.ADD_SUB_RESP:
				aResp := new(ops.CountResponse)
				if _, err := aResp.ReadFrom(s.conn); err != nil {
					if nErr, ok := err.(net.Error); ok && !nErr.Temporary() {
						return
					}
				}

			case ops.UNSUB_RESP:
				aResp := new(ops.CountResponse)
				if _, err := aResp.ReadFrom(s.conn); err != nil {
					if nErr, ok := err.(net.Error); ok && !nErr.Temporary() {
						return
					}
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

			default:
				return

			}

		}
	}
}

// AddSubscription - After a subscriber has been registered ( knows its ID ),
// more topics can be subscribed to
func (s *Subscriber) AddSubscription(topics ...string) (uint64, error) {
	if len(topics) == 0 {
		return 0, errors.New("non-empty topic set required")
	}

	s.topicLock.Lock()
	defer s.topicLock.Unlock()

	_topics := make([]string, 0, len(topics))

	var subCount uint64

	for i := 0; i < len(topics); i++ {
		if _, ok := s.topics[topics[i]]; ok {
			continue
		}
		_topics = append(_topics, topics[i])
		s.topics[topics[i]] = true
		subCount++
	}

	op := ops.ADD_SUB_REQ
	if _, err := op.WriteTo(s.conn); err != nil {
		return 0, err
	}
	sReq := ops.AddSubscriptionRequest{Id: s.id, Topics: _topics}
	if _, err := sReq.WriteTo(s.conn); err != nil {
		return 0, err
	}

	return subCount, nil
}

// Unsubscribe - Unsubscribe from a non-empty set of topics, no
// message to be received from those anymore
func (s *Subscriber) Unsubscribe(topics ...string) (uint64, error) {
	if len(topics) == 0 {
		return 0, errors.New("non-empty topic set required")
	}

	s.topicLock.Lock()
	defer s.topicLock.Unlock()

	_topics := make([]string, 0, len(topics))

	var unsubCount uint64
	for i := 0; i < len(topics); i++ {
		if _, ok := s.topics[topics[i]]; !ok {
			continue
		}
		_topics = append(_topics, topics[i])
		delete(s.topics, topics[i])
		unsubCount++
	}

	op := ops.UNSUB_REQ
	if _, err := op.WriteTo(s.conn); err != nil {
		return 0, err
	}
	uReq := ops.UnsubcriptionRequest{Id: s.id, Topics: _topics}
	if _, err := uReq.WriteTo(s.conn); err != nil {
		return 0, err
	}

	return unsubCount, nil
}

// UnsubscribeAll - Client not interested in receiving any messages
// from any of currently subscribed topics
func (s *Subscriber) UnsubscribeAll() (uint64, error) {
	s.topicLock.Lock()
	defer s.topicLock.Unlock()

	if len(s.topics) == 0 {
		return 0, errors.New("no topics to unsubscribe from")
	}

	_topics := make([]string, 0, len(s.topics))

	var unsubCount uint64
	for topic := range s.topics {
		_topics = append(_topics, topic)
		delete(s.topics, topic)
		unsubCount++
	}

	op := ops.UNSUB_REQ
	if _, err := op.WriteTo(s.conn); err != nil {
		return 0, err
	}
	uReq := ops.UnsubcriptionRequest{Id: s.id, Topics: _topics}
	if _, err := uReq.WriteTo(s.conn); err != nil {
		return 0, err
	}

	return unsubCount, nil
}

// Queued - Checks existance of any consumable message in buffer
func (s *Subscriber) Queued() bool {
	s.bufferLock.RLock()
	defer s.bufferLock.RUnlock()

	return len(s.buffer) != 0
}

// Next - Pulls out oldest queued message from buffer
func (s *Subscriber) Next() *ops.PushedMessage {
	if !s.Queued() {
		return nil
	}

	s.bufferLock.Lock()
	defer s.bufferLock.Unlock()

	msg := s.buffer[0]

	len := len(s.buffer)
	copy(s.buffer[:], s.buffer[1:])
	s.buffer[len-1] = nil
	s.buffer = s.buffer[:len-1]

	return msg
}
