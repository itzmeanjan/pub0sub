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

	op := ops.ADD_SUB_REQ
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

// AddSubscription - After a subscriber has been created, more topics
// can be subscribed to
func (s *Subscriber) AddSubscription(topics ...string) uint64 {
	if len(topics) == 0 {
		return 0
	}

	s.topicLock.Lock()
	defer s.topicLock.Unlock()

	var subCount uint64

	for i := 0; i < len(topics); i++ {
		if _, ok := s.topics[topics[i]]; ok {
			continue
		}
		s.topics[topics[i]] = true
		subCount++
	}

	return subCount
}

// Unsubscribe - Unsubscribe from a non-empty set of topics
func (s *Subscriber) Unsubscribe(topics ...string) uint64 {
	if len(topics) == 0 {
		return 0
	}

	s.topicLock.Lock()
	defer s.topicLock.Unlock()

	var unsubCount uint64

	for i := 0; i < len(topics); i++ {
		if _, ok := s.topics[topics[i]]; !ok {
			continue
		}
		delete(s.topics, topics[i])
		unsubCount++
	}

	return unsubCount
}

// UnsubscribeAll - Client not interested in receiving any messages
// from any of currently subscribed topics
func (s *Subscriber) UnsubscribeAll() uint64 {
	s.topicLock.Lock()
	defer s.topicLock.Unlock()

	var unsubCount uint64

	for topic := range s.topics {
		delete(s.topics, topic)
		unsubCount++
	}

	return unsubCount
}
