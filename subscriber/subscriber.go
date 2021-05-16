package subscriber

import (
	"context"
	"log"
	"net"
	"sync"

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
