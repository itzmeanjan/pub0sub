package subscriber

import (
	"net"
	"sync"
)

// Subscriber - Abstraction layer at which subscriber interacts, while
// all underlying networking details are kept hidden
type Subscriber struct {
	Id        uint64
	Conn      net.Conn
	topicLock *sync.RWMutex
	topics    map[string]bool
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
