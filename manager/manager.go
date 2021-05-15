package manager

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"github.com/itzmeanjan/pub0sub/hub"
	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/itzmeanjan/pub0sub/subscriber"
)

// Manager - A bridge between hub & remote clients, connected
// over network ( TCP, QUIC )
type Manager struct {
	ping        chan struct{}
	hub         *hub.Hub
	subLock     *sync.RWMutex
	subscribers map[string]map[uint64]*subscriber.Subscriber
}

// New - One new manager instance, ready to be used, with all components running
func New(ctx context.Context, addr string, ping chan struct{}, hub *hub.Hub) *Manager {
	manager := Manager{
		ping:        ping,
		hub:         hub,
		subLock:     &sync.RWMutex{},
		subscribers: make(map[string]map[uint64]*subscriber.Subscriber),
	}

	done := make(chan bool)
	manager.listen(ctx, addr, done)
	if !<-done {
		return nil
	}

	running := make(chan struct{})
	manager.process(ctx, running)
	<-running

	return &manager
}

// process - Processes message publish request from HUB
func (m *Manager) process(ctx context.Context, running chan struct{}) {
	close(running)

	for {
		select {
		case <-ctx.Done():
			return

		case <-m.ping:
			msg := m.hub.Next()
			op := ops.MSG_PUSH

			for i := 0; i < len(msg.Topics); i++ {
				m.subLock.RLock()
				subs, ok := m.subscribers[msg.Topics[i]]
				if !ok {
					m.subLock.RUnlock()
					continue
				}

				pushMsg := ops.PushedMessage{
					Topic: msg.Topics[i],
					Data:  msg.Data,
				}

				for _, sub := range subs {
					// handle error
					op.WriteTo(sub.Conn)
					pushMsg.WriteTo(sub.Conn)
				}
				m.subLock.RUnlock()
			}

		case <-time.After(time.Duration(100) * time.Millisecond):
			started := time.Now()
			op := ops.MSG_PUSH

			for msg := m.hub.Next(); msg != nil; {
				if time.Since(started) > time.Duration(100)*time.Millisecond {
					break
				}

				for i := 0; i < len(msg.Topics); i++ {
					m.subLock.RLock()
					subs, ok := m.subscribers[msg.Topics[i]]
					if !ok {
						m.subLock.RUnlock()
						continue
					}

					pushMsg := ops.PushedMessage{
						Topic: msg.Topics[i],
						Data:  msg.Data,
					}

					for _, sub := range subs {
						// handle error
						op.WriteTo(sub.Conn)
						pushMsg.WriteTo(sub.Conn)
					}
					m.subLock.RUnlock()
				}
			}

		}
	}
}

// listen - Listens to TCP connection on specified <address:port>
// accepts those & starts processing
func (m *Manager) listen(ctx context.Context, addr string, done chan bool) {
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
				go m.handleTCPConnection(ctx, conn)
			}(conn)

		}
	}
}

// handleTCPConnection - Each publisher/ subscriber connection is handled
// in its own go routine
//
// @note Improve error handling
func (m *Manager) handleTCPConnection(ctx context.Context, conn net.Conn) {
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

				// return listener count
				m.hub.Publish(msg)
				rOp := ops.PUB_RESP
				if _, err := rOp.WriteTo(conn); err != nil {
					break STOP
				}

				// fill it up with listener count
				pResp := ops.PubResponse(0)
				if _, err := pResp.WriteTo(conn); err != nil {
					break STOP
				}

			case ops.NEW_SUB_REQ:
				msg := new(ops.NewSubscriptionRequest)
				if _, err := msg.ReadFrom(conn); err != nil {
					break STOP
				}

				subId, topicCount := m.hub.Subscribe(msg.Topics...)
				sub := subscriber.Subscriber{Id: subId, Conn: conn}

				m.subLock.Lock()
				for i := 0; i < len(msg.Topics); i++ {
					topic := msg.Topics[i]
					subs, ok := m.subscribers[topic]
					if !ok {
						subs = make(map[uint64]*subscriber.Subscriber)
						subs[sub.Id] = &sub

						m.subscribers[topic] = subs
						continue
					}

					subs[sub.Id] = &sub
				}
				m.subLock.Unlock()

				rOp := ops.NEW_SUB_RESP
				if _, err := rOp.WriteTo(conn); err != nil {
					break STOP
				}

				sResp := ops.NewSubResponse{Id: subId, TopicCount: topicCount}
				if _, err := sResp.WriteTo(conn); err != nil {
					break STOP
				}

			case ops.MSG_PUSH:
			case ops.ADD_SUB_REQ:
			case ops.UNSUB_REQ:
			case ops.UNSUPPORTED:
				break STOP
			}

		}
	}

}
