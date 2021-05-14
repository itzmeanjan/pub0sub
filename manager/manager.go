package manager

import (
	"context"
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

// start - Flow of manager during its lifetime
func (m *Manager) start(ctx context.Context, running chan struct{}) {
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
