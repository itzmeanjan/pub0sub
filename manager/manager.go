package manager

import (
	"context"
	"sync"
	"time"

	"github.com/itzmeanjan/pub0sub/hub"
)

// Manager - A bridge between hub & remote clients, connected
// over network ( TCP, QUIC )
type Manager struct {
	ping        chan struct{}
	hub         *hub.Hub
	subLock     *sync.RWMutex
	subscribers map[string]map[uint64]struct{}
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

			for i := 0; i < len(msg.Topics); i++ {
				_, ok := m.subscribers[msg.Topics[i].String()]
				if !ok {
					continue
				}

			}

		case <-time.After(time.Duration(100) * time.Millisecond):
			started := time.Now()

			for msg := m.hub.Next(); msg != nil; {

				if time.Since(started) > time.Duration(100)*time.Millisecond {
					break
				}

				for i := 0; i < len(msg.Topics); i++ {
					_, ok := m.subscribers[msg.Topics[i].String()]
					if !ok {
						continue
					}
				}

			}

		}
	}
}
