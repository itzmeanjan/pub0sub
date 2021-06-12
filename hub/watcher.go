package hub

import (
	"context"
	"fmt"
	"log"

	"github.com/xtaci/gaio"
)

func (h *Hub) watch(ctx context.Context, id uint, done chan struct{}) {
	// notifying that watcher started
	done <- struct{}{}

	watcher := h.watchers[id]
	defer func() {
		if err := watcher.eventLoop.Close(); err != nil {
			log.Printf("[pub0sub] Error : %s\n", err.Error())
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		default:
			results, err := watcher.eventLoop.WaitIO()
			if err != nil {
				log.Printf("[pub0sub] Error : %s\n", err.Error())
				return
			}

			for _, res := range results {

				switch res.Operation {
				case gaio.OpRead:
					if err := h.handleRead(ctx, id, res); err != nil {

						// best effort mechanism, don't ever block
						if len(h.Disconnected) < cap(h.Disconnected) {
							h.Disconnected <- fmt.Sprintf("%s://%s", res.Conn.RemoteAddr().Network(), res.Conn.RemoteAddr().String())
						}

						h.connectedSubscribersLock.Lock()
						if id, ok := h.connectedSubscribers[res.Conn]; ok {
							h.evict <- id
							delete(h.connectedSubscribers, res.Conn)
						}
						h.connectedSubscribersLock.Unlock()

						watcher.lock.Lock()
						delete(watcher.ongoingRead, res.Conn)
						watcher.lock.Unlock()

						if err := watcher.eventLoop.Free(res.Conn); err != nil {
							log.Printf("[pub0sub] Error : %s\n", err.Error())
						}

					}

				case gaio.OpWrite:
					if err := h.handleWrite(ctx, id, res); err != nil {

						// best effort mechanism, don't ever block
						if len(h.Disconnected) < cap(h.Disconnected) {
							h.Disconnected <- fmt.Sprintf("%s://%s", res.Conn.RemoteAddr().Network(), res.Conn.RemoteAddr().String())
						}

						h.connectedSubscribersLock.Lock()
						if id, ok := h.connectedSubscribers[res.Conn]; ok {
							h.evict <- id
							delete(h.connectedSubscribers, res.Conn)
						}
						h.connectedSubscribersLock.Unlock()

						watcher.lock.Lock()
						delete(watcher.ongoingRead, res.Conn)
						watcher.lock.Unlock()

						if err := watcher.eventLoop.Free(res.Conn); err != nil {
							log.Printf("[pub0sub] Error : %s\n", err.Error())
						}

					}
				}

			}

		}
	}
}
