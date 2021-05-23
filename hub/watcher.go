package hub

import (
	"context"
	"fmt"
	"log"

	"github.com/xtaci/gaio"
)

func (h *Hub) watch(ctx context.Context, done chan struct{}) {
	close(done)

	defer func() {
		if err := h.watcher.Close(); err != nil {
			log.Printf("[pub0sub] Error : %s\n", err.Error())
		}
	}()

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

			for _, res := range results {

				switch res.Operation {
				case gaio.OpRead:
					if err := h.handleRead(ctx, res); err != nil {

						if id, ok := h.connectedSubscribers[res.Conn]; ok {
							h.evict <- id
							delete(h.connectedSubscribers, res.Conn)
						}

						h.enqueuedReadLock.Lock()
						delete(h.enqueuedRead, res.Conn)
						h.enqueuedReadLock.Unlock()

						if err := h.watcher.Free(res.Conn); err != nil {
							log.Printf("[pub0sub] Error : %s\n", err.Error())
						}

						addr := fmt.Sprintf("%s://%s",
							res.Conn.RemoteAddr().Network(),
							res.Conn.RemoteAddr().String())
						log.Printf("[pub0sub] ❌ Disconnected %s\n", addr)

					}

				case gaio.OpWrite:
					if err := h.handleWrite(ctx, res); err != nil {

						if id, ok := h.connectedSubscribers[res.Conn]; ok {
							h.evict <- id
							delete(h.connectedSubscribers, res.Conn)
						}

						h.enqueuedReadLock.Lock()
						delete(h.enqueuedRead, res.Conn)
						h.enqueuedReadLock.Unlock()

						if err := h.watcher.Free(res.Conn); err != nil {
							log.Printf("[pub0sub] Error : %s\n", err.Error())
						}

						addr := fmt.Sprintf("%s://%s",
							res.Conn.RemoteAddr().Network(),
							res.Conn.RemoteAddr().String())
						log.Printf("[pub0sub] ❌ Disconnected %s\n", addr)

					}
				}

			}

		}
	}
}
