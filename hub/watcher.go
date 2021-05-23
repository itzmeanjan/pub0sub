package hub

import (
	"context"
	"io"
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

			for i := 0; i < len(results); i++ {

				switch results[i].Operation {
				case gaio.OpRead:
					if err := h.handleRead(ctx, results[i]); err != nil {
						if err == io.EOF {
							break
						}

						log.Printf("[pub0sub] Error : %s\n", err.Error())

						if id, ok := h.connectedSubscribers[results[i].Conn]; ok {
							h.evict <- id
							delete(h.connectedSubscribers, results[i].Conn)
						}

						h.enqueuedReadLock.Lock()
						delete(h.enqueuedRead, results[i].Conn)
						h.enqueuedReadLock.Unlock()

						if err := h.watcher.Free(results[i].Conn); err != nil {
							log.Printf("[pub0sub] Error : %s\n", err.Error())
						}
					}

				case gaio.OpWrite:
					if err := h.handleWrite(ctx, results[i]); err != nil {
						log.Printf("[pub0sub] Error : %s\n", err.Error())

						if id, ok := h.connectedSubscribers[results[i].Conn]; ok {
							h.evict <- id
							delete(h.connectedSubscribers, results[i].Conn)
						}

						h.enqueuedReadLock.Lock()
						delete(h.enqueuedRead, results[i].Conn)
						h.enqueuedReadLock.Unlock()

						if err := h.watcher.Free(results[i].Conn); err != nil {
							log.Printf("[pub0sub] Error : %s\n", err.Error())
						}
					}
				}

			}
		}
	}
}
