package hub

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/itzmeanjan/pub0sub/ops"
)

func (h *Hub) listen(ctx context.Context, addr string, done chan bool) {
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

	h.addr = lis.Addr().String()
	done <- true
	var nextWatcher uint

	for {
		select {
		case <-ctx.Done():
			return

		default:
			conn, err := lis.Accept()
			if err != nil {
				log.Printf("[pub0sub] Error : %s\n", err.Error())
				return
			}

			// best effort mechanism, don't ever block
			if len(h.Connected) < cap(h.Connected) {
				h.Connected <- fmt.Sprintf("%s://%s", conn.RemoteAddr().Network(), conn.RemoteAddr().String())
			}

			buf := make([]byte, 5)
			nextWatcher = (nextWatcher + 1) % h.watcherCount

			h.watchersLock.RLock()
			watcher := h.watchers[nextWatcher]
			h.watchersLock.RUnlock()

			watcher.lock.Lock()
			watcher.ongoingRead[conn] = &readState{buf: buf, opcode: ops.UNSUPPORTED}
			watcher.lock.Unlock()

			if err := watcher.eventLoop.Read(ctx, conn, buf); err != nil {
				log.Printf("[pub0sub] Error : %s\n", err.Error())
				return
			}
		}
	}
}
