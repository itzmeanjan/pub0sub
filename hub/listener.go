package hub

import (
	"context"
	"fmt"
	"log"
	"net"
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

	done <- true

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

			addr := fmt.Sprintf("%s://%s",
				conn.RemoteAddr().Network(),
				conn.RemoteAddr().String())
			log.Printf("[pub0sub] ✅ Connected %s\n", addr)

			buf := make([]byte, 5)

			h.enqueuedReadLock.Lock()
			h.enqueuedRead[conn] = &enqueuedRead{yes: true, buf: buf}
			h.enqueuedReadLock.Unlock()

			h.watcher.Read(ctx, conn, buf)
		}
	}
}
