package hub

import (
	"context"

	"github.com/xtaci/gaio"
)

func (h *Hub) handleWrite(ctx context.Context, result gaio.OpResult) error {
	if result.Error != nil {
		return result.Error
	}

	h.enqueuedReadLock.RLock()
	defer h.enqueuedReadLock.RUnlock()

	if enqueued, ok := h.enqueuedRead[result.Conn]; ok && !enqueued.yes {
		enqueued.yes = true
		return h.watcher.Read(ctx, result.Conn, enqueued.buf)
	}

	return nil
}
