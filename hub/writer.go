package hub

import (
	"context"

	"github.com/xtaci/gaio"
)

func (h *Hub) handleWrite(ctx context.Context, result gaio.OpResult) error {
	if result.Error != nil {
		return result.Error
	}

	if enqueued, ok := h.enqueuedRead[result.Conn]; ok && !enqueued.yes {
		enqueued.yes = true
		return h.watcher.Read(ctx, result.Conn, enqueued.buf)
	}

	return nil
}
