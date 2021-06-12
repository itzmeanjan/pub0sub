package hub

import (
	"context"

	"github.com/xtaci/gaio"
)

func (h *Hub) handleWrite(ctx context.Context, id uint, result gaio.OpResult) error {
	if result.Error != nil {
		return result.Error
	}

	watcher := h.watchers[id]
	watcher.lock.RLock()
	defer watcher.lock.RUnlock()

	if enqueued, ok := watcher.ongoingRead[result.Conn]; ok && enqueued.envelopeRead {
		enqueued.envelopeRead = false
		return watcher.eventLoop.Read(ctx, result.Conn, enqueued.buf)
	}

	return nil
}
