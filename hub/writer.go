package hub

import (
	"context"

	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/xtaci/gaio"
)

func (h *Hub) handleWrite(ctx context.Context, id uint, result gaio.OpResult) error {
	if result.Error != nil {
		return result.Error
	}

	watcher := h.watchers[id]
	watcher.lock.RLock()
	defer watcher.lock.RUnlock()

	if ongoing, ok := watcher.ongoingRead[result.Conn]; ok && ongoing.envelopeRead {
		ongoing.envelopeRead = false
		ongoing.opcode = ops.UNSUPPORTED

		return watcher.eventLoop.Read(ctx, result.Conn, ongoing.buf)
	}

	return nil
}
