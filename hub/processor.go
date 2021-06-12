package hub

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/itzmeanjan/pub0sub/ops"
)

func (h *Hub) process(ctx context.Context, id uint, running chan struct{}) {
	running <- struct{}{}

	op := ops.MSG_PUSH
	for {
		select {
		case <-ctx.Done():
			return

		case <-h.ping:
			if msg := h.next(); msg != nil {
				h.writeMessage(ctx, id, &op, msg)
			}

		case <-time.After(time.Duration(256) * time.Millisecond):
			started := time.Now()

			for msg := h.next(); msg != nil; {
				h.writeMessage(ctx, id, &op, msg)

				if time.Since(started) > time.Duration(512)*time.Millisecond {
					break
				}
			}

		}
	}
}

func (h *Hub) writeMessage(ctx context.Context, id uint, op *ops.OP, msg *ops.Msg) {
	if msg == nil {
		return
	}

	h.subLock.RLock()
	defer h.subLock.RUnlock()

	pushMsg := ops.PushedMessage{Data: msg.Data}
	for i := 0; i < len(msg.Topics); i++ {
		subs, ok := h.subscribers[msg.Topics[i]]
		if !ok {
			continue
		}

		pushMsg.Topic = msg.Topics[i]

		buf := new(bytes.Buffer)
		if _, err := op.WriteTo(buf); err != nil {
			continue
		}
		if _, err := pushMsg.WriteTo(buf); err != nil {
			continue
		}

		for _, conn := range subs {
			if err := h.watchers[id].eventLoop.Write(ctx, conn, buf.Bytes()); err != nil {
				log.Printf("[pub0sub] Error : %s\n", err.Error())
				continue
			}
		}
	}
}
