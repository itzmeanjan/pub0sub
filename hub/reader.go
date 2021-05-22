package hub

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/xtaci/gaio"
)

func (h *Hub) handleRead(ctx context.Context, result gaio.OpResult) error {
	if result.Error != nil {
		return result.Error
	}

	if result.Size == 0 {
		return errors.New("read zero bytes")
	}

	data := result.Buffer[:result.Size]

	{
		if _, ok := h.pendingPublishers[result.Conn]; ok {
			iStream := bytes.NewReader(data[:])

			msg := new(ops.Msg)
			if _, err := msg.ReadFrom(iStream); err != nil {
				return err
			}

			subCount := h.publish(msg)
			oStream := new(bytes.Buffer)

			rOp := ops.PUB_RESP
			if _, err := rOp.WriteTo(oStream); err != nil {
				return err
			}

			cResp := ops.CountResponse(subCount)
			if _, err := cResp.WriteTo(oStream); err != nil {
				return err
			}

			delete(h.pendingPublishers, result.Conn)
			return h.watcher.Write(ctx, result.Conn, oStream.Bytes())
		}
	}

	{
		if _, ok := h.pendingNewSubscribers[result.Conn]; ok {
			iStream := bytes.NewReader(data[:])

			msg := new(ops.NewSubscriptionRequest)
			if _, err := msg.ReadFrom(iStream); err != nil {
				return err
			}

			subId, topicCount := h.subscribe(result.Conn, msg.Topics...)

			// keeping track of active subscriber, so that
			// when need can run eviction routine targeting
			// this subscriber ( unique id )
			h.connectedSubscribers[result.Conn] = subId

			oStream := new(bytes.Buffer)

			rOp := ops.NEW_SUB_RESP
			if _, err := rOp.WriteTo(oStream); err != nil {
				return err
			}

			sResp := ops.NewSubResponse{Id: subId, TopicCount: topicCount}
			if _, err := sResp.WriteTo(oStream); err != nil {
				return err
			}

			delete(h.pendingNewSubscribers, result.Conn)
			return h.watcher.Write(ctx, result.Conn, oStream.Bytes())
		}
	}

	{
		if _, ok := h.pendingExistingSubscribers[result.Conn]; ok {
			iStream := bytes.NewReader(data)

			msg := new(ops.AddSubscriptionRequest)
			if _, err := msg.ReadFrom(iStream); err != nil {
				return err
			}

			topicCount := h.addSubscription(msg.Id, result.Conn, msg.Topics...)
			oStream := new(bytes.Buffer)

			rOp := ops.ADD_SUB_RESP
			if _, err := rOp.WriteTo(oStream); err != nil {
				return err
			}

			pResp := ops.CountResponse(topicCount)
			if _, err := pResp.WriteTo(oStream); err != nil {
				return err
			}

			delete(h.pendingExistingSubscribers, result.Conn)
			return h.watcher.Write(ctx, result.Conn, oStream.Bytes())
		}
	}

	{
		if _, ok := h.pendingUnsubscribers[result.Conn]; ok {
			iStream := bytes.NewReader(data)

			msg := new(ops.UnsubcriptionRequest)
			if _, err := msg.ReadFrom(iStream); err != nil {
				return err
			}

			topicCount := h.unsubscribe(msg.Id, msg.Topics...)
			oStream := new(bytes.Buffer)

			rOp := ops.UNSUB_RESP
			if _, err := rOp.WriteTo(oStream); err != nil {
				return err
			}

			pResp := ops.CountResponse(topicCount)
			if _, err := pResp.WriteTo(oStream); err != nil {
				return err
			}

			delete(h.pendingUnsubscribers, result.Conn)
			return h.watcher.Write(ctx, result.Conn, oStream.Bytes())
		}
	}

	switch op := ops.OP(data[0]); op {
	case ops.PUB_REQ, ops.NEW_SUB_REQ, ops.ADD_SUB_REQ, ops.UNSUB_REQ:
		payloadSize := bytes.NewReader(data[1:])

		var size uint32
		if err := binary.Read(payloadSize, binary.BigEndian, &size); err != nil {
			return err
		}

		if op == ops.PUB_REQ {
			h.pendingPublishers[result.Conn] = true
		}

		if op == ops.NEW_SUB_REQ {
			h.pendingNewSubscribers[result.Conn] = true
		}

		if op == ops.ADD_SUB_REQ {
			h.pendingExistingSubscribers[result.Conn] = true
		}

		if op == ops.UNSUB_REQ {
			h.pendingUnsubscribers[result.Conn] = true
		}

		if enqueued, ok := h.enqueuedRead[result.Conn]; ok && enqueued.yes {
			enqueued.yes = false
			return h.watcher.Read(ctx, result.Conn, enqueued.buf)
		}

		return errors.New("illegal envelope read completion event")

	default:
		// non-defined behaviour as of now
	}

	return nil
}
