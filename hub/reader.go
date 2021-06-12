package hub

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"

	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/xtaci/gaio"
)

func (h *Hub) handleRead(ctx context.Context, id uint, result gaio.OpResult) error {
	if result.Error != nil {
		return result.Error
	}

	if result.Size == 0 {
		return ops.ErrEmptyRead
	}

	data := result.Buffer[:result.Size]

	// actual message body sent by client & body
	// is ready for consumption
	//
	// Same scenario in {1, 2, 3, 4}, handling different opcodes

	// 1
	if _, ok := h.pendingPublishers[result.Conn]; ok {
		return h.handleMessagePublish(ctx, id, result.Conn, data[:])
	}

	// 2
	if _, ok := h.pendingNewSubscribers[result.Conn]; ok {
		return h.handleNewSubscription(ctx, id, result.Conn, data[:])
	}

	// 3
	if _, ok := h.pendingExistingSubscribers[result.Conn]; ok {
		return h.handleUpdateSubscription(ctx, id, result.Conn, data[:])
	}

	// 4
	if _, ok := h.pendingUnsubscribers[result.Conn]; ok {
		return h.handleUnsubscription(ctx, id, result.Conn, data[:])
	}

	// Envelope sent by client
	switch op := ops.OP(data[0]); op {

	case ops.PUB_REQ, ops.NEW_SUB_REQ, ops.ADD_SUB_REQ, ops.UNSUB_REQ:
		payloadSize := bytes.NewReader(data[1:])

		var size uint32
		if err := binary.Read(payloadSize, binary.BigEndian, &size); err != nil {
			return err
		}

		// remembering when next time data is read from this
		// connection, consider it as message payload read
		// instead of message envelope read
		switch op {
		case ops.PUB_REQ:
			h.pendingPublishers[result.Conn] = true

		case ops.NEW_SUB_REQ:
			h.pendingNewSubscribers[result.Conn] = true

		case ops.ADD_SUB_REQ:
			h.pendingExistingSubscribers[result.Conn] = true

		case ops.UNSUB_REQ:
			h.pendingUnsubscribers[result.Conn] = true

		}

		watcher := h.watchers[id]
		watcher.lock.RLock()
		defer watcher.lock.RUnlock()
		if enqueued, ok := watcher.ongoingRead[result.Conn]; ok && !enqueued.envelopeRead {
			enqueued.envelopeRead = true

			buf := make([]byte, size)
			return watcher.eventLoop.Read(ctx, result.Conn, buf)
		}

		return ops.ErrIllegalRead

	default:
		return ops.ErrIllegalRead

	}
}

func (h *Hub) handleMessagePublish(ctx context.Context, id uint, conn net.Conn, data []byte) error {
	// reading message from stream
	iStream := bytes.NewReader(data[:])
	msg := new(ops.Msg)
	if _, err := msg.ReadFrom(iStream); err != nil {
		return err
	}

	subCount := h.publish(msg)

	// writing message into stream
	oStream := new(bytes.Buffer)
	rOp := ops.PUB_RESP
	if _, err := rOp.WriteTo(oStream); err != nil {
		return err
	}
	cResp := ops.CountResponse(subCount)
	if _, err := cResp.WriteTo(oStream); err != nil {
		return err
	}

	delete(h.pendingPublishers, conn)
	return h.watchers[id].eventLoop.Write(ctx, conn, oStream.Bytes())
}

func (h *Hub) handleNewSubscription(ctx context.Context, id uint, conn net.Conn, data []byte) error {
	// reading message from stream
	iStream := bytes.NewReader(data[:])
	msg := new(ops.NewSubscriptionRequest)
	if _, err := msg.ReadFrom(iStream); err != nil {
		return err
	}

	subId, topicCount := h.subscribe(conn, msg.Topics...)

	// keeping track of active subscriber, so that
	// when need can run eviction routine targeting
	// this subscriber ( unique id )
	h.connectedSubscribersLock.Lock()
	h.connectedSubscribers[conn] = subId
	h.connectedSubscribersLock.Unlock()

	// writing message into stream
	oStream := new(bytes.Buffer)
	rOp := ops.NEW_SUB_RESP
	if _, err := rOp.WriteTo(oStream); err != nil {
		return err
	}
	sResp := ops.NewSubResponse{Id: subId, TopicCount: topicCount}
	if _, err := sResp.WriteTo(oStream); err != nil {
		return err
	}

	delete(h.pendingNewSubscribers, conn)
	return h.watchers[id].eventLoop.Write(ctx, conn, oStream.Bytes())
}

func (h *Hub) handleUpdateSubscription(ctx context.Context, id uint, conn net.Conn, data []byte) error {
	// reading message from stream
	iStream := bytes.NewReader(data)
	msg := new(ops.AddSubscriptionRequest)
	if _, err := msg.ReadFrom(iStream); err != nil {
		return err
	}

	topicCount := h.addSubscription(msg.Id, conn, msg.Topics...)

	// writing message into stream
	oStream := new(bytes.Buffer)
	rOp := ops.ADD_SUB_RESP
	if _, err := rOp.WriteTo(oStream); err != nil {
		return err
	}
	pResp := ops.CountResponse(topicCount)
	if _, err := pResp.WriteTo(oStream); err != nil {
		return err
	}

	delete(h.pendingExistingSubscribers, conn)
	return h.watchers[id].eventLoop.Write(ctx, conn, oStream.Bytes())
}

func (h *Hub) handleUnsubscription(ctx context.Context, id uint, conn net.Conn, data []byte) error {
	// reading message from stream
	iStream := bytes.NewReader(data)
	msg := new(ops.UnsubcriptionRequest)
	if _, err := msg.ReadFrom(iStream); err != nil {
		return err
	}

	topicCount := h.unsubscribe(msg.Id, msg.Topics...)

	// writing message into stream
	oStream := new(bytes.Buffer)
	rOp := ops.UNSUB_RESP
	if _, err := rOp.WriteTo(oStream); err != nil {
		return err
	}
	pResp := ops.CountResponse(topicCount)
	if _, err := pResp.WriteTo(oStream); err != nil {
		return err
	}

	delete(h.pendingUnsubscribers, conn)
	return h.watchers[id].eventLoop.Write(ctx, conn, oStream.Bytes())
}
