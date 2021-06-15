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

	h.watchersLock.RLock()
	watcher := h.watchers[id]
	h.watchersLock.RUnlock()

	watcher.lock.RLock()
	defer watcher.lock.RUnlock()

	if v, ok := watcher.ongoingRead[result.Conn]; ok && v.envelopeRead {
		switch v.opcode {
		case ops.PUB_REQ:
			return h.handleMessagePublish(ctx, id, result.Conn, data[:])

		case ops.NEW_SUB_REQ:
			return h.handleNewSubscription(ctx, id, result.Conn, data[:])

		case ops.ADD_SUB_REQ:
			return h.handleUpdateSubscription(ctx, id, result.Conn, data[:])

		case ops.UNSUB_REQ:
			return h.handleUnsubscription(ctx, id, result.Conn, data[:])

		}
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
		// socket, consider it as message payload read
		// instead of message envelope read
		//
		// Opcode also helps in understanding how to convert byte slice
		// into structured message
		watcher.ongoingRead[result.Conn].envelopeRead = true
		watcher.ongoingRead[result.Conn].opcode = op

		buf := make([]byte, size)
		return watcher.eventLoop.Read(ctx, result.Conn, buf)

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

	h.watchersLock.RLock()
	defer h.watchersLock.RUnlock()
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
	h.connectedSubscribers[conn] = &subInfo{id: subId, watcherId: id}
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

	h.watchersLock.RLock()
	defer h.watchersLock.RUnlock()
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

	h.watchersLock.RLock()
	defer h.watchersLock.RUnlock()
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

	h.watchersLock.RLock()
	defer h.watchersLock.RUnlock()
	return h.watchers[id].eventLoop.Write(ctx, conn, oStream.Bytes())
}
