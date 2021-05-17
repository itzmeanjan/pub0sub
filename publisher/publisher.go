package publisher

import (
	"context"
	"errors"
	"log"
	"net"
	"time"

	"github.com/itzmeanjan/pub0sub/ops"
)

// Publisher - Abstraction layer at which publisher operates
type Publisher struct {
	conn    net.Conn
	msgChan chan *msgPublishRequest
}

// msgPublishRequest - Message publish request to be received
// in this form
type msgPublishRequest struct {
	msg     *ops.Msg
	resChan chan uint64
}

// start - Lifecycle manager of publisher, accepts publish request
// acts on it & responds back
func (p *Publisher) start(ctx context.Context, running chan struct{}) {
	close(running)
	defer func() {
		if err := p.conn.Close(); err != nil {
			log.Printf("[pub0sub] Error : %s\n", err.Error())
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case req := <-p.msgChan:
			receiverC, err := p.send(req.msg)
			if err != nil {
				// something wrong detected, connection to be
				// teared down
				if errors.Is(err, ops.ErrTerminateConnection) {
					req.resChan <- 0
					return
				}

				if nErr, ok := err.(net.Error); ok && !nErr.Temporary() {
					req.resChan <- 0
					return
				}
			}

			req.resChan <- receiverC

		}
	}
}

// send - Writes publish intent message to stream & reads response back
func (p *Publisher) send(msg *ops.Msg) (uint64, error) {

	// first write opcode
	op := ops.PUB_REQ
	if _, err := op.WriteTo(p.conn); err != nil {
		return 0, err
	}

	// then write actual message
	if _, err := msg.WriteTo(p.conn); err != nil {
		return 0, err
	}

	// read peer's opcode i.e. determine message intent
	rOp := new(ops.OP)
	if _, err := rOp.ReadFrom(p.conn); err != nil {
		return 0, err
	}

	// check whether supported or not
	if *rOp != ops.PUB_RESP {
		return 0, ops.ErrTerminateConnection
	}

	// attempt to read response
	pResp := new(ops.CountResponse)
	if _, err := pResp.ReadFrom(p.conn); err != nil {
		return 0, err
	}

	return uint64(*pResp), nil

}

// New - New publisher instance, attempts to establish connection with remote
// & returns handle with open connection, ready for use
func New(ctx context.Context, proto, addr string) (*Publisher, error) {
	d := net.Dialer{
		Timeout:  time.Duration(10) * time.Second,
		Deadline: time.Now().Add(time.Duration(20) * time.Second),
	}

	conn, err := d.DialContext(ctx, proto, addr)
	if err != nil {
		return nil, err
	}

	pub := &Publisher{conn: conn, msgChan: make(chan *msgPublishRequest, 1)}
	running := make(chan struct{})
	go pub.start(ctx, running)
	<-running

	return pub, nil
}

// Publish - Sends message publish request over network,
// returns back how many of subscribers received this message
func (p *Publisher) Publish(msg *ops.Msg) uint64 {
	resChan := make(chan uint64, 1)
	p.msgChan <- &msgPublishRequest{msg: msg, resChan: resChan}

	return <-resChan
}
