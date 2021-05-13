package publisher

import (
	"context"
	"encoding/binary"
	"log"
	"net"
	"time"

	"github.com/itzmeanjan/pubsub"
)

// Publisher - Abstraction layer at which publisher operates
type Publisher struct {
	conn    net.Conn
	msgChan chan *msgPublishRequest
}

// msgPublishRequest - Message publish request to be received
// in this form
type msgPublishRequest struct {
	msg     *pubsub.Message
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
func (p *Publisher) send(msg *pubsub.Message) (uint64, error) {

	if err := binary.Write(p.conn, binary.BigEndian, uint32(len(msg.Topics))); err != nil {
		return 0, err
	}

	for i := 0; i < len(msg.Topics); i++ {
		if err := binary.Write(p.conn, binary.BigEndian, uint32(len(msg.Topics[i]))); err != nil {
			return 0, err
		}

		if n, err := p.conn.Write([]byte(msg.Topics[i])); n != len(msg.Topics[i]) {
			return 0, err
		}
	}

	if err := binary.Write(p.conn, binary.BigEndian, uint32(len(msg.Data))); err != nil {
		return 0, err
	}

	if n, err := p.conn.Write(msg.Data); n != len(msg.Data) {
		return 0, err
	}

	// reading to be implemented
	return 0, nil

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
func (p *Publisher) Publish(msg *pubsub.Message) uint64 {
	resChan := make(chan uint64, 1)
	p.msgChan <- &msgPublishRequest{msg: msg, resChan: resChan}

	return <-resChan
}
