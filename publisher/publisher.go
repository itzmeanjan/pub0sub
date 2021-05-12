package publisher

import (
	"context"
	"log"
	"net"

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

	for {
		select {
		case <-ctx.Done():
			if err := p.conn.Close(); err != nil {
				log.Printf("[pub0sub] Error : %s\n", err.Error())
			}
			return

		case req := <-p.msgChan:
			req.resChan <- 0

		}
	}
}

// New - New publisher instance, attempts to establish connection with remote
// & returns handle with open connection, ready for use
func New(ctx context.Context, proto, addr string) (*Publisher, error) {
	var d net.Dialer

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
