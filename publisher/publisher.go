package publisher

import (
	"context"
	"net"

	"github.com/itzmeanjan/pubsub"
)

// Publisher - Abstraction layer at which publisher operates
type Publisher struct {
	Conn net.Conn
}

// New - New publisher instance, attempts to establish connection with remote
// & returns handle with open connection
func New(ctx context.Context, proto, addr string) (*Publisher, error) {
	var d net.Dialer

	conn, err := d.DialContext(ctx, proto, addr)
	if err != nil {
		return nil, err
	}

	return &Publisher{Conn: conn}, nil
}

// Publish - Sends message publish request over network,
// returns back how many of subscribers received this message
func (p *Publisher) Publish(msg *pubsub.Message) uint64 {
	return 0
}

// Close - Publisher is supposed to invoke this when
// it's done with its publishing interest
func (p *Publisher) Close() error {
	return p.Conn.Close()
}
