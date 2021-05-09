package hub

import (
	"net"

	"github.com/itzmeanjan/pubsub"
)

// By inspecting opcode when it's determined to be one publish request
// it's handled here
//
// First message is read from stream, then acted on, finally response
// sent back to client over network
func handlePubReq(conn net.Conn, hub *pubsub.PubSub) error {

	msg := new(pubsub.Message)
	_, err := msg.ReadFrom(conn)
	if err != nil {
		return err
	}

	ok, n := hub.Publish(msg)
	resp := PubResponse{Status: ok, ReceiverC: n}
	if _, err := resp.WriteTo(conn); err != nil {
		return err
	}

	return nil

}
