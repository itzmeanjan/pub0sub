package pub0sub

import (
	"context"
	"log"
	"net"

	"github.com/itzmeanjan/pubsub"
)

// StartPubManager - Run publisher manager as independent go routine
// & it'll keep accepting connections & forwarding message publishing request to hub
func StartPubManager(ctx context.Context, on string, hub *pubsub.PubSub, done chan bool) {
	lis, err := net.Listen("tcp", on)
	if err != nil {
		log.Printf("[pub0sub] Error : %s\n", err.Error())

		done <- false
		return
	}

	for {
		select {
		case <-ctx.Done():
			return

		default:
			_, err := lis.Accept()
			if err != nil {
				log.Printf("[pub0sub] Error : %s\n", err.Error())
				continue
			}

		}
	}

}
