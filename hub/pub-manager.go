package hub

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

	defer func() {
		if err := lis.Close(); err != nil {
			log.Printf("[pub0sub] Error : %s\n", err.Error())
		}
	}()

	done <- true

	for {
		select {
		case <-ctx.Done():
			return

		default:
			conn, err := lis.Accept()
			if err != nil {
				log.Printf("[pub0sub] Error : %s\n", err.Error())
				break
			}

			go handlePublisher(ctx, conn, hub)

		}
	}
}

// handlePublisher - Each publisher connection is handled in its own go routine
func handlePublisher(ctx context.Context, conn net.Conn, hub *pubsub.PubSub) {

	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("[pub0sub] Error : %s\n", err.Error())
		}
	}()

STOP:
	for {
		select {
		case <-ctx.Done():
			return

		default:
			msg := new(pubsub.Message)
			_, err := msg.ReadFrom(conn)
			if err != nil {
				if nErr, ok := err.(net.Error); ok && nErr.Temporary() {
					log.Printf("[pub0sub] Error : %s\n", nErr.Error())
					break
				}

				log.Printf("[pub0sub] Error : %s\n", err.Error())
				break STOP
			}

			ok, n := hub.Publish(msg)
			resp := PubResponse{Status: ok, ReceiverC: n}
			if _, err := resp.WriteTo(conn); err != nil {

				if nErr, ok := err.(net.Error); ok && nErr.Temporary() {
					log.Printf("[pub0sub] Error : %s\n", nErr.Error())
					break
				}

				log.Printf("[pub0sub] Error : %s\n", err.Error())
				break STOP
			}

		}
	}

}
