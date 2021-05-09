package hub

import (
	"context"
	"log"
	"net"

	"github.com/itzmeanjan/pubsub"
)

// StartTCPManager - ...
func StartTCPManager(ctx context.Context, addr string, hub *pubsub.PubSub, done chan bool) {
	lis, err := net.Listen("tcp", addr)
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

			go handleTCPConnection(ctx, conn, hub)
		}
	}
}

func handleTCPConnection(ctx context.Context, conn net.Conn, hub *pubsub.PubSub) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("[pub0sub] Error : %s\n", err.Error())
		}
	}()

STOP:
	for {

	CONTINUE:
		select {
		case <-ctx.Done():
			return

		default:
			op, err := getOPType(conn)
			if err != nil {
				log.Printf("[pub0sub] Error : %s\n", err.Error())
				break STOP
			}

			switch op {
			case PUB_REQ:
			case NEW_SUB_REQ:
			case MSG_REQ:
			case ADD_SUB_REQ:
			case UNSUB_REQ:
			default:
				break CONTINUE
			}
		}
	}

}