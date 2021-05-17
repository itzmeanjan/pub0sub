package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/itzmeanjan/pub0sub/subscriber"
)

func main() {
	var (
		proto    = "tcp"
		addr     = flag.String("addr", "127.0.0.1", "Connect to address")
		port     = flag.Uint64("port", 13000, "Connect to port")
		capacity = flag.Uint64("capacity", 1024, "Pending message queue capacity")
		topic    = flag.String("topic", "topic_1", "Topic to subscribe")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	fullAddr := fmt.Sprintf("%s:%d", *addr, *port)
	sub, err := subscriber.New(ctx, proto, fullAddr, *capacity, *topic)
	if err != nil {
		log.Printf("[0sub] Error : %s\n", err.Error())
		return
	}

	log.Printf("[0sub] Connected to %s\n", fullAddr)
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Printf("[0sub] Stopping listener\n")
				return

			case <-sub.Watch():
				if msg := sub.Next(); msg != nil {
					log.Printf("[0sub] Received |>| Data : `%s`, Topic : `%s`\n", msg.Data, msg.Topic)
				}
			}
		}
	}()

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, syscall.SIGTERM, syscall.SIGINT)

	<-interruptChan
	cancel()
	<-time.After(time.Second)

	log.Printf("[0sub] Graceful shutdown\n")
}
