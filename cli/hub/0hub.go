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

	"github.com/itzmeanjan/pub0sub/hub"
)

func main() {
	var (
		addr     = flag.String("addr", getAddr(), "Address to listen on")
		port     = flag.Uint64("port", getPort(), "Port to listen on")
		capacity = flag.Uint64("capacity", getCapacity(), "Pending message queue capacity")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	fullAddr := fmt.Sprintf("%s:%d", *addr, *port)
	h, err := hub.New(ctx, fullAddr, *capacity)
	if err != nil {
		log.Printf("[0hub] Error : %s\n", err.Error())
		return
	}
	log.Printf("[0hub] Listening on %s\n", h.Addr())

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, syscall.SIGTERM, syscall.SIGINT)

LOOP:
	for {
		select {
		case <-interruptChan:
			cancel()

			<-time.After(time.Second)
			break LOOP

		case addr := <-h.Connected:
			log.Printf("[0hub] Connected %s\n", addr)

		case addr := <-h.Disconnected:
			log.Printf("[0hub] Disconnected %s\n", addr)

		}
	}

	log.Printf("[0hub] Graceful shutdown\n")
}
