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
		addr     = flag.String("addr", "127.0.0.1", "Address to listen on")
		port     = flag.Uint64("port", 13000, "Port to listen on")
		capacity = flag.Uint64("capacity", 4096, "Pending message queue capacity")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	fullAddr := fmt.Sprintf("%s:%d", *addr, *port)
	_, err := hub.New(ctx, fullAddr, *capacity)
	if err != nil {
		log.Printf("[0hub] Error : %s\n", err.Error())
		return
	}
	log.Printf("[0hub] Listening on %s\n", fullAddr)

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, syscall.SIGTERM, syscall.SIGINT)

	<-interruptChan
	cancel()
	<-time.After(time.Second)

	log.Printf("[0hub] Graceful shutdown\n")
}