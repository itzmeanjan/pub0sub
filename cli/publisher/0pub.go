package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/itzmeanjan/pub0sub/publisher"
)

func main() {
	var (
		proto = "tcp"
		addr  = flag.String("addr", "127.0.0.1", "Connect to address")
		port  = flag.Uint64("port", 13000, "Connect to port")
		topic = flag.String("topic", "topic_1", "Topic to publish data on")
		data  = flag.String("data", "hello", "Data to publish")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	fullAddr := fmt.Sprintf("%s:%d", *addr, *port)
	pub, err := publisher.New(ctx, proto, fullAddr)
	if err != nil {
		log.Printf("[0pub] Error : %s\n", err.Error())
		return
	}

	log.Printf("[0pub] Connected to %s\n", fullAddr)
	msg := ops.Msg{Topics: []string{*topic}, Data: []byte(*data)}
	log.Printf("[0pub] Approximate message reach %d\n", pub.Publish(&msg))

	cancel()
	<-time.After(time.Second)

	log.Printf("[0pub] Graceful shutdown\n")
}
