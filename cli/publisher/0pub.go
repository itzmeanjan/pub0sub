package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/itzmeanjan/pub0sub/publisher"
)

type topicList []string

func (t *topicList) String() string {
	if t == nil {
		return ""
	}

	return strings.Join(*t, "")
}

func (t *topicList) Set(val string) error {
	*t = append(*t, val)
	return nil
}

func main() {
	var (
		proto  = "tcp"
		addr   = flag.String("addr", "127.0.0.1", "Connect to address")
		port   = flag.Uint64("port", 13000, "Connect to port")
		data   = flag.String("data", "hello", "Data to publish")
		repeat = flag.Uint64("repeat", 1, "Repeat publish ( = 0 :-> infinite )")
		delay  = flag.Duration("delay", time.Duration(100)*time.Millisecond, "gap between two message publish")
		topics topicList
	)
	flag.Var(&topics, "topic", "Topic to publish data on")
	flag.Parse()

	if len(topics) == 0 {
		log.Printf("[0pub] Error : no topics specified\n")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	fullAddr := fmt.Sprintf("%s:%d", *addr, *port)
	pub, err := publisher.New(ctx, proto, fullAddr)
	if err != nil {
		log.Printf("[0pub] Error : %s\n", err.Error())
		return
	}

	log.Printf("[0pub] Connected to %s\n", fullAddr)
	msg := ops.Msg{Topics: topics, Data: []byte(*data)}
	if *repeat == 0 {
		interruptChan := make(chan os.Signal, 1)
		signal.Notify(interruptChan, syscall.SIGTERM, syscall.SIGINT)

		var i uint64
	OUT:
		for ; ; i++ {
			select {
			case <-interruptChan:
				break OUT

			default:
				<-time.After(*delay)

				n, err := pub.Publish(&msg)
				if err != nil {
					log.Printf("[0pub] Error : %s\n", err.Error())
					break OUT
				}
				log.Printf("[0pub] (%d/ ∞) Approximate message reach %d\n", i+1, n)

			}
		}
	} else {
		var i uint64
		for ; i < *repeat; i++ {

			n, err := pub.Publish(&msg)
			if err != nil {
				log.Printf("[0pub] Error : %s\n", err.Error())
				break
			}
			log.Printf("[0pub] (%d/ %d) Approximate message reach %d\n", i+1, *repeat, n)

		}
	}

	cancel()
	<-time.After(time.Second)

	log.Printf("[0pub] Graceful shutdown\n")
}
