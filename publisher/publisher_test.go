package publisher

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/itzmeanjan/pub0sub/hub"
	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/itzmeanjan/pub0sub/subscriber"
)

func TestPublisher(t *testing.T) {
	addr := "127.0.0.1:13000"
	proto := "tcp"
	capacity := uint64(256)
	topic_1 := "topic_1"
	topic_2 := "topic_2"
	topics := []string{topic_1, topic_2}
	msg := ops.Msg{Topics: topics}
	count := uint64(16)
	pubs := make([]*Publisher, 0, count)
	delay := time.Duration(5) * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	_, err := hub.New(ctx, addr, capacity)
	if err != nil {
		t.Fatalf("Failed to start Hub : %s\n", err.Error())
	}

	var i uint64
	for ; i < count; i++ {
		pub, err := New(ctx, proto, addr)
		if err != nil {
			t.Fatalf("Failed to start publisher %d : %s\n", i+1, err.Error())
		}

		pubs = append(pubs, pub)
	}

	sub, err := subscriber.New(ctx, proto, addr, capacity, topics...)
	if err != nil {
		t.Fatalf("Failed to start subscriber : %s\n", err.Error())
	}

	for i, pub := range pubs {
		msg.Data = []byte(fmt.Sprintf("%d", i+1))
		if n := pub.Publish(&msg); n != 2 {
			t.Fatalf("Expected to publish to 2 subscribers, did to %d\n", n)
		}
	}

	<-time.After(delay)

	i = 0
	for ; i < count; i++ {
		data := []byte(fmt.Sprintf("%d", i+1))

		{
			conMsg := sub.Next()
			if conMsg == nil {
				t.Fatalf("Expected to consume msg, found nothing\n")
			}
			if conMsg.Topic != topic_1 {
				t.Fatalf("Expected message from `%s`, got from `%s`\n", topic_1, conMsg.Topic)
			}
			if !bytes.Equal(conMsg.Data, data) {
				t.Fatalf("Expected message `%s`, got `%s`\n", data, conMsg.Data)
			}
		}

		{
			conMsg := sub.Next()
			if conMsg == nil {
				t.Fatalf("Expected to consume msg, found nothing\n")
			}
			if conMsg.Topic != topic_2 {
				t.Fatalf("Expected message from `%s`, got from `%s`\n", topic_2, conMsg.Topic)
			}
			if !bytes.Equal(conMsg.Data, data) {
				t.Fatalf("Expected message `%s`, got `%s`\n", data, conMsg.Data)
			}
		}
	}

	cancel()
	<-time.After(delay)
}
