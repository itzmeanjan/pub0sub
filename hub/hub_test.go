package hub

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/itzmeanjan/pub0sub/publisher"
	"github.com/itzmeanjan/pub0sub/subscriber"
)

func TestHub(t *testing.T) {
	addr := "127.0.0.1:0"
	proto := "tcp"
	capacity := uint64(256)
	topic_1 := "topic_1"
	topics := []string{topic_1}
	data := []byte("hello")
	msg := ops.Msg{Topics: topics, Data: data}
	delay := time.Duration(5) * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	hub, err := New(ctx, addr, capacity)
	if err != nil {
		t.Fatalf("Failed to start Hub : %s\n", err.Error())
	}

	pub, err := publisher.New(ctx, proto, hub.Addr())
	if err != nil {
		t.Fatalf("Failed to start publisher : %s\n", err.Error())
	}

	sub, err := subscriber.New(ctx, proto, hub.Addr(), capacity, topics...)
	if err != nil {
		t.Fatalf("Failed to start subscriber : %s\n", err.Error())
	}

	n, err := pub.Publish(&msg)
	if err != nil {
		t.Fatalf("Failed to publish : %s\n", err.Error())
	}
	if n != 1 {
		t.Fatalf("Expected to publish to 1 subscriber, did to %d\n", n)
	}

	<-sub.Watch()
	conMsg := sub.Next()
	if conMsg == nil {
		t.Fatalf("Expected to consume msg, found nothing\n")
	}
	if conMsg.Topic != topic_1 {
		t.Errorf("Expected message from `%s`, got from `%s`\n", topic_1, conMsg.Topic)
	}
	if !bytes.Equal(conMsg.Data, data) {
		t.Errorf("Expected message `%s`, got `%s`\n", data, conMsg.Topic)
	}

	if err := sub.Disconnect(); err != nil {
		t.Logf("Failed to disconnect subscriber : %s\n", err.Error())
	}

	cancel()
	<-time.After(delay)

	if pub.Connected() {
		t.Fatalf("Expected to see publisher disconnected\n")
	}
	if sub.Connected() {
		t.Fatalf("Expected to see subscriber disconnected\n")
	}
}
