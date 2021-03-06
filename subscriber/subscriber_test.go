package subscriber

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/itzmeanjan/pub0sub/hub"
	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/itzmeanjan/pub0sub/publisher"
)

func TestSubscriber(t *testing.T) {
	addr := "127.0.0.1:0"
	proto := "tcp"
	capacity := uint64(256)
	topic_1 := "topic_1"
	topic_2 := "topic_2"
	topic_3 := "topic_3"
	topics := []string{topic_1, topic_2}
	data := []byte("hello")
	msg := ops.Msg{Topics: topics, Data: data}
	count := uint64(16)
	subs := make([]*Subscriber, 0, count)
	delay := time.Duration(5) * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	h, err := hub.New(ctx, addr, capacity)
	if err != nil {
		t.Fatalf("Failed to start Hub : %s\n", err.Error())
	}

	pub, err := publisher.New(ctx, proto, h.Addr())
	if err != nil {
		t.Fatalf("Failed to start publisher : %s\n", err.Error())
	}

	var i uint64
	for ; i < count; i++ {
		sub, err := New(ctx, proto, h.Addr(), capacity, topics...)
		if err != nil {
			t.Fatalf("Failed to start subscriber %d : %s\n", i+1, err.Error())
		}

		subs = append(subs, sub)
	}

	n, err := pub.Publish(&msg)
	if err != nil {
		t.Fatalf("Failed to publish : %s\n", err.Error())
	}
	if n != 2*count {
		t.Fatalf("Expected to publish to %d subscribers, did to %d\n", 2*count, n)
	}

	<-time.After(delay)

	for _, sub := range subs {
		{
			conMsg := sub.Next()
			if conMsg == nil {
				t.Fatalf("Expected to consume msg, found nothing\n")
			}
			if conMsg.Topic != topic_1 {
				t.Fatalf("Expected message from `%s`, got from `%s`\n", topic_1, conMsg.Topic)
			}
			if !bytes.Equal(conMsg.Data, data) {
				t.Fatalf("Expected message `%s`, got `%s`\n", data, conMsg.Topic)
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
				t.Fatalf("Expected message `%s`, got `%s`\n", data, conMsg.Topic)
			}
		}
	}

	for _, sub := range subs {
		n, err := sub.AddSubscription(topic_3)
		if err != nil {
			t.Fatalf("On-the-fly subscription attempt failed : %s\n", err.Error())
		}
		if n != 1 {
			t.Errorf("Expected to subscribe to 1 topic, did to %d\n", n)
		}
	}

	msg.Topics = append(msg.Topics, topic_3)
	n, err = pub.Publish(&msg)
	if err != nil {
		t.Fatalf("Failed to publish : %s\n", err.Error())
	}
	if n != 3*count {
		t.Fatalf("Expected to publish to %d subscribers, did to %d\n", 3*count, n)
	}

	<-time.After(delay)

	for _, sub := range subs {
		{
			conMsg := sub.Next()
			if conMsg == nil {
				t.Fatalf("Expected to consume msg, found nothing\n")
			}
			if conMsg.Topic != topic_1 {
				t.Fatalf("Expected message from `%s`, got from `%s`\n", topic_1, conMsg.Topic)
			}
			if !bytes.Equal(conMsg.Data, data) {
				t.Fatalf("Expected message `%s`, got `%s`\n", data, conMsg.Topic)
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
				t.Fatalf("Expected message `%s`, got `%s`\n", data, conMsg.Topic)
			}
		}

		{
			conMsg := sub.Next()
			if conMsg == nil {
				t.Fatalf("Expected to consume msg, found nothing\n")
			}
			if conMsg.Topic != topic_3 {
				t.Fatalf("Expected message from `%s`, got from `%s`\n", topic_3, conMsg.Topic)
			}
			if !bytes.Equal(conMsg.Data, data) {
				t.Fatalf("Expected message `%s`, got `%s`\n", data, conMsg.Topic)
			}
		}
	}

	for _, sub := range subs {
		n, err := sub.Unsubscribe(topic_3)
		if err != nil {
			t.Fatalf("Failed to unsubscribe : %s\n", err.Error())
		}
		if n != 1 {
			t.Errorf("Expected to unsubscribe from 1 topic, did from %d\n", n)
		}
	}

	for _, sub := range subs {
		n, err := sub.UnsubscribeAll()
		if err != nil {
			t.Fatalf("Failed to unsubscribe : %s\n", err.Error())
		}
		if n != 2 {
			t.Errorf("Expected to unsubscribe from 2 topics, did from %d\n", n)
		}
	}

	n, err = pub.Publish(&msg)
	if err != nil {
		t.Fatalf("Failed to publish : %s\n", err.Error())
	}
	if n != 0 {
		t.Fatalf("Expected to publish to 0 subscribers, did to %d\n", n)
	}

	<-time.After(delay)

	for i, sub := range subs {
		if sub.Queued() {
			t.Fatalf("Expected to see empty inbox, found non-empty for [SUB%d]\n", i+1)
		}
	}

	for _, sub := range subs {

		var pingC uint64
		for range sub.Watch() {
			pingC++
			if pingC == 5 {
				break
			}
		}
	}

	for _, sub := range subs {
		if err := sub.Disconnect(); err != nil {
			t.Logf("Failed to disconnect subscriber : %s\n", err.Error())
		}
	}

	cancel()
	<-time.After(delay)

	for _, sub := range subs {
		if sub.Connected() {
			t.Fatalf("Expected to see subscriber disconnected\n")
		}
	}
}
