// +build stress

package pub0sub

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/itzmeanjan/pub0sub/hub"
	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/itzmeanjan/pub0sub/publisher"
	"github.com/itzmeanjan/pub0sub/subscriber"
)

func Test1k(t *testing.T) {
	parallelConnection(t, 1024)
}
func Test2k(t *testing.T) {
	parallelConnection(t, 2048)
}
func Test4k(t *testing.T) {
	parallelConnection(t, 4096)
}
func Test8k(t *testing.T) {
	parallelConnection(t, 8192)
}

func parallelConnection(t *testing.T, count uint64) {
	addr := "127.0.0.1:13000"
	proto := "tcp"
	capacity := uint64(16)
	topic_1 := "topic_1"
	topics := []string{topic_1}
	data := []byte("hello")
	msg := ops.Msg{Topics: topics, Data: data}
	delay := time.Duration(5) * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())

	if _, err := hub.New(ctx, addr, capacity); err != nil {
		t.Fatalf("Failed to start Hub : %s\n", err.Error())
	}

	sub, err := subscriber.New(ctx, proto, addr, capacity, topics...)
	if err != nil {
		t.Fatalf("Failed to start subscriber : %s\n", err.Error())
	}

	consumed := make(chan struct{})
	go func() {
		var received uint64
		for range sub.Watch() {
			msg := sub.Next()
			if msg.Topic != topic_1 {
				t.Errorf("Expected message from %s, received %s\n", topic_1, msg.Topic)
			}
			if !bytes.Equal(msg.Data, data) {
				t.Errorf("Expected message %s, received %s\n", data, msg.Topic)
			}

			received++
			if received >= count {
				close(consumed)
				break
			}
		}
	}()

	pubs := make([]*publisher.Publisher, 0, count)

	var i uint64
	for ; i < count; i++ {
		pub, err := publisher.New(ctx, proto, addr)
		if err != nil {
			t.Fatalf("Failed to start publisher : %s\n", err.Error())
		}

		pubs = append(pubs, pub)
	}

	for _, pub := range pubs {
		n, err := pub.Publish(&msg)
		if err != nil {
			t.Fatalf("Failed to publish : %s\n", err.Error())
		}
		if n != 1 {
			t.Fatalf("Expected to publish to 1 subscriber, did to %d\n", n)
		}
	}

	cancel()
	<-time.After(delay)
	<-consumed
}

func BenchmarkPublisher4Topics(b *testing.B) {
	benchmarkPublisher(b, 4)
}

func BenchmarkPublisher32Topics(b *testing.B) {
	benchmarkPublisher(b, 32)
}

func BenchmarkPublisher255Topics(b *testing.B) {
	benchmarkPublisher(b, 255)
}

func generateTopics(count uint64) ([]string, uint64) {
	topics := make([]string, count)
	var bLen uint64

	for i := 0; i < int(count); i++ {
		topic := fmt.Sprintf("topic_%d", i+1)
		topics[i] = topic
		bLen += uint64(1 + len(topic))
	}

	return topics, bLen
}

func benchmarkPublisher(b *testing.B, topicC uint64) {
	addr := "127.0.0.1:0"
	proto := "tcp"
	capacity := uint64(16)
	topics, bLen := generateTopics(topicC)
	data := []byte("hello")
	msg := ops.Msg{Topics: topics, Data: data}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		<-time.After(time.Duration(100) * time.Millisecond)
	}()

	hub, err := hub.New(ctx, addr, capacity)
	if err != nil {
		b.Fatalf("Failed to start Hub : %s\n", err.Error())
	}

	sub, err := subscriber.New(ctx, proto, hub.Addr(), capacity, topics...)
	if err != nil {
		b.Fatalf("Failed to start subscriber : %s\n", err.Error())
	}
	defer func() {
		sub.UnsubscribeAll()
		sub.Disconnect()
	}()

	pub, err := publisher.New(ctx, proto, hub.Addr())
	if err != nil {
		b.Fatalf("Failed to start publisher : %s\n", err.Error())
	}

	b.ReportAllocs()
	b.SetBytes(int64(15 + bLen))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n, err := pub.Publish(&msg)
		if err != nil {
			b.Fatalf("Failed to publish : %s\n", err.Error())
		}
		if n != topicC {
			b.Fatalf("Expected to publish to %d subscribers, did to %d\n", topicC, n)
		}
	}
}
