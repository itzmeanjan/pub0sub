package pub0sub

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/itzmeanjan/pub0sub/hub"
	"github.com/itzmeanjan/pub0sub/ops"
	"github.com/itzmeanjan/pub0sub/publisher"
	"github.com/itzmeanjan/pub0sub/subscriber"
)

func prepareData(w io.Writer, num uint64) error {
	return binary.Write(w, binary.BigEndian, num)
}

func recoverData(r io.Reader) (uint64, error) {
	var num uint64
	if err := binary.Read(r, binary.BigEndian, &num); err != nil {
		return 0, err
	}

	return num, nil
}

func TestDeliveryOrderliness(t *testing.T) {
	addr := "127.0.0.1:0"
	proto := "tcp"
	capacity := uint64(256)
	topic_1 := "topic_1"
	topics := []string{topic_1}
	end := uint64(100_000)
	delay := 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	h, err := hub.New(ctx, addr, capacity)
	if err != nil {
		t.Fatalf("Failed to start Hub : %s\n", err.Error())
	}

	pub, err := publisher.New(ctx, proto, h.Addr())
	if err != nil {
		t.Fatalf("Failed to start publisher : %s\n", err.Error())
	}

	sub, err := subscriber.New(ctx, proto, h.Addr(), capacity, topics...)
	if err != nil {
		t.Fatalf("Failed to start subscriber %s\n", err.Error())
	}

	pubDone := make(chan struct{})
	go func() {
		var current uint64 = 1
		buf := new(bytes.Buffer)
		msg := ops.Msg{Topics: topics}

		for ; current <= end; current++ {
			if err := prepareData(buf, current); err != nil {
				t.Errorf("Failed to prepare data : %s\n", err.Error())
			}

			msg.Data = buf.Bytes()
			if _, err := pub.Publish(&msg); err != nil {
				t.Errorf("Failed to publish : %s\n", err.Error())
			}
			buf.Reset()
		}

		close(pubDone)
	}()

	subDone := make(chan struct{})
	go func() {
		var current uint64 = 1

	OUT:
		for {
			select {
			case <-ctx.Done():
				break OUT

			case <-sub.Watch():
				msg := sub.Next()
				if msg == nil {
					t.Errorf("Received nil msg\n")
					break
				}

				num, err := recoverData(bytes.NewReader(msg.Data))
				if err != nil {
					t.Errorf("Failed to recover from stream : %s\n", err.Error())
				}

				if current != num {
					t.Errorf("Expected to receive %d, received : %d\n", current, num)
				}

				current++
				if num >= end {
					break OUT
				}

			}
		}

		close(subDone)
	}()

	<-pubDone
	<-subDone

	if sub.Queued() {
		t.Fatalf("Expected zero queued messages\n")
	}

	if _, err := sub.UnsubscribeAll(); err != nil {
		t.Errorf("Failed to unsubscribe : %s\n", err.Error())
	}

	if err := sub.Disconnect(); err != nil {
		t.Errorf("Failed to disconnect subscriber : %s\n", err.Error())
	}

	cancel()
	<-time.After(delay)
}
