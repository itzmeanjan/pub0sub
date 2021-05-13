package ops

import (
	"bytes"
	"testing"
)

func TestNewSubscriptionRequest(t *testing.T) {
	topics := []string{"topic_1", "topic_2"}
	stream := new(bytes.Buffer)

	wNewSubReq := NewSubscriptionRequest{Topics: topics}
	nW, err := wNewSubReq.WriteTo(stream)
	if err != nil {
		t.Errorf("Failed to write : %s\n", err.Error())
	}

	rNewSubReq := new(NewSubscriptionRequest)
	nR, err := rNewSubReq.ReadFrom(stream)
	if err != nil {
		t.Errorf("Failed to read : %s\n", err.Error())
	}

	if nW != nR {
		t.Errorf("Wrote %d bytes, Read %d bytes", nW, nR)
	}

	for i := 0; i < len(wNewSubReq.Topics); i++ {
		if wNewSubReq.Topics[i] != rNewSubReq.Topics[i] {
			t.Errorf("Wrote %s, Read %s\n", wNewSubReq.Topics[i], rNewSubReq.Topics[i])
		}
	}
}
