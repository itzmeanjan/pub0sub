package ops

import (
	"bytes"
	"testing"
)

func TestAddSubscriptionRequest(t *testing.T) {
	id := uint64(123)
	topics := []string{"topic_1", "topic_2"}
	stream := new(bytes.Buffer)

	wAddSubReq := AddSubscriptionRequest{Id: id, Topics: topics}
	nW, err := wAddSubReq.WriteTo(stream)
	if err != nil {
		t.Errorf("Failed to write : %s\n", err.Error())
	}

	rAddSubReq := new(AddSubscriptionRequest)
	nR, err := rAddSubReq.ReadFrom(stream)
	if err != nil {
		t.Errorf("Failed to read : %s\n", err.Error())
	}

	if nW != nR {
		t.Errorf("Wrote %d bytes, Read %d bytes", nW, nR)
	}

	if wAddSubReq.Id != rAddSubReq.Id {
		t.Errorf("Wrote Id %d, Read Id %d", wAddSubReq.Id, rAddSubReq.Id)
	}

	for i := 0; i < len(wAddSubReq.Topics); i++ {
		if wAddSubReq.Topics[i] != rAddSubReq.Topics[i] {
			t.Errorf("Wrote %s, Read %s\n", wAddSubReq.Topics[i], rAddSubReq.Topics[i])
		}
	}
}
