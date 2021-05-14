package ops

import (
	"bytes"
	"testing"
)

func TestUnsubcriptionRequest(t *testing.T) {
	id := uint64(123)
	topics := []string{"topic_1", "topic_2"}
	stream := new(bytes.Buffer)

	wUnsubReq := UnsubcriptionRequest{Id: id, Topics: topics}
	nW, err := wUnsubReq.WriteTo(stream)
	if err != nil {
		t.Errorf("Failed to write : %s\n", err.Error())
	}

	rUnsubReq := new(UnsubcriptionRequest)
	nR, err := rUnsubReq.ReadFrom(stream)
	if err != nil {
		t.Errorf("Failed to read : %s\n", err.Error())
	}

	if nW != nR {
		t.Errorf("Wrote %d bytes, Read %d bytes", nW, nR)
	}

	if wUnsubReq.Id != rUnsubReq.Id {
		t.Errorf("Wrote Id %d, Read Id %d", wUnsubReq.Id, rUnsubReq.Id)
	}

	for i := 0; i < len(wUnsubReq.Topics); i++ {
		if wUnsubReq.Topics[i] != rUnsubReq.Topics[i] {
			t.Errorf("Wrote %s, Read %s\n", wUnsubReq.Topics[i], rUnsubReq.Topics[i])
		}
	}
}
