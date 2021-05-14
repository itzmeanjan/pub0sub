package ops

import (
	"bytes"
	"testing"
)

func TestPubResponse(t *testing.T) {
	wResp := PubResponse(10)
	stream := new(bytes.Buffer)

	nWrote, err := wResp.WriteTo(stream)
	if err != nil {
		t.Errorf("Failed to write : %s\n", err.Error())
	}

	rResp := new(PubResponse)
	nRead, err := rResp.ReadFrom(stream)
	if err != nil {
		t.Errorf("Failed to read : %s\n", err.Error())
	}

	if nWrote != nRead {
		t.Errorf("Wrote %d bytes, Read %d bytes\n", nWrote, nRead)
	}

	if wResp != *rResp {
		t.Errorf("Wrote %d, Read %d\n", wResp, *rResp)
	}
}

func TestNewSubResponse(t *testing.T) {
	wResp := NewSubResponse{Id: 123, TopicCount: 5}
	stream := new(bytes.Buffer)

	nWrote, err := wResp.WriteTo(stream)
	if err != nil {
		t.Errorf("Failed to write : %s\n", err.Error())
	}

	rResp := new(NewSubResponse)
	nRead, err := rResp.ReadFrom(stream)
	if err != nil {
		t.Errorf("Failed to read : %s\n", err.Error())
	}

	if nWrote != nRead {
		t.Errorf("Wrote %d bytes, Read %d bytes\n", nWrote, nRead)
	}

	if wResp.Id != rResp.Id {
		t.Errorf("Wrote Id %d, Read Id %d\n", wResp.Id, rResp.Id)
	}

	if wResp.TopicCount != rResp.TopicCount {
		t.Errorf("Wrote topic count %d, Read topic count %d\n", wResp.TopicCount, rResp.TopicCount)
	}
}
