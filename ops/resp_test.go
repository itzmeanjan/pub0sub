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
