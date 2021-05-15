package ops

import (
	"bytes"
	"testing"
)

func TestPushedMessage(t *testing.T) {
	var (
		TOPIC_1 = "topic_1"
		DATA    = []byte("hello")
	)

	stream := new(bytes.Buffer)
	pMsg := PushedMessage{Topic: TOPIC_1, Data: DATA}

	wrote, err := pMsg.WriteTo(stream)
	if err != nil {
		t.Errorf("Failed to write : %s\n", err.Error())
	}

	nMsg := new(PushedMessage)
	read, err := nMsg.ReadFrom(stream)
	if err != nil {
		t.Errorf("Failed to read : %s\n", err.Error())
	}

	if wrote != read {
		t.Errorf("Wrote %d bytes, Read %d bytes\n", wrote, read)
	}

	if pMsg.Topic != nMsg.Topic {
		t.Errorf("Wrote %s, Read %s\n", pMsg.Topic, nMsg.Topic)
	}

	if !bytes.Equal(pMsg.Data, nMsg.Data) {
		t.Errorf("Wrote %s, Read %s\n", pMsg.Data, nMsg.Data)
	}

}
