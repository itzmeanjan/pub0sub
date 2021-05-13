package ops

import (
	"bytes"
	"testing"
)

func TestMessage(t *testing.T) {
	var (
		TOPIC_1 = "topic_1"
		TOPIC_2 = "topic_2"
		DATA    = []byte("hello")
	)

	stream := new(bytes.Buffer)
	msg := Msg{Topics: []string{TOPIC_1, TOPIC_2}, Data: DATA}

	wrote, err := msg.WriteTo(stream)
	if err != nil {
		t.Errorf("Failed to write : %s\n", err.Error())
	}

	nMsg := new(Msg)
	read, err := nMsg.ReadFrom(stream)
	if err != nil {
		t.Errorf("Failed to read : %s\n", err.Error())
	}

	if wrote != read {
		t.Errorf("Wrote %d, Read %d\n", wrote, read)
	}

	if msg.Topics[0] != nMsg.Topics[0] {
		t.Errorf("Wrote %s, Read %s\n", msg.Topics[0], nMsg.Topics[0])
	}

	if msg.Topics[1] != nMsg.Topics[1] {
		t.Errorf("Wrote %s, Read %s\n", msg.Topics[1], nMsg.Topics[1])
	}

	if !bytes.Equal(msg.Data, nMsg.Data) {
		t.Errorf("Wrote %s, Read %s\n", msg.Data, nMsg.Data)
	}

}
