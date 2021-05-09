package hub

import (
	"encoding/binary"
	"io"
)

type OP uint8

const (
	PUB_REQ OP = iota + 1
	NEW_SUB_REQ
	MSG_REQ
	ADD_SUB_REQ
	UNSUB_REQ
)

// Reads operation type ( client intension ) from readable stream
func getOPType(r io.Reader) (OP, error) {
	var op OP
	if err := binary.Read(r, binary.BigEndian, &op); err != nil {
		return 0, err
	}

	return op, nil
}
