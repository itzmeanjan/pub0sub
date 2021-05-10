package hub

import (
	"encoding/binary"
	"io"
)

type OP uint8

const (
	UNSUPPORTED OP = iota
	PUB_REQ
	NEW_SUB_REQ
	MSG_REQ
	ADD_SUB_REQ
	UNSUB_REQ
)

// Reads operation type ( client intension ) from readable stream
func getOPType(r io.Reader) (OP, error) {
	var op uint8
	if err := binary.Read(r, binary.BigEndian, &op); err != nil {
		return UNSUPPORTED, err
	}

	return OP(op), nil
}
