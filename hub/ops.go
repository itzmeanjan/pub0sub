package hub

import (
	"encoding/binary"
	"io"
)

type OP uint8

const (
	UNSUPPORTED OP = iota + 1
	PUB_REQ
	PUB_RESP
	NEW_SUB_REQ
	NEW_SUB_RESP
	MSG_REQ
	MSG_RESP
	ADD_SUB_REQ
	ADD_SUB_RESP
	UNSUB_REQ
	UNSUB_RESP
)

// Reads operation type ( client intension ) from readable stream
func getOPType(r io.Reader) (OP, error) {
	var op uint8
	if err := binary.Read(r, binary.BigEndian, &op); err != nil {
		return UNSUPPORTED, err
	}

	return OP(op), nil
}
