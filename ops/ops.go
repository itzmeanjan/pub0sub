package ops

import (
	"encoding/binary"
	"errors"
	"io"
)

type OP uint8

const (
	UNSUPPORTED OP = iota + 1
	PUB_REQ
	PUB_RESP
	NEW_SUB_REQ
	NEW_SUB_RESP
	MSG_PUSH
	ADD_SUB_REQ
	ADD_SUB_RESP
	UNSUB_REQ
	UNSUB_RESP
)

var (
	ErrTerminateConnection  = errors.New("unsupported opcode, begin connection tear down")
	ErrConnectionTerminated = errors.New("connection already teared down")
	ErrEmptyTopicSet        = errors.New("topic list is empty")
)

// WriteTo - Writes operation type to stream, so that
// receiving party can understand intention of message
// it's going to read
func (o OP) WriteTo(w io.Writer) (int64, error) {
	return 1, binary.Write(w, binary.BigEndian, o)
}

// ReadFrom - Reads operation code from stream & uses
// proper handler method for reading messsage, eventually
// acts on it
func (o *OP) ReadFrom(r io.Reader) (int64, error) {
	var v uint8
	if err := binary.Read(r, binary.BigEndian, &v); err != nil {
		return 0, err
	}

	*o = OP(v)
	return 1, nil
}
