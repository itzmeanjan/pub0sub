package ops

import (
	"encoding/binary"
	"io"
)

// PubResponse - After sending message publish request to broker
// publisher expects to hear back with `how many subscribers to receive message ?`
type PubResponse uint64

// WriteTo - Manager writes response received from hub, into stream
func (p PubResponse) WriteTo(w io.Writer) (int64, error) {
	return 4, binary.Write(w, binary.BigEndian, uint32(p))
}

// ReadFrom - Publisher reads response from stream
func (p *PubResponse) ReadFrom(r io.Reader) (int64, error) {
	var v uint32

	if err := binary.Read(r, binary.BigEndian, &v); err != nil {
		return 0, err
	}
	return 4, nil
}
