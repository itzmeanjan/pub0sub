package ops

import (
	"encoding/binary"
	"io"
)

// PubResponse - After sending message publish request to broker
// publisher expects to hear back with `how many subscribers to receive message ?`
type PubResponse uint32

// WriteTo - Manager writes response received from hub, into stream
func (p PubResponse) WriteTo(w io.Writer) (int64, error) {
	return 4, binary.Write(w, binary.BigEndian, p)
}

// ReadFrom - Publisher reads response from stream
func (p *PubResponse) ReadFrom(r io.Reader) (int64, error) {
	var v uint32

	if err := binary.Read(r, binary.BigEndian, &v); err != nil {
		return 0, err
	}

	*p = PubResponse(v)
	return 4, nil
}

// NewSubResponse - After sending subscriber registration request
// along with topic list of interest, it expects to hear back with
// `how many topics it successfully got subscribed to ?`
type NewSubResponse uint32

// WriteTo - Writes to stream
func (n NewSubResponse) WriteTo(w io.Writer) (int64, error) {
	return 4, binary.Write(w, binary.BigEndian, n)
}

// ReadFrom - Reads back from stream
func (n *NewSubResponse) ReadFrom(r io.Reader) (int64, error) {
	var v uint32

	if err := binary.Read(r, binary.BigEndian, &v); err != nil {
		return 0, err
	}

	*n = NewSubResponse(v)
	return 4, nil
}
