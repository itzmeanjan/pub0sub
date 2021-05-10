package hub

import (
	"encoding/binary"
	"io"

	"github.com/itzmeanjan/pubsub"
)

// NewSubRequest - Client to send new subscription request
// in this form
type NewSubRequest struct {
	Topics []pubsub.String
}

// WriteTo - Writes subscription request i.e. list of topics
// to stream
func (s *NewSubRequest) WriteTo(w io.Writer) (int64, error) {
	var n int64

	if err := binary.Write(w, binary.BigEndian, uint16(len(s.Topics))); err != nil {
		return 0, err
	}

	n += 2

	for i := 0; i < len(s.Topics); i++ {
		_n, err := s.Topics[i].WriteTo(w)
		if err != nil {
			return n, err
		}

		n += _n
	}

	return n, nil
}

// ReadFrom - Reads subscription request from stream
func (s *NewSubRequest) ReadFrom(r io.Reader) (int64, error) {
	var n int64

	var size uint16
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return n, err
	}

	n += 2
	buf := make([]pubsub.String, 0, size)

	for i := 0; i < int(size); i++ {
		topic := new(pubsub.String)
		_n, err := topic.ReadFrom(r)
		if err != nil {
			return n, err
		}

		n += _n
		buf = append(buf, *topic)
	}

	s.Topics = buf

	return n, nil
}

// AddSubRequest - Add subscriptions to more topics on-the-fly
type AddSubRequest struct {
	Id     uint64
	Topics []pubsub.String
}

func (a *AddSubRequest) WriteTo(w io.Writer) (int64, error) {
	var n int64

	if err := binary.Write(w, binary.BigEndian, a.Id); err != nil {
		return n, err
	}

	n += 8

	if err := binary.Write(w, binary.BigEndian, uint16(len(a.Topics))); err != nil {
		return 0, err
	}

	n += 2

	for i := 0; i < len(a.Topics); i++ {
		_n, err := a.Topics[i].WriteTo(w)
		if err != nil {
			return n, err
		}

		n += _n
	}

	return n, nil
}
