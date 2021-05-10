package hub

import (
	"encoding/binary"
	"io"

	"github.com/itzmeanjan/pubsub"
)

type NewSubRequest struct {
	Topics []pubsub.String
}

func (r *NewSubRequest) WriteTo(w io.Writer) (int64, error) {
	var n int64

	if err := binary.Write(w, binary.BigEndian, uint16(len(r.Topics))); err != nil {
		return 0, err
	}

	n += 2

	for i := 0; i < len(r.Topics); i++ {
		_n, err := r.Topics[i].WriteTo(w)
		if err != nil {
			return n, err
		}

		n += _n
	}

	return n, nil
}

type AddSubRequest struct{}
