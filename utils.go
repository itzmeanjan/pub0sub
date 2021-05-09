package pub0sub

import (
	"encoding/binary"
	"io"
)

type PubResponse struct {
	Status    bool
	ReceiverC uint64
}

func (p *PubResponse) WriteTo(w io.Writer) (int64, error) {
	var n int64

	if err := binary.Write(w, binary.BigEndian, p.Status); err != nil {
		return n, err
	}

	n += 1
	if err := binary.Write(w, binary.BigEndian, p.ReceiverC); err != nil {
		return 1, err
	}

	n += 8
	return n, nil
}
