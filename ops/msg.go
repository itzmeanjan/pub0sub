package ops

import (
	"encoding/binary"
	"io"

	"github.com/itzmeanjan/pubsub"
)

type Msg pubsub.Message

func (m *Msg) WriteTo(w io.Writer) (int64, error) {
	var size int64

	lTopics := len(m.Topics)
	if err := binary.Write(w, binary.BigEndian, uint32(lTopics)); err != nil {
		return size, err
	}

	size += 4

	for i := 0; i < len(m.Topics); i++ {
		lTopic := len(m.Topics[i])
		if err := binary.Write(w, binary.BigEndian, uint32(lTopic)); err != nil {
			return size, err
		}

		size += 4

		if n, err := w.Write([]byte(m.Topics[i])); n != lTopic {
			return size, err
		}

		size += int64(lTopic)
	}

	lMsg := len(m.Data)
	if err := binary.Write(w, binary.BigEndian, uint32(lMsg)); err != nil {
		return size, err
	}

	size += 4

	if n, err := w.Write(m.Data); n != lMsg {
		return size, err
	}

	size += int64(lMsg)

	return size, nil
}
