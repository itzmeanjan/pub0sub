package ops

import (
	"encoding/binary"
	"io"

	"github.com/itzmeanjan/pubsub"
)

// Publisher to send message in this form
type Msg pubsub.Message

// WriteTo - Writes message to byte stream in recoverable form
func (m *Msg) WriteTo(w io.Writer) (int64, error) {
	var size int64

	lTopics := len(m.Topics)
	if err := binary.Write(w, binary.BigEndian, uint32(lTopics)); err != nil {
		return size, err
	}

	size += 4

	for i := 0; i < lTopics; i++ {
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

// ReadFrom - Reconstructs message back from stream
func (m *Msg) ReadFrom(r io.Reader) (int64, error) {
	var size int64

	var lTopics uint32
	if err := binary.Read(r, binary.BigEndian, &lTopics); err != nil {
		return size, err
	}

	size += 4
	topics := make([]string, 0, lTopics)

	for i := 0; i < int(lTopics); i++ {
		var lTopic uint32
		if err := binary.Read(r, binary.BigEndian, &lTopic); err != nil {
			return size, err
		}

		size += 4

		topic := make([]byte, lTopic)
		if n, err := r.Read(topic); n != int(lTopic) {
			return size, err
		}

		size += int64(lTopic)

		topics = append(topics, string(topic))
	}

	var lMsg uint32
	if err := binary.Read(r, binary.BigEndian, &lMsg); err != nil {
		return size, err
	}

	size += 4
	data := make([]byte, lMsg)

	if n, err := r.Read(data); n != int(lMsg) {
		return size, err
	}

	size += int64(lMsg)

	m.Data = data
	m.Topics = topics

	return size, nil
}
