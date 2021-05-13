package ops

import (
	"encoding/binary"
	"io"
)

// NewSubscriptionRequest - Subscriber to send new topic subscription request over stream
type NewSubscriptionRequest struct {
	Topics []string
}

// WriteTo - Writes to subscription request to stream in recoverable form
func (n *NewSubscriptionRequest) WriteTo(w io.Writer) (int64, error) {
	var size int64

	lTopics := len(n.Topics)
	if err := binary.Write(w, binary.BigEndian, uint32(lTopics)); err != nil {
		return size, err
	}

	size += 4

	for i := 0; i < lTopics; i++ {
		lTopic := len(n.Topics[i])
		if err := binary.Write(w, binary.BigEndian, uint32(lTopic)); err != nil {
			return size, err
		}

		size += 4

		if n, err := w.Write([]byte(n.Topics[i])); n != lTopic {
			return size, err
		}

		size += int64(lTopic)
	}

	return size, nil
}

// ReadFrom - Read subscription request back from stream & constructs object
func (n *NewSubscriptionRequest) ReadFrom(r io.Reader) (int64, error) {
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

	n.Topics = topics
	return size, nil
}
