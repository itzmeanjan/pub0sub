package ops

import (
	"encoding/binary"
	"io"
)

// UnsubcriptionRequest - Client to send topic unsubscription request
// in this form
type UnsubcriptionRequest struct {
	Id     uint64
	Topics []string
}

// WriteTo - Subscriber to write topic unsubcription request to stream
func (u *UnsubcriptionRequest) WriteTo(w io.Writer) (int64, error) {
	var size int64

	if err := binary.Write(w, binary.BigEndian, u.Id); err != nil {
		return size, err
	}

	size += 8

	lTopics := len(u.Topics)
	if err := binary.Write(w, binary.BigEndian, uint32(lTopics)); err != nil {
		return size, err
	}

	size += 4

	for i := 0; i < lTopics; i++ {
		lTopic := len(u.Topics[i])
		if err := binary.Write(w, binary.BigEndian, uint32(lTopic)); err != nil {
			return size, err
		}

		size += 4

		if n, err := w.Write([]byte(u.Topics[i])); n != lTopic {
			return size, err
		}

		size += int64(lTopic)
	}

	return size, nil
}

// ReadFrom - Read unsubscription request back from stream & constructs object
func (u *UnsubcriptionRequest) ReadFrom(r io.Reader) (int64, error) {
	var size int64

	var id uint64
	if err := binary.Read(r, binary.BigEndian, &id); err != nil {
		return size, err
	}

	size += 8
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

	u.Id = id
	u.Topics = topics
	return size, nil
}
