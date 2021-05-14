package ops

import (
	"encoding/binary"
	"io"
)

// AddSubscriptionRequest - After a subcriber is registered i.e. has been allocated
// one unique subscription id they can subscribe to more topics, that's what's being done here
type AddSubscriptionRequest struct {
	Id     uint64
	Topics []string
}

// WriteTo - Writes subscription request to stream in recoverable form
func (a *AddSubscriptionRequest) WriteTo(w io.Writer) (int64, error) {
	var size int64

	if err := binary.Write(w, binary.BigEndian, a.Id); err != nil {
		return size, err
	}

	size += 8

	lTopics := len(a.Topics)
	if err := binary.Write(w, binary.BigEndian, uint32(lTopics)); err != nil {
		return size, err
	}

	size += 4

	for i := 0; i < lTopics; i++ {
		lTopic := len(a.Topics[i])
		if err := binary.Write(w, binary.BigEndian, uint32(lTopic)); err != nil {
			return size, err
		}

		size += 4

		if n, err := w.Write([]byte(a.Topics[i])); n != lTopic {
			return size, err
		}

		size += int64(lTopic)
	}

	return size, nil
}

// ReadFrom - Read subscription request back from stream & constructs object
func (a *AddSubscriptionRequest) ReadFrom(r io.Reader) (int64, error) {
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

	a.Id = id
	a.Topics = topics
	return size, nil
}
