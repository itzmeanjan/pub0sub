package ops

import (
	"bytes"
	"encoding/binary"
	"io"
)

// NewSubscriptionRequest - Subscriber to send new topic subscription request over stream
type NewSubscriptionRequest struct {
	Topics []string
}

// Total size of message to be written into
// stream, so that Hub can allocate appropriate size
// of buffer for reading full message
func (n *NewSubscriptionRequest) size() uint32 {
	var size uint32
	size += 5 // ( --- <4B> + <1B> --- )

	for i := 0; i < len(n.Topics); i++ {
		size += (1 + uint32(len(n.Topics[i])))
	}

	return size
}

// WriteEnvelope - Subscriber invokes for writing message envelope
// so that Hub can understand `how to handle message ?`
//
// It also includes size of total message except first 1-byte opcode
//
// It should write 5-bytes into stream, in ideal condition
func (n *NewSubscriptionRequest) WriteEnvelope(w io.Writer) (int64, error) {
	buf := new(bytes.Buffer)
	var size int64

	opCode := NEW_SUB_REQ
	if _, err := opCode.WriteTo(buf); err != nil {
		return size, err
	}

	size += 1

	if err := binary.Write(buf, binary.BigEndian, n.size()); err != nil {
		return size, err
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		return size, err
	}

	return size + 4, nil
}

// WriteTo - Writes subscription request to stream in recoverable form
func (n *NewSubscriptionRequest) WriteTo(w io.Writer) (int64, error) {
	buf := new(bytes.Buffer)
	var size int64

	lTopics := len(n.Topics)
	if err := binary.Write(buf, binary.BigEndian, uint8(lTopics)); err != nil {
		return size, err
	}

	size += 1

	for i := 0; i < lTopics; i++ {
		lTopic := len(n.Topics[i])
		if err := binary.Write(buf, binary.BigEndian, uint8(lTopic)); err != nil {
			return size, err
		}

		size += 1

		if n, err := buf.Write([]byte(n.Topics[i])); n != lTopic {
			return size, err
		}

		size += int64(lTopic)
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		return size, err
	}

	return size, nil
}

// ReadFrom - Read subscription request back from stream & constructs object
func (n *NewSubscriptionRequest) ReadFrom(r io.Reader) (int64, error) {
	var size int64

	var lTopics uint8
	if err := binary.Read(r, binary.BigEndian, &lTopics); err != nil {
		return size, err
	}

	size += 1
	topics := make([]string, 0, lTopics)

	for i := 0; i < int(lTopics); i++ {
		var lTopic uint8
		if err := binary.Read(r, binary.BigEndian, &lTopic); err != nil {
			return size, err
		}

		size += 1

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
