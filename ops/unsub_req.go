package ops

import (
	"bytes"
	"encoding/binary"
	"io"
)

// UnsubcriptionRequest - Client to send topic unsubscription request
// in this form
type UnsubcriptionRequest struct {
	Id     uint64
	Topics []string
}

// Total size of message to be written into stream
// as message envelope, so that Hub can figure out
// how large buffer should it allocate for reading
// whole message body
func (u *UnsubcriptionRequest) size() uint32 {
	var size uint32
	size += 9 // ( --- <8B> + <1B> --- )

	for i := 0; i < len(u.Topics); i++ {
		size += (1 + uint32(len(u.Topics[i])))
	}

	return size
}

// WriteEnvelope - Subscriber invokes for writing message envelope
// so that Hub can understand `how to handle message ?`
//
// It should write 5-bytes into stream, in ideal condition
func (u *UnsubcriptionRequest) WriteEnvelope(w io.Writer) (int64, error) {
	buf := new(bytes.Buffer)
	var size int64

	opCode := UNSUB_REQ
	if _, err := opCode.WriteTo(buf); err != nil {
		return size, err
	}

	size += 1

	if err := binary.Write(buf, binary.BigEndian, u.size()); err != nil {
		return size, err
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		return size, err
	}

	return size + 4, nil
}

// WriteTo - Subscriber to write topic unsubcription request to stream
func (u *UnsubcriptionRequest) WriteTo(w io.Writer) (int64, error) {
	buf := new(bytes.Buffer)
	var size int64

	if err := binary.Write(buf, binary.BigEndian, u.Id); err != nil {
		return size, err
	}

	size += 8

	lTopics := len(u.Topics)
	if err := binary.Write(buf, binary.BigEndian, uint8(lTopics)); err != nil {
		return size, err
	}

	size += 1

	for i := 0; i < lTopics; i++ {
		lTopic := len(u.Topics[i])
		if err := binary.Write(buf, binary.BigEndian, uint8(lTopic)); err != nil {
			return size, err
		}

		size += 1

		if n, err := buf.Write([]byte(u.Topics[i])); n != lTopic {
			return size, err
		}

		size += int64(lTopic)
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		return size, err
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

	u.Id = id
	u.Topics = topics
	return size, nil
}
