package ops

import (
	"encoding/binary"
	"io"

	"github.com/itzmeanjan/pubsub"
)

// Publisher to send message in this form
type Msg pubsub.Message

// Total size of message to be written into bytes stream
func (m *Msg) size() uint32 {
	var size uint32
	size += 5 // ( --- <4B> + <1B> --- )

	for i := 0; i < len(m.Topics); i++ {
		size += (1 + uint32(len(m.Topics[i])))
	}

	size += (4 + uint32(len(m.Data)))
	return size
}

// WriteEnvelope - Publisher invokes for writing message envelope
// so that Hub can understand `how to handle message ?`
//
// It also includes size of total message except first 1-byte opcode
//
// It should write 5-bytes into stream, in ideal condition
func (m *Msg) WriteEnvelope(w io.Writer) (int64, error) {
	var size int64

	opCode := PUB_REQ
	n, err := opCode.WriteTo(w)
	if err != nil {
		return size, err
	}

	size += n

	if err := binary.Write(w, binary.BigEndian, m.size()); err != nil {
		return size, err
	}

	return size + 4, nil
}

// WriteTo - Writes message to byte stream in recoverable form
func (m *Msg) WriteTo(w io.Writer) (int64, error) {
	var size int64

	lTopics := len(m.Topics)
	if err := binary.Write(w, binary.BigEndian, uint8(lTopics)); err != nil {
		return size, err
	}

	size += 1

	for i := 0; i < lTopics; i++ {
		lTopic := len(m.Topics[i])
		if err := binary.Write(w, binary.BigEndian, uint8(lTopic)); err != nil {
			return size, err
		}

		size += 1

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
