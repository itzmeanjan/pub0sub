package ops

import (
	"encoding/binary"
	"io"

	"github.com/itzmeanjan/pubsub"
)

// PushedMessage - Manager to push message to subscriber in this form
// as soon as it's ready to push it, after getting notified by HUB
type PushedMessage pubsub.PublishedMessage

// WriteTo - Writes to message binary to byte stream
func (p *PushedMessage) WriteTo(w io.Writer) (int64, error) {
	var size int64

	lTopic := len(p.Topic)
	if err := binary.Write(w, binary.BigEndian, uint32(lTopic)); err != nil {
		return size, err
	}

	size += 4

	if n, err := w.Write([]byte(p.Topic)); n != lTopic {
		return size, err
	}

	size += int64(lTopic)

	lMsg := len(p.Data)
	if err := binary.Write(w, binary.BigEndian, uint32(lMsg)); err != nil {
		return size, err
	}

	size += 4

	if n, err := w.Write(p.Data); n != lMsg {
		return size, err
	}

	size += int64(lMsg)

	return size, nil
}

// ReadFrom - Subscriber reads received pushed message from byte stream
// into structured data
func (p *PushedMessage) ReadFrom(r io.Reader) (int64, error) {
	var size int64

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

	p.Topic = string(topic)
	p.Data = data

	return size, nil
}
