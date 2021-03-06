package ops

import (
	"encoding/binary"
	"io"
)

// CountResponse - Lets publisher/ subscriber know of some sort of count
// which has context specific meaning like `how many topics were successfully
// subscribed to ?` or `how many subscribers received message ?`
type CountResponse uint32

// WriteTo - Write to stream
func (p CountResponse) WriteTo(w io.Writer) (int64, error) {
	return 4, binary.Write(w, binary.BigEndian, p)
}

// ReadFrom - Recover content back from stream
func (p *CountResponse) ReadFrom(r io.Reader) (int64, error) {
	var v uint32

	if err := binary.Read(r, binary.BigEndian, &v); err != nil {
		return 0, err
	}

	*p = CountResponse(v)
	return 4, nil
}

// NewSubResponse - After sending subscriber registration request
// along with topic list of interest, it expects to hear back with
// `what's its subcriber id ?` & `how many topics it successfully got subscribed to ?`
type NewSubResponse struct {
	Id         uint64
	TopicCount uint32
}

// WriteTo - Writes to stream
func (n *NewSubResponse) WriteTo(w io.Writer) (int64, error) {
	var size int64

	if err := binary.Write(w, binary.BigEndian, n.Id); err != nil {
		return size, err
	}

	size += 8
	if err := binary.Write(w, binary.BigEndian, n.TopicCount); err != nil {
		return size, err
	}

	size += 4
	return size, nil
}

// ReadFrom - Reads back from stream into structured form
func (n *NewSubResponse) ReadFrom(r io.Reader) (int64, error) {
	var size int64
	var id uint64

	if err := binary.Read(r, binary.BigEndian, &id); err != nil {
		return size, err
	}

	size += 8
	var count uint32

	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return 0, err
	}

	size += 4
	n.Id = id
	n.TopicCount = count
	return size, nil
}
