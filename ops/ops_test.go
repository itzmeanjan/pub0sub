package ops

import (
	"io"
	"testing"
)

func TestOP(t *testing.T) {
	op := PUB_REQ
	r, w := io.Pipe()

	go func() {
		if _, err := op.WriteTo(w); err != nil {
			t.Errorf("Failed to write : %s\n", err.Error())
		}
	}()

	rOp := new(OP)
	if _, err := rOp.ReadFrom(r); err != nil {
		t.Errorf("Failed to read : %s\n", err.Error())
	}

	if op != *rOp {
		t.Errorf("Wrote : %d, Read %d\n", op, *rOp)
	}
}
