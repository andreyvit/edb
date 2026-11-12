package msgpack

import "io"

func ensureCapacity(buf []byte, minCap int) []byte {
	c := cap(buf)
	if minCap > c {
		if c < 16 {
			c = 16
		}
		for minCap > c {
			c <<= 1
		}
		old := buf
		buf = make([]byte, len(old), c)
		copy(buf, old)
	}
	return buf
}

func grow(buf []byte, n int) (int, []byte) {
	off := len(buf)
	newLen := off + n
	buf = ensureCapacity(buf, newLen)
	return off, buf[:newLen]
}

func appendRaw(buf []byte, chunk []byte) []byte {
	n := len(chunk)
	off, buf := grow(buf, n)
	copy(buf[off:], chunk)
	return buf
}

type bytesBuilder struct {
	Buf []byte
}

var _ io.Writer = (*bytesBuilder)(nil)

func (bb *bytesBuilder) Grow(n int) (off int) {
	off, bb.Buf = grow(bb.Buf, n)
	return
}

func (bb *bytesBuilder) Write(b []byte) (int, error) {
	bb.Buf = appendRaw(bb.Buf, b)
	return len(b), nil
}

func (bb *bytesBuilder) WriteByte(v byte) error {
	off := bb.Grow(1)
	bb.Buf[off] = v
	return nil
}
