package edb

import (
	"encoding/binary"
	"io"
	"math"
	"reflect"
)

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

func appendRawVal(buf []byte, chunk reflect.Value) []byte {
	n := chunk.Len()
	off, buf := grow(buf, n)
	reflect.Copy(reflect.ValueOf(buf[off:]), chunk)
	return buf
}

func putRaw(buf []byte, chunk []byte) int {
	copy(buf, chunk)
	return len(chunk)
}

type bytesBuilder struct {
	Buf []byte
}

var _ io.Writer = (*bytesBuilder)(nil)

func (bb *bytesBuilder) EnsureExtra(n int) {
	bb.Buf = ensureCapacity(bb.Buf, len(bb.Buf)+n)
}

func (bb *bytesBuilder) Grow(n int) (off int) {
	off, bb.Buf = grow(bb.Buf, n)
	return
}

func (bb *bytesBuilder) Trim(off int) {
	bb.Buf = bb.Buf[:off]
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

func (bb *bytesBuilder) AppendByte(v byte) {
	off := bb.Grow(1)
	bb.Buf[off] = v
}

func (bb *bytesBuilder) AppendFixedUint64(v uint64) {
	off := bb.Grow(8)
	binary.BigEndian.PutUint64(bb.Buf[off:], v)
}

func (bb *bytesBuilder) AppendUvarint(v uint64) {
	off := bb.Grow(binary.MaxVarintLen64)
	n := binary.PutUvarint(bb.Buf[off:], v)
	bb.Trim(off + n)
}

func appendUvarint(buf []byte, v uint64) []byte {
	off, buf := grow(buf, binary.MaxVarintLen64)
	off += binary.PutUvarint(buf[off:], v)
	return buf[:off]
}

func appendVarbytes(buf []byte, v []byte) []byte {
	n := len(v)
	off, buf := grow(buf, binary.MaxVarintLen64+n)
	off += binary.PutUvarint(buf[off:], uint64(n))
	copy(buf[off:], v)
	return buf[:off+n]
}

type byteBuf struct {
	Buf []byte
	Off int
}

func (b *byteBuf) Trimmed() []byte {
	return b.Buf[:b.Off]
}

func (b *byteBuf) AppendRaw(v []byte) {
	copy(b.Buf[b.Off:], v)
	b.Off += len(v)
}

func (b *byteBuf) AppendByte(v byte) {
	b.Buf[b.Off] = v
	b.Off++
}

func (b *byteBuf) AppendUvarint(v uint64) {
	b.Off += binary.PutUvarint(b.Buf[b.Off:], v)
}
func (b *byteBuf) AppendUvarinti64(v int64) {
	if v < 0 {
		panic("invalid negative value")
	}
	b.AppendUvarint(uint64(v))
}
func (b *byteBuf) AppendUvarinti(v int) {
	b.AppendUvarinti64(int64(v))
}
func (b *byteBuf) AppendVarBytes(v []byte) {
	b.AppendUvarint(uint64(len(v)))
	b.AppendRaw(v)
}

func prealloc(buf []byte, n int) byteBuf {
	off, buf := grow(buf, n)
	return byteBuf{buf, off}
}

type byteDecoder struct {
	Orig []byte
	Buf  []byte
}

func makeByteDecoder(buf []byte) byteDecoder {
	return byteDecoder{buf, buf}
}

func (d *byteDecoder) Off() int {
	return len(d.Orig) - len(d.Buf)
}

func (d *byteDecoder) Uvarint() (uint64, error) {
	v, n := binary.Uvarint(d.Buf)
	if n <= 0 {
		return 0, dataErrf(d.Orig, d.Off(), nil, "invalid uvarint")
	}
	d.Buf = d.Buf[n:]
	return v, nil
}
func (d *byteDecoder) Uvarinti() (int, error) {
	v, err := d.Uvarint()
	if v > math.MaxInt {
		return 0, dataErrf(d.Orig, d.Off(), nil, "value does not fit into int: %d", v)
	}
	return int(v), err
}

func (d *byteDecoder) Raw(n int) ([]byte, error) {
	if len(d.Buf) < n {
		return nil, dataErrf(d.Orig, d.Off(), nil, "not enough data: %d bytes remaining, %d wanted", len(d.Buf), n)
	}
	v := d.Buf[:n]
	d.Buf = d.Buf[n:]
	return v, nil
}

func (d *byteDecoder) VarBytes() ([]byte, error) {
	n, err := d.Uvarinti()
	if err != nil {
		return nil, err
	}
	return d.Raw(n)
}
