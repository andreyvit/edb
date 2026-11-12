package msgpack

import (
	"encoding/binary"
	"time"

	"github.com/andreyvit/edb/values"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// Time reads the standard 32/64/96-bit integer timestamp extensions, the
// historical [seconds,nanoseconds] format, and RFC3339 text. It borrows bytes
// for wide dates instead of decoding a Go object or allocating a timestamp.
func (v View) Time() values.Value {
	if v.Nil() {
		return values.Value{}
	}
	b := v.raw
	if b[0] == 0x92 {
		it := v.Iter()
		_, s, _ := it.Next()
		_, n, _ := it.Next()
		return values.Time(time.Unix(s.Int(), n.Int()))
	}
	if msgpcode.IsString(b[0]) {
		t, err := time.Parse(time.RFC3339Nano, string(v.Bytes()))
		if err != nil {
			panic(err)
		}
		return values.Time(t)
	}
	if !msgpcode.IsExt(b[0]) {
		panic(ErrFormat)
	}
	h, n, _ := header(b)
	need(b, h+n)
	if b[h-1] != 255 && b[h-1] != 13 {
		panic(ErrFormat)
	}
	b = b[h : h+n]
	var sec, nano int64
	switch n {
	case 4:
		sec = int64(binary.BigEndian.Uint32(b))
	case 8:
		x := binary.BigEndian.Uint64(b)
		sec = int64(x & 0x3ffffffff)
		nano = int64(x >> 34)
	case 12:
		sec = int64(binary.BigEndian.Uint64(b[4:]))
		nano = int64(binary.BigEndian.Uint32(b))
		t := time.Unix(sec, nano)
		if t.IsZero() {
			return values.Value{}
		}
		// Retain normalized integer data directly, including very old/future dates.
		if nano < 1e9 {
			return values.Bytes(b)
		}
	default:
		panic(ErrFormat)
	}
	return values.Time(time.Unix(sec, nano))
}
