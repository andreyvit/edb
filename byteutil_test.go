package edb

import (
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestBytesBuilder_Basics(t *testing.T) {
	var bb bytesBuilder
	bb.EnsureExtra(128)
	if cap(bb.Buf) < 128 {
		t.Fatalf("cap(bb.Buf) = %d, wanted >= 128", cap(bb.Buf))
	}

	off := bb.Grow(3)
	copy(bb.Buf[off:], []byte{1, 2, 3})
	bb.AppendByte(4)
	bb.AppendFixedUint64(0x0102030405060708)
	bb.AppendUvarint(0x42)

	want := make([]byte, 0, 1+3+8+binary.MaxVarintLen64)
	want = append(want, 1, 2, 3, 4)
	var u64 [8]byte
	binary.BigEndian.PutUint64(u64[:], 0x0102030405060708)
	want = append(want, u64[:]...)
	want = appendUvarint(want, 0x42)

	if !reflect.DeepEqual(bb.Buf, want) {
		t.Fatalf("bb.Buf = %x, wanted %x", bb.Buf, want)
	}

	bb.Trim(2)
	if !reflect.DeepEqual(bb.Buf, []byte{1, 2}) {
		t.Fatalf("after Trim: bb.Buf = %x, wanted 0102", bb.Buf)
	}

	_, _ = bb.Write([]byte{9, 8})
	if !reflect.DeepEqual(bb.Buf, []byte{1, 2, 9, 8}) {
		t.Fatalf("after Write: bb.Buf = %x, wanted 01020908", bb.Buf)
	}

	_ = bb.WriteByte(7)
	if !reflect.DeepEqual(bb.Buf, []byte{1, 2, 9, 8, 7}) {
		t.Fatalf("after WriteByte: bb.Buf = %x, wanted 0102090807", bb.Buf)
	}
}

func TestByteUtil_AppendHelpers(t *testing.T) {
	src := []byte{0xAA, 0xBB, 0xCC}
	buf := appendRaw(nil, src)
	if !reflect.DeepEqual(buf, src) {
		t.Fatalf("appendRaw = %x, wanted %x", buf, src)
	}

	var dst [3]byte
	n := putRaw(dst[:], src)
	if n != 3 || !reflect.DeepEqual(dst[:], src) {
		t.Fatalf("putRaw = (n=%d, dst=%x), wanted (n=3, dst=%x)", n, dst[:], src)
	}

	val := reflect.ValueOf([]byte{1, 2, 3})
	buf = appendRawVal(nil, val)
	if !reflect.DeepEqual(buf, []byte{1, 2, 3}) {
		t.Fatalf("appendRawVal = %x, wanted 010203", buf)
	}

	got := appendVarbytes(nil, []byte("hi"))
	var d = makeByteDecoder(got)
	v, err := d.VarBytes()
	if err != nil || string(v) != "hi" || len(d.Buf) != 0 {
		t.Fatalf("VarBytes = (%q, %v), remaining=%d, wanted (\"hi\", nil), remaining=0", v, err, len(d.Buf))
	}
}

func TestByteDecoder_Errors(t *testing.T) {
	t.Run("invalid uvarint", func(t *testing.T) {
		d := makeByteDecoder([]byte{0x80}) // continuation bit with no terminator
		_, err := d.Uvarint()
		var de *DataError
		if !errors.As(err, &de) {
			t.Fatalf("Uvarint err = %T %v, wanted *DataError", err, err)
		}
		if de.Off != 0 {
			t.Fatalf("DataError.Off = %d, wanted 0", de.Off)
		}
	})

	t.Run("uvarint overflows int", func(t *testing.T) {
		var b [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(b[:], uint64(math.MaxInt)+1)
		d := makeByteDecoder(b[:n])
		_, err := d.Uvarinti()
		if err == nil {
			t.Fatalf("Uvarinti err = nil, wanted error")
		}
	})

	t.Run("Raw not enough data", func(t *testing.T) {
		d := makeByteDecoder([]byte{1, 2})
		_, err := d.Raw(3)
		if err == nil {
			t.Fatalf("Raw err = nil, wanted error")
		}
	})
}

func TestByteBuf_AppendUvarinti64PanicsOnNegative(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic")
		}
	}()
	var b byteBuf
	b.Buf = make([]byte, 16)
	b.AppendUvarinti64(-1)
}

func TestByteBuf_AppendAndOff(t *testing.T) {
	b := prealloc(nil, 8)
	if b.Off != 0 || len(b.Buf) != 8 {
		t.Fatalf("prealloc = (off=%d, len=%d), wanted (0, 8)", b.Off, len(b.Buf))
	}

	b.AppendByte(1)
	b.AppendRaw([]byte{2, 3})
	if off := b.Off; off != 3 {
		t.Fatalf("Off = %d, wanted 3", off)
	}
	if got := b.Trimmed(); !reflect.DeepEqual(got, []byte{1, 2, 3}) {
		t.Fatalf("Trimmed = %x, wanted 010203", got)
	}
}
