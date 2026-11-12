package values

import (
	"math"
	"testing"
)

type codecID uint64
type codecName string

func checkCodec[T comparable](t *testing.T, c Codec[T], want T) {
	t.Helper()
	if got := c.Decode(c.Encode(want)); got != want {
		t.Fatalf("got %v want %v", got, want)
	}
	if n := testing.AllocsPerRun(100, func() {
		if c.Decode(c.Encode(want)) != want {
			t.Fatal("round trip")
		}
	}); n != 0 {
		t.Fatalf("%g allocations", n)
	}
}
func TestCodecs(t *testing.T) {
	checkCodec(t, SignedCodec[int8](), -128)
	checkCodec(t, SignedCodec[int16](), -32768)
	checkCodec(t, SignedCodec[int32](), math.MinInt32)
	checkCodec(t, SignedCodec[int64](), math.MinInt64)
	checkCodec(t, SignedCodec[int](), -7)
	checkCodec(t, UnsignedCodec[codecID](), math.MaxUint64)
	checkCodec(t, UnsignedCodec[uint8](), 255)
	checkCodec(t, UnsignedCodec[uint16](), 65535)
	checkCodec(t, UnsignedCodec[uint32](), math.MaxUint32)
	checkCodec(t, UnsignedCodec[uint](), 7)
	checkCodec(t, UnsignedCodec[uintptr](), 7)
	checkCodec(t, BoolCodec[bool](), true)
	checkCodec(t, Float32Codec[float32](), 1.25)
	checkCodec(t, Float64Codec[float64](), math.Inf(1))
	checkCodec(t, StringCodec[codecName](), "borrowed")
	c := BytesCodec[[]byte]()
	b := []byte{1, 2, 3}
	out := c.Decode(c.Encode(b))
	out[0] = 9
	if b[0] != 9 {
		t.Fatal("bytes unexpectedly copied")
	}
	if c.Decode(c.Encode(nil)) != nil {
		t.Fatal("nil bytes")
	}
}
func BenchmarkCodecRoundTrip(b *testing.B) {
	c := SignedCodec[int64]()
	var n int64
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		n = c.Decode(c.Encode(int64(i)))
	}
	if n != int64(b.N-1) {
		b.Fatal(n)
	}
}
