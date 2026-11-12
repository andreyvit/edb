package kvolab

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"runtime"
	"testing"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

func tagStringBytes(s string) []byte { return unsafe.Slice(unsafe.StringData(s), len(s)) }

// View16 keeps a GC-visible pointer and length. Its byte view deliberately has
// cap == len. Borrowing/transaction lifetime rules remain the same as a slice.
type view16 struct {
	ptr *byte
	n   int
}

func newView16(b []byte) view16 { return view16{unsafe.SliceData(b), len(b)} }
func (v view16) bytes() []byte  { return unsafe.Slice(v.ptr, v.n) }

// Full physical-class preservation in 16 bytes is also possible. Scalar marker
// pointers refer to real static objects, not fabricated low addresses. The
// cheaper type-directed alternative is tagCell (nil pointer + scalar word).
var valueMarkers = [6]byte{0, 1, 2, 3, 4, 5}

type value16 struct {
	ptr *byte
	num uint64
}

func newValue16(v draft.Value) value16 {
	if v.IsZero() {
		return value16{}
	}
	if v.Wire() == draft.Bytes {
		b := v.Bytes()
		return value16{unsafe.SliceData(b), uint64(len(b))}
	}
	return value16{&valueMarkers[v.Wire()], v.Uint()}
}
func (v value16) wire() draft.Wire {
	if v.ptr == nil {
		return draft.Zero
	}
	// Integer addresses are compared only; they are never retained or converted
	// back to pointers. v.ptr itself keeps the underlying allocation alive.
	delta := uintptr(unsafe.Pointer(v.ptr)) - uintptr(unsafe.Pointer(&valueMarkers[0]))
	if delta < uintptr(len(valueMarkers)) {
		return draft.Wire(delta)
	}
	return draft.Bytes
}
func (v value16) uint() uint64 {
	if v.wire() == draft.Bytes {
		panic(draft.ErrType)
	}
	return v.num
}
func (v value16) bytes() []byte {
	if v.ptr == nil {
		return nil
	}
	if v.wire() != draft.Bytes {
		panic(draft.ErrType)
	}
	return unsafe.Slice(v.ptr, int(v.num))
}

// Returning a heap-owned span through only the packed pointer must keep it live
// across collections. Interior pointers and stack growth are tested too.
//
//go:noinline
func makePackedBorrow(n int) (view16, value16, tagCell) {
	b := make([]byte, n+17)
	for i := range b {
		b[i] = byte(i*7 + 9)
	}
	b = b[17:]
	return newView16(b), newValue16(draft.ByteString(b)), tagSpan(b, false)
}

//go:noinline
func growPackedStack(depth int, v view16) uint64 {
	var padding [1024]byte
	padding[depth%len(padding)] = byte(depth)
	if depth > 0 {
		return growPackedStack(depth-1, v) + uint64(padding[depth%len(padding)])
	}
	return uint64(v.bytes()[0])
}
func TestPackedValues(t *testing.T) {
	t.Logf("Value=%d View=%d slog.Value=%d value16=%d view16=%d directed=%d", unsafe.Sizeof(draft.Value{}), unsafe.Sizeof(draft.View{}), unsafe.Sizeof(slog.Value{}), unsafe.Sizeof(value16{}), unsafe.Sizeof(view16{}), unsafe.Sizeof(tagCell{}))
	for _, v := range []draft.Value{draft.ZeroValue(), draft.Uint(1), draft.Uint(256), draft.Uint(1 << 20), draft.Uint(1 << 32), draft.Word(^uint64(0)), draft.ByteString([]byte("sample"))} {
		p := newValue16(v)
		if p.wire() != v.Wire() {
			t.Fatal("wire")
		}
		if p.wire() == draft.Bytes {
			if !bytes.Equal(p.bytes(), v.Bytes()) {
				t.Fatal("bytes")
			}
		} else if p.uint() != v.Uint() {
			t.Fatal("scalar")
		}
	}
	views := make([]view16, 128)
	values := make([]value16, 128)
	cells := make([]tagCell, 128)
	for i := range views {
		views[i], values[i], cells[i] = makePackedBorrow(i + 1)
	}
	for range 4 {
		runtime.GC()
	}
	for i := range views {
		want := byte(17*7 + 9)
		if views[i].bytes()[0] != want || values[i].bytes()[0] != want || cells[i].raw()[0] != want {
			t.Fatal("pointer not live")
		}
		if cap(views[i].bytes()) != i+1 {
			t.Fatal("view capacity")
		}
	}
	var local [64]byte
	local[0] = 19
	v := newView16(local[:])
	if got := growPackedStack(64, v); got != 19+64*65/2 {
		t.Fatal("stack pointer")
	}
}

//go:noinline
func packedCall40(v draft.Value) uint64 { return v.Uint() }

//go:noinline
func packedCall16(v value16) uint64 { return v.uint() }

//go:noinline
func packedCallDirected(v tagCell) uint64 { return v.num }

func BenchmarkPackedValues(b *testing.B) {
	for _, n := range []int{128, 1 << 20} {
		wide := make([]draft.Value, n)
		narrow := make([]value16, n)
		directed := make([]tagCell, n)
		for i := range wide {
			wide[i] = draft.Word(uint64(i + 1))
			narrow[i] = newValue16(wide[i])
			directed[i] = tagCell{num: uint64(i + 1)}
		}
		for _, mode := range []string{"wide40", "marked16", "directed16"} {
			b.Run(fmt.Sprintf("n%d/%s", n, mode), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					j := (uint(i) * 2654435761) & uint(n-1)
					switch mode {
					case "wide40":
						sum += packedCall40(wide[j])
					case "marked16":
						sum += packedCall16(narrow[j])
					case "directed16":
						sum += packedCallDirected(directed[j])
					}
				}
				sinkU = sum
			})
		}
	}
}
func BenchmarkPackedView(b *testing.B) {
	raw := draftObject(numeric(128, 0), draft.Indexed, draft.Indexed)
	wide, narrow := draft.Trusted(raw), newView16(raw)
	b.Run("slice24", func(b *testing.B) {
		b.ReportAllocs()
		var sum uint64
		for i := 0; i < b.N; i++ {
			sum += wide.Get(query(i, 128)).Uint()
		}
		sinkU = sum
	})
	b.Run("pointer16", func(b *testing.B) {
		b.ReportAllocs()
		var sum uint64
		for i := 0; i < b.N; i++ {
			sum += draft.Trusted(narrow.bytes()).Get(query(i, 128)).Uint()
		}
		sinkU = sum
	})
}

//go:noinline
func alignmentLoad(b []byte) uint64 { return binary.LittleEndian.Uint64(b) }

//go:noinline
func alignmentStore(b []byte, u uint64) { binary.LittleEndian.PutUint64(b, u) }

func BenchmarkAlignmentStore(b *testing.B) {
	for _, rows := range []int{128, 1 << 18} {
		buf := make([]byte, rows*128+256)
		base := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
		buf = buf[int((-base)&127):]
		for _, offset := range []int{0, 1, 121} {
			b.Run(fmt.Sprintf("rows%d/offset%d", rows, offset), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					j := int((uint(i) * 2654435761) & uint(rows-1))
					alignmentStore(buf[j*128+offset:], uint64(i+1))
				}
				sinkBytes = buf
			})
		}
	}
}

func BenchmarkAlignment(b *testing.B) {
	// Cache line size reported by this host is 128 bytes. Explicit alignment
	// makes offsets 121..127 straddle lines instead of relying on allocator luck.
	for _, rows := range []int{128, 1 << 18} {
		buf := make([]byte, rows*128+256)
		base := uintptr(unsafe.Pointer(unsafe.SliceData(buf)))
		shift := int((-base) & 127)
		buf = buf[shift:]
		for i := range buf {
			buf[i] = byte(i*37 + 1)
		}
		for _, offset := range []int{0, 1, 2, 3, 4, 5, 6, 7, 120, 121, 127} {
			b.Run(fmt.Sprintf("rows%d/offset%d", rows, offset), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					j := int((uint(i) * 2654435761) & uint(rows-1))
					sum += alignmentLoad(buf[j*128+offset:])
				}
				sinkU = sum
			})
		}
	}
}

//go:noinline
func alignmentIndexGet(b []byte, n, w, keys, cells int, key uint64) uint64 {
	lo, hi := 0, n
	for lo < hi {
		m := lo + (hi-lo)/2
		if getWidth(b[keys+m*w:], w) < key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == n || getWidth(b[keys+lo*w:], w) != key {
		return 0
	}
	return binary.LittleEndian.Uint64(b[cells+lo*8:])
}
func BenchmarkAlignmentIndex(b *testing.B) {
	const n = 128
	for _, w := range []int{1, 8} {
		for _, aligned := range []bool{false, true} {
			keys, cells := 5, 5+n*w
			if aligned {
				keys = (keys + 7) &^ 7
				cells = (keys + n*w + 7) &^ 7
			}
			size := cells + n*8
			for _, count := range []int{1, 16384} {
				// Equal stride isolates padding/access from working-set size.
				stride := (size + 127) &^ 127
				storage := make([]byte, count*stride+128)
				base := uintptr(unsafe.Pointer(unsafe.SliceData(storage)))
				storage = storage[int((-base)&127):]
				for i := 0; i < count; i++ {
					row := storage[i*stride:]
					for j := 0; j < n; j++ {
						putWidth(row[keys+j*w:], uint64(j+1), w)
						binary.LittleEndian.PutUint64(row[cells+j*8:], uint64(j*37+1))
					}
				}
				b.Run(fmt.Sprintf("width%d/aligned%t/rows%d", w, aligned, count), func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(float64(size), "row-B")
					var sum uint64
					for i := 0; i < b.N; i++ {
						row := int((uint(i) * 11400714819323198485) & uint(count-1))
						sum += alignmentIndexGet(storage[row*stride:], n, w, keys, cells, query(i, n))
					}
					sinkU = sum
				})
			}
		}
	}
}
