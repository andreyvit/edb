package kvolab

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

// Minimal executable API sketch: ordered numeric records, direct payload writes,
// nested Begin/End, and reusable metadata. No Value tree, child payload buffer,
// size prepass, or backpatching. A full API would add string keys and sequences.
type catalogStream struct {
	out    []byte
	frames []catalogFrame
	depth  int
}
type catalogFrame struct {
	entries        []classMeta
	key, lastInput uint64
	start, count   int
	seen           bool
	shared         byte
}

func (w *catalogStream) Reset(dst []byte) {
	w.out, w.depth = dst, 0
}
func (w *catalogStream) checkKey(key uint64) {
	if w.depth == 0 {
		panic(errTag)
	}
	f := &w.frames[w.depth-1]
	if f.seen && key <= f.lastInput {
		panic(errTag)
	}
	f.lastInput, f.seen = key, true
}
func (w *catalogStream) item(key uint64, code byte, word uint64) {
	f := &w.frames[w.depth-1]
	f.entries = append(f.entries, classMeta{key: key, wire: draft.Wire(code), word: word})
	if f.count == 0 {
		f.shared = code
	} else if f.shared != code {
		f.shared = 15
	}
	f.count++
	if f.count > tagMaxEntries {
		panic(errTag)
	}
}
func (w *catalogStream) Begin(key uint64) {
	if w.depth != 0 {
		w.checkKey(key)
	}
	if w.depth == len(w.frames) {
		w.frames = append(w.frames, catalogFrame{})
	}
	entries := w.frames[w.depth].entries[:0]
	w.frames[w.depth] = catalogFrame{key: key, start: len(w.out), entries: entries, shared: 15}
	w.depth++
}
func (w *catalogStream) Uint(key, u uint64) {
	w.checkKey(key)
	if u == 0 {
		return
	}
	n := (bits.Len64(u) + 7) / 8
	w.item(key, byte(n), u)
	w.out = tagAppendWord(w.out, u, n)
}
func (w *catalogStream) Bytes(key uint64, data []byte) {
	w.checkKey(key)
	if len(data) == 0 {
		return
	}
	if len(data) > tagMaxBytes {
		panic(errTag)
	}
	w.item(key, 9, uint64(len(data))<<32)
	w.out = append(w.out, data...)
}
func (w *catalogStream) End() {
	if w.depth == 0 {
		panic(errTag)
	}
	f := w.frames[w.depth-1]
	if f.count == 0 && w.depth > 1 {
		w.out = w.out[:f.start]
		w.depth--
		return
	}
	cat := len(w.out)
	w.out = binary.AppendUvarint(w.out, uint64(f.count)<<4|uint64(f.shared))
	var previous uint64
	for _, m := range f.entries {
		delta := m.key - previous
		if f.shared != 15 {
			w.out = binary.AppendUvarint(w.out, delta)
		} else if delta <= math.MaxUint64>>4 {
			w.out = binary.AppendUvarint(w.out, delta<<4|uint64(m.wire))
		} else {
			w.out = append(w.out, 15)
			w.out = binary.AppendUvarint(w.out, delta)
			w.out = append(w.out, byte(m.wire))
		}
		if m.wire == 9 {
			w.out = binary.AppendUvarint(w.out, m.word>>32)
		}
		previous = m.key
	}
	w.out = tagAppendTail(w.out, uint64(len(w.out)-cat)<<3)
	if len(w.out)-f.start > tagMaxBytes {
		panic(errTag)
	}
	w.depth--
	if w.depth != 0 {
		w.item(f.key, 9, uint64(len(w.out)-f.start)<<32)
	}
}
func (w *catalogStream) BytesWritten() []byte {
	if w.depth != 0 {
		panic(errTag)
	}
	return w.out
}
func TestIncrementalCatalog(t *testing.T) {
	var w catalogStream
	w.Begin(0)
	w.Uint(1, 7)
	w.Begin(2)
	w.Uint(1, 0)
	w.End() // Empty nested record is omitted.
	w.Begin(3)
	w.Uint(1, 1<<58)
	w.Bytes(2, []byte("uninterpreted child bytes"))
	w.End()
	w.End()
	v := Obj(Field{1, U(7)}, Field{2, Obj(Field{1, U(0)})}, Field{3, Obj(Field{1, U(1 << 58)}, Field{2, S("uninterpreted child bytes")})})
	e := classEncoder{rawLengths: true}
	if !bytes.Equal(w.BytesWritten(), e.object(nil, v, 0)) {
		t.Fatal("stream differs from recursive candidate")
	}
	checkClassTree(t, v, w.BytesWritten(), true)
}
func Example_catalogStream() {
	var w catalogStream
	w.Begin(0) // Root record.
	w.Uint(1, 7)
	w.Begin(2) // Child length is not yet known.
	w.Bytes(1, []byte("large payload"))
	w.End() // Record its length in the parent's metadata.
	w.End()
	root := w.BytesWritten()
	fmt.Println(classGet(root, 1, true).uint())
	child := classGet(root, 2, true).raw()
	fmt.Println(string(classGet(child, 1, true).raw()))
	// Output:
	// 7
	// large payload
}

func BenchmarkCatalogStream(b *testing.B) {
	for _, depth := range []int{4, 16} {
		data := bytes.Repeat([]byte{0x65}, 32768)
		b.Run(fmt.Sprintf("depth%d", depth), func(b *testing.B) {
			var w catalogStream
			encode := func(dst []byte) []byte {
				w.Reset(dst[:0])
				for i := 0; i < depth; i++ {
					w.Begin(2)
					w.Uint(1, 1)
				}
				w.Bytes(2, data)
				for range depth {
					w.End()
				}
				return w.BytesWritten()
			}
			dst := encode(nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = encode(dst)
			}
			sinkBytes = dst
		})
	}
}
