package kvolab

// Control inspired by backward construction in FlatBuffers: build the existing
// draft bytes from the end of one buffer. Requires a replayable/random-access
// source for the shared-class/count prepass and descending field traversal.

import (
	"encoding/binary"
	"math"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type reverseWireWriter struct {
	buf   []byte
	pos   int
	moved int // only backing-buffer growth moves existing output
}

func (w *reverseWireWriter) reset() { w.pos, w.moved = len(w.buf), 0 }
func (w *reverseWireWriter) room(n int) []byte {
	if n > w.pos {
		used := len(w.buf) - w.pos
		next := make([]byte, max(64, max(len(w.buf)*2, used+n)))
		copy(next[len(next)-used:], w.buf[w.pos:])
		w.buf, w.pos = next, len(next)-used
		w.moved += used
	}
	w.pos -= n
	return w.buf[w.pos : w.pos+n]
}
func (w *reverseWireWriter) uvar(u uint64) {
	var b [10]byte
	n := binary.PutUvarint(b[:], u)
	copy(w.room(n), b[:n])
}
func (w *reverseWireWriter) byte(v byte) { w.room(1)[0] = v }
func (w *reverseWireWriter) value(v Value) {
	switch v.Kind {
	case Record, Dict, List:
		if tagZero(v) {
			return
		}
		before := len(w.buf) - w.pos
		w.object(v)
		n := len(w.buf) - w.pos - before
		w.uvar(uint64(n))
	case Text:
		if v.S != "" {
			copy(w.room(len(v.S)), v.S)
			w.uvar(uint64(len(v.S)))
		}
	case Blob:
		if len(v.B) != 0 {
			copy(w.room(len(v.B)), v.B)
			w.uvar(uint64(len(v.B)))
		}
	default:
		x := wireScalar(v)
		u := x.Uint()
		switch x.Wire() {
		case draft.Zero:
		case draft.Varint:
			w.uvar(u)
		case draft.Fixed1:
			w.byte(byte(u))
		case draft.Fixed2:
			binary.LittleEndian.PutUint16(w.room(2), uint16(u))
		case draft.Fixed4:
			binary.LittleEndian.PutUint32(w.room(4), uint32(u))
		case draft.Fixed8:
			binary.LittleEndian.PutUint64(w.room(8), u)
		}
	}
}
func (w *reverseWireWriter) object(v Value) {
	n, shared := 0, draft.Wire(7)
	count := func(x Value) {
		c := wireClass(x)
		if v.Kind != List && c == draft.Zero {
			return
		}
		if n == 0 {
			shared = c
		} else if shared != c {
			shared = 7
		}
		n++
	}
	for i, x := range v.Fields {
		if i > 0 && x.Key <= v.Fields[i-1].Key {
			panic(errTag)
		}
		count(x.Value)
	}
	for i, x := range v.Entries {
		if i > 0 && x.Key <= v.Entries[i-1].Key {
			panic(errTag)
		}
		count(x.Value)
	}
	for _, x := range v.Items {
		count(x)
	}
	if n > tagMaxEntries {
		panic(errTag)
	}
	before := len(w.buf) - w.pos
	switch v.Kind {
	case Record:
		i := len(v.Fields) - 1
		for i >= 0 {
			x := v.Fields[i]
			if tagZero(x.Value) {
				i--
				continue
			}
			w.value(x.Value)
			j := i - 1
			for j >= 0 && tagZero(v.Fields[j].Value) {
				j--
			}
			delta := x.Key
			if j >= 0 {
				delta -= v.Fields[j].Key
			}
			if shared != 7 {
				w.uvar(delta)
			} else if delta <= math.MaxUint64>>3 {
				w.uvar(delta<<3 | uint64(wireClass(x.Value)))
			} else {
				w.byte(byte(wireClass(x.Value)))
				w.uvar(delta)
				w.byte(7)
			}
			i = j
		}
	case Dict:
		for i := len(v.Entries) - 1; i >= 0; i-- {
			x := v.Entries[i]
			if tagZero(x.Value) {
				continue
			}
			w.value(x.Value)
			if shared == 7 {
				w.byte(byte(wireClass(x.Value)))
			}
			copy(w.room(len(x.Key)), x.Key)
			w.uvar(uint64(len(x.Key)))
		}
	case List:
		for i := len(v.Items) - 1; i >= 0; i-- {
			w.value(v.Items[i])
			if shared == 7 {
				w.byte(byte(wireClass(v.Items[i])))
			}
		}
	default:
		panic(errTag)
	}
	w.uvar(uint64(n)<<6 | uint64(shared)<<3 | tagContainerKind(v.Kind)<<1)
	if len(w.buf)-w.pos-before > tagMaxBytes {
		panic(errTag)
	}
}
func (w *reverseWireWriter) bytes() []byte { return w.buf[w.pos:] }

func TestReverseWriter(t *testing.T) {
	all := tagExtraFixtures()
	all["zero-keys"] = Obj(Field{0, U(0)}, Field{1, U(7)}, Field{2, U(0)}, Field{math.MaxUint64, U(1 << 63)})
	all["zero-list"] = Array(U(0), S(""), U(7), Obj())
	all["deep16"] = tagDeep(16, 32768)
	for name, v := range all {
		var w reverseWireWriter
		w.object(v)
		want := draftObject(v, draft.Compact, draft.Compact)
		if string(w.bytes()) != string(want) {
			t.Fatal(name, "reversed construction changed format")
		}
		w.reset()
		w.object(v)
		if w.moved != 0 || string(w.bytes()) != string(want) {
			t.Fatal(name, "reused buffer moved output")
		}
	}
}
func BenchmarkReverseWriter(b *testing.B) {
	all := fixtures()
	all["deep16"] = tagDeep(16, 32768)
	for _, name := range []string{"profile", "state", "settings", "transaction", "dictionary-128", "blob-32k", "deep16"} {
		v := all[name]
		b.Run(name, func(b *testing.B) {
			var w reverseWireWriter
			w.object(v)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w.reset()
				w.object(v)
			}
			sinkBytes = w.bytes()
		})
	}
}
