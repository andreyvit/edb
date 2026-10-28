package kvolab

// Controls that keep the old on-disk grammar. They separate a format improvement
// from simpler generated-style code and avoiding materialization of skipped words.

import (
	"encoding/binary"
	"math"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

func wireScalar(v Value) draft.Value {
	if v.Kind == Text || v.Kind == Blob || v.Kind == Record || v.Kind == Dict || v.Kind == List {
		panic(errTag)
	}
	return draftValue(v, draft.Compact)
}
func wireClass(v Value) draft.Wire {
	switch v.Kind {
	case Text, Blob, Record, Dict, List:
		if tagZero(v) {
			return draft.Zero
		}
		return draft.Bytes
	default:
		return wireScalar(v).Wire()
	}
}

type wireInlineEncoder struct{ buffers [][]byte }

func (e *wireInlineEncoder) object(dst []byte, v Value, depth int) []byte {
	for len(e.buffers) <= depth {
		e.buffers = append(e.buffers, nil)
	}
	n, shared := 0, draft.Wire(7)
	selectClass := func(x Value) {
		w := wireClass(x)
		if v.Kind != List && w == draft.Zero {
			return
		}
		if n == 0 {
			shared = w
		} else if shared != w {
			shared = 7
		}
		n++
	}
	for _, x := range v.Fields {
		selectClass(x.Value)
	}
	for _, x := range v.Entries {
		selectClass(x.Value)
	}
	for _, x := range v.Items {
		selectClass(x)
	}
	start := len(dst)
	dst = binary.AppendUvarint(dst, uint64(n)<<6|uint64(shared)<<3|tagContainerKind(v.Kind)<<1)
	var previous uint64
	emit := func(key uint64, text string, x Value) {
		w := wireClass(x)
		if v.Kind != List && w == draft.Zero {
			return
		}
		if v.Kind == Record {
			delta := key - previous
			if shared != 7 {
				dst = binary.AppendUvarint(dst, delta)
			} else if delta <= math.MaxUint64>>3 {
				dst = binary.AppendUvarint(dst, delta<<3|uint64(w))
			} else {
				dst = append(dst, 7)
				dst = binary.AppendUvarint(dst, delta)
				dst = append(dst, byte(w))
			}
			previous = key
		} else {
			if v.Kind == Dict {
				dst = binary.AppendUvarint(dst, uint64(len(text)))
				dst = append(dst, text...)
			}
			if shared == 7 {
				dst = append(dst, byte(w))
			}
		}
		if w == draft.Zero {
			return
		}
		switch x.Kind {
		case Record, Dict, List:
			child := e.object(e.buffers[depth][:0], x, depth+1)
			e.buffers[depth] = child
			dst = binary.AppendUvarint(dst, uint64(len(child)))
			dst = append(dst, child...)
		case Text:
			dst = binary.AppendUvarint(dst, uint64(len(x.S)))
			dst = append(dst, x.S...)
		case Blob:
			dst = binary.AppendUvarint(dst, uint64(len(x.B)))
			dst = append(dst, x.B...)
		default:
			u := wireScalar(x).Uint()
			switch w {
			case draft.Varint:
				dst = binary.AppendUvarint(dst, u)
			case draft.Fixed1:
				dst = append(dst, byte(u))
			case draft.Fixed2:
				dst = binary.LittleEndian.AppendUint16(dst, uint16(u))
			case draft.Fixed4:
				dst = binary.LittleEndian.AppendUint32(dst, uint32(u))
			case draft.Fixed8:
				dst = binary.LittleEndian.AppendUint64(dst, u)
			}
		}
		if len(dst)-start > tagMaxBytes {
			panic(errTag)
		}
	}
	switch v.Kind {
	case Record:
		for i, x := range v.Fields {
			if i > 0 && x.Key <= v.Fields[i-1].Key {
				panic(errTag)
			}
			emit(x.Key, "", x.Value)
		}
	case Dict:
		for i, x := range v.Entries {
			if i > 0 && x.Key <= v.Entries[i-1].Key {
				panic(errTag)
			}
			emit(0, x.Key, x.Value)
		}
	case List:
		for _, x := range v.Items {
			emit(0, "", x)
		}
	default:
		panic(errTag)
	}
	return dst
}

func wireSkipGet(raw []byte, want uint64) tagCell {
	tag, b := tagReadU(raw)
	n, shared := tag>>6, draft.Wire(tag>>3)&7
	var key uint64
	for range n {
		u, r := tagReadU(b)
		w := shared
		if w == 7 {
			w = draft.Wire(u & 7)
			if w == 7 {
				u, r = tagReadU(r)
				w, r = draft.Wire(r[0]), r[1:]
			} else {
				u >>= 3
			}
		}
		key += u
		if key > want {
			return tagCell{}
		}
		b = r
		size := 0
		switch w {
		case draft.Zero:
		case draft.Varint:
			u, b = tagReadU(b)
			if key == want {
				return tagCell{num: u}
			}
		case draft.Fixed1:
			size = 1
		case draft.Fixed2:
			size = 2
		case draft.Fixed4:
			size = 4
		case draft.Fixed8:
			size = 8
		case draft.Bytes:
			u, b = tagReadU(b)
			size = int(u)
		}
		if key == want {
			if w == draft.Bytes {
				return tagSpan(b[:size], false)
			}
			return tagCell{num: tagWord(b[:size])}
		}
		b = b[size:]
	}
	return tagCell{}
}
func TestWireControls(t *testing.T) {
	for name, v := range tagExtraFixtures() {
		var e wireInlineEncoder
		got := e.object(nil, v, 0)
		want := draftObject(v, draft.Compact, draft.Compact)
		if string(got) != string(want) {
			t.Fatal(name, "same-format control differs")
		}
		if v.Kind == Record {
			for _, x := range v.Fields {
				if x.Value.Kind == Uint && wireSkipGet(got, x.Key).uint() != x.Value.U {
					t.Fatal(name, "skip lookup")
				}
			}
		}
	}
}
func BenchmarkWireControl(b *testing.B) {
	for _, name := range []string{"profile", "state", "settings", "transaction", "dictionary-128", "blob-32k"} {
		v := fixtures()[name]
		b.Run(name+"/encode", func(b *testing.B) {
			var e wireInlineEncoder
			dst := e.object(nil, v, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = e.object(dst[:0], v, 0)
			}
			sinkBytes = dst
		})
	}
	raw := draftObject(numeric(128, 0), draft.Compact, draft.Compact)
	b.Run("numeric128/get", func(b *testing.B) {
		b.ReportAllocs()
		var sum uint64
		for i := 0; i < b.N; i++ {
			sum += wireSkipGet(raw, query(i, 128)).uint()
		}
		sinkU = sum
	})
}

// Same old bytes: a homogeneous fixed-width list already supports direct
// indexing. Compare this control before attributing the speedup to new framing.
func wireFixedAt(raw []byte, index int) uint64 {
	tag, b := tagReadU(raw)
	if tag&1 != 0 || tag>>1&3 != 2 || index < 0 || uint64(index) >= tag>>6 {
		panic(errTag)
	}
	var size int
	switch draft.Wire(tag>>3) & 7 {
	case draft.Zero:
		return 0
	case draft.Fixed1:
		size = 1
	case draft.Fixed2:
		size = 2
	case draft.Fixed4:
		size = 4
	case draft.Fixed8:
		size = 8
	default:
		panic(errTag)
	}
	return tagWord(b[index*size : (index+1)*size])
}
func BenchmarkLengthListControl(b *testing.B) {
	source := Array()
	for i := 0; i < 128; i++ {
		source.Items = append(source.Items, U(uint64(256+i)))
	}
	raw := draftObject(source, draft.Compact, draft.Compact)
	old := draft.Trusted(raw)
	var w lengthWriter
	next := lengthTrusted(lengthEncode(&w, nil, source, nil))
	for _, name := range []string{"previous", "previous-direct", "next"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			var sum uint64
			for i := 0; i < b.N; i++ {
				at := int(query(i, 128) - 1)
				switch name {
				case "previous":
					sum += old.At(at).Uint()
				case "previous-direct":
					sum += wireFixedAt(raw, at)
				case "next":
					sum += next.At(at).Uint()
				}
			}
			sinkU = sum
		})
	}
}
func TestWireFixedAt(t *testing.T) {
	for _, u := range []uint64{0, 1, 256, 1 << 24, 1 << 56} {
		source := Array(U(u), U(u+1))
		raw := draftObject(source, draft.Compact, draft.Compact)
		if u == 0 {
			continue
		}
		for i := 0; i < 2; i++ {
			if wireFixedAt(raw, i) != source.Items[i].U {
				t.Fatal("fixed list")
			}
		}
	}
}
