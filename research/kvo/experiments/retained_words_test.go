package kvolab

// Reconstructed universal indexed word-directory layout from the first common
// prototype. String keys use uint32 offsets; every value gets an eight-byte
// cell. New timings are for this reconstruction, not the overwritten code.
import (
	"encoding/binary"
	"fmt"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"math/bits"
	"testing"
)

type universalIndexEncoder struct {
	entries  []classMeta
	children lengthWriter
}

func (e *universalIndexEncoder) encode(dst []byte, source Value) []byte {
	start := len(dst)
	e.entries = e.entries[:0]
	kind := int(tagContainerKind(source.Kind))
	shared := byte(8)
	put := func(key uint64, text string, x Value) {
		if source.Kind != List && tagZero(x) {
			return
		}
		m := classMeta{key: key, wire: 8}
		if source.Kind == Dict {
			m.key = uint64(len(dst) - start)
			dst = binary.AppendUvarint(dst, uint64(len(text)))
			dst = append(dst, text...)
		}
		at := len(dst)
		switch x.Kind {
		case Record, Dict, List:
			if !tagZero(x) {
				dst = lengthEncode(&e.children, dst, x, nil)
				m.wire = 9
			}
		case Text:
			if x.S != "" {
				dst = append(dst, x.S...)
				m.wire = 9
			}
		case Blob:
			if len(x.B) != 0 {
				dst = append(dst, x.B...)
				m.wire = 9
			}
		default:
			m.word = draftValue(x, draft.Compact).Uint()
		}
		if m.wire == 9 {
			m.word = uint64(at-start) | uint64(len(dst)-at)<<32
		}
		if len(e.entries) == 0 {
			shared = byte(m.wire)
		} else if shared != byte(m.wire) {
			shared = 15
		}
		e.entries = append(e.entries, m)
	}
	switch source.Kind {
	case Record:
		for _, f := range source.Fields {
			put(f.Key, "", f.Value)
		}
	case Dict:
		for _, p := range source.Entries {
			put(0, p.Key, p.Value)
		}
	case List:
		for _, x := range source.Items {
			put(0, "", x)
		}
	default:
		panic(draft.ErrKind)
	}
	w := 1
	if kind == 1 {
		w = 4
	} else if kind == 0 && len(e.entries) > 0 {
		w = tagKeyWidth(e.entries[len(e.entries)-1].key)
	}
	if kind != 2 {
		for _, m := range e.entries {
			dst = tagAppendWord(dst, m.key, w)
		}
	}
	mode := byte(0)
	if shared == 9 {
		mode = 1
	} else if shared == 15 {
		mode = 2
		at := len(dst)
		dst = append(dst, make([]byte, (len(e.entries)+7)/8)...)
		for i, m := range e.entries {
			if m.wire == 9 {
				dst[at+i/8] |= 1 << uint(i%8)
			}
		}
	}
	for _, m := range e.entries {
		dst = binary.LittleEndian.AppendUint64(dst, m.word)
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(e.entries)))
	return append(dst, 1|byte(kind)<<1|mode<<3|byte(bits.Len(uint(w))-1)<<6)
}

type universalIndexView struct{ lengthIndexHeader }

func universalIndex(raw []byte) universalIndexView {
	t := raw[len(raw)-1]
	n := int(binary.LittleEndian.Uint32(raw[len(raw)-5:]))
	h := lengthIndexHeader{raw: raw, n: n, kind: int(t >> 1 & 3), width: 1 << uint(t>>6), mode: t >> 3 & 3}
	h.cells = len(raw) - 5 - 8*n
	h.codes = h.cells
	if h.mode == 2 {
		h.codes -= (n + 7) / 8
	}
	h.keys = h.codes
	if h.kind != 2 {
		h.keys -= n * h.width
	}
	return universalIndexView{h}
}
func (v universalIndexView) value(i int) lengthValue {
	if i < 0 {
		return lengthValue{}
	}
	u := binary.LittleEndian.Uint64(v.raw[v.cells+8*i:])
	if !v.span(i) {
		return lengthWord(u)
	}
	return lengthBytes(v.raw[int(uint32(u)) : int(uint32(u))+int(u>>32)])
}
func (v universalIndexView) text(i int) []byte {
	at := int(getWidth(v.raw[v.keys+i*v.width:], v.width))
	n, b := lengthU(v.raw[at:])
	return b[:int(n)]
}
func (v universalIndexView) getText(key string) lengthValue {
	lo, hi := 0, v.n
	for lo < hi {
		m := lo + (hi-lo)/2
		if string(v.text(m)) < key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == v.n || string(v.text(lo)) != key {
		return lengthValue{}
	}
	return v.value(lo)
}
func TestRetainedUniversalIndex(t *testing.T) {
	all := fixtures()
	all["mixed-list"] = Array(U(7), S("value"), U(0), Obj(Field{1, U(2)}))
	for name, source := range all {
		var e universalIndexEncoder
		raw := e.encode(nil, source)
		v := universalIndex(raw)
		switch source.Kind {
		case Record:
			for _, f := range source.Fields {
				checkLengthValue(t, f.Value, v.value(v.findNumber(f.Key)))
			}
		case Dict:
			for _, p := range source.Entries {
				checkLengthValue(t, p.Value, v.getText(p.Key))
			}
		case List:
			for i, x := range source.Items {
				checkLengthValue(t, x, v.value(i))
			}
		}
		if name == "dictionary-128" && len(raw) != 3973 {
			t.Fatalf("historical layout size: %d", len(raw))
		}
		t.Logf("%s bytes=%d", name, len(raw))
	}
}
func BenchmarkRetainedUniversalIndex(b *testing.B) {
	for _, name := range []string{"dictionary-128", "settings", "state"} {
		source := fixtures()[name]
		b.Run(name+"/encode", func(b *testing.B) {
			var e universalIndexEncoder
			dst := e.encode(nil, source)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = e.encode(dst[:0], source)
			}
			sinkBytes = dst
			b.ReportMetric(float64(len(dst)), "encoded-B")
		})
	}
	for _, n := range []int{8, 128, 512} {
		source := dictionary(n, false)
		var e universalIndexEncoder
		v := universalIndex(e.encode(nil, source))
		b.Run(fmt.Sprintf("dictionary%d/get", n), func(b *testing.B) {
			b.ReportAllocs()
			var sum uint64
			for i := 0; i < b.N; i++ {
				sum += uint64(len(v.getText(source.Entries[query(i, n)-1].Key).Bytes()))
			}
			sinkU = sum
		})
	}
}
