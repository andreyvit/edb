package kvolab

// A control experiment: move the existing wire classes into trailing metadata
// instead of conflating independent tags with incremental construction. Values
// go straight into the output. Class selection never moves an existing payload.

import (
	"encoding/binary"
	"math"
	"math/bits"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type classMeta struct {
	key  uint64 // numeric key, or dictionary key offset/length in key scratch
	word uint64 // scalar word or packed offset/length
	wire draft.Wire
}
type classLevel struct {
	entries []classMeta // pointer-free, 24 bytes per entry on this host
	keys    []byte
}
type classEncoder struct {
	levels      []classLevel
	indexedRoot bool
	rawLengths  bool // compact control: 0..8 raw scalar bytes, 9 span, 15 mixed
}

func (e *classEncoder) object(dst []byte, v Value, depth int) []byte {
	for len(e.levels) <= depth {
		e.levels = append(e.levels, classLevel{})
	}
	entries := e.levels[depth].entries[:0]
	keys := e.levels[depth].keys[:0]
	start := len(dst)
	indexed := e.indexedRoot && depth == 0 && v.Kind == Record
	lengths := e.rawLengths && !indexed
	span, mixed, classBits := draft.Bytes, draft.Wire(7), uint(3)
	if lengths {
		span, mixed, classBits = 9, 15, 4
	}
	shared := mixed
	put := func(key uint64, text string, x Value) {
		if v.Kind != List && tagZero(x) {
			return
		}
		m := classMeta{key: key}
		if v.Kind == Dict {
			if len(text) > tagMaxBytes-len(keys) {
				panic(errTag)
			}
			m.key = uint64(len(keys)) | uint64(len(text))<<32
			keys = append(keys, text...)
		}
		p := len(dst)
		switch x.Kind {
		case Record, Dict, List:
			if !tagZero(x) {
				dst = e.object(dst, x, depth+1)
				m.wire = span
			}
		case Text:
			if x.S != "" {
				dst = append(dst, x.S...)
				m.wire = span
			}
		case Blob:
			if len(x.B) != 0 {
				dst = append(dst, x.B...)
				m.wire = span
			}
		default:
			if e.rawLengths {
				m.word = x.U
				switch x.Kind {
				case Null, False:
					m.word = 0
				case True:
					m.word = 1
				case Int:
					i := int64(x.U)
					m.word = uint64(i<<1) ^ uint64(i>>63)
				case Float:
					if math.Float64frombits(x.U) == 0 {
						m.word = 0
					}
				}
				if indexed {
					m.wire = draft.Fixed8 // indexed scalar slots are always words
					if m.word == 0 {
						m.wire = draft.Zero
					}
				} else {
					n := (bits.Len64(m.word) + 7) / 8
					m.wire = draft.Wire(n)
					dst = tagAppendWord(dst, m.word, n)
				}
			} else {
				value := draftValue(x, draft.Compact)
				m.wire, m.word = value.Wire(), value.Uint()
				if !indexed {
					switch m.wire {
					case draft.Varint:
						dst = binary.AppendUvarint(dst, m.word)
					case draft.Fixed1:
						dst = append(dst, byte(m.word))
					case draft.Fixed2:
						dst = binary.LittleEndian.AppendUint16(dst, uint16(m.word))
					case draft.Fixed4:
						dst = binary.LittleEndian.AppendUint32(dst, uint32(m.word))
					case draft.Fixed8:
						dst = binary.LittleEndian.AppendUint64(dst, m.word)
					}
				}
			}
		}
		if m.wire == span {
			m.word = uint64(p-start) | uint64(len(dst)-p)<<32
		}
		if len(entries) == 0 {
			shared = m.wire
		} else if shared != m.wire {
			shared = mixed
		}
		entries = append(entries, m)
		if len(entries) > tagMaxEntries || len(dst)-start > tagMaxBytes {
			panic(errTag)
		}
	}
	switch v.Kind {
	case Record:
		for i, x := range v.Fields {
			if i > 0 && x.Key <= v.Fields[i-1].Key {
				panic(errTag)
			}
			put(x.Key, "", x.Value)
		}
	case Dict:
		for i, x := range v.Entries {
			if i > 0 && x.Key <= v.Entries[i-1].Key {
				panic(errTag)
			}
			put(0, x.Key, x.Value)
		}
	case List:
		for _, x := range v.Items {
			put(0, "", x)
		}
	default:
		panic(errTag)
	}
	e.levels[depth].entries = entries
	e.levels[depth].keys = keys
	if indexed {
		return classAppendIndex(dst, entries, shared)
	}
	cat := len(dst)
	dst = binary.AppendUvarint(dst, uint64(len(entries))<<classBits|uint64(shared))
	var prev uint64
	for _, m := range entries {
		if v.Kind == Record {
			delta := m.key - prev
			if shared != mixed {
				dst = binary.AppendUvarint(dst, delta)
			} else if delta <= math.MaxUint64>>classBits {
				dst = binary.AppendUvarint(dst, delta<<classBits|uint64(m.wire))
			} else {
				dst = append(dst, byte(mixed))
				dst = binary.AppendUvarint(dst, delta)
				dst = append(dst, byte(m.wire))
			}
			prev = m.key
		} else {
			if v.Kind == Dict {
				start, n := int(uint32(m.key)), int(m.key>>32)
				dst = binary.AppendUvarint(dst, uint64(n))
				dst = append(dst, keys[start:start+n]...)
			}
			if shared == mixed {
				dst = append(dst, byte(m.wire))
			}
		}
		if m.wire == span {
			dst = binary.AppendUvarint(dst, m.word>>32)
		}
	}
	dst = tagAppendTail(dst, uint64(len(dst)-cat)<<3|tagContainerKind(v.Kind)<<1)
	if len(dst)-start > tagMaxBytes {
		panic(errTag)
	}
	return dst
}
func classAppendIndex(dst []byte, entries []classMeta, shared draft.Wire) []byte {
	w := 1
	if len(entries) != 0 {
		w = width(entries[len(entries)-1].key)
	}
	for _, m := range entries {
		dst = tagAppendWord(dst, m.key, w)
	}
	if shared == 7 {
		start := len(dst)
		dst = append(dst, make([]byte, (len(entries)*3+7)/8)...)
		for i, m := range entries {
			bit := i * 3
			dst[start+bit/8] |= byte(m.wire) << (bit % 8)
			if bit%8 > 5 {
				dst[start+bit/8+1] |= byte(m.wire) >> (8 - bit%8)
			}
		}
	}
	for _, m := range entries {
		dst = binary.LittleEndian.AppendUint64(dst, m.word)
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(entries)))
	return append(dst, 1|byte(shared)<<3|byte(bits.Len(uint(w))-1)<<6)
}

type classIterator struct {
	meta, payload []byte
	kind          uint64
	shared        draft.Wire
	left, index   int
	key           uint64
	rawLengths    bool
}

func classIter(raw []byte, rawLengths ...bool) classIterator {
	tail, b := tagTail(raw)
	n := tail >> 3
	if tail&1 != 0 || tail>>1&3 == 3 || n > uint64(len(b)) {
		panic(errTag)
	}
	payload, meta := b[:len(b)-int(n)], b[len(b)-int(n):]
	tag, meta := tagReadU(meta)
	classBits := uint(3)
	lengths := len(rawLengths) != 0 && rawLengths[0]
	if lengths {
		classBits = 4
	}
	if tag>>classBits > tagMaxEntries {
		panic(errTag)
	}
	if lengths && tag&15 > 9 && tag&15 != 15 {
		panic(errTag)
	}
	return classIterator{meta: meta, payload: payload, kind: tail >> 1 & 3, shared: draft.Wire(tag & ((1 << classBits) - 1)), left: int(tag >> classBits), rawLengths: lengths}
}
func (it *classIterator) next() (key uint64, text []byte, c tagCell, ok bool) {
	if it.left == 0 {
		if len(it.meta) != 0 || len(it.payload) != 0 {
			panic(errTag)
		}
		return
	}
	w := it.shared
	span, mixed, classBits := draft.Bytes, draft.Wire(7), uint(3)
	if it.rawLengths {
		span, mixed, classBits = 9, 15, 4
	}
	if it.kind == 0 {
		delta, r := tagReadU(it.meta)
		it.meta = r
		if w == mixed {
			w = draft.Wire(delta & uint64(mixed))
			if w == mixed {
				if delta != uint64(mixed) {
					panic(errTag)
				}
				delta, it.meta = tagReadU(it.meta)
				if delta <= math.MaxUint64>>classBits || len(it.meta) == 0 {
					panic(errTag)
				}
				w, it.meta = draft.Wire(it.meta[0]), it.meta[1:]
			} else {
				delta >>= classBits
			}
		}
		if delta > math.MaxUint64-it.key || it.index > 0 && delta == 0 {
			panic(errTag)
		}
		it.key += delta
		key = it.key
	} else {
		if it.kind == 1 {
			n, r := tagReadU(it.meta)
			if n > uint64(len(r)) {
				panic(errTag)
			}
			text, it.meta = r[:int(n)], r[int(n):]
		} else {
			key = uint64(it.index)
		}
		if w == mixed {
			if len(it.meta) == 0 {
				panic(errTag)
			}
			w, it.meta = draft.Wire(it.meta[0]), it.meta[1:]
		}
	}
	n := 0
	if it.rawLengths && w <= 8 {
		n = int(w)
	} else if w == span {
		u, r := tagReadU(it.meta)
		if u > uint64(len(it.payload)) {
			panic(errTag)
		}
		n, it.meta = int(u), r
	} else if it.rawLengths {
		panic(errTag)
	} else {
		switch w {
		case draft.Zero:
		case draft.Varint:
			c.num, it.payload = tagReadU(it.payload)
		case draft.Fixed1:
			n = 1
		case draft.Fixed2:
			n = 2
		case draft.Fixed4:
			n = 4
		case draft.Fixed8:
			n = 8
		default:
			panic(errTag)
		}
	}
	if n > len(it.payload) {
		panic(errTag)
	}
	if w == span {
		c = tagSpan(it.payload[:n], false)
	} else if n != 0 {
		if it.rawLengths {
			// No reason to load an unrequested scalar just to skip it. The
			// returned borrow interprets its bytes only when an accessor runs.
			c = tagSpan(it.payload[:n], true)
		} else {
			c.num = tagWord(it.payload[:n])
		}
	}
	it.payload = it.payload[n:]
	it.left--
	it.index++
	return key, text, c, true
}
func classGet(raw []byte, key uint64, rawLengths ...bool) tagCell {
	it := classIter(raw, rawLengths...)
	for {
		k, _, c, ok := it.next()
		if !ok || k > key {
			return tagCell{}
		}
		if k == key {
			return c
		}
	}
}
func classIndexGet(raw []byte, key uint64) tagCell {
	t := raw[len(raw)-1]
	n := int(binary.LittleEndian.Uint32(raw[len(raw)-5:]))
	w := 1 << (t >> 6)
	shared := draft.Wire(t>>3) & 7
	codes := 0
	if shared == 7 {
		codes = (n*3 + 7) / 8
	}
	cells := len(raw) - 5 - n*8
	keys := cells - codes - n*w
	lo, hi := 0, n
	for lo < hi {
		m := lo + (hi-lo)/2
		if getWidth(raw[keys+m*w:], w) < key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == n || getWidth(raw[keys+lo*w:], w) != key {
		return tagCell{}
	}
	if shared == 7 {
		bit := lo * 3
		pos := keys + n*w + bit/8
		code := uint16(raw[pos])
		if bit%8 > 5 {
			code |= uint16(raw[pos+1]) << 8
		}
		shared = draft.Wire(code>>uint(bit%8)) & 7
	}
	u := binary.LittleEndian.Uint64(raw[cells+lo*8:])
	if shared != draft.Bytes {
		return tagCell{num: u}
	}
	start, end := int(uint32(u)), int(uint32(u))+int(u>>32)
	return tagSpan(raw[start:end], false)
}

// Owned, validated indexed-numeric bytes only. The selected length-catalog
// family uses Fixed8 word slots; zero assignment still requires rebuilding.
func classIndexPatch(raw []byte, key, word uint64) bool {
	if word == 0 {
		return false
	}
	t := raw[len(raw)-1]
	n := int(binary.LittleEndian.Uint32(raw[len(raw)-5:]))
	w := 1 << (t >> 6)
	shared := draft.Wire(t>>3) & 7
	codes := 0
	if shared == 7 {
		codes = (n*3 + 7) / 8
	}
	cells := len(raw) - 5 - n*8
	keys := cells - codes - n*w
	lo, hi := 0, n
	for lo < hi {
		m := lo + (hi-lo)/2
		if getWidth(raw[keys+m*w:], w) < key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == n || getWidth(raw[keys+lo*w:], w) != key {
		return false
	}
	if shared == 7 {
		bit := lo * 3
		pos := keys + n*w + bit/8
		code := uint16(raw[pos])
		if bit%8 > 5 {
			code |= uint16(raw[pos+1]) << 8
		}
		shared = draft.Wire(code>>uint(bit%8)) & 7
	}
	if shared != draft.Fixed8 {
		return false
	}
	binary.LittleEndian.PutUint64(raw[cells+lo*8:], word)
	return true
}

func TestLengthIndexedPatch(t *testing.T) {
	e := classEncoder{rawLengths: true, indexedRoot: true}
	raw := e.object(nil, Obj(Field{1, U(1)}, Field{2, S("opaque")}), 0)
	if !classIndexPatch(raw, 1, math.MaxUint64) || classIndexGet(raw, 1).uint() != math.MaxUint64 {
		t.Fatal("word patch")
	}
	before := string(raw)
	if classIndexPatch(raw, 1, 0) || classIndexPatch(raw, 2, 4) || classIndexPatch(raw, 3, 4) || string(raw) != before {
		t.Fatal("unsupported patch changed bytes")
	}
}
func BenchmarkLengthIndexedPatch(b *testing.B) {
	e := classEncoder{rawLengths: true, indexedRoot: true}
	raw := e.object(nil, numeric(128, 0), 0)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !classIndexPatch(raw, uint64(i%128+1), uint64(i+1)) {
			b.Fatal("patch")
		}
	}
	sinkBytes = raw
}
func checkClassTree(t testing.TB, want Value, raw []byte, rawLengths ...bool) {
	t.Helper()
	it := classIter(raw, rawLengths...)
	check := func(w Value, c tagCell) {
		t.Helper()
		switch w.Kind {
		case Record, Dict, List:
			if tagZero(w) {
				if c.uint() != 0 {
					t.Fatal("empty")
				}
			} else {
				checkClassTree(t, w, c.raw(), rawLengths...)
			}
		case Text:
			if string(c.raw()) != w.S {
				t.Fatal("text")
			}
		case Blob:
			if string(c.raw()) != string(w.B) {
				t.Fatal("blob")
			}
		default:
			if c.uint() != draftValue(w, draft.Compact).Uint() {
				t.Fatal("word")
			}
		}
	}
	switch want.Kind {
	case Record:
		for _, x := range want.Fields {
			if tagZero(x.Value) {
				continue
			}
			k, _, c, ok := it.next()
			if !ok || k != x.Key {
				t.Fatal("key")
			}
			check(x.Value, c)
		}
	case Dict:
		for _, x := range want.Entries {
			if tagZero(x.Value) {
				continue
			}
			_, k, c, ok := it.next()
			if !ok || string(k) != x.Key {
				t.Fatal("string key")
			}
			check(x.Value, c)
		}
	case List:
		for _, x := range want.Items {
			_, _, c, ok := it.next()
			if !ok {
				t.Fatal("position")
			}
			check(x, c)
		}
	}
	if _, _, _, ok := it.next(); ok {
		t.Fatal("count")
	}
}
func TestClassCatalog(t *testing.T) {
	t.Log("fixture draft-compact class-catalog draft-index-root class-index-root")
	for name, v := range tagExtraFixtures() {
		var e classEncoder
		raw := e.object(nil, v, 0)
		checkClassTree(t, v, raw)
		e.indexedRoot = true
		indexed := e.object(nil, v, 0)
		if v.Kind == Record {
			for _, x := range v.Fields {
				c := classIndexGet(indexed, x.Key)
				if x.Value.Kind == Record || x.Value.Kind == Dict || x.Value.Kind == List {
					checkClassTree(t, x.Value, c.raw())
				} else if x.Value.Kind == Text {
					if string(c.raw()) != x.Value.S {
						t.Fatal("indexed text")
					}
				} else if x.Value.Kind == Blob {
					if string(c.raw()) != string(x.Value.B) {
						t.Fatal("indexed blob")
					}
				} else if c.uint() != draftValue(x.Value, draft.Compact).Uint() {
					t.Fatal("indexed word")
				}
			}
		}
		t.Logf("%s %d %d %d %d", name, len(draftObject(v, draft.Compact, draft.Compact)), len(raw), len(draftObject(v, draft.Indexed, draft.Compact)), len(indexed))
	}
}

func TestClassCatalogGolden(t *testing.T) {
	var e classEncoder
	v := Obj(Field{1, U(7)}, Field{2, S("hi")})
	got := e.object(nil, v, 0)
	want := []byte{7, 'h', 'i', 0x17, 0x0a, 0x0e, 2, 0x20}
	if string(got) != string(want) {
		t.Fatalf("got %x want %x", got, want)
	}
	e.rawLengths = true
	got = e.object(nil, v, 0)
	want = []byte{7, 'h', 'i', 0x2f, 0x11, 0x19, 2, 0x20}
	if string(got) != string(want) {
		t.Fatalf("length catalog got %x want %x", got, want)
	}
}

func TestRawLengthCatalog(t *testing.T) {
	t.Log("fixture draft class-catalog raw-length-catalog raw-index-root")
	all := tagExtraFixtures()
	all["scalar-edges"] = Array(U(0), U(math.MaxUint64), I(math.MinInt64), I(math.MaxInt64), Value{Kind: Float, U: 1}, Value{Kind: Float, U: 0x7ff8000000000001})
	all["keys-edges"] = Obj(Field{0, U(1 << 63)}, Field{15, U(1 << 24)}, Field{16, U(1 << 48)}, Field{math.MaxUint64, S("opaque")})
	for name, v := range all {
		old := classEncoder{}
		e := classEncoder{rawLengths: true}
		bytes := e.object(nil, v, 0)
		checkClassTree(t, v, bytes, true)
		indexedSize := 0
		if v.Kind == Record {
			e.indexedRoot = true
			indexed := e.object(nil, v, 0)
			indexedSize = len(indexed)
			for _, x := range v.Fields {
				got := classIndexGet(indexed, x.Key)
				switch x.Value.Kind {
				case Record, Dict, List:
					checkClassTree(t, x.Value, got.raw(), true)
				case Text:
					if string(got.raw()) != x.Value.S {
						t.Fatal("indexed text")
					}
				case Blob:
					if string(got.raw()) != string(x.Value.B) {
						t.Fatal("indexed blob")
					}
				default:
					if got.uint() != draftValue(x.Value, draft.Compact).Uint() {
						t.Fatal("indexed scalar")
					}
				}
			}
		}
		t.Logf("%s %d %d %d %d", name, len(draftObject(v, draft.Compact, draft.Compact)), len(old.object(nil, v, 0)), len(bytes), indexedSize)
	}
}

func TestLengthOpaqueRelocation(t *testing.T) {
	e := classEncoder{rawLengths: true}
	opaque := []byte{0xff, 0x00, 0xfe} // deliberately not a valid child object
	raw := e.object(nil, Obj(Field{1, U(7)}, Field{2, Value{Kind: Blob, B: opaque}}), 0)
	it := classIter(raw, true)
	it.next() // Change only the known first field.
	u := uint64(1 << 55)
	n := (bits.Len64(u) + 7) / 8
	out := tagAppendWord(nil, u, n)
	out = append(out, it.payload...)
	cat := len(out)
	out = append(out, 2<<4|15) // two entries, mixed byte lengths
	out = binary.AppendUvarint(out, 1<<4|uint64(n))
	out = append(out, it.meta...) // unknown key/class/length copied opaquely
	out = tagAppendTail(out, uint64(len(out)-cat)<<3)
	if classGet(out, 1, true).uint() != u || string(classGet(out, 2, true).raw()) != string(opaque) {
		t.Fatal("unknown value lost during relocation")
	}
}
func BenchmarkRawLengthCatalog(b *testing.B) {
	all := fixtures()
	all["deep16"] = tagDeep(16, 32768)
	for _, name := range []string{"profile", "state", "settings", "transaction", "dictionary-128", "blob-32k", "deep16"} {
		v := all[name]
		b.Run(name, func(b *testing.B) {
			e := classEncoder{rawLengths: true}
			dst := e.object(nil, v, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = e.object(dst[:0], v, 0)
			}
			sinkBytes = dst
		})
	}
	e := classEncoder{rawLengths: true}
	raw := e.object(nil, numeric(128, 0), 0)
	b.Run("numeric128/get", func(b *testing.B) {
		b.ReportAllocs()
		var sum uint64
		for i := 0; i < b.N; i++ {
			sum += classGet(raw, query(i, 128), true).uint()
		}
		sinkU = sum
	})
	e.indexedRoot = true
	indexed := e.object(nil, numeric(128, 0), 0)
	b.Run("numeric128/index-get", func(b *testing.B) {
		b.ReportAllocs()
		var sum uint64
		for i := 0; i < b.N; i++ {
			sum += classIndexGet(indexed, query(i, 128)).uint()
		}
		sinkU = sum
	})
}

func FuzzRawLengthCatalog(f *testing.F) {
	for _, v := range fixtures() {
		e := classEncoder{rawLengths: true}
		f.Add(e.object(nil, v, 0))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		defer func() {
			if p := recover(); p != nil && p != errTag {
				panic(p)
			}
		}()
		if len(raw) > tagMaxBytes {
			return
		}
		it := classIter(raw, true)
		for {
			if _, _, _, ok := it.next(); !ok {
				return
			}
		}
	})
}

func FuzzClassCatalog(f *testing.F) {
	for _, v := range fixtures() {
		var e classEncoder
		f.Add(e.object(nil, v, 0))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		defer func() {
			if p := recover(); p != nil && p != errTag {
				panic(p)
			}
		}()
		if len(raw) > tagMaxBytes {
			return
		}
		it := classIter(raw)
		var last string
		for {
			_, k, _, ok := it.next()
			if !ok {
				return
			}
			if it.kind == 1 && it.index > 1 && string(k) <= last {
				return
			}
			last = string(k)
		}
	})
}

func BenchmarkClassCatalog(b *testing.B) {
	for _, name := range []string{"profile", "state", "settings", "transaction", "dictionary-128", "blob-32k"} {
		v := fixtures()[name]
		b.Run(name+"/encode", func(b *testing.B) {
			var e classEncoder
			dst := e.object(nil, v, 0)
			checkClassTree(b, v, dst)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dst = e.object(dst[:0], v, 0)
			}
			sinkBytes = dst
		})
	}
	v := numeric(128, 0)
	var e classEncoder
	raw := e.object(nil, v, 0)
	b.Run("numeric128/get", func(b *testing.B) {
		b.ReportAllocs()
		var sum uint64
		for i := 0; i < b.N; i++ {
			sum += classGet(raw, query(i, 128)).uint()
		}
		sinkU = sum
	})
	e.indexedRoot = true
	index := e.object(nil, v, 0)
	b.Run("numeric128/index-get", func(b *testing.B) {
		b.ReportAllocs()
		var sum uint64
		for i := 0; i < b.N; i++ {
			sum += classIndexGet(index, query(i, 128)).uint()
		}
		sinkU = sum
	})
	b.Run("numeric128/index-encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			index = e.object(index[:0], v, 0)
		}
		sinkBytes = index
	})
	deep := tagDeep(16, 32768)
	e.indexedRoot = false
	dst := e.object(nil, deep, 0)
	b.Run("deep16/encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			dst = e.object(dst[:0], deep, 0)
		}
		sinkBytes = dst
	})
}
