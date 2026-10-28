package kvolab

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

func checkTagTree(t testing.TB, want Value, raw []byte, f tagFormat, catalog bool, fused ...bool) {
	t.Helper()
	it := tagIter(raw, f, catalog, fused...)
	if it.kind != tagContainerKind(want.Kind) {
		t.Fatal("container kind")
	}
	check := func(w Value, c tagCell) {
		t.Helper()
		switch w.Kind {
		case Uint, True, False, Null:
			u := w.U
			if w.Kind == True {
				u = 1
			}
			if c.uint() != u {
				t.Fatalf("unsigned: got %d, want %d", c.uint(), u)
			}
		case Int:
			if c.sint(f) != int64(w.U) {
				t.Fatalf("signed: got %d, want %d", c.sint(f), int64(w.U))
			}
		case Float:
			if math.Float64frombits(w.U) != 0 && c.uint() != w.U {
				t.Fatal("float bits")
			}
		case Text:
			if string(c.raw()) != w.S {
				t.Fatal("text")
			}
		case Blob:
			if !bytes.Equal(c.raw(), w.B) {
				t.Fatal("blob")
			}
		default:
			if tagZero(w) {
				if c.uint() != 0 {
					t.Fatal("empty container")
				}
				return
			}
			checkTagTree(t, w, c.raw(), f, catalog, fused...)
		}
	}
	switch want.Kind {
	case Record:
		for _, x := range want.Fields {
			if tagZero(x.Value) {
				continue
			}
			key, _, c, ok := it.next()
			if !ok || key != x.Key {
				t.Fatal("field key")
			}
			check(x.Value, c)
		}
	case Dict:
		for _, x := range want.Entries {
			if tagZero(x.Value) {
				continue
			}
			_, key, c, ok := it.next()
			if !ok || string(key) != x.Key {
				t.Fatal("string key")
			}
			check(x.Value, c)
		}
	case List:
		for i, x := range want.Items {
			key, _, c, ok := it.next()
			if !ok || key != uint64(i) {
				t.Fatal("list position")
			}
			check(x, c)
		}
	}
	if _, _, _, ok := it.next(); ok {
		t.Fatal("extra entries")
	}
}

func tagExtraFixtures() map[string]Value {
	m := fixtures()
	for _, n := range []int{8, 128, 512} {
		m[fmt.Sprintf("numeric-%d", n)] = numeric(n, 0)
	}
	for name, base := range map[string]uint64{"small-ints": 1, "u16": 1 << 10, "u32": 1 << 27, "u64": 1 << 57} {
		v := Array()
		for i := 0; i < 128; i++ {
			v.Items = append(v.Items, U(base+uint64(i%127)))
		}
		m["list-"+name] = v
	}
	for _, n := range []int{16, 63, 64, 103, 104} {
		v := Array()
		for range 128 {
			v.Items = append(v.Items, S(strings.Repeat("x", n)))
		}
		m[fmt.Sprintf("list-bytes-%d", n)] = v
	}
	for name, first := range map[string]int64{"negative-small": -1, "negative-mid": -17, "negative-large": -(1 << 31)} {
		v := Array()
		for i := 0; i < 128; i++ {
			v.Items = append(v.Items, I(first-int64(i%16)))
		}
		m["list-"+name] = v
	}
	return m
}
func TestTagFixtures(t *testing.T) {
	m := tagExtraFixtures()
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	t.Log("fixture draft-compact length unified split tuned tuned-catalog fused-inline fused-catalog")
	for _, name := range names {
		v := m[name]
		sizes := []int{len(draftObject(v, draft.Compact, draft.Compact))}
		for f := tagLength; f <= tagTuned; f++ {
			var first []byte
			for strategy := tagBuffered; strategy <= tagCatalog; strategy++ {
				e := tagEncoder{format: f, strategy: strategy}
				raw := e.object(nil, v, 0)
				checkTagTree(t, v, raw, f, strategy == tagCatalog)
				if strategy == tagBuffered {
					sizes = append(sizes, len(raw))
					first = raw
				} else if strategy == tagBackpatch && !bytes.Equal(first, raw) {
					t.Fatal("construction strategies differ")
				} else if strategy == tagCatalog && f == tagTuned {
					sizes = append(sizes, len(raw))
				}
			}
		}
		for _, strategy := range []tagStrategy{tagBuffered, tagCatalog} {
			e := tagEncoder{format: tagTuned, strategy: strategy, fuseKeys: true}
			raw := e.object(nil, v, 0)
			checkTagTree(t, v, raw, tagTuned, strategy == tagCatalog, true)
			sizes = append(sizes, len(raw))
		}
		t.Logf("%s %v", name, sizes)
	}
}
func TestTagEdges(t *testing.T) {
	v := Array(U(0), U(1), U(127), U(128), U(255), U(256), U(math.MaxUint64),
		I(-1), I(-16), I(-17), I(-63), I(-64), I(-127), I(-128), I(-129),
		I(127), I(128), I(255), I(256), I(math.MinInt64), I(math.MaxInt64),
		Value{Kind: Float, U: math.Float64bits(math.Pi)},
		Value{Kind: Float, U: 0x7ff8000000000001}, Value{Kind: Float, U: 1 << 63},
		S(""), Obj(), Map(), Array())
	for _, n := range []int{1, 8, 30, 31, 62, 63, 64, 102, 103, 104, 127, 128, 255, 4096} {
		v.Items = append(v.Items, S(strings.Repeat("z", n)))
	}
	for f := tagLength; f <= tagTuned; f++ {
		for strategy := tagBuffered; strategy <= tagCatalog; strategy++ {
			e := tagEncoder{format: f, strategy: strategy}
			checkTagTree(t, v, e.object(nil, v, 0), f, strategy == tagCatalog)
			zeros := Obj(Field{0, U(0)}, Field{1, U(7)}, Field{math.MaxUint64, I(-9)})
			checkTagTree(t, zeros, e.object(nil, zeros, 0), f, strategy == tagCatalog)
		}
	}
	for _, key := range []uint64{0, 31, 32, 127, 1 << 62, math.MaxUint64} {
		for _, val := range []Value{U(1), U(256), U(1 << 30), U(1 << 63), I(-32769)} {
			for _, strategy := range []tagStrategy{tagBuffered, tagCatalog} {
				e := tagEncoder{format: tagTuned, strategy: strategy, fuseKeys: true}
				x := Obj(Field{key, val})
				checkTagTree(t, x, e.object(nil, x, 0), tagTuned, strategy == tagCatalog, true)
			}
		}
	}
}
func TestTagRandom(t *testing.T) {
	r := rand.New(rand.NewPCG(27, 61))
	for range 100 {
		v := Array()
		for range 128 {
			if r.IntN(2) == 0 {
				v.Items = append(v.Items, U(r.Uint64()))
			} else {
				v.Items = append(v.Items, I(int64(r.Uint64())))
			}
		}
		for f := tagLength; f <= tagTuned; f++ {
			for strategy := tagBuffered; strategy <= tagCatalog; strategy++ {
				e := tagEncoder{format: f, strategy: strategy}
				checkTagTree(t, v, e.object(nil, v, 0), f, strategy == tagCatalog)
			}
		}
	}
}

// No nested interpretation is needed to move unknown fields. Retain the exact
// body and metadata, even when the bytes are not a valid child object.
func TestTagOpaqueCopy(t *testing.T) {
	v := Obj(Field{1, U(7)}, Field{2, Value{Kind: Blob, B: []byte{0xff, 0xfe, 0xfd}}})
	for f := tagLength; f <= tagTuned; f++ {
		e := tagEncoder{format: f, strategy: tagCatalog}
		raw := e.object(nil, v, 0)
		it := tagIter(raw, f, true)
		it.next()
		_, _, got, _ := it.next()
		if !bytes.Equal(got.raw(), v.Fields[1].Value.B) {
			t.Fatal("opaque bytes")
		}
		// Enlarge the first scalar, copying the second field's physical
		// metadata and payload verbatim without interpreting its model.
		it = tagIter(raw, f, true)
		_, _, _, _ = it.next()
		remainingMeta, remainingPayload := it.meta, it.payload
		var out, meta []byte
		u, n, immediate, code := tagScalar(U(1<<55), f)
		meta = append(meta, 1) // unchanged first key delta
		if immediate {
			meta = append(meta, code)
		} else {
			meta = tagAppendHead(meta, f, n, true)
			out = tagAppendWord(out, u, n)
		}
		out = append(out, remainingPayload...)
		meta = append(meta, remainingMeta...)
		cat := len(out)
		out = append(out, 2)
		out = append(out, meta...)
		out = tagAppendTail(out, uint64(len(out)-cat)<<3)
		if tagGet(out, f, true, 1).uint() != 1<<55 || !bytes.Equal(tagGet(out, f, true, 2).raw(), got.raw()) {
			t.Fatal("opaque relocation")
		}
	}
}

func tagValidate(b []byte, f tagFormat, catalog bool, fused ...bool) (err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != errTag {
				panic(p)
			}
			err = errTag
		}
	}()
	if len(b) > tagMaxBytes {
		return errTag
	}
	it := tagIter(b, f, catalog, fused...)
	var previous []byte
	for {
		_, key, _, ok := it.next()
		if !ok {
			return nil
		}
		if it.kind == 1 && it.index > 1 && bytes.Compare(previous, key) >= 0 {
			return errTag
		}
		previous = key
	}
}
func FuzzTagParse(f *testing.F) {
	for _, v := range fixtures() {
		for codec := tagLength; codec <= tagTuned; codec++ {
			for _, strategy := range []tagStrategy{tagBuffered, tagCatalog} {
				e := tagEncoder{format: codec, strategy: strategy}
				f.Add(e.object(nil, v, 0), byte(codec), strategy == tagCatalog)
				e.fuseKeys = true
				f.Add(e.object(nil, v, 0), byte(codec)|4, strategy == tagCatalog)
			}
		}
	}
	f.Fuzz(func(t *testing.T, raw []byte, kind byte, catalog bool) {
		_ = tagValidate(raw, tagFormat(kind%4), catalog, kind&4 != 0)
	})
}

// Complete baseline construction: child payload arenas and field/pair/value
// slices are reused. No reflection or freshly allocated intermediary tree is
// charged to this baseline; the source tree is the same for every candidate.
type tagDraftLevel struct {
	children []byte
	fields   []draft.Field
	pairs    []draft.Pair
	values   []draft.Value
}
type tagDraftEncoder struct{ levels []tagDraftLevel }

func (e *tagDraftEncoder) object(dst []byte, v Value, depth int) []byte {
	for len(e.levels) <= depth {
		e.levels = append(e.levels, tagDraftLevel{})
	}
	level := e.levels[depth]
	level.children = level.children[:0]
	level.fields, level.pairs, level.values = level.fields[:0], level.pairs[:0], level.values[:0]
	value := func(v Value) draft.Value {
		switch v.Kind {
		case Record, Dict, List:
			start := len(level.children)
			level.children = e.object(level.children, v, depth+1)
			return draft.ObjectValue(draft.Trusted(level.children[start:]))
		case Text:
			// Fixture strings are immutable. The production-like binder borrows
			// their bytes rather than allocating []byte for each field.
			return draft.ByteString(tagStringBytes(v.S))
		default:
			return draftValue(v, draft.Compact)
		}
	}
	switch v.Kind {
	case Record:
		for _, x := range v.Fields {
			level.fields = append(level.fields, draft.Field{Key: x.Key, Value: value(x.Value)})
		}
		dst = draft.AppendNumeric(dst, level.fields, draft.Compact)
	case Dict:
		for _, x := range v.Entries {
			level.pairs = append(level.pairs, draft.Pair{Key: x.Key, Value: value(x.Value)})
		}
		dst = draft.AppendDictionary(dst, level.pairs, draft.Compact)
	case List:
		for _, x := range v.Items {
			level.values = append(level.values, value(x))
		}
		dst = draft.AppendList(dst, level.values, draft.Compact)
	}
	e.levels[depth] = level
	return dst
}

func tagDeep(depth, size int) Value {
	v := Value{Kind: Blob, B: bytes.Repeat([]byte{0x65}, size)}
	for range depth {
		v = Obj(Field{1, U(1)}, Field{2, v})
	}
	return v
}
func TestTagCopyAccounting(t *testing.T) {
	for _, depth := range []int{1, 4, 16} {
		v := tagDeep(depth, 32768)
		for _, strategy := range []tagStrategy{tagBuffered, tagBackpatch, tagCatalog} {
			a := tagAccounting{}
			e := tagEncoder{format: tagTuned, strategy: strategy, account: &a}
			raw := e.object(nil, v, 0)
			checkTagTree(t, v, raw, tagTuned, strategy == tagCatalog)
			scratch := 0
			for _, l := range e.levels {
				scratch += cap(l.scratch)
			}
			t.Logf("depth=%d strategy=%d output=%d leaf=%d child-copied=%d moved=%d metadata-copied=%d scratch-cap=%d", depth, strategy, len(raw), a.leafBytes, a.childCopied, a.headerMoved, a.metadataCopied, scratch)
			if strategy == tagCatalog && (a.childCopied != 0 || a.headerMoved != 0) {
				t.Fatal("catalog moved payload")
			}
		}
	}
}

func TestTagGolden(t *testing.T) {
	v := Obj(Field{1, U(7)}, Field{2, S("hi")})
	e := tagEncoder{format: tagTuned, strategy: tagBuffered}
	a := e.object(nil, v, 0)
	if !bytes.Equal(a, []byte{1, 7, 1, 0x82, 'h', 'i', 16}) {
		t.Fatalf("inline: %x", a)
	}
	e.strategy = tagCatalog
	a = e.object(nil, v, 0)
	if !bytes.Equal(a, []byte{'h', 'i', 2, 1, 7, 1, 0x82, 40}) {
		t.Fatalf("catalog: %x", a)
	}
	// Append footer backwards, read it backwards, preserving the prefix.
	for _, u := range []uint64{0, 1, 127, 128, 16383, 16384, math.MaxUint64} {
		b := tagAppendTail([]byte{0x55}, u)
		got, body := tagTail(b)
		if got != u || !bytes.Equal(body, []byte{0x55}) {
			t.Fatal("footer")
		}
	}
}
