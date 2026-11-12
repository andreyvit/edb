package kvolab

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"unsafe"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

func checkLengthValue(t testing.TB, want Value, got lengthValue) {
	t.Helper()
	if tagZero(want) {
		if !got.IsZero() {
			t.Fatalf("zero: %+v", got)
		}
		return
	}
	switch want.Kind {
	case Record, Dict, List:
		checkLengthTree(t, want, got.Bytes())
	case Text:
		if string(got.Bytes()) != want.S {
			t.Fatal("string")
		}
	case Blob:
		if !bytes.Equal(got.Bytes(), want.B) {
			t.Fatal("blob")
		}
	default:
		if got.Uint() != draftValue(want, draft.Compact).Uint() {
			t.Fatalf("word %x != %x", got.Uint(), draftValue(want, draft.Compact).Uint())
		}
	}
}
func checkLengthTree(t testing.TB, want Value, raw []byte) {
	t.Helper()
	view, err := lengthOpen(raw)
	if err != nil {
		t.Fatalf("open %x: %v", raw, err)
	}
	it := view.Iter()
	count := 0
	check := func(key uint64, text string, x Value) {
		if want.Kind != List && tagZero(x) {
			return
		}
		k, s, value, ok := it.Next()
		if !ok {
			t.Fatal("short iterator")
		}
		if want.Kind == Record && k != key || want.Kind == Dict && string(s) != text || want.Kind == List && k != uint64(count) {
			t.Fatal("key")
		}
		checkLengthValue(t, x, value)
		switch want.Kind {
		case Record:
			checkLengthValue(t, x, view.Get(key))
		case Dict:
			checkLengthValue(t, x, view.GetString(text))
		case List:
			checkLengthValue(t, x, view.At(count))
		}
		count++
	}
	switch want.Kind {
	case Record:
		for _, f := range want.Fields {
			check(f.Key, "", f.Value)
		}
	case Dict:
		for _, p := range want.Entries {
			check(0, p.Key, p.Value)
		}
	case List:
		for _, v := range want.Items {
			check(0, "", v)
		}
	}
	if count != view.Len() {
		t.Fatal("count")
	}
	if _, _, _, ok := it.Next(); ok {
		t.Fatal("extra iterator")
	}
}
func TestLengthCommon(t *testing.T) {
	all := fixtures()
	all["zeros"] = Obj(Field{0, U(0)}, Field{1, U(7)}, Field{2, Obj(Field{1, U(0)})}, Field{3, Value{Kind: List, Items: []Value{U(0), S(""), Obj(Field{1, U(0)}), U(9)}}})
	all["keys"] = Obj(Field{0, U(2)}, Field{math.MaxUint64, U(3)})
	all["scalars"] = Value{Kind: List, Items: []Value{U(0), U(math.MaxUint64), I(math.MinInt64), I(math.MaxInt64), Value{Kind: Float, U: math.Float64bits(math.SmallestNonzeroFloat64)}, Value{Kind: Float, U: 0x7ff0000000000001}}}
	policies := []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d%2 == 0 }, func(d int, _ Value) bool { return d%2 == 1 }}
	for name, v := range all {
		for i, policy := range policies {
			t.Run(fmt.Sprintf("%s/%d", name, i), func(t *testing.T) {
				var w lengthWriter
				raw := lengthEncode(&w, nil, v, policy)
				checkLengthTree(t, v, raw)
				if policy == nil {
					e := classEncoder{rawLengths: true}
					if !bytes.Equal(raw, e.object(nil, v, 0)) {
						t.Fatal("compact bytes changed")
					}
				}
				reused := lengthEncode(&w, w.out[:0], v, policy)
				checkLengthTree(t, v, reused)

			})
		}
	}
	if unsafe.Sizeof(lengthView{}) != 16 || unsafe.Sizeof(lengthValue{}) != 16 {
		t.Fatal("not two words")
	}
}
func TestLengthMalformedAndPatch(t *testing.T) {
	source := Obj(Field{1, U(7)}, Field{2, S("opaque")}, Field{3, U(9)})
	dict := Value{Kind: Dict, Entries: []Entry{{"a", U(7)}, {"b", S("opaque")}, {"c", U(9)}}}
	list := Value{Kind: List, Items: []Value{U(7), S("opaque"), U(9)}}
	for _, v := range []Value{source, dict, list} {
		var w lengthWriter
		raw := lengthEncode(&w, nil, v, func(int, Value) bool { return true })
		view, err := lengthOpen(raw)
		if err != nil {
			t.Fatal(err)
		}
		key, text := uint64(1), "a"
		if v.Kind == List {
			key = 0
		}
		if !view.Patch(key, text, 123) {
			t.Fatal("patch")
		}
		if _, err = lengthOpen(raw); err != nil {
			t.Fatal("patch invalidated", err)
		}
		original := bytes.Clone(raw)
		if view.Patch(2, "b", 42) && v.Kind != List {
			t.Fatal("patched a span")
		}
		if v.Kind != List && !bytes.Equal(original, raw) {
			t.Fatal("failed patch changed bytes")
		}
		if v.Kind != List && view.Patch(key, text, 0) {
			t.Fatal("stored map zero")
		}
		if v.Kind == List && !view.Patch(0, "", 0) {
			t.Fatal("list zero")
		}
		// Truncating an indexed trailer while retaining its tag must fail.
		for n := 0; n < len(raw)-5; n++ {
			broken := append(bytes.Clone(raw[:n]), raw[len(raw)-5:]...)
			if _, err = lengthOpen(broken); err == nil {
				t.Fatalf("accepted truncated %x", broken)
			}
		}
		bad := bytes.Clone(raw)
		bad[len(bad)-1] |= 32
		if _, err = lengthOpen(bad); err == nil {
			t.Fatal("reserved bit")
		}
		bad = bytes.Clone(raw)
		binary.LittleEndian.PutUint32(bad[len(bad)-5:], math.MaxUint32)
		if _, err = lengthOpen(bad); err == nil {
			t.Fatal("huge count")
		}
	}
	// Valid outer framing deliberately preserves an uninterpretable span.
	var w lengthWriter
	raw := lengthEncode(&w, nil, Obj(Field{1, Value{Kind: Blob, B: []byte{255, 255, 255}}}), nil)
	if _, err := lengthOpen(raw); err != nil {
		t.Fatal("interpreted opaque payload")
	}
}
func FuzzLengthCommon(f *testing.F) {
	for _, v := range fixtures() {
		for _, indexed := range []bool{false, true} {
			var w lengthWriter
			f.Add(lengthEncode(&w, nil, v, func(int, Value) bool { return indexed }))
		}
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			return
		}
		view, err := lengthOpen(raw)
		if err != nil {
			return
		}
		it := view.Iter()
		i := 0
		for {
			k, s, value, ok := it.Next()
			if !ok {
				break
			}
			if i%(view.Len()/16+1) != 0 {
				i++
				continue
			}
			var got lengthValue
			switch view.Kind() {
			case 0:
				got = view.Get(k)
			case 1:
				got = view.GetString(string(s))
			case 2:
				got = view.At(i)
			}
			if value.ptr == nil {
				if got.Uint() != value.Uint() {
					t.Fatal("word")
				}
			} else if !bytes.Equal(got.Bytes(), value.Bytes()) {
				t.Fatal("bytes")
			}
			i++
		}
		if i != view.Len() {
			t.Fatal("len")
		}
	})
}

func BenchmarkLengthCommon(b *testing.B) {
	for _, n := range []int{8, 32, 128, 512} {
		source := numeric(n, 0)
		var w lengthWriter
		raw := lengthEncode(&w, nil, source, nil)
		view, err := lengthOpen(raw)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("numeric%d/get", n), func(b *testing.B) {
			b.ReportAllocs()
			var sum uint64
			for i := 0; i < b.N; i++ {
				sum += view.Get(query(i, n)).Uint()
			}
			sinkU = sum
		})
		b.Run(fmt.Sprintf("numeric%d/validate", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, err = lengthOpen(raw)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("numeric%d/scan", n), func(b *testing.B) {
			b.ReportAllocs()
			var sum uint64
			for i := 0; i < b.N; i++ {
				it := view.Iter()
				for {
					_, _, v, ok := it.Next()
					if !ok {
						break
					}
					sum += v.Uint()
				}
			}
			sinkU = sum
		})
		indexed := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
		idx, _ := lengthOpen(indexed)
		b.Run(fmt.Sprintf("numeric%d/index-get", n), func(b *testing.B) {
			b.ReportAllocs()
			var sum uint64
			for i := 0; i < b.N; i++ {
				sum += idx.Get(query(i, n)).Uint()
			}
			sinkU = sum
		})
	}
	all := fixtures()
	all["deep16"] = tagDeep(16, 32768)
	for _, name := range []string{"profile", "state", "settings", "transaction", "dictionary-128", "blob-32k", "deep16"} {
		source := all[name]
		for _, cold := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/cold%v", name, cold), func(b *testing.B) {
				var w lengthWriter
				dst := lengthEncode(&w, nil, source, nil)
				checkLengthTree(b, source, dst)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if cold {
						w = lengthWriter{}
						dst = nil
					}
					dst = lengthEncode(&w, dst[:0], source, nil)
				}
				sinkBytes = dst
			})
		}
	}
}

func TestLengthCommonSizes(t *testing.T) {
	for _, name := range []string{"profile", "state", "settings", "transaction", "dictionary-128", "blob-32k"} {
		source := fixtures()[name]
		var w lengthWriter
		compact := lengthEncode(&w, nil, source, nil)
		indexed := lengthEncode(&w, nil, source, func(d int, _ Value) bool { return d == 0 })
		old := draftObject(source, draft.Indexed, draft.Compact)
		t.Logf("%s compact=%d indexed-root=%d old-indexed-root=%d", name, len(compact), len(indexed), len(old))
	}
}

func TestLengthLastLargeEntry(t *testing.T) {
	// Entry offsets do not need to encode the end of the payload. A final large
	// span must not widen every directory entry when all starts remain small.
	source := Array(U(7), Value{Kind: Blob, B: bytes.Repeat([]byte{3}, 1<<20)})
	var w lengthWriter
	raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
	h := lengthIndex(raw)
	if h.width != 1 {
		t.Fatal("widened offsets because of last span")
	}
	checkLengthTree(t, source, raw)
}
