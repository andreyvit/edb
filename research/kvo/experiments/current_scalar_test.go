package kvolab

import (
	"bytes"
	"fmt"
	"maps"
	"math"
	"testing"
	"unsafe"
)

func TestCurrentSignedTags(t *testing.T) {
	t.Logf("iterator bytes: selected=%d, wrapped=%d", unsafe.Sizeof(currentIterator{}), unsafe.Sizeof(currentWrappedIterator{}))
	if unsafe.Sizeof(currentValue{}) != 16 || unsafe.Sizeof(currentView{}) != 16 {
		t.Fatal("values and views must remain two words")
	}
	for _, tc := range []struct {
		n       int64
		payload []byte
		tag     byte
	}{
		{0, nil, 0}, {1, nil, 1}, {63, nil, 63}, {64, nil, 64}, {127, nil, 127},
		{128, []byte{0x80, 0}, 0x82}, {255, []byte{0xff, 0}, 0x82},
		{-1, []byte{0xff}, 0x81}, {-128, []byte{0x80}, 0x81},
		{-129, []byte{0x7f, 0xff}, 0x82},
	} {
		t.Run(fmt.Sprint(tc.n), func(t *testing.T) {
			var w currentWriter
			w.Begin(0, "", 0, false)
			w.PutPresent(1, "", currentInt(tc.n))
			w.End()
			want := append(bytes.Clone(tc.payload), 1, 2, tc.tag, 0x18)
			if !bytes.Equal(w.BytesWritten(), want) {
				t.Fatalf("got %x, want %x", w.BytesWritten(), want)
			}
			v, err := currentOpen(w.BytesWritten())
			if err != nil || v.Get(1).Int() != tc.n {
				t.Fatalf("round trip %d: %v", tc.n, err)
			}
		})
	}
	var w currentWriter
	w.Begin(0, "", 0, false)
	w.Put(1, "", currentWord(128))
	w.End()
	if !bytes.Equal(w.BytesWritten(), []byte{0x80, 1, 2, 0x81, 0x18}) {
		t.Fatal("unsigned width")
	}
	v := currentTrusted(w.BytesWritten()).Get(1)
	if v.Uint() != 128 || v.Int() != -128 {
		t.Fatal("schema must supply signedness")
	}
}

func currentSignedBoundaries() []int64 {
	out := []int64{0, 1, -1, 63, 64, 127, 128, 255, 256, math.MinInt64, math.MaxInt64}
	for width := uint(1); width < 8; width++ {
		x := int64(1) << (8*width - 1)
		out = append(out, x-1, x, x+1, -x-1, -x, -x+1)
	}
	return out
}

func TestCurrentScalarBoundaries(t *testing.T) {
	for _, fast := range []bool{false, true} {
		for kind := 0; kind < 3; kind++ {
			for _, n := range currentSignedBoundaries() {
				var w currentWriter
				w.Begin(0, "", kind, fast)
				w.PutPresent(1, "k", currentInt(n))
				w.End()
				view, err := currentOpen(w.BytesWritten())
				if err != nil {
					t.Fatal(err)
				}
				var value currentValue
				switch kind {
				case 0:
					value = view.Get(1)
				case 1:
					value = view.GetString("k")
				case 2:
					value = view.At(0)
				}
				if value.Int() != n {
					t.Fatalf("fast=%v kind=%d: %d != %d", fast, kind, value.Int(), n)
				}
				if value.Uint() != value.UintBeforeInlining() {
					t.Fatal("outlined scalar accessor changed bits")
				}
				it := view.Iter()
				_, _, v, ok := it.Next()
				if !ok || v.Int() != n {
					t.Fatal("iterator")
				}
				// Rewrite an unknown field without consulting its signed schema,
				// including a Fast-to-Compact change of the containing object.
				var copyWriter currentWriter
				copyWriter.Begin(0, "", kind, false)
				copyWriter.PutPresent(1, "k", value)
				copyWriter.End()
				copyIter := currentTrusted(copyWriter.BytesWritten()).Iter()
				_, _, copied, ok := copyIter.Next()
				if !ok || copied.Int() != n {
					t.Fatalf("opaque copy changed %d to %d", n, copied.Int())
				}
			}
		}
	}
}

func checkCurrentTree(t testing.TB, source Value, raw []byte) {
	t.Helper()
	v, err := currentOpen(raw)
	if err != nil {
		t.Fatal(err)
	}
	it := v.Iter()
	wrapped, fused := v.IterWrapped(), v.IterWrapped()
	count := 0
	check := func(key uint64, text string, x Value) {
		if source.Kind != List && tagZero(x) {
			return
		}
		k, s, value, ok := it.Next()
		wk, ws, wv, wok := wrapped.Next()
		fk, fs, fv, fok := fused.NextFused()
		if k != wk || k != fk || !bytes.Equal(s, ws) || !bytes.Equal(s, fs) || value != wv || value != fv || ok != wok || ok != fok {
			t.Fatal("iterator controls differ")
		}
		if !ok {
			t.Fatal("missing entry")
		}
		if source.Kind == Record && k != key || source.Kind == Dict && string(s) != text || source.Kind == List && k != uint64(count) {
			t.Fatal("key")
		}
		checkCurrentValue(t, x, value)
		switch source.Kind {
		case Record:
			checkCurrentValue(t, x, v.Get(key))
		case Dict:
			checkCurrentValue(t, x, v.GetString(text))
		case List:
			checkCurrentValue(t, x, v.At(count))
		}
		count++
	}
	switch source.Kind {
	case Record:
		for _, f := range source.Fields {
			check(f.Key, "", f.Value)
		}
	case Dict:
		for _, p := range source.Entries {
			check(0, p.Key, p.Value)
		}
	case List:
		for i, x := range source.Items {
			check(uint64(i), "", x)
		}
	}
	if _, _, _, ok := it.Next(); ok || count != v.Len() {
		t.Fatal("count")
	}
	if _, _, _, ok := wrapped.Next(); ok {
		t.Fatal("wrapped count")
	}
	if _, _, _, ok := fused.NextFused(); ok {
		t.Fatal("fused count")
	}
}
func checkCurrentValue(t testing.TB, x Value, v currentValue) {
	t.Helper()
	if tagZero(x) {
		if !v.IsZero() {
			t.Fatal("zero")
		}
		return
	}
	switch x.Kind {
	case Record, Dict, List:
		checkCurrentTree(t, x, v.Bytes())
	case Text:
		if string(v.Bytes()) != x.S {
			t.Fatal("text")
		}
	case Blob:
		if !bytes.Equal(v.Bytes(), x.B) {
			t.Fatal("bytes")
		}
	case Int:
		if v.Int() != int64(x.U) {
			t.Fatalf("signed: %d != %d", v.Int(), int64(x.U))
		}
	case True:
		if !v.Bool() {
			t.Fatal("true")
		}
	default:
		if v.Uint() != x.U {
			t.Fatalf("bits: %x != %x", v.Uint(), x.U)
		}
	}
}

func TestCurrentCodecReplay(t *testing.T) {
	all := maps.Clone(workloadFixtures(t))
	for name, v := range fixtures() {
		all[name] = v
	}
	all["full-key"] = Obj(Field{0, I(-1)}, Field{math.MaxUint64, U(math.MaxUint64)})
	all["limits"] = Array(I(math.MinInt64), I(math.MaxInt64), U(math.MaxUint64), U(0), S(""), Obj())
	for name, source := range all {
		for _, policy := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
			var w currentWriter
			t.Run(name, func(t *testing.T) { checkCurrentTree(t, source, currentEncode(&w, nil, source, policy)) })
		}
	}
}

func TestCurrentSignedWriterAllocations(t *testing.T) {
	for _, fast := range []bool{false, true} {
		var w currentWriter
		dst := make([]byte, 0, 256)
		write := func() {
			w.Reset(dst[:0])
			w.Begin(0, "", 0, fast)
			w.Put(1, "", currentInt(-129))
			w.Put(2, "", currentInt(128))
			w.End()
			dst = w.BytesWritten()
		}
		write()
		if n := testing.AllocsPerRun(100, write); n != 0 {
			t.Fatalf("fast=%v allocations=%g", fast, n)
		}
	}
}
