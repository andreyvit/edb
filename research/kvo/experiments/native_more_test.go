package kvolab

import (
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type nativeText string
type nativeCode uint16
type nativeBinary []byte
type nativeAggregate struct {
	Text   nativeText                   `edb:"text"`
	Binary nativeBinary                 `edb:"binary"`
	Groups map[nativeText][]uint64      `edb:"groups"`
	Ptrs   map[nativeCode]*nativeDetail `edb:"ptrs"`
	Fixed  [3]int64                     `edb:"fixed"`
	Tiny   int8                         `edb:"tiny"`
	Small  float32                      `edb:"small"`
	Empty  Optional[string]             `edb:"empty"`
	Values Optional[[]uint64]           `edb:"values"`
}

var nativeAggregateType = func() *kvo.EntityType {
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "aggregate", reflect.TypeFor[nativeAggregate]())
	specs := []struct {
		name string
		typ  kvo.AnyType
	}{
		{"text", kvo.TMachineString}, {"binary", kvo.TBytes}, {"groups", kvo.Map(kvo.TMachineString, kvo.ListOf(kvo.TUint64))},
		{"ptrs", kvo.Map(kvo.TUint64, nativeTypes[1])}, {"fixed", kvo.ListOf(kvo.TInt64)}, {"tiny", kvo.TInt64},
		{"small", kvo.NewFloatType[float32]("f32", func(*kvo.FmtContext, float32) string { return "" })},
		{"empty", kvo.TMachineString}, {"values", kvo.ListOf(kvo.TUint64)},
	}
	for i, p := range specs {
		kvo.NewProp(s, uint64(i+1), p.name, p.typ, nil)
	}
	kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) {
		for i := uint64(1); i <= 9; i++ {
			if i >= 8 {
				b.Prop(i, kvo.PresenceIsSignificant)
			} else if i == 3 || i == 4 {
				b.Prop(i, kvo.KeepDictionaryZeroValues)
			} else {
				b.Prop(i)
			}
		}
	})
	return typ
}()

func TestNativeAggregateOwnershipAndReuse(t *testing.T) {
	src := nativeAggregate{Text: "named", Binary: nativeBinary{1, 2, 3}, Groups: map[nativeText][]uint64{"one": {1, 2}, "two": {3, 4}, "empty": nil}, Ptrs: map[nativeCode]*nativeDetail{1: {Delta: 1}, 2: {Delta: 2}, 3: nil}, Fixed: [3]int64{0, -1, 128}, Tiny: -128, Small: 1.5, Empty: Optional[string]{Present: true}, Values: Optional[[]uint64]{Present: true}}
	for _, fast := range []bool{false, true} {
		c := bindNative[nativeAggregate](nativeAggregateType)
		raw := c.Put(nil, &src, fast)
		got := c.Get(currentTrusted(raw))
		if !reflect.DeepEqual(*got, src) {
			t.Fatalf("aggregate mismatch: %#v", got)
		}
		for i := 0; i < 3; i++ {
			c.GetFast(currentTrusted(raw), got)
		}
		got.Groups["one"][0] = 99
		got.Ptrs[1].Delta = 99
		if got.Groups["two"][0] != 3 || got.Ptrs[2].Delta != 2 || src.Groups["one"][0] != 1 || src.Ptrs[1].Delta != 1 {
			t.Fatal("mutable map entries aliased")
		}
		before := bytes.Clone(raw)
		got.Binary[0] = 99
		if !bytes.Equal(raw, before) {
			t.Fatal("binary alias")
		}
	}
}

func TestNativeLookupPresenceAndNarrowing(t *testing.T) {
	c := bindNative[nativeAggregate](nativeAggregateType)
	for _, fast := range []bool{false, true} {
		src := nativeAggregate{Tiny: 127, Small: float32(math.Inf(1)), Empty: Optional[string]{Present: true}, Values: Optional[[]uint64]{Present: true}}
		raw := c.Put(nil, &src, fast)
		var out nativeAggregate
		c.GetFastKeys(currentTrusted(raw), &out)
		if out.Tiny != 127 || !out.Empty.Present || !out.Values.Present || !math.IsInf(float64(out.Small), 1) {
			t.Fatal("lookup contract")
		}
		c.GetFastKeys(currentView{}, &out)
		if out.Tiny != 0 || out.Empty.Present || out.Values.Present {
			t.Fatal("absent lookup state")
		}
		var w currentWriter
		w.Begin(0, "", 0, fast)
		w.Put(6, "", currentInt(128))
		w.End()
		func() {
			defer func() {
				if e := recover(); e != draft.ErrValue {
					t.Fatalf("narrowing panic=%v", e)
				}
			}()
			c.Get(currentTrusted(w.BytesWritten()))
		}()
	}
}

type nativeRecursive struct {
	Value uint64           `edb:"value"`
	Child *nativeRecursive `edb:"child"`
}

var nativeRecursiveType = func() *kvo.EntityType {
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "recursive", reflect.TypeFor[nativeRecursive]())
	kvo.NewProp(s, 1, "value", kvo.TUint64, nil)
	kvo.NewProp(s, 2, "child", typ, nil)
	kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) { b.Prop(1); b.Prop(2) })
	return typ
}()

func TestNativeRecursivePlans(t *testing.T) {
	c := bindNative[nativeRecursive](nativeRecursiveType)
	src := nativeRecursive{Value: 1, Child: &nativeRecursive{Value: 2}}
	raw := c.Put(nil, &src, true)
	if !reflect.DeepEqual(src, *c.Get(currentTrusted(raw))) {
		t.Fatal("recursive binding")
	}
	src.Child = &src
	defer func() {
		if e := recover(); e != draft.ErrLimit {
			t.Fatalf("cycle panic=%v", e)
		}
	}()
	c.Put(nil, &src, true)
}

func nativeAggregateMapSample() nativeAggregate {
	src := nativeAggregate{Groups: map[nativeText][]uint64{}, Ptrs: map[nativeCode]*nativeDetail{}}
	for i := 0; i < 32; i++ {
		src.Groups[nativeText(string(rune('a'+i)))] = []uint64{1, 2, 3}
		src.Ptrs[nativeCode(i)] = &nativeDetail{Delta: int64(i + 1)}
	}
	return src
}

func TestNativeMapStorageReuse(t *testing.T) {
	c := bindNative[nativeAggregate](nativeAggregateType)
	src := nativeAggregateMapSample()
	raw := c.Put(nil, &src, true)
	view := currentTrusted(raw)
	var out nativeAggregate
	c.GetFast(view, &out)
	c.GetFast(view, &out)
	for i := 0; i < 32; i++ {
		if out.Ptrs[nativeCode(i)].Delta != int64(i+1) || len(out.Groups[nativeText(string(rune('a'+i)))]) != 3 {
			t.Fatal("wrong reused value")
		}
	}
	var independent nativeAggregate
	c.GetFast(view, &independent)
	independent.Groups["a"][0] = 99
	independent.Ptrs[0].Delta = 99
	if out.Groups["a"][0] != 1 || out.Ptrs[0].Delta != 1 {
		t.Fatal("codec retained and shared mutable storage")
	}
	delete(src.Groups, "a")
	delete(src.Ptrs, 0)
	src.Groups["new"] = []uint64{4, 5}
	src.Ptrs[99] = &nativeDetail{Delta: 99}
	changed := c.Put(nil, &src, true)
	c.GetFast(currentTrusted(changed), &out)
	if _, ok := out.Groups["a"]; ok {
		t.Fatal("stale key")
	}
	if _, ok := out.Ptrs[0]; ok {
		t.Fatal("stale pointer key")
	}
	if !reflect.DeepEqual(src, out) {
		t.Fatal("insert/delete map reuse mismatch")
	}
}

func BenchmarkNativeAggregateMapValues(b *testing.B) {
	src := nativeAggregateMapSample()
	c := bindNative[nativeAggregate](nativeAggregateType)
	raw := c.Put(nil, &src, true)
	for _, mode := range []string{"reuse", "sorted", "rebuild"} {
		b.Run(mode, func(b *testing.B) {
			var out nativeAggregate
			c.GetFast(currentTrusted(raw), &out)
			c.GetFast(currentTrusted(raw), &out)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if mode == "rebuild" {
					c.GetFastRebuild(currentTrusted(raw), &out)
				} else if mode == "sorted" {
					c.GetFastSortedMaps(currentTrusted(raw), &out)
				} else {
					c.GetFast(currentTrusted(raw), &out)
				}
			}
			sinkU = uint64(len(out.Groups))
		})
	}
}

func TestNativeMapAllocations(t *testing.T) {
	c := bindNative[nativeAggregate](nativeAggregateType)
	src := nativeAggregateMapSample()
	raw := c.Put(nil, &src, true)
	view := currentTrusted(raw)
	var out nativeAggregate
	c.GetFast(view, &out)
	c.GetFast(view, &out)
	if n := testing.AllocsPerRun(100, func() { c.GetFast(view, &out) }); n != 0 {
		t.Fatalf("reused aggregate maps: %v allocs", n)
	}
}

func FuzzNativeMapReuse(f *testing.F) {
	f.Add("one", "two", int64(7), []byte{0, 1, 2})
	f.Add("", "old", int64(-129), []byte{})
	f.Fuzz(func(t *testing.T, a, b string, n int64, data []byte) {
		if len(a) > 512 || len(b) > 512 || len(data) > 64 {
			return
		}
		c := bindNative[nativeAggregate](nativeAggregateType)
		old := nativeAggregate{Groups: map[nativeText][]uint64{"old": {1, 2}, "two": {3}}, Ptrs: map[nativeCode]*nativeDetail{1: {Delta: 1}, 2: {Delta: 2}}}
		next := nativeAggregate{Groups: map[nativeText][]uint64{}, Ptrs: map[nativeCode]*nativeDetail{nativeCode(n): {Delta: n | 1}}}
		var values []uint64
		for _, x := range data {
			values = append(values, uint64(x))
		}
		next.Groups[nativeText(a)] = values
		next.Groups[nativeText(b)] = []uint64{uint64(n)}
		for _, fast := range []bool{false, true} {
			before := c.Put(nil, &old, fast)
			after := c.Put(nil, &next, fast)
			var out nativeAggregate
			c.GetFast(currentTrusted(before), &out)
			c.GetFast(currentTrusted(after), &out)
			if !reflect.DeepEqual(out, next) {
				t.Fatal("map update")
			}
			c.GetFast(currentTrusted(before), &out)
			if !reflect.DeepEqual(out, old) {
				t.Fatal("map restore")
			}
		}
	})
}
