package kvolab

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type nativeDetail struct {
	Enabled bool   `edb:"enabled"`
	Delta   int64  `edb:"delta"`
	Memo    string `edb:"memo"`
}
type nativeRow struct {
	ID             uint64                  `edb:"-"`
	Count          int64                   `edb:"count"`
	Title          string                  `edb:"title"`
	Ratio          float64                 `edb:"ratio"`
	Updated        time.Time               `edb:"updated"`
	Labels         map[string]string       `edb:"labels,compact"`
	Counters       map[uint64]uint64       `edb:"counters"`
	Items          []nativeDetail          `edb:"items"`
	Detail         nativeDetail            `edb:"detail,compact"`
	Choice         Optional[int64]         `edb:"choice"`
	OptionalDetail Optional[nativeDetail]  `edb:"optional_detail"`
	Values         []int64                 `edb:"values"`
	Payload        []byte                  `edb:"payload"`
	Ref            *nativeDetail           `edb:"ref"`
	Extra          map[string]nativeDetail `edb:"extra"`
	Cache          any                     `edb:"-"`
}
type nativeSummary struct {
	Count int64  `edb:"count"`
	Title string `edb:"title"`
}
type nativeEdit struct {
	Count  int64           `edb:"count"`
	Choice Optional[int64] `edb:"choice"`
	Detail nativeDetail    `edb:"detail"`
}

var nativeTypes = func() []*kvo.EntityType {
	s := kvo.NewSchema()
	detail := kvo.NewEntityType(s, "detail", reflect.TypeFor[nativeDetail]())
	row := kvo.NewEntityType(s, "row", reflect.TypeFor[nativeRow](), reflect.TypeFor[nativeSummary](), reflect.TypeFor[nativeEdit]())
	fl := kvo.NewFloatType[float64]("float", func(*kvo.FmtContext, float64) string { return "" })
	props := []struct {
		key  uint64
		name string
		typ  kvo.AnyType
	}{
		{1, "count", kvo.TInt64}, {2, "title", kvo.TMachineString}, {3, "ratio", fl}, {4, "updated", kvo.TTime},
		{5, "labels", kvo.Map(kvo.TMachineString, kvo.TMachineString)}, {6, "counters", kvo.Map(kvo.TUint64, kvo.TUint64)},
		{7, "items", kvo.ListOf(detail)}, {8, "detail", detail}, {9, "choice", kvo.TInt64}, {10, "optional_detail", detail},
		{11, "values", kvo.ListOf(kvo.TInt64)}, {12, "payload", kvo.TBytes}, {13, "ref", detail}, {14, "extra", kvo.Map(kvo.TMachineString, detail)},
		{101, "enabled", kvo.TBool}, {102, "delta", kvo.TInt64}, {103, "memo", kvo.TMachineString},
	}
	for _, p := range props {
		kvo.NewProp(s, p.key, p.name, p.typ, nil)
	}
	kvo.NewModel(s, detail, func(b *kvo.ModelBuilder) { b.Prop(101); b.Prop(102); b.Prop(103) })
	kvo.NewModel(s, row, func(b *kvo.ModelBuilder) {
		for i := uint64(1); i <= 14; i++ {
			if i == 9 || i == 10 {
				b.Prop(i, kvo.PresenceIsSignificant)
			} else if i == 5 || i == 6 {
				// This fixture deliberately exercises retained zero membership.
				b.Prop(i, kvo.KeepDictionaryZeroValues)
			} else {
				b.Prop(i)
			}
		}
	})
	return []*kvo.EntityType{row, detail}
}()

func nativeSample() nativeRow {
	return nativeRow{
		Count: -129, Title: "sample text", Ratio: 1.25, Updated: time.Date(2025, 1, 2, 3, 4, 5, 6000, time.UTC),
		Labels: map[string]string{"alpha": "", "beta": "text"}, Counters: map[uint64]uint64{0: 0, 1: 128, math.MaxUint64: math.MaxUint64},
		Items: []nativeDetail{{true, -1, "a"}, {}, {Delta: 128, Memo: "b"}}, Detail: nativeDetail{true, 255, "cold"},
		Choice: Optional[int64]{Value: 0, Present: true}, OptionalDetail: Optional[nativeDetail]{Present: true},
		Values: []int64{0, -1, 127, 128, 255, -129, math.MinInt64, math.MaxInt64}, Payload: []byte{0, 1, 0, 255},
		Ref: &nativeDetail{Delta: -128}, Extra: map[string]nativeDetail{"one": {Delta: 1}, "two": {Delta: 2}},
	}
}

func TestNativeRoundTrip(t *testing.T) {
	for _, fast := range []bool{false, true} {
		c := bindNative[nativeRow](nativeTypes[0])
		src := nativeSample()
		raw := c.Put(nil, &src, fast)
		view, err := currentOpen(raw)
		if err != nil {
			t.Fatal(err)
		}
		got := c.Get(view)
		if !reflect.DeepEqual(src, *got) {
			t.Fatalf("fast=%v\nsrc=%#v\ngot=%#v", fast, src, *got)
		}
		if !got.Choice.Present || !got.OptionalDetail.Present {
			t.Fatal("present zero lost")
		}
		if currentTrusted(view.Get(5).Bytes()).Indexed() || currentTrusted(view.Get(8).Bytes()).Indexed() {
			t.Fatal("compact annotation ignored")
		}
		for i := 0; i < 3; i++ {
			c.GetFast(view, got)
			if !reflect.DeepEqual(src, *got) {
				t.Fatal("reuse changed value")
			}
		}
		encoded := c.Put(nil, got, fast)
		if !bytes.Equal(raw, encoded) {
			t.Fatal("noncanonical native round trip")
		}
		small := nativeRow{Count: 1}
		smallRaw := c.Put(nil, &small, fast)
		got.Cache = "keep excluded field"
		c.GetFast(currentTrusted(smallRaw), got)
		if got.Count != 1 || got.Title != "" || len(got.Labels) != 0 || len(got.Items) != 0 || len(got.Payload) != 0 || got.Choice.Present || got.OptionalDetail.Present || got.Ref != nil || len(got.Extra) != 0 || got.Cache != "keep excluded field" {
			t.Fatalf("stale fields: %#v", got)
		}
	}
}

func TestNativePatchProjectionAndUnknowns(t *testing.T) {
	for _, fast := range []bool{false, true} {
		c := bindNative[nativeRow](nativeTypes[0])
		src := nativeSample()
		base := c.Put(nil, &src, fast)
		var w currentWriter
		var e currentLeafMerger
		unknown := []currentLeaf{{[]perfStep{{kind: 0, key: 90}}, currentInt(-129)}, {[]perfStep{{kind: 0, key: 91}}, currentBytes([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})}, {[]perfStep{{kind: 0, key: 92}}, currentValue{}}}
		base = e.Append(&w, nil, currentTrusted(base), unknown, []bool{true, true, true}, fast)
		before := bytes.Clone(base)
		edit := nativeEdit{Count: 0, Detail: nativeDetail{Memo: "new"}, Choice: Optional[int64]{}}
		ec := bindNative[nativeEdit](nativeTypes[0])
		out := ec.Patch(nil, currentTrusted(base), &edit)
		if !bytes.Equal(before, base) {
			t.Fatal("base mutated")
		}
		v := currentTrusted(out)
		got := c.Get(v)
		if got.Count != 0 || got.Title != src.Title || got.Detail.Memo != "new" || got.Detail.Delta != 0 || got.Detail.Enabled || got.Choice.Present {
			t.Fatalf("projection patch: %#v", got)
		}
		if !reflect.DeepEqual(got.Extra, src.Extra) {
			t.Fatal("unrepresented map lost")
		}
		if v.Get(90).Int() != -129 || !bytes.Equal(v.Get(91).Bytes(), unknown[1].value.Bytes()) {
			t.Fatal("unknown changed")
		}
		foundZero := false
		it := v.Iter()
		for {
			key, _, value, ok := it.Next()
			if !ok {
				break
			}
			if key == 92 {
				foundZero = value.IsZero()
			}
		}
		if !foundZero {
			t.Fatal("unknown present zero lost")
		}
		put := ec.Put(nil, &edit, fast)
		if currentTrusted(put).Get(2).num != 0 || currentTrusted(put).Get(91).num != 0 {
			t.Fatal("Put preserved undeclared properties")
		}
		edit.Choice = Optional[int64]{Present: true}
		out = ec.Patch(nil, currentTrusted(base), &edit)
		if !c.Get(currentTrusted(out)).Choice.Present {
			t.Fatal("patch zero became absence")
		}
	}
}

func TestNativeOwnership(t *testing.T) {
	c := bindNative[nativeRow](nativeTypes[0])
	src := nativeSample()
	raw := c.Put(nil, &src, true)
	view := currentTrusted(raw)
	own := c.Get(view)
	var borrowed nativeRow
	c.GetFast(view, &borrowed)
	rawString := view.Get(2).Bytes()
	if unsafe.StringData(borrowed.Title) != unsafe.SliceData(rawString) {
		t.Fatal("GetFast did not borrow text")
	}
	if unsafe.StringData(own.Title) == unsafe.SliceData(rawString) {
		t.Fatal("Get returned borrowed text")
	}
	before := bytes.Clone(raw)
	borrowed.Payload[0] = 77
	if !bytes.Equal(before, raw) {
		t.Fatal("writable binary data aliased row")
	}
	oldTitle := own.Title
	rawString[0] = 'X' // owned test buffer, never a Bolt mmap
	if own.Title != oldTitle || borrowed.Title[0] != 'X' {
		t.Fatal("string ownership contract")
	}
}

func TestNativeFastAndWriterAllocations(t *testing.T) {
	c := bindNative[nativeRow](nativeTypes[0])
	src := nativeSample()
	src.Extra = nil
	raw := c.Put(nil, &src, true)
	v := currentTrusted(raw)
	var out nativeRow
	c.GetFast(v, &out)
	if n := testing.AllocsPerRun(100, func() { c.GetFast(v, &out) }); n != 0 {
		t.Fatalf("reused GetFast: %v allocs", n)
	}
	dst := make([]byte, 0, len(raw)*2)
	dst = c.Put(dst[:0], &src, true)
	if n := testing.AllocsPerRun(100, func() { dst = c.Put(dst[:0], &src, true) }); n != 0 {
		t.Fatalf("reused Put: %v allocs", n)
	}
	ec := bindNative[nativeSummary](nativeTypes[0])
	edit := nativeSummary{Count: 2, Title: "changed"}
	dst = ec.Patch(dst[:0], v, &edit)
	if n := testing.AllocsPerRun(100, func() { dst = ec.Patch(dst[:0], v, &edit) }); n != 0 {
		t.Fatalf("reused Patch: %v allocs", n)
	}
}

func TestNativeBindingTypeErrors(t *testing.T) {
	type bad struct {
		Count string `edb:"count"`
	}
	type badPresence struct {
		Choice int64 `edb:"choice"`
	}
	type badOptional struct {
		Count Optional[int64] `edb:"count"`
	}
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "bad", reflect.TypeFor[bad](), reflect.TypeFor[badPresence](), reflect.TypeFor[badOptional]())
	kvo.NewProp(s, 1, "count", kvo.TInt64, nil)
	kvo.NewProp(s, 2, "choice", kvo.TInt64, nil)
	kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) { b.Prop(1); b.Prop(2, kvo.PresenceIsSignificant) })
	for _, f := range []func(){func() { bindNative[bad](typ) }, func() { bindNative[badPresence](typ) }, func() { bindNative[badOptional](typ) }, func() { bindNative[struct{}](typ) }} {
		func() {
			defer func() {
				e, ok := recover().(error)
				if !ok || !errors.Is(e, kvo.ErrNativeMapping) {
					t.Fatalf("panic=%v", e)
				}
			}()
			f()
		}()
	}
	c := bindNative[nativeRow](nativeTypes[0])
	for _, f := range []func(){func() { c.GetFast(currentView{}, nil) }, func() { c.Put(nil, nil, true) }} {
		func() {
			defer func() {
				if e := recover(); e != draft.ErrValue {
					t.Fatalf("panic=%v", e)
				}
			}()
			f()
		}()
	}
}

func FuzzNativeMapping(f *testing.F) {
	f.Add(int64(-129), "short", []byte{0, 1, 2}, true)
	f.Add(int64(math.MaxInt64), "", []byte{}, false)
	f.Fuzz(func(t *testing.T, n int64, s string, data []byte, present bool) {
		if len(s) > 4096 || len(data) > 4096 {
			return
		}
		c := bindNative[nativeRow](nativeTypes[0])
		src := nativeRow{Count: n, Title: s, Payload: data, Values: []int64{0, n}, Choice: Optional[int64]{Value: n, Present: present}}
		for _, fast := range []bool{true, false} {
			raw := c.Put(nil, &src, fast)
			v, err := currentOpen(raw)
			if err != nil {
				t.Fatal(err)
			}
			dst := c.Get(v)
			if dst.Count != n || dst.Title != s || !bytes.Equal(dst.Payload, data) || dst.Choice.Present != present || present && dst.Choice.Value != n || dst.Values[1] != n {
				t.Fatal("round trip")
			}
		}
	})
}
