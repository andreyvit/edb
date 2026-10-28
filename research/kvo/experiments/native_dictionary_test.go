package kvolab

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/andreyvit/edb/kvo"
	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type nativeDictionaryRow struct {
	Drop         map[string]int64            `edb:"drop"`
	Keep         map[string]int64            `edb:"keep"`
	DropNumbers  map[uint64]int64            `edb:"drop_numbers"`
	KeepNumbers  map[uint64]int64            `edb:"keep_numbers"`
	DropChildren map[string][]uint64         `edb:"drop_children"`
	KeepChildren map[string][]uint64         `edb:"keep_children"`
	OptionalDrop Optional[map[string]int64]  `edb:"optional_drop"`
	OptionalKeep Optional[map[string]int64]  `edb:"optional_keep"`
	Nested       map[string]map[string]int64 `edb:"nested"`
	KeepPtr      *map[string]int64           `edb:"keep_ptr"`
}

type nativeDictionaryEdit struct {
	Drop map[string]int64 `edb:"drop"`
	Keep map[string]int64 `edb:"keep"`
}

var nativeDictionaryType = func() *kvo.EntityType {
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "dictionary_policies", reflect.TypeFor[nativeDictionaryRow](), reflect.TypeFor[nativeDictionaryEdit]())
	texts := kvo.Map(kvo.TMachineString, kvo.TInt64)
	numbers := kvo.Map(kvo.TUint64, kvo.TInt64)
	children := kvo.Map(kvo.TMachineString, kvo.ListOf(kvo.TUint64))
	props := []struct {
		name  string
		typ   kvo.AnyType
		flags kvo.PropFlag
	}{
		{"drop", texts, 0}, {"keep", texts, kvo.KeepDictionaryZeroValues},
		{"drop_numbers", numbers, 0}, {"keep_numbers", numbers, kvo.KeepDictionaryZeroValues},
		{"drop_children", children, 0}, {"keep_children", children, kvo.KeepDictionaryZeroValues},
		{"optional_drop", texts, kvo.PresenceIsSignificant},
		{"optional_keep", texts, kvo.PresenceIsSignificant | kvo.KeepDictionaryZeroValues},
		{"nested", kvo.Map(kvo.TMachineString, texts), kvo.KeepDictionaryZeroValues},
		{"keep_ptr", texts, kvo.KeepDictionaryZeroValues},
	}
	for i, p := range props {
		kvo.NewProp(s, uint64(i+1), p.name, p.typ, nil)
	}
	kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) {
		for i, p := range props {
			b.Prop(uint64(i+1), p.flags)
		}
	})
	return typ
}()

func dictionaryPolicySample() nativeDictionaryRow {
	words := map[string]int64{"": 0, "large": 128, "negative": -129}
	numbers := map[uint64]int64{0: 0, 256: 128, math.MaxUint64: -1}
	children := map[string][]uint64{"empty": nil, "values": {0, 7, 0}}
	return nativeDictionaryRow{
		Drop: words, Keep: words, DropNumbers: numbers, KeepNumbers: numbers,
		DropChildren: children, KeepChildren: children,
		OptionalDrop: Optional[map[string]int64]{Value: map[string]int64{"zero": 0}, Present: true},
		OptionalKeep: Optional[map[string]int64]{Value: map[string]int64{"zero": 0}, Present: true},
		Nested:       map[string]map[string]int64{"child": {"zero": 0}}, KeepPtr: &words,
	}
}

func checkDictionaryPolicyRow(t *testing.T, got *nativeDictionaryRow) {
	t.Helper()
	want := dictionaryPolicySample()
	want.Drop = map[string]int64{"large": 128, "negative": -129}
	want.DropNumbers = map[uint64]int64{256: 128, math.MaxUint64: -1}
	want.DropChildren = map[string][]uint64{"values": {0, 7, 0}}
	want.OptionalDrop.Value = nil
	want.Nested = map[string]map[string]int64{"child": nil}
	// Empty and nil collections are equivalent, including reused storage.
	if len(got.OptionalDrop.Value) == 0 {
		got.OptionalDrop.Value = nil
	}
	if len(got.Nested["child"]) == 0 {
		got.Nested["child"] = nil
	}
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("dictionary policy mismatch\n got: %#v\nwant: %#v", *got, want)
	}
}

func TestNativeDictionaryPolicies(t *testing.T) {
	for _, fast := range []bool{false, true} {
		t.Run(fmt.Sprint(fast), func(t *testing.T) {
			c := bindNative[nativeDictionaryRow](nativeDictionaryType)
			src := dictionaryPolicySample()
			raw := c.Put(nil, &src, fast)
			view, err := currentOpen(raw)
			if err != nil {
				t.Fatal(err)
			}
			checkDictionaryPolicyRow(t, c.Get(view))
			// Every decoding path applies the policy, including mutable-child reuse.
			for _, decode := range []func(currentView, *nativeDictionaryRow){c.GetFast, c.GetFastKeys, c.GetFastScan, c.GetFastSortedMaps, c.GetFastRebuild} {
				var got nativeDictionaryRow
				for i := 0; i < 3; i++ {
					decode(view, &got)
					checkDictionaryPolicyRow(t, &got)
				}
			}
			// Explicit zeros from an older/retaining writer are hidden by default,
			// even though schema-free readers can still preserve their raw bytes.
			var w currentWriter
			w.Begin(0, "", 0, fast)
			it := view.Iter()
			for {
				key, _, value, ok := it.Next()
				if !ok {
					break
				}
				if key == 1 || key == 3 || key == 5 {
					value = view.Get(key + 1)
				}
				w.PutPresent(key, "", value)
			}
			w.End()
			older := w.BytesWritten()
			before := bytes.Clone(older)
			var got nativeDictionaryRow
			for i := 0; i < 3; i++ {
				c.GetFast(currentTrusted(older), &got)
				checkDictionaryPolicyRow(t, &got)
			}
			if !bytes.Equal(c.Put(nil, &got, fast), raw) || !bytes.Equal(before, older) {
				t.Fatal("normalization changed source or canonical output")
			}
			// Projection Patch replaces only declared maps, using their own policy.
			edit := nativeDictionaryEdit{Drop: map[string]int64{"zero": 0}, Keep: map[string]int64{"zero": 0}}
			patched := bindNative[nativeDictionaryEdit](nativeDictionaryType).Patch(nil, view, &edit)
			pv := currentTrusted(patched)
			pg := c.Get(pv)
			if len(pg.Drop) != 0 || len(pg.Keep) != 1 || !reflect.DeepEqual(pg.KeepChildren, src.KeepChildren) {
				t.Fatal("patch policy or undeclared sibling preservation")
			}
			if _, present := currentLookup(pv, 1, ""); present {
				t.Fatal("empty ordinary dictionary property retained")
			}
			// Neither entry flag implies property presence; the independent flag does.
			empty := nativeDictionaryRow{OptionalDrop: Optional[map[string]int64]{Present: true}, OptionalKeep: Optional[map[string]int64]{Present: true}}
			ev := currentTrusted(c.Put(nil, &empty, fast))
			if ev.Len() != 2 {
				t.Fatal("empty property presence")
			}
			for _, key := range []uint64{7, 8} {
				value, present := currentLookup(ev, key, "")
				if !present || !value.IsZero() {
					t.Fatal("optional empty dictionary lost")
				}
			}
		})
	}
}

// Handwritten generated-wrapper equivalents: the schema chooses the signature,
// not the native Go map type. A generator can emit these delegations directly.
type dictionaryCounts struct {
	view   currentView
	policy currentDictionaryPolicy
}

func (d dictionaryCounts) Get(key string) int64 { return d.policy.Get(d.view, 0, key).Int() }

type dictionaryMembershipCounts struct {
	view   currentView
	policy currentDictionaryPolicy
}

func (d dictionaryMembershipCounts) Get(key string) (int64, bool) {
	v, ok := d.policy.Lookup(d.view, 0, key)
	return v.Int(), ok
}

func TestDictionaryKeyAndNativeAgreement(t *testing.T) {
	for _, fast := range []bool{false, true} {
		for _, keys := range [][2]uint64{{1, 2}, {3, 4}} {
			var w currentWriter
			w.Begin(0, "", 0, fast)
			for _, key := range keys {
				p := bindCurrentDictionary(nativeDictionaryType.Model(), key)
				w.Begin(key, "", p.kind, fast)
				p.Put(&w, 0, "", currentInt(0))
				p.Put(&w, 256, "large", currentInt(128))
				p.Put(&w, math.MaxUint64, "negative", currentInt(-129))
				w.End()
			}
			w.End()
			view := currentTrusted(w.BytesWritten())
			c := bindNative[nativeDictionaryRow](nativeDictionaryType)
			native := c.Get(view)
			if !bytes.Equal(c.Put(nil, native, fast), view.Raw()) {
				t.Fatal("key writer and native encoder disagree")
			}
			for _, key := range keys {
				p := bindCurrentDictionary(nativeDictionaryType.Model(), key)
				child := currentTrusted(view.Get(key).Bytes())
				if p.Get(child, 256, "large").Int() != 128 || p.Get(child, 1, "missing").Int() != 0 {
					t.Fatal("key access")
				}
				if p.keepZero {
					if v, present := p.Lookup(child, 0, ""); !present || !v.IsZero() {
						t.Fatal("retained zero lookup")
					}
					if _, present := p.Lookup(child, 1, "missing"); present {
						t.Fatal("missing lookup")
					}
				}
				// Retaining input under an ordinary schema has the same enumeration
				// as native mapping, without altering the raw schema-free iterator.
				retained := currentTrusted(view.Get(keys[1]).Bytes())
				it := p.Iter(retained)
				n := 0
				for {
					_, _, _, ok := it.Next()
					if !ok {
						break
					}
					n++
				}
				if n != 2+btoi(p.keepZero) {
					t.Fatal("schema-aware enumeration")
				}
				e := p.Edit(retained, nil, nil)
				p.Set(&e, 256, "large", currentInt(0))
				p.Delete(&e, math.MaxUint64, "negative")
				if p.keepZero {
					if v, present := p.LookupEdit(&e, 256, "large"); !present || !v.IsZero() {
						t.Fatal("read after set zero")
					}
					if _, present := p.LookupEdit(&e, math.MaxUint64, "negative"); present {
						t.Fatal("read after delete")
					}
				}
				out := p.Append(&e, &currentWriter{}, nil, fast)
				ov, err := currentOpen(out)
				if err != nil || ov.Len() != 2*btoi(p.keepZero) {
					t.Fatal("edit materialization", err, ov.Len())
				}
				// The schema provides the kind when the property was absent.
				fresh := p.Edit(currentView{}, nil, nil)
				p.Set(&fresh, 0, "", currentInt(0))
				p.Set(&fresh, 256, "large", currentInt(128))
				fv := currentTrusted(p.Append(&fresh, &currentWriter{}, nil, fast))
				if fv.Kind() != p.kind || fv.Len() != 1+btoi(p.keepZero) || p.Get(fv, 256, "large").Int() != 128 {
					t.Fatal("creation from an absent dictionary")
				}
			}
		}
		c := bindNative[nativeDictionaryRow](nativeDictionaryType)
		src := dictionaryPolicySample()
		view := currentTrusted(c.Put(nil, &src, fast))
		d := dictionaryCounts{currentTrusted(view.Get(1).Bytes()), bindCurrentDictionary(nativeDictionaryType.Model(), 1)}
		k := dictionaryMembershipCounts{currentTrusted(view.Get(2).Bytes()), bindCurrentDictionary(nativeDictionaryType.Model(), 2)}
		if d.Get("") != 0 {
			t.Fatal("ordinary getter")
		}
		if value, present := k.Get(""); value != 0 || !present {
			t.Fatal("presence-aware getter")
		}
	}
}

func TestDictionaryPolicyErrors(t *testing.T) {
	p := bindCurrentDictionary(nativeDictionaryType.Model(), 1)
	for _, f := range []func(){
		func() { p.Lookup(currentView{}, 0, "") },
		func() { bindCurrentDictionary(nativeDictionaryType.Model(), 99) },
		func() { bindCurrentDictionary(nativeTypes[0].Model(), 1) },
	} {
		func() {
			defer func() {
				if err := recover(); err != draft.ErrValue && err != draft.ErrUnknownKey && err != draft.ErrType {
					t.Fatalf("panic = %v", err)
				}
			}()
			f()
		}()
	}
}

func TestDictionaryLookupMatchesDirectory(t *testing.T) {
	for _, size := range []int{1, 300, 70_000} {
		for _, mode := range []int{0, 1, 2} {
			var w currentWriter
			w.Begin(0, "", 1, true)
			for i, key := range []string{"", "a", "z", "\xff"} {
				value := currentWord(uint64(i))
				if mode == 1 || mode == 2 && i%2 != 0 {
					value = currentBytes(bytes.Repeat([]byte{byte(i)}, size))
				}
				w.PutPresent(0, key, value)
			}
			w.End()
			v, err := currentOpen(w.BytesWritten())
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"", "a", "b", "z", "zz", "\xff", "\xffx"} {
				got, present := currentLookup(v, 0, key)
				want, expected := currentDictionaryLookupDirectory(v, key)
				if got != want || present != expected {
					t.Fatal("presence search differs", size, mode, key)
				}
			}
		}
	}
}

type nativeDictionaryValues[T any] struct {
	Items map[string]T `edb:"items"`
}

func nativeDictionaryValueType[T any](itemType kvo.AnyType, keep bool) *kvo.EntityType {
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "dictionary_values", reflect.TypeFor[nativeDictionaryValues[T]]())
	kvo.NewProp(s, 1, "items", kvo.Map(kvo.TMachineString, itemType), nil)
	kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) {
		if keep {
			b.Prop(1, kvo.KeepDictionaryZeroValues)
		} else {
			b.Prop(1)
		}
	})
	return typ
}

func checkNativeDictionaryValue[T any](t *testing.T, itemType kvo.AnyType, zero, nonzero T) {
	t.Helper()
	for _, fast := range []bool{false, true} {
		for _, keep := range []bool{false, true} {
			typ := nativeDictionaryValueType[T](itemType, keep)
			c := bindNative[nativeDictionaryValues[T]](typ)
			src := nativeDictionaryValues[T]{Items: map[string]T{"a": zero, "b": nonzero}}
			raw := c.Put(nil, &src, fast)
			var got nativeDictionaryValues[T]
			for i := 0; i < 3; i++ {
				c.GetFast(currentTrusted(raw), &got)
				_, present := got.Items["a"]
				if present != keep || len(got.Items) != 1+btoi(keep) || !reflect.DeepEqual(got.Items["b"], nonzero) {
					t.Fatalf("%v fast=%v keep=%v: %#v", itemType, fast, keep, got)
				}
			}
			// A dictionary consisting only of ordinary zeros disappears entirely.
			delete(src.Items, "b")
			v := currentTrusted(c.Put(nil, &src, fast))
			_, present := currentLookup(v, 1, "")
			if present != keep {
				t.Fatal("all-zero dictionary normalization")
			}
		}
	}
}

func TestNativeDictionaryZeroKinds(t *testing.T) {
	checkNativeDictionaryValue(t, kvo.TInt64, int64(0), int64(-129))
	checkNativeDictionaryValue(t, kvo.TUint64, uint64(0), uint64(math.MaxUint64))
	checkNativeDictionaryValue(t, kvo.TBool, false, true)
	checkNativeDictionaryValue(t, kvo.NewFloatType[float64]("float", func(*kvo.FmtContext, float64) string { return "" }), math.Copysign(0, -1), 1.5)
	checkNativeDictionaryValue(t, kvo.TTime, time.Time{}, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	checkNativeDictionaryValue(t, kvo.TMachineString, "", "text")
	checkNativeDictionaryValue(t, kvo.TBytes, []byte{}, []byte{0, 1})
	checkNativeDictionaryValue(t, nativeTypes[1], nativeDetail{}, nativeDetail{Delta: -1})
	checkNativeDictionaryValue(t, nativeTypes[1], &nativeDetail{}, &nativeDetail{Delta: -1})
	checkNativeDictionaryValue(t, kvo.ListOf(kvo.TUint64), []uint64{}, []uint64{0, 0})
}

func FuzzDictionaryPolicy(f *testing.F) {
	f.Add("", "other", int64(0), int64(-129))
	f.Add("same", "same", int64(128), int64(0))
	f.Fuzz(func(t *testing.T, a, b string, x, y int64) {
		if len(a)+len(b) > 1024 {
			return
		}
		for _, fast := range []bool{false, true} {
			for _, keep := range []bool{false, true} {
				typ := nativeDictionaryValueType[int64](kvo.TInt64, keep)
				c := bindNative[nativeDictionaryValues[int64]](typ)
				src := nativeDictionaryValues[int64]{Items: map[string]int64{a: x}}
				src.Items[b] = y
				raw := c.Put(nil, &src, fast)
				var out nativeDictionaryValues[int64]
				for i := 0; i < 2; i++ {
					c.GetFast(currentTrusted(raw), &out)
				}
				want := map[string]int64{}
				for key, value := range src.Items {
					if keep || value != 0 {
						want[key] = value
					}
				}
				if len(out.Items) != len(want) {
					t.Fatal("membership count")
				}
				for key, value := range want {
					if got, ok := out.Items[key]; !ok || got != value {
						t.Fatal("value or membership")
					}
				}
				if !bytes.Equal(raw, c.Put(nil, &out, fast)) {
					t.Fatal("canonical round trip")
				}
			}
		}
	})
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
