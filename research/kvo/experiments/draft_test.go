package kvolab

import (
	"fmt"
	"math"
	"sort"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

// Adapt the original generic fixtures so the final framing is measured against
// exactly the same data. This adapter is setup, never timed codec work.
func draftObject(v Value, root, children draft.Layout) []byte {
	var out []byte
	switch v.Kind {
	case Record:
		fields := make([]draft.Field, len(v.Fields))
		for i, f := range v.Fields {
			fields[i] = draft.Field{Key: f.Key, Value: draftValue(f.Value, children)}
		}
		out = draft.AppendNumeric(nil, fields, root)
	case Dict:
		pairs := make([]draft.Pair, len(v.Entries))
		for i, p := range v.Entries {
			pairs[i] = draft.Pair{Key: p.Key, Value: draftValue(p.Value, children)}
		}
		out = draft.AppendDictionary(nil, pairs, root)
	case List:
		values := make([]draft.Value, len(v.Items))
		for i, x := range v.Items {
			values[i] = draftValue(x, children)
		}
		out = draft.AppendList(nil, values, root)
	default:
		panic("expected container")
	}
	return out
}
func draftValue(v Value, layout draft.Layout) draft.Value {
	switch v.Kind {
	case Null:
		return draft.ZeroValue()
	case False, True:
		return draft.Bool(v.Kind == True)
	case Uint:
		return draft.Uint(v.U)
	case Int:
		return draft.Int(int64(v.U))
	case Float:
		return draft.Float(math.Float64frombits(v.U))
	case Text:
		return draft.ByteString([]byte(v.S))
	case Blob:
		return draft.ByteString(v.B)
	default:
		return draft.ObjectValue(draft.Trusted(draftObject(v, layout, layout)))
	}
}
func checkDraftTree(t testing.TB, want Value, raw []byte) {
	t.Helper()
	v, err := draft.Open(raw)
	if err != nil {
		t.Fatal(err)
	}
	check := func(w Value, got draft.Value) {
		t.Helper()
		switch w.Kind {
		case Record, Dict, List:
			b := got.Bytes()
			checkDraftTree(t, w, b)
		default:
			a := draft.AppendValue(nil, draftValue(w, draft.Compact))
			b := draft.AppendValue(nil, got)
			if string(a) != string(b) {
				t.Fatalf("value differs: %x != %x", a, b)
			}
		}
	}
	switch want.Kind {
	case Record:
		expected := 0
		for _, field := range want.Fields {
			if !draftValue(field.Value, draft.Compact).IsZero() {
				expected++
			}
		}
		if v.Kind() != draft.Numeric || v.Len() != expected {
			t.Fatal("record shape")
		}
		for _, f := range want.Fields {
			x := v.Get(f.Key)
			check(f.Value, x)
		}
	case Dict:
		expected := 0
		for _, pair := range want.Entries {
			if !draftValue(pair.Value, draft.Compact).IsZero() {
				expected++
			}
		}
		if v.Kind() != draft.Dictionary || v.Len() != expected {
			t.Fatal("dictionary shape")
		}
		for _, p := range want.Entries {
			x := v.GetString(p.Key)
			check(p.Value, x)
		}
	case List:
		if v.Kind() != draft.Sequence || v.Len() != len(want.Items) {
			t.Fatal("list shape")
		}
		for i, w := range want.Items {
			x := v.At(i)
			check(w, x)
		}
	}
}
func TestDraftFixtureSizes(t *testing.T) {
	all := fixtures()
	names := make([]string, 0, len(all))
	for name := range all {
		names = append(names, name)
	}
	sort.Strings(names)
	t.Log("fixture compact indexed mixed integer-MP short-key-MP")
	for _, name := range names {
		v := all[name]
		checkNoOmittedFixtureEntries(t, v)
		compact := draftObject(v, draft.Compact, draft.Compact)
		indexed := draftObject(v, draft.Indexed, draft.Indexed)
		mixed := draftObject(v, draft.Indexed, draft.Compact)
		for _, raw := range [][]byte{compact, indexed, mixed} {
			checkDraftTree(t, v, raw)
		}
		t.Logf("%s %d %d %d %d %d", name, len(compact), len(indexed), len(mixed), len(mpEncode(v, false)), len(mpEncode(v, true)))
	}
}

// Keep the MessagePack comparison equivalent: a fixture that introduces zero
// map entries needs the same omission policy applied to both encodings.
func checkNoOmittedFixtureEntries(t *testing.T, v Value) {
	t.Helper()
	for _, field := range v.Fields {
		if draftValue(field.Value, draft.Compact).IsZero() {
			t.Fatal("normalize zero entries equally before comparing fixture sizes")
		}
		checkNoOmittedFixtureEntries(t, field.Value)
	}
	for _, pair := range v.Entries {
		if draftValue(pair.Value, draft.Compact).IsZero() {
			t.Fatal("normalize zero entries equally before comparing fixture sizes")
		}
		checkNoOmittedFixtureEntries(t, pair.Value)
	}
	for _, item := range v.Items {
		checkNoOmittedFixtureEntries(t, item)
	}
}

func BenchmarkDraftRead(b *testing.B) {
	for _, n := range []int{8, 128, 512} {
		fixture := numeric(n, 0)
		old := oldKVO(fixture).Record(nil).Root()
		cv := draft.Trusted(draftObject(fixture, draft.Compact, draft.Compact))
		iv := draft.Trusted(draftObject(fixture, draft.Indexed, draft.Indexed))
		mp := MPView(mpEncode(fixture, false))
		cases := []struct {
			name string
			get  func(uint64) uint64
		}{
			{"kvo", old.Get},
			{"compact", func(k uint64) uint64 { return cv.Get(k).Uint() }},
			{"indexed", func(k uint64) uint64 { return iv.Get(k).Uint() }},
			{"msgpack", func(k uint64) uint64 { v, _ := mp.Get(k); return v.Uint() }},
		}
		for _, c := range cases {
			b.Run(fmt.Sprintf("n%d/%s", n, c.name), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					sum += c.get(query(i, n))
				}
				sinkU = sum
			})
		}
	}
}

func BenchmarkDraftDictionary(b *testing.B) {
	fixture := dictionary(128, false)
	for _, layout := range []draft.Layout{draft.Compact, draft.Indexed} {
		v := draft.Trusted(draftObject(fixture, layout, layout))
		b.Run(fmt.Sprint(layout), func(b *testing.B) {
			b.ReportAllocs()
			var sum uint64
			for i := 0; i < b.N; i++ {
				x := v.GetString(fixture.Entries[query(i, 128)-1].Key)
				xb := x.Bytes()
				sum += uint64(len(xb))
			}
			sinkU = sum
		})
	}
}
