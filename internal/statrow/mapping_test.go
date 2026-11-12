package statrow

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

func TestGeneratedAndReflectedWireAgreement(t *testing.T) {
	_, m := Model()
	r := kvo.Bind(reflect.TypeFor[State](), m)
	g := r.WithGenerated(&Static, nativebind.StaticOnly)
	for _, layout := range []row2.Layout{row2.Fast2, row2.Compact2} {
		for i := uint64(1); i <= 64; i++ {
			v := Sample(i)
			v.Key = 0
			if i == 1 {
				v.Tags = []uint64{}
				v.History = map[uint64]int64{}
				v.Progress = map[uint64]Profile{2: {}}
			}
			raw := r.Encode(nil, reflect.ValueOf(v).Elem(), layout)
			if !bytes.Equal(raw, g.Encode(nil, reflect.ValueOf(v).Elem(), layout)) {
				t.Fatal("wire mismatch", i)
			}
			for _, b := range []*kvo.Binding{g, r} {
				var got State
				if err := b.TryDecode(raw, reflect.ValueOf(&got).Elem()); err != nil || !reflect.DeepEqual(got, *v) {
					t.Fatal("round trip", i, err, got, *v)
				}
			}
		}
	}
	stale := Static
	stale.Fingerprint += "stale"
	stale.Encode = func([]byte, any, row2.Layout) []byte { panic("stale binding used") }
	r.WithGenerated(&stale, nativebind.Automatic).Encode(nil, reflect.ValueOf(Sample(1)).Elem(), row2.Fast2)
	defer func() {
		if recover() == nil {
			t.Fatal("accepted stale static binding")
		}
	}()
	r.WithGenerated(&stale, nativebind.StaticOnly)
}

// Raw puts must reject the same malformed rows as a typed table. Seed both
// layouts, including a malformed child behind an otherwise valid directory.
func FuzzPackedValidation(f *testing.F) {
	_, m := Model()
	b := kvo.Bind(reflect.TypeFor[State](), m)
	g := b.WithGenerated(&Static, nativebind.StaticOnly)
	for _, layout := range []row2.Layout{row2.Fast2, row2.Compact2} {
		v := reflect.ValueOf(Sample(3)).Elem()
		raw := b.Encode(nil, v, layout)
		f.Add(raw)
		f.Add(raw[:len(raw)-1])
		for _, prop := range []uint64{3, 9, 17, 999} {
			var w row2.Writer
			w.Begin(0, "", row2.Object, layout)
			w.Put(prop, "", values.Bytes([]byte{1, 2, 3}), row2.Data)
			w.End()
			f.Add(w.BytesWritten())
		}
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 2048 {
			return
		}
		valid := b.TryValidate(raw) == nil
		for _, binding := range []*kvo.Binding{b, g} {
			var dst State
			if ok := binding.TryDecode(raw, reflect.ValueOf(&dst).Elem()) == nil; ok != valid {
				t.Fatalf("validate=%t, decode=%t", valid, ok)
			}
		}
	})
}

func TestGeneratedNativeAccess(t *testing.T) {
	_, m := Model()
	base := kvo.Bind(reflect.TypeFor[State](), m)
	for _, binding := range []*kvo.Binding{base, base.WithGenerated(&Static, nativebind.StaticOnly)} {
		src := Sample(13)
		r := binding.Record(reflect.ValueOf(src))
		wire := binding.Encoded(row2.Trusted(binding.Encode(nil, reflect.ValueOf(src).Elem(), row2.Fast2)))
		for _, p := range m.AllProps() {
			a, z := r.GetValue(p.Code()), wire.GetValue(p.Code())
			if p.AnyType().ValueKind() == kvo.ValueKindWord {
				if a.AsUint() != z.AsUint() {
					t.Fatal(p.Name(), a, z)
				}
			}
		}
		if err := r.Edit().TryFastSetPathValue([]row2.Step{{Key: 9}, {Key: 5}}, values.Uint(500)); err != nil {
			t.Fatal(err)
		}
		if src.Profile.Balance != 500 || r.Child(row2.Step{Key: 9}).GetValue(5).AsUint() != 500 {
			t.Fatal("detached child")
		}
		if r.Native() != src {
			t.Fatal("native conversion copied object")
		}
		decoded := binding.Encoded(wire.Snapshot()).Fastest()
		if decoded.Native() == src || decoded.GetValue(1).AsUint() != src.ID {
			t.Fatal("decode ownership")
		}
	}
}
