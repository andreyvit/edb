package kvo_test

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestTypedMapsAcrossRepresentations(t *testing.T) {
	type row struct {
		IDs    map[uint32]string `kvo:"ids"`
		Counts map[string]int64  `kvo:"counts"`
	}
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "Mapped", reflect.TypeFor[row]())
	ids := kvo.NewMapProp(s, 1, "ids", kvo.MapOf(kvo.TUint32, kvo.TMachineString), values.UnsignedCodec[uint32](), values.StringCodec[string]())
	counts := kvo.NewMapProp(s, 2, "counts", kvo.MapOf(kvo.TMachineString, kvo.TInt64), values.StringCodec[string](), values.SignedCodec[int64]())
	m := kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) {
		b.Prop(ids, kvo.KeepDictionaryZeroValues)
		b.Prop(counts, kvo.KeepDictionaryZeroValues)
	})
	b := kvo.Bind(reflect.TypeFor[row](), m)
	src := &row{map[uint32]string{1: "one", 2: ""}, map[string]int64{"a": -3, "b": 0}}
	records := []kvo.Record{b.Record(reflect.ValueOf(src)), b.MessagePack(msgpack.Encode(nil, reflect.ValueOf(src)))}
	for _, layout := range []row2.Layout{row2.Fast2, row2.Compact2} {
		records = append(records, b.Encoded(row2.Open(b.Encode(nil, reflect.ValueOf(src).Elem(), layout))))
	}
	for _, r := range records {
		read := func() {
			v := r.Get(ids)
			if unsafe.Sizeof(v) != 16 || v.Len() != 2 {
				t.Fatal(v.Len())
			}
			if x, ok := v.Lookup(2); !ok || x != "" {
				t.Fatal(x, ok)
			}
			if _, ok := v.Lookup(3); ok {
				t.Fatal("missing entry")
			}
			n := 0
			for k, v := range v.Iter() {
				if src.IDs[k] != v {
					t.Fatal(k, v)
				}
				n++
			}
			if n != 2 {
				t.Fatal(n)
			}
			c := r.Get(counts)
			if c.Get("a") != -3 {
				t.Fatal(c.Get("a"))
			}
			if _, ok := c.Lookup("b"); !ok {
				t.Fatal("zero entry")
			}
			if _, ok := c.Lookup("c"); ok {
				t.Fatal("missing string key")
			}
		}
		read()
		if n := testing.AllocsPerRun(100, read); n != 0 {
			t.Fatal(n)
		}
		e := r.Clone().Edit()
		e.Set(ids, r.Get(ids))
		if e.Get(ids).Get(1) != "one" {
			t.Fatal("map assignment")
		}
	}
	// Duplicate keys normalize exactly as Go map decoding: last value wins.
	raw := []byte{0x81, 0xa3, 'I', 'D', 's', 0x82, 1, 0xa1, 'a', 1, 0xa1, 'b'}
	v := b.MessagePack(raw).Get(ids)
	if v.Len() != 1 || v.Get(1) != "b" {
		t.Fatal(v.Len(), v.Get(1))
	}
	for _, r := range []kvo.Record{{}, b.MessagePack([]byte{0x80}), b.Record(reflect.ValueOf(&row{}))} {
		if r.Get(ids).Len() != 0 {
			t.Fatal("missing map")
		}
	}
}
