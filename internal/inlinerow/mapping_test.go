package inlinerow

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

type sharedFields struct{}

func (sharedFields) NativeFields(t reflect.Type) *nativebind.Struct {
	if g := Static.Fields[t]; g != nil {
		return nativebind.BindFields(t, g, nativebind.StaticOnly)
	}
	return nil
}
func (sharedFields) NativeType(t reflect.Type) *values.Type {
	return values.NewType(t.String(), values.Pointer)
}

func TestInlinedAccess(t *testing.T) {
	typ := reflect.TypeFor[Row]()
	base := kvo.Bind(typ, Model)
	bindings := []*kvo.Binding{base, base.WithGenerated(&Static, nativebind.StaticOnly), base.WithNativeSchema(sharedFields{})}
	fields := Model.NativeStruct(typ).Fields()
	fields[1].Path[0] = 99
	if Model.NativeStruct(typ).Fields()[1].Path[0] == 99 {
		t.Fatal("mutable field metadata")
	}
	if base.NativeFieldIndex(Earned.Code()) != -1 || base.NativeFieldIndex(Marker.Code()) != 0 {
		t.Fatal("direct key field identity")
	}
	for _, b := range bindings {
		if !b.DirectMessagePack() {
			t.Fatal("inlining required a decode")
		}
		for _, layout := range []row2.Layout{row2.Compact2, row2.Fast2} {
			row := &Row{Marker: 17, Group: Group{Padding: 23, Counts: Counts{Earned: 101, Spent: 7}}}
			raw := b.Encode(nil, reflect.ValueOf(row).Elem(), layout)
			if !bytes.Equal(raw, base.Encode(nil, reflect.ValueOf(row).Elem(), layout)) {
				t.Fatal("generated wire differs")
			}
			var decoded Row
			b.Decode(raw, reflect.ValueOf(&decoded).Elem())
			if decoded != *row {
				t.Fatal(decoded, *row)
			}
			mp := msgpack.Encode(nil, reflect.ValueOf(row))
			mp = msgpack.Trusted(mp).Set("unknown", []byte{42}).Raw()
			for _, r := range []kvo.Record{b.Record(reflect.ValueOf(row)), b.MessagePack(mp), b.Encoded(row2.Trusted(raw)), b.Encoded(row2.Trusted(raw)).Fastest()} {
				read := func() {
					if r.Get(Earned) != 101 || r.Get(Spent) != 7 || r.Get(Marker) != 17 || r.Get(Padding) != 23 {
						t.Fatal("flattened read")
					}
				}
				read()
				if n := testing.AllocsPerRun(100, read); n != 0 {
					t.Fatal("read allocations", n)
				}
				edit := r.Edit()
				edit.Set(Earned, int64(150))
				if edit.Get(Earned) != 150 || edit.Get(Spent) != 7 {
					t.Fatal("flattened edit")
				}
				if data := edit.MessagePack(); data != nil {
					if msgpack.Trusted(data).Lookup("unknown").Uint() != 42 {
						t.Fatal("lost unknown wire field")
					}
					var got Row
					if err := msgpack.TryDecode(data, reflect.ValueOf(&got)); err != nil || got.Earned != 150 || got.Marker != 17 {
						t.Fatal("wire edit", got, err)
					}
				}
			}
			if row.Earned != 150 {
				t.Fatal("native pointer identity lost")
			}
		}
	}
}
