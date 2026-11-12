package kvo_test

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/andreyvit/edb/kvo"
	wire "github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
	"github.com/vmihailenco/msgpack/v5"
)

type collectionKey uint64

func (k collectionKey) EncodeMsgpack(e *msgpack.Encoder) error {
	return e.EncodeString(fmt.Sprintf("key:%d", k))
}
func (k *collectionKey) DecodeMsgpack(d *msgpack.Decoder) error {
	s, err := d.DecodeString()
	if err != nil {
		return err
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(s, "key:"), 10, 64)
	*k = collectionKey(n)
	return err
}

type collectionMap map[string]string

func (m collectionMap) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeArrayLen(1); err != nil {
		return err
	}
	return e.Encode(map[string]string(m))
}
func (m *collectionMap) DecodeMsgpack(d *msgpack.Decoder) error {
	n, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("map wrapper length %d", n)
	}
	return d.Decode((*map[string]string)(m))
}

type collectionStrings []string

func (s collectionStrings) EncodeMsgpack(e *msgpack.Encoder) error {
	return e.EncodeString(strings.Join(s, "|"))
}
func (s *collectionStrings) DecodeMsgpack(d *msgpack.Decoder) error {
	v, err := d.DecodeString()
	if v == "" {
		*s = nil
	} else {
		*s = strings.Split(v, "|")
	}
	return err
}

type collectionChild struct {
	Name   string `kvo:"name"`
	Hidden string `kvo:"-"`
}
type collectionChildren []collectionChild

func (s collectionChildren) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeMapLen(1); err != nil {
		return err
	}
	if err := e.EncodeString("items"); err != nil {
		return err
	}
	return e.Encode([]collectionChild(s))
}
func (s *collectionChildren) DecodeMsgpack(d *msgpack.Decoder) error {
	n, err := d.DecodeMapLen()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("list wrapper length %d", n)
	}
	if _, err := d.DecodeString(); err != nil {
		return err
	}
	return d.Decode((*[]collectionChild)(s))
}

func TestCollectionEditsWithCustomMessagePack(t *testing.T) {
	type row struct {
		Name     string                              `kvo:"name"`
		Codes    map[collectionKey]string            `kvo:"codes" msgpack:",omitempty"`
		Map      collectionMap                       `kvo:"map" msgpack:",omitempty"`
		Strings  collectionStrings                   `kvo:"strings" msgpack:",omitempty"`
		Children collectionChildren                  `kvo:"children" msgpack:",omitempty"`
		Nested   map[string]map[collectionKey]string `kvo:"nested" msgpack:",omitempty"`
	}
	s := kvo.NewSchema()
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	ct := kvo.NewEntityType(s, "child", reflect.TypeFor[collectionChild]())
	kvo.NewModel(s, ct, func(b *kvo.ModelBuilder) { b.Prop(name) })
	keyCodec := kvo.NewMapCodec(kvo.MapOf(kvo.TUint64, kvo.TMachineString), values.UnsignedCodec[collectionKey](), values.StringCodec[string]())
	codes := keyCodec.Prop(s, 2, "codes")
	mapping := kvo.NewMapProp(s, 3, "map", kvo.MapOf(kvo.TMachineString, kvo.TMachineString), values.StringCodec[string](), values.StringCodec[string]())
	strs := kvo.NewListProp(s, 4, "strings", kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())
	children := kvo.NewRecordListProp(s, 5, "children", kvo.ListOf(ct))
	nested := kvo.NewNestedMapCodec(kvo.TMachineString, values.StringCodec[string](), keyCodec).Prop(s, 6, "nested")
	m := kvo.NewModel(s, kvo.NewEntityType(s, "row", reflect.TypeFor[row]()), func(b *kvo.ModelBuilder) {
		b.Prop(name)
		b.Prop(codes)
		b.Prop(mapping)
		b.Prop(strs)
		b.Prop(children)
		b.Prop(nested)
	})
	b := kvo.Bind(reflect.TypeFor[row](), m)
	for _, format := range []string{"MessagePack", "native", "Fast2", "Compact2"} {
		for _, populated := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/populated=%v", format, populated), func(t *testing.T) {
				src := &row{Name: "untouched"}
				want := row{Name: "untouched", Codes: map[collectionKey]string{2: "last"}, Map: collectionMap{"new": "last"}, Strings: collectionStrings{"new"}, Children: collectionChildren{{Name: "new"}}, Nested: map[string]map[collectionKey]string{"new": {2: "nested"}}}
				if populated {
					src.Codes = map[collectionKey]string{1: "keep", 3: "remove"}
					src.Map = collectionMap{"keep": "value"}
					src.Strings = collectionStrings{"old"}
					src.Children = collectionChildren{{Name: "old"}}
					src.Nested = map[string]map[collectionKey]string{"old": {1: "keep"}}
					want.Codes[1] = "keep"
					want.Map["keep"] = "value"
					want.Strings = append(collectionStrings{"old"}, want.Strings...)
					want.Children = append(collectionChildren{{Name: "edited"}}, want.Children...)
					want.Nested["old"] = map[collectionKey]string{1: "keep", 2: "edited"}
					if format == "MessagePack" || format == "native" {
						src.Children[0].Hidden = "unmodeled"
						want.Children[0].Hidden = "unmodeled"
					}
				}
				var r kvo.Record
				switch format {
				case "MessagePack":
					r = b.MessagePack(wire.Encode(nil, reflect.ValueOf(src)))
				case "native":
					r = b.Record(reflect.ValueOf(src))
				default:
					layout := row2.Fast2
					if format == "Compact2" {
						layout = row2.Compact2
					}
					r = b.Encoded(row2.Open(b.Encode(nil, reflect.ValueOf(src).Elem(), layout)))
				}
				e := r.Edit()
				e.Map(codes).Set(2, "first")
				e.Map(codes).Delete(3)
				e.Map(mapping).Set("new", "first")
				e.List(strs).Append("new")
				if populated {
					e.List(children).Edit(0).Set(name, "edited")
					kvo.EditMap(e.Map(nested), "old").Set(2, "edited")
				}
				e.List(children).New().Set(name, "new")
				kvo.NewMap(e.Map(nested), "new").Set(2, "nested")
				_ = e.Record()
				e.Map(codes).Set(2, "last")
				e.Map(mapping).Set("new", "last")
				var got row
				if format == "MessagePack" {
					if err := wire.TryDecode(e.MessagePack(), reflect.ValueOf(&got)); err != nil {
						t.Fatal(err)
					}
				} else {
					got = *e.Record().Native().(*row)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %#v, want %#v", got, want)
				}
			})
		}
	}
}
