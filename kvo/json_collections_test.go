package kvo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	wire "github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/vmihailenco/msgpack/v5"
)

type jsonCollectionChild struct {
	Name  string `kvo:"name" json:"name"`
	Count uint64 `kvo:"count" json:"count"`
}
type jsonCustomChildren []*jsonCollectionChild

func (v jsonCustomChildren) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeMapLen(1); err != nil {
		return err
	}
	if err := e.EncodeString("items"); err != nil {
		return err
	}
	return e.Encode([]*jsonCollectionChild(v))
}
func (v *jsonCustomChildren) DecodeMsgpack(d *msgpack.Decoder) error {
	if _, err := d.DecodeMapLen(); err != nil {
		return err
	}
	if _, err := d.DecodeString(); err != nil {
		return err
	}
	return d.Decode((*[]*jsonCollectionChild)(v))
}

type jsonCollectionRow struct {
	Items    []*jsonCollectionChild            `kvo:"items" json:"items"`
	Dict     map[string]*jsonCollectionChild   `kvo:"dict" json:"dict"`
	Nested   map[string][]*jsonCollectionChild `kvo:"nested" json:"nested"`
	Lists    [][]*jsonCollectionChild          `kvo:"lists" json:"lists"`
	Integers map[int64][]string                `kvo:"integers" json:"integers"`
	Custom   jsonCustomChildren                `kvo:"custom" json:"custom"`
	Fixed    [3]string                         `kvo:"fixed" json:"fixed"`
}

func jsonCollectionBinding() (*Model, *Binding) {
	s := NewSchema()
	ct := NewEntityType(s, "json-child", reflect.TypeFor[jsonCollectionChild]())
	NewProp(s, 1, "name", TMachineString, nil)
	NewProp(s, 2, "count", TUint64, nil)
	NewModel(s, ct, func(b *ModelBuilder) { b.Prop(s.MustPropByCode(1)); b.Prop(s.MustPropByCode(2)) })
	for i, p := range []struct {
		name string
		typ  AnyType
	}{
		{"items", ListOf(ct)}, {"dict", MapOf(TMachineString, ct)},
		{"nested", MapOf(TMachineString, ListOf(ct))}, {"lists", ListOf(ListOf(ct))},
		{"integers", MapOf(TInt64, ListOf(TMachineString))}, {"custom", ListOf(ct)}, {"fixed", ListOf(TMachineString)},
	} {
		NewProp(s, uint64(i+3), p.name, p.typ, func(b *PropBuilder) {
			if _, ok := p.typ.(*MapType); ok {
				b.Flags = KeepDictionaryZeroValues
			}
		})
	}
	m := NewModel(s, NewEntityType(s, "json-row", reflect.TypeFor[jsonCollectionRow]()), func(b *ModelBuilder) {
		for k := uint64(3); k <= 9; k++ {
			b.Prop(s.MustPropByCode(k))
		}
	})
	return m, Bind(reflect.TypeFor[jsonCollectionRow](), m)
}
func TestRecordJSONCollections(t *testing.T) {
	m, b := jsonCollectionBinding()
	child := &jsonCollectionChild{"retained-é", math.MaxUint64}
	for _, nulls := range []bool{false, true} {
		src := &jsonCollectionRow{Items: []*jsonCollectionChild{child, {}}, Dict: map[string]*jsonCollectionChild{"kept": child}, Nested: map[string][]*jsonCollectionChild{"empty": {}, "items": {child}}, Lists: [][]*jsonCollectionChild{{}, {child}}, Integers: map[int64][]string{-7: {"negative"}, 0: {}}, Custom: jsonCustomChildren{child}, Fixed: [3]string{"first"}}
		if nulls {
			src.Items = append(src.Items, nil)
			src.Dict["nil"] = nil
			src.Nested["nil"] = nil
			src.Lists = append(src.Lists, nil, []*jsonCollectionChild{nil})
			src.Integers[42] = nil
			src.Custom = append(src.Custom, nil)
		}
		expected, err := json.Marshal(src)
		if err != nil {
			t.Fatal(err)
		}
		for _, format := range []string{"native", "MessagePack", "Fast2", "Compact2"} {
			if nulls && (format == "Fast2" || format == "Compact2") {
				continue
			} // row2 rejects nil collection entries on purpose.
			t.Run(fmt.Sprintf("%s/nulls=%v", format, nulls), func(t *testing.T) {
				var r Record
				var encoded []byte
				switch format {
				case "native":
					r = b.Record(reflect.ValueOf(src))
				case "MessagePack":
					encoded = wire.Encode(nil, reflect.ValueOf(src))
					r = b.MessagePack(encoded)
				default:
					layout := row2.Fast2
					if format == "Compact2" {
						layout = row2.Compact2
					}
					encoded = b.Encode(nil, reflect.ValueOf(src).Elem(), layout)
					r = b.Encoded(row2.Trusted(encoded))
				}
				got, err := r.TryToJSON(m, JSONOptions{})
				if err != nil {
					t.Fatal(err)
				}
				actual, err := json.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				var a, z any
				json.Unmarshal(expected, &a)
				json.Unmarshal(actual, &z)
				if !reflect.DeepEqual(a, z) {
					t.Fatalf("got %s, want %s", actual, expected)
				}
				if got["items"].([]any)[0].(map[string]any)["count"] != uint64(math.MaxUint64) {
					t.Fatal("lost integer precision")
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := r.TryToJSON(m, JSONOptions{Context: ctx}); !errors.Is(err, context.Canceled) {
					t.Fatal("context", err)
				}
				exhausted := errors.New("allocation budget")
				charged := uint64(0)
				if _, err := r.TryToJSON(m, JSONOptions{Charge: func(n uint64) error {
					charged += n
					if charged > 1300 {
						return exhausted
					}
					return nil
				}}); !errors.Is(err, exhausted) {
					t.Fatal("budget", charged, err)
				}
				clear(encoded)
				old := child.Name
				child.Name = "mutated"
				defer func() { child.Name = old }()
				again, _ := json.Marshal(got)
				if string(again) != string(actual) {
					t.Fatal("JSON result borrowed source data")
				}
			})
		}
	}
	// Nil and empty root collections remain distinct.
	for _, src := range []*jsonCollectionRow{{}, {Items: []*jsonCollectionChild{}, Dict: map[string]*jsonCollectionChild{}}} {
		for _, r := range []Record{b.Record(reflect.ValueOf(src)), b.MessagePack(wire.Encode(nil, reflect.ValueOf(src)))} {
			got, err := r.TryToJSON(m, JSONOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if (got["items"] == nil) != (src.Items == nil) || (got["dict"] == nil) != (src.Dict == nil) {
				t.Fatal("lost nil/empty distinction")
			}
		}
	}
}
func BenchmarkRecordJSONCollections(b *testing.B) {
	m, bind := jsonCollectionBinding()
	src := &jsonCollectionRow{Dict: map[string]*jsonCollectionChild{}, Nested: map[string][]*jsonCollectionChild{}, Lists: [][]*jsonCollectionChild{}, Integers: map[int64][]string{}, Custom: jsonCustomChildren{}}
	for i := range 32 {
		src.Items = append(src.Items, &jsonCollectionChild{fmt.Sprint("item-", i), uint64(i)})
	}
	for _, format := range []string{"native", "MessagePack", "Fast2", "Compact2"} {
		b.Run(format, func(b *testing.B) {
			r := bind.Record(reflect.ValueOf(src))
			if format == "MessagePack" {
				r = bind.MessagePack(wire.Encode(nil, reflect.ValueOf(src)))
			} else if format != "native" {
				layout := row2.Fast2
				if format == "Compact2" {
					layout = row2.Compact2
				}
				r = bind.Encoded(row2.Trusted(bind.Encode(nil, reflect.ValueOf(src).Elem(), layout)))
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := r.TryToJSON(m, JSONOptions{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type jsonScalarCollections struct {
	Pointers []*int64 `kvo:"pointers"`
	Bytes    []byte   `kvo:"bytes"`
	Fixed    [3]byte  `kvo:"fixed"`
}

func TestRecordJSONScalarCollections(t *testing.T) {
	s := NewSchema()
	pointers := NewProp(s, 1, "pointers", ListOf(TInt64), nil)
	bytes := NewProp(s, 2, "bytes", ListOf(TUint8), nil)
	fixed := NewProp(s, 3, "fixed", ListOf(TUint8), nil)
	m := NewModel(s, NewEntityType(s, "scalars", reflect.TypeFor[jsonScalarCollections]()), func(b *ModelBuilder) {
		b.Prop(s.MustPropByCode(pointers))
		b.Prop(s.MustPropByCode(bytes))
		b.Prop(s.MustPropByCode(fixed))
	})
	b := Bind(reflect.TypeFor[jsonScalarCollections](), m)
	n := int64(-7)
	src := &jsonScalarCollections{[]*int64{&n, nil}, []byte{0, 255}, [3]byte{42}}
	want := map[string]any{"pointers": []any{int64(-7), nil}, "bytes": []any{uint8(0), uint8(255)}, "fixed": []any{uint8(42), uint8(0), uint8(0)}}
	for _, r := range []Record{b.Record(reflect.ValueOf(src)), b.MessagePack(wire.Encode(nil, reflect.ValueOf(src)))} {
		got, err := r.TryToJSON(m, JSONOptions{})
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v, %v; want %#v", got, err, want)
		}
	}
}

func TestRecordJSONMalformedCollectionReturnsError(t *testing.T) {
	m, b := jsonCollectionBinding()
	// Valid root map, truncated array value. TryToJSON must return the codec error.
	r := b.MessagePack([]byte{0x81, 0xa5, 'I', 't', 'e', 'm', 's', 0x92, 0xc0})
	got, err := r.TryToJSON(m, JSONOptions{})
	if got != nil || !errors.Is(err, wire.ErrFormat) {
		t.Fatalf("got %#v, %v", got, err)
	}
}
