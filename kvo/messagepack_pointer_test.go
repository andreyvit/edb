package kvo

import (
	"reflect"
	"testing"
	"time"

	wire "github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/values"
	"github.com/vmihailenco/msgpack/v5"
)

type pointerDirectScalar int64

func (v *pointerDirectScalar) DecodeMsgpack(d *msgpack.Decoder) error {
	n, err := d.DecodeInt64()
	*v = pointerDirectScalar(n + 3)
	return err
}

type pointerFallbackScalar int64

func (v *pointerFallbackScalar) DecodeMsgpack(d *msgpack.Decoder) error {
	n, err := d.DecodeInt64()
	*v = pointerFallbackScalar(n + 5)
	return err
}
func init() {
	RegisterMessagePackScalar(reflect.TypeFor[pointerDirectScalar](), func(v wire.View) values.Value { return values.Int(v.Int() + 3) })
}

type pointerScalarRow struct {
	Time     *time.Time             `msgpack:"t" kvo:"time"`
	Direct   *pointerDirectScalar   `msgpack:"d" kvo:"direct"`
	Fallback *pointerFallbackScalar `msgpack:"f" kvo:"fallback"`
}

func TestMessagePackPointerScalarPlans(t *testing.T) {
	schema := NewSchema()
	when := NewTimeProp(schema, 1, "time")
	direct := NewSignedProp[int64](schema, 2, "direct", TInt64)
	fallback := NewSignedProp[int64](schema, 3, "fallback", TInt64)
	model := NewModel(schema, NewEntityType(schema, "pointer-scalars", reflect.TypeFor[pointerScalarRow]()), func(b *ModelBuilder) { b.Prop(when); b.Prop(direct); b.Prop(fallback) })
	projection := Project(reflect.TypeFor[pointerScalarRow](), model, nil)
	at := time.Date(2026, 6, 15, 12, 0, 0, 123456789, time.UTC)
	d, f := pointerDirectScalar(7), pointerFallbackScalar(11)
	for _, row := range []*pointerScalarRow{{}, {Time: &at, Direct: &d, Fallback: &f}} {
		raw := wire.Encode(nil, reflect.ValueOf(row))
		var decoded pointerScalarRow
		if err := wire.TryDecode(raw, reflect.ValueOf(&decoded).Elem()); err != nil {
			t.Fatal(err)
		}
		r := projection.MessagePack(raw)
		read := func() {
			got := r.Get(when)
			if decoded.Time == nil {
				if !got.IsZero() {
					t.Fatal(got)
				}
			} else if !got.Equal(*decoded.Time) {
				t.Fatalf("time: %v, want %v", got, *decoded.Time)
			}
			var want int64
			if decoded.Direct != nil {
				want = int64(*decoded.Direct)
			}
			if got := r.Get(direct); got != want {
				t.Fatalf("direct: %d, want %d", got, want)
			}
		}
		read()
		if n := testing.AllocsPerRun(100, read); n != 0 {
			t.Fatalf("direct pointer fields: %g allocations", n)
		}
		var want int64
		if decoded.Fallback != nil {
			want = int64(*decoded.Fallback)
		}
		if got := r.Get(fallback); got != want {
			t.Fatalf("fallback: %d, want %d", got, want)
		}
	}
}
