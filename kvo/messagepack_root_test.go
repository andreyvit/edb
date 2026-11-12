package kvo

import (
	"reflect"
	"testing"

	wire "github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/vmihailenco/msgpack/v5"
)

type nullableRoot struct {
	Amount int64 `kvo:"amount"`
}

func (v *nullableRoot) EncodeMsgpack(e *msgpack.Encoder) error {
	if v.Amount == 0 {
		return e.EncodeNil()
	}
	if err := e.EncodeArrayLen(1); err != nil {
		return err
	}
	return e.EncodeInt64(v.Amount)
}
func (v *nullableRoot) DecodeMsgpack(d *msgpack.Decoder) error {
	n, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}
	if n < 0 {
		*v = nullableRoot{}
		return nil
	}
	v.Amount, err = d.DecodeInt64()
	return err
}
func TestMessagePackCustomRootNil(t *testing.T) {
	s := NewSchema()
	amount := NewSignedProp[int64](s, 1, "amount", TInt64)
	m := NewModel(s, NewEntityType(s, "nullable-root", reflect.TypeFor[nullableRoot]()), func(b *ModelBuilder) { b.Prop(amount) })
	p, b := Project(reflect.TypeFor[nullableRoot](), m, nil), Bind(reflect.TypeFor[nullableRoot](), m)
	for name, open := range map[string]func([]byte) Record{"projection": p.MessagePack, "binding": b.MessagePack} {
		t.Run(name, func(t *testing.T) {
			if !open(nil).Missing() {
				t.Fatal("missing bytes must remain missing")
			}
			for _, n := range []int64{0, 42, -7, 0} {
				raw := wire.Encode(nil, reflect.ValueOf(&nullableRoot{n}))
				var want nullableRoot
				if err := wire.TryDecode(raw, reflect.ValueOf(&want).Elem()); err != nil {
					t.Fatal(err)
				}
				r := open(raw)
				if r.Missing() || r.Get(amount) != want.Amount || r.Fastest().Get(amount) != want.Amount {
					t.Fatalf("custom root %d did not match native decoder", n)
				}
				e := r.Edit()
				e.Set(amount, n+1)
				if r.Native().(*nullableRoot).Amount != n+1 {
					t.Fatal("fallback editor lost its native carrier")
				}
			}
		})
	}
}
