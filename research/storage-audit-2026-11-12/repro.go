//go:build ignore

// Run from EDB: go run ./research/storage-audit-2026-11-12/repro.go
// Observes reported failures; exits nonzero only if a control fails.
package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/andreyvit/edb/kvo"
	wire "github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/vmihailenco/msgpack/v5"
)

type optionalTime time.Time

func (v *optionalTime) EncodeMsgpack(e *msgpack.Encoder) error {
	if time.Time(*v).IsZero() {
		return e.EncodeNil()
	}
	return e.EncodeTime(time.Time(*v))
}
func (v *optionalTime) DecodeMsgpack(d *msgpack.Decoder) error {
	c, err := d.PeekCode()
	if err != nil {
		return err
	}
	if c == 0xc0 {
		*v = optionalTime{}
		return d.DecodeNil()
	}
	t, err := d.DecodeTime()
	*v = optionalTime(t)
	return err
}

type timed struct {
	Time optionalTime `msgpack:"tm" kvo:"time"`
}

type nullObject struct {
	Amount int64 `kvo:"amount"`
}

func (*nullObject) EncodeMsgpack(e *msgpack.Encoder) error { return e.EncodeNil() }
func (*nullObject) DecodeMsgpack(d *msgpack.Decoder) error { return d.DecodeNil() }

type child struct {
	Name string `kvo:"name"`
}
type parent struct {
	Children []*child `kvo:"children"`
}

func observe(label string, f func()) {
	func() {
		defer func() {
			if e := recover(); e != nil {
				fmt.Printf("%s: PANIC %v\n", label, e)
			}
		}()
		f()
		fmt.Printf("%s: OK\n", label)
	}()
}
func main() {
	s := kvo.NewSchema()
	tm := kvo.NewTimeProp(s, 1, "time")
	m := kvo.NewModel(s, kvo.NewEntityType(s, "timed", reflect.TypeFor[timed]()), func(b *kvo.ModelBuilder) { b.Prop(tm) })
	p := kvo.Project(reflect.TypeFor[timed](), m, nil)
	for _, t := range []time.Time{{}, time.Unix(1700000000, 123456789).UTC()} {
		raw := wire.Encode(nil, reflect.ValueOf(&timed{optionalTime(t)}))
		r := p.MessagePack(raw)
		if !r.Get(tm).Equal(t) {
			panic("typed timestamp control failed")
		}
		observe("named timestamp Has", func() { fmt.Printf("present=%v\n", r.Has(tm.Code())) })
		dst := reflect.New(reflect.TypeFor[optionalTime]()).Elem()
		fmt.Printf("named timestamp DecodeProperty: %v\n", p.TryDecodeProperty(dst, r, tm.Code()))
	}
	s = kvo.NewSchema()
	amount := kvo.NewSignedProp[int64](s, 1, "amount", kvo.TInt64)
	m = kvo.NewModel(s, kvo.NewEntityType(s, "nullObject", reflect.TypeFor[nullObject]()), func(b *kvo.ModelBuilder) { b.Prop(amount) })
	p = kvo.Project(reflect.TypeFor[nullObject](), m, nil)
	raw := wire.Encode(nil, reflect.ValueOf(&nullObject{}))
	dst := reflect.New(reflect.TypeFor[nullObject]())
	wire.Decode(raw, dst.Elem())
	fmt.Println("custom nil struct ordinary decode: OK")
	observe("custom nil struct Projection.MessagePack", func() { p.MessagePack(raw) })
	s = kvo.NewSchema()
	name := kvo.NewStringProp[string](s, 1, "name", kvo.TMachineString)
	ct := kvo.NewEntityType(s, "child", reflect.TypeFor[child]())
	kvo.NewModel(s, ct, func(b *kvo.ModelBuilder) { b.Prop(name) })
	children := kvo.NewRecordListProp(s, 2, "children", kvo.ListOf(ct))
	m = kvo.NewModel(s, kvo.NewEntityType(s, "parent", reflect.TypeFor[parent]()), func(b *kvo.ModelBuilder) { b.Prop(children) })
	p = kvo.Project(reflect.TypeFor[parent](), m, nil)
	src := &parent{[]*child{{"kept"}, nil}}
	ordinary, err := json.Marshal(src)
	if err != nil {
		panic(err)
	}
	fmt.Printf("ordinary Go JSON: %s\n", ordinary)
	raw = wire.Encode(nil, reflect.ValueOf(src))
	for _, r := range []kvo.Record{p.Record(reflect.ValueOf(src)), p.MessagePack(raw)} {
		n := 0
		for c := range r.Get(children).Iter() {
			if n == 0 && c.Get(name) != "kept" {
				panic("list item changed")
			}
			if n == 1 && !c.Missing() {
				panic("nil item lost")
			}
			n++
		}
		if n != 2 {
			panic("list count changed")
		}
		fmt.Println("borrowed list iteration with nil element: OK")
		observe("record TryToJSON", func() { _, err := r.TryToJSON(m, kvo.JSONOptions{}); fmt.Printf("error=%v\n", err) })
	}
}
