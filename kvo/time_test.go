package kvo_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestTimestampRepresentations(t *testing.T) {
	type row struct {
		At time.Time `kvo:"at"`
	}
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "Timed", reflect.TypeFor[row]())
	at := kvo.NewTimeProp(s, 1, "at")
	model := kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) { b.Prop(at) })
	binding := kvo.Bind(reflect.TypeFor[row](), model)
	projection := kvo.Project(reflect.TypeFor[row](), model, nil)
	times := []time.Time{{}, time.Unix(0, 0), time.Unix(0, -1), time.Unix(1234567890, 123456789), time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC), time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2026, 11, 10, 12, 0, 0, 123456789, time.FixedZone("offset", 18000))}
	for _, want := range times {
		row := &row{At: want}
		native := projection.Record(reflect.ValueOf(row))
		packed := projection.MessagePack(msgpack.Encode(nil, reflect.ValueOf(row)))
		records := []kvo.Record{native, packed, packed.Fastest()}
		encoded := []kvo.Record{packed}
		for _, layout := range []row2.Layout{row2.Fast2, row2.Compact2} {
			raw := binding.Encode(nil, reflect.ValueOf(row).Elem(), layout)
			binding.Validate(raw)
			records = append(records, kvo.Encoded(row2.Open(raw)).WithModel(model))
			encoded = append(encoded, records[len(records)-1])
			var decoded = *row
			binding.Decode(raw, reflect.ValueOf(&decoded).Elem())
			if !decoded.At.Equal(want) {
				t.Fatal(decoded.At, want)
			}
		}
		for _, r := range records {
			if got := r.Get(at); !got.Equal(want) {
				t.Fatalf("got %v want %v", got, want)
			}
			if r.Has(at.Code()) == want.IsZero() {
				t.Fatalf("presence: %v", want)
			}
			e := r.Clone().Edit()
			changed := time.Unix(100, 17).UTC()
			e.Set(at, changed)
			if !e.Get(at).Equal(changed) || !r.Get(at).Equal(want) {
				t.Fatal("edit changed original or lost nanoseconds")
			}
		}
		for _, r := range encoded {
			if n := testing.AllocsPerRun(100, func() { _ = r.Get(at) }); n != 0 {
				t.Fatalf("%v: %g allocations", want, n)
			}
		}
	}
	var w row2.Writer
	w.Begin(0, "", row2.Object, row2.Fast2)
	legacy := "2026-11-10T12:34:56.123456789Z"
	w.Put(1, "", values.String(legacy), row2.Data)
	w.Finish(true)
	if got := kvo.Encoded(row2.Open(w.BytesWritten())).WithModel(model).Get(at).Format(time.RFC3339Nano); got != legacy {
		t.Fatal(got)
	}
}

func TestTimestampEVM(t *testing.T) {
	type row struct {
		At time.Time `kvo:"at"`
	}
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "Timed", reflect.TypeFor[row]())
	at := kvo.NewTimeProp(s, 1, "at")
	m := kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) { b.Prop(at) })
	pkg, err := evm.ModelPackage("m", s)
	if err != nil {
		t.Fatal(err)
	}
	mod, err := evmcompiler.Compile("time.go", `import "m"; import "time";import "fmt"
 func Read(r *m.Timed) int64 { if r.at.IsZero() {return 0}; return r.at.UnixNano() }
 func Same(a,b *m.Timed) bool {return a.at == b.at && a.at.Equal(b.at) && !a.at.Before(b.at)}
 func Text(r *m.Timed)string{return fmt.Sprint(r.at)}
 func Write(r *m.TimedEdit, at time.Time) {r.at=at}
 func Repeat(r *m.Timed, n int64) int64 {var sum int64;for i:=int64(0);i<n;i++{sum+=r.at.UnixNano()};return sum}
 `, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(mod, pkg)
	if err != nil {
		t.Fatal(err)
	}
	b := kvo.Bind(reflect.TypeFor[row](), m)
	src := &row{time.Unix(1234567890, 123456789)}
	r := b.MessagePack(msgpack.Encode(nil, reflect.ValueOf(src)))
	ctx, scope := evm.WithRecords(context.Background())
	defer scope.Close()
	vm := evm.New(p)
	out := make([]values.Value, 1)
	if err := vm.Run(ctx, "Read", []values.Value{scope.ReadRecord(m, r)}, out); err != nil || out[0].AsInt() != src.At.UnixNano() {
		t.Fatal(out, err)
	}
	if err := vm.Run(ctx, "Same", []values.Value{scope.ReadRecord(m, r), scope.ReadRecord(m, r.Fastest())}, out); err != nil || !out[0].AsBool() {
		t.Fatal(out, err)
	}
	other := &row{At: src.At.Add(time.Second)}
	if err := vm.Run(ctx, "Same", []values.Value{scope.ReadRecord(m, r), scope.ReadRecord(m, b.Record(reflect.ValueOf(other)))}, out); err != nil || out[0].AsBool() {
		t.Fatal(out, err)
	}

	if err := vm.Run(ctx, "Text", []values.Value{scope.ReadRecord(m, r)}, out); err != nil || out[0].AsString() != src.At.UTC().String() {
		t.Fatal(out, err)
	}

	args := []values.Value{scope.ReadRecord(m, r), values.Int(1)}
	run := func() {
		if err := vm.Run(ctx, "Repeat", args, out); err != nil {
			t.Fatal(err)
		}
	}
	one := testing.AllocsPerRun(100, run)
	args[1] = values.Int(1000)
	many := testing.AllocsPerRun(100, run)
	if out[0].AsInt() != src.At.UnixNano()*1000 {
		t.Fatal("loop result", out[0].AsInt())
	}
	if many != one {
		t.Fatalf("timestamp loop allocates per read: one=%g thousand=%g", one, many)
	}

	e := r.Edit()
	changed := time.Unix(123, 456)
	if err := vm.Run(ctx, "Write", []values.Value{scope.EditRecord(m, e), values.Time(changed)}, nil); err != nil || !e.Get(at).Equal(changed) {
		t.Fatal(e.Get(at), err)
	}
}
