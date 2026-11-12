package evm_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type jsonBenchItem struct {
	Name  string `kvo:"name"`
	Count int64  `kvo:"count"`
}
type jsonBenchRow struct {
	Name   string          `kvo:"name"`
	Items  []jsonBenchItem `kvo:"items"`
	Labels []string        `kvo:"labels"`
}

func recordJSONFixture(tb testing.TB) (*evm.Program, kvo.Record) {
	tb.Helper()
	s := kvo.NewSchema()
	i := kvo.NewEntityType(s, "item", reflect.TypeFor[jsonBenchItem]())
	r := kvo.NewEntityType(s, "row", reflect.TypeFor[jsonBenchRow]())
	kvo.NewProp(s, 1, "name", kvo.TMachineString, nil)
	kvo.NewProp(s, 2, "count", kvo.TInt64, nil)
	kvo.NewProp(s, 3, "items", kvo.ListOf(i), nil)
	kvo.NewProp(s, 4, "labels", kvo.ListOf(kvo.TMachineString), nil)
	kvo.NewModel(s, i, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1)); b.Prop(s.MustPropByCode(2)) })
	m := kvo.NewModel(s, r, func(b *kvo.ModelBuilder) {
		b.Prop(s.MustPropByCode(1))
		b.Prop(s.MustPropByCode(3))
		b.Prop(s.MustPropByCode(4))
	})
	bind := kvo.Bind(reflect.TypeFor[jsonBenchRow](), m)
	row := jsonBenchRow{Name: "Customer & friends", Labels: []string{"vip", "newsletter"}}
	for range 32 {
		row.Items = append(row.Items, jsonBenchItem{"Nested item", 9007199254740993})
	}
	raw := bind.Encode(nil, reflect.ValueOf(row), row2.Fast2)
	pkg, err := evm.ModelPackage("m", s)
	if err != nil {
		tb.Fatal(err)
	}
	mod, err := evmcompiler.Compile("json.go", `import "m"
func Export(r *m.Row) any { return r.ToJSON() }`, pkg)
	if err != nil {
		tb.Fatal(err)
	}
	p, err := evm.Link(mod, pkg)
	if err != nil {
		tb.Fatal(err)
	}
	return p, bind.Encoded(row2.Trusted(raw))
}

func BenchmarkRecordJSON(b *testing.B) {
	p, r := recordJSONFixture(b)
	vm := evm.New(p)
	// The benchmark reuses a VM across independent exports; do not exhaust
	// its cumulative session budget while timing the conversion itself.
	vm.Limits.AllocationBytes, vm.Limits.Allocations = ^uint64(0), ^uint64(0)
	args := []values.Value{r.Value()}
	var out [1]values.Value
	b.ReportAllocs()
	for b.Loop() {
		if err := vm.Run(context.Background(), "Export", args, out[:]); err != nil {
			b.Fatal(err)
		}
	}
}
