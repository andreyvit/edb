package evmcompiler_test

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/values"
)

type borrowedListRow struct {
	Tags []string `msgpack:"tags"`
}

func TestBorrowedRecordList(t *testing.T) {
	s := kvo.NewSchema()
	e := kvo.NewEntityType(s, "customer")
	kvo.NewProp(s, 1, "tags", kvo.ListOf(kvo.TMachineString), nil)
	m := kvo.NewModel(s, e, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1)) })
	projection := kvo.Project(reflect.TypeFor[borrowedListRow](), m, kvo.FieldMap{reflect.TypeFor[borrowedListRow](): {1: "Tags"}})
	pkg := evm.Package{Path: "db", Records: []*evm.RecordType{evm.NewRecordType("Customer", m, false)}}
	var invalidate func()
	pkg.Functions = []evm.ExternalFunction{{Name: "End", Signature: evm.Signature{}, Call: func(c *evm.Call) error { invalidate(); return nil }}}
	mod, err := evmcompiler.Compile("lists.go", `import "db"
func Count(c *db.Customer, n int) int { sum:=0; for i:=0;i<n;i++ {for _,t:=range c.tags {if t=="two" {sum++;break}}};return sum }
func Length(c *db.Customer) int {t:=c.tags;return len(t)+cap(t)}
func At(c *db.Customer,i int) bool { return c.tags[i]=="two" }
func DirectLength(c *db.Customer) int {return len(c.tags)}
func CalculatedIndex(c *db.Customer,i int) bool {return c.tags[1/i]=="two"}
func Pair(c *db.Customer,d *db.Customer) int {a,b:=c.tags,d.tags;sum:=len(a)+len(b);for _,x:=range a {if x=="two" {sum++}};for _,x:=range b {if x=="two" {sum++}};return sum}
func Nested(c *db.Customer,d *db.Customer) int {sum:=0;for _,x:=range c.tags {for _,y:=range d.tags {if x==y {sum++}}};return sum}
func Escape(c *db.Customer) []string {return c.tags}
func Element(c *db.Customer) string {return c.tags[1]}
func Mutate(c *db.Customer) string {t:=c.tags;t[1]="changed";return t[1]}
func Retain(c *db.Customer) bool {t:=c.tags;db.End();return t[1]=="two"}
`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	for _, fn := range mod.Functions {
		borrowed := false
		for _, ins := range fn.Code {
			if ins.Op() == evm.OpExtended && (mod.ExtendedOps[ins.BC()].Kind == evm.ExtendedRecordList || mod.ExtendedOps[ins.BC()].Kind == evm.ExtendedRecordListIndex || mod.ExtendedOps[ins.BC()].Kind == evm.ExtendedRecordListLen) {
				borrowed = true
			}
		}
		want := fn.Name == "Count" || fn.Name == "Length" || fn.Name == "At" || fn.Name == "DirectLength" || fn.Name == "CalculatedIndex" || fn.Name == "Pair" || fn.Name == "Nested"
		if borrowed != want {
			t.Fatalf("%s borrowed=%v\n%s", fn.Name, borrowed, mod.Disassemble())
		}
	}
	prog, err := evm.Link(mod, pkg)
	if err != nil {
		t.Fatal(err)
	}
	// A module can also come from outside the compiler. Reject malformed list
	// operands before resolving access metadata or executing their input cells.
	for _, flags := range []uint8{evm.RecordListDirect, 128} {
		invalid := *mod
		invalid.ExtendedOps = slices.Clone(mod.ExtendedOps)
		for i := range invalid.ExtendedOps {
			if invalid.ExtendedOps[i].Kind == evm.ExtendedRecordList {
				invalid.ExtendedOps[i].Flags = flags
				break
			}
		}
		if _, err := evm.Link(&invalid, pkg); err == nil {
			t.Fatal("invalid list acquisition flags accepted", flags)
		}
	}
	vm := evm.New(prog)
	ctx := context.Background()
	raw := msgpack.Encode(nil, reflect.ValueOf(&borrowedListRow{[]string{"one", "two", "three"}}))
	r := projection.MessagePack(raw)
	compact := kvo.Encoded(r.Snapshot()).WithModel(m)
	for _, r := range []kvo.Record{r, r.Fastest(), compact, compact.Fastest()} {
		var out [1]values.Value
		args := []values.Value{r.Value(), values.Int(1)}
		run := func() {
			if err := vm.Run(ctx, "Count", args, out[:]); err != nil || out[0].AsInt() != args[1].AsInt() {
				t.Fatalf("%v %v", out, err)
			}
		}
		one := testing.AllocsPerRun(5, run)
		args[1] = values.Int(1000)
		many := testing.AllocsPerRun(5, run)
		if many != one {
			t.Fatalf("allocations scale with reads: %g -> %g", one, many)
		}
		if err := vm.Run(ctx, "Length", args[:1], out[:]); err != nil || out[0].AsInt() != 6 {
			t.Fatalf("len/cap: %v %v", out, err)
		}
		for _, test := range []struct {
			name string
			args []values.Value
			want int64
		}{{"DirectLength", args[:1], 3}, {"Pair", []values.Value{r.Value(), r.Value()}, 8}, {"Nested", []values.Value{r.Value(), r.Value()}, 3}} {
			if err := vm.Run(ctx, test.name, test.args, out[:]); err != nil || out[0].AsInt() != test.want {
				t.Fatal(test.name, out, err)
			}
		}
		for _, i := range []int64{-1, 3} {
			if err := vm.Reset(); err != nil {
				t.Fatal(err)
			}
			args[1] = values.Int(i)
			if err := vm.Run(ctx, "At", args, out[:]); err == nil || !strings.Contains(err.Error(), "bounds") {
				t.Fatalf("index %d: %v", i, err)
			}
		}
		if err := vm.Reset(); err != nil {
			t.Fatal(err)
		}
	}
	args := []values.Value{r.Value()}
	var out [1]values.Value
	for _, fn := range []string{"DirectLength", "CalculatedIndex"} {
		nilargs := []values.Value{{}, values.Int(0)}
		if fn == "DirectLength" {
			nilargs = nilargs[:1]
		}
		if err := vm.Run(ctx, fn, nilargs, out[:]); err == nil || !strings.Contains(err.Error(), "missing") {
			t.Fatal("evaluation order", fn, err)
		}
		if err := vm.Reset(); err != nil {
			t.Fatal(err)
		}
	}

	if err := vm.Run(ctx, "Mutate", args, out[:]); err != nil || out[0].AsString() != "changed" || r.ListValue(1).At(1).AsString() != "two" {
		t.Fatalf("mutation: %v %v", out, err)
	}
	invalidate = func() { clear(raw) }
	if err := vm.Run(ctx, "Retain", args, out[:]); err != nil || !out[0].AsBool() {
		t.Fatalf("retained list: %v %v", out, err)
	}
	for _, tags := range [][]string{nil, {}} {
		x := projection.MessagePack(msgpack.Encode(nil, reflect.ValueOf(&borrowedListRow{tags})))
		args := []values.Value{x.Value(), values.Int(5)}
		if err := vm.Run(ctx, "Count", args, out[:]); err != nil || out[0].AsInt() != 0 {
			t.Fatalf("empty: %v %v", out, err)
		}
	}
}
