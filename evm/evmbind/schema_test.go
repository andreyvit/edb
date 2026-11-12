//go:build !evmreflect

package evmbind_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmbind"
	"github.com/andreyvit/edb/evm/evmbind/internal/fixture"
	"github.com/andreyvit/edb/evm/evmbind/internal/fixturebindings"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type transaction struct {
	tenant   string
	balance  int
	line     int
	observed bool
	saved    func()
}

func (t *transaction) Tenant() string {
	if t == nil {
		return "nil"
	}
	return t.tenant
}
func (t *transaction) Credit(n int) int { t.balance += n; return t.balance }

func (t *transaction) During(f func()) {
	t.saved = f
	if f != nil {
		f()
	}
}
func (t *transaction) Mark()      {}
func (t *transaction) ResetMark() {}
func (t *transaction) Observe(columns []string, row map[string]any) {
	if !reflect.DeepEqual(columns, []string{"n", "items"}) || row["n"] != json.Number("9223372036854775815") || !reflect.DeepEqual(row["items"], []any{"a", true, nil}) || t.line == 0 {
		panic("incorrect bound inputs")
	}
	columns[0] = "changed"
	delete(row, "n")
	t.observed = true
}
func (t *transaction) forCall(c *evm.Call) *transaction {
	if t != nil {
		t.line = c.Position.Line
	}
	return t
}

func (t *transaction) Secret() string { return "host only" }

type host struct {
	seen fixture.Context
	row  *fixture.Row
	mark fixture.Count
}

func (h *host) Pair(_ *evm.Call) (*fixture.Row, *fixture.Row) {
	r := &fixture.Row{Ratio: -1.25, Amount: 1.5, Bits: 65535}
	h.row = r
	return r, r
}

func (h *host) Roundtrip(_ *evm.Call, ctx fixture.Context, a, b *fixture.Row) (fixture.Context, *fixture.Row, fixture.Row) {
	if a != b || a.Count != 5 || a.Detail.Text != "hello" || !a.Active {
		panic("bad native args")
	}
	h.seen = ctx
	b.Detail.Text = ctx.Tenant()
	return ctx, b, *a
}
func options() evmbind.Options {
	return evmbind.Options{Host: reflect.TypeFor[*host](), Types: []reflect.Type{reflect.TypeFor[fixture.Observer]()}, Implementations: []evmbind.Implementation{evmbind.PointerForCall((*transaction).forCall)}}
}
func TestSharedSchema(t *testing.T) {
	for _, mode := range []string{"generated", "reflected", "stale", "staticOnly"} {
		t.Run(mode, func(t *testing.T) {
			opt := options()
			static := fixturebindings.Bindings
			opt.Static = &static
			switch mode {
			case "staticOnly":
				opt.Mode = evmbind.StaticOnly
			case "reflected":
				opt.Static = nil
			case "stale":
				static.Fingerprint = "old"
			}
			s, err := evmbind.New("h", os.DirFS("internal/fixture"), opt)
			if err != nil {
				t.Fatal(err)
			}
			if s.UsesStatic() != (mode == "generated" || mode == "staticOnly") {
				t.Fatal("wrong binding mode")
			}
			m, err := evmcompiler.Compile("main.go", `import "h"
func Run(ctx h.Context) bool {
 if ctx==nil {return true}
 if _,ok:=ctx.(interface{Secret() string});ok{panic("host method leaked")}
 x,y:=h.Pair()
 if x!=y || x.Ratio != -1.25 || x.Amount!=1.5 || x.Bits!=65535 || (h.Huge >> 100)!=1 {return false}
 if ctx.Tenant()=="nil" {return true}
 var o h.Observer=ctx
 cols:=[]string{"n","items"}
 row:=map[string]any{"n":uint64(1<<63)+7,"items":[]any{"a",true,nil}}
 var captured int
 o.During(func(){ o.During(func(){ if ctx.Tenant()!="" { captured++ } }); captured+=2 })
 if captured != 3 {return false}
 o.Mark()
 o.Observe(cols,row)
 o.ResetMark()
 if cols[0]!="n" || row["n"]==nil {return false}
 a:=h.Row{Detail:h.Detail{Text:"hello"},Count:h.Start,Active:true}
 if a.Bump(3)!=5{return false}
 ctx2,b,snapshot:=h.Roundtrip(ctx,&a,&a)
 if ctx2!=ctx || b!=&a || a.Detail.Text!=ctx.Tenant() || snapshot!=a{return false}
 b.Count++;return snapshot.Count==5 && ctx2.Credit(int(b.Count))==6
}`, s.Package())
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"one", "two"} {
				h := &host{}
				p, err := evm.Link(m, s.Bind(h))
				if err != nil {
					t.Fatal(err, m.Disassemble())
				}
				vm := evm.New(p)
				tx := &transaction{tenant: name}
				arg, err := s.Interface(p, tx)
				if err != nil || arg.P != unsafe.Pointer(tx) || arg.R == 0 {
					t.Fatalf("boxed context: %v %v", arg, err)
				}
				if alloc := testing.AllocsPerRun(100, func() { _, err = s.Interface(p, tx) }); alloc != 0 {
					t.Fatalf("context packing allocated %v", alloc)
				}
				nilPtr, err := s.Interface(p, (*transaction)(nil))
				if err != nil || nilPtr.P != nil || nilPtr.R != arg.R {
					t.Fatal("lost typed nil")
				}
				for _, v := range []values.Value{arg, nilPtr, {}} {
					out := make([]values.Value, 1)
					if err := vm.Run(context.Background(), "Run", []values.Value{v}, out); err != nil || out[0].R != 1 {
						t.Fatalf("run %s: %v %v", name, out, err)
					}
				}
				if h.seen != tx || tx.balance != 6 || !tx.observed {
					t.Fatal("wrong native receiver or transaction")
				}
			}
		})
	}
}
func TestBorrowedCallback(t *testing.T) {
	for _, mode := range []string{"generated", "reflection"} {
		t.Run(mode, func(t *testing.T) {
			opt := options()
			if mode == "generated" {
				opt.Static = &fixturebindings.Bindings
			}
			s, err := evmbind.New("h", os.DirFS("internal/fixture"), opt)
			if err != nil {
				t.Fatal(err)
			}
			const source = `import "h"
var total int
func bump(){ total++ }
func recurse(ctx h.Context){ ctx.During(func(){ recurse(ctx) }) }
func Run(ctx h.Context) int {
    ctx.During(nil)
    ctx.During(bump)
    defer ctx.During(func(){total++})
    ctx.During(func(){defer bump(); total+=2})
    return total
}
func Total() int {return total}
func Forever(ctx h.Context) {for {ctx.During(bump)}}
func Deep(ctx h.Context) {recurse(ctx)}
`
			m, err := evmcompiler.Compile("callback.go", source, s.Package())
			if err != nil {
				t.Fatal(err)
			}
			p, err := evm.Link(m, s.Bind(&host{}))
			if err != nil {
				t.Fatal(err)
			}
			tx := &transaction{}
			v, err := s.Interface(p, tx)
			if err != nil {
				t.Fatal(err)
			}
			args := []values.Value{v}
			vm := evm.New(p)
			out := make([]values.Value, 1)
			if err := vm.Run(context.Background(), "Run", args, out); err != nil || out[0].AsInt() != 4 {
				t.Fatal(err, out)
			}
			if err := vm.Run(context.Background(), "Total", nil, out); err != nil || out[0].AsInt() != 5 {
				t.Fatal(err, out)
			}
			func() {
				defer func() {
					if p := recover(); p == nil || !strings.Contains(p.(string), "after its external call") {
						t.Fatal(p)
					}
				}()
				tx.saved()
			}()
			if err := vm.RunWithBudget(context.Background(), "Forever", args, nil, evm.Budget{Steps: 900}); !errors.Is(err, evm.ErrSteps) {
				t.Fatal(err)
			}
			vm = evm.New(p)
			vm.Limits.Depth = 8
			if err := vm.Run(context.Background(), "Deep", args, nil); !errors.Is(err, evm.ErrDepth) {
				t.Fatal(err)
			}
		})
	}
}

func TestBindingDrift(t *testing.T) {
	files := fstest.MapFS{}
	for _, f := range []string{"api.go", "calls.go"} {
		b, e := os.ReadFile("internal/fixture/" + f)
		if e != nil {
			t.Fatal(e)
		}
		files[f] = &fstest.MapFile{Data: b}
	}
	opt := options()
	opt.Static = &fixturebindings.Bindings
	files["api.go"].Data = append(files["api.go"].Data, []byte("\n// whitespace and docs are not ABI\n")...)
	s, err := evmbind.New("h", files, opt)
	if err != nil || !s.UsesStatic() {
		t.Fatal(err)
	}

	files["api.go"].Data = bytes.Replace(files["api.go"].Data, []byte("= 2"), []byte("= 3"), 1)
	s, err = evmbind.New("h", files, opt)
	if err != nil || s.UsesStatic() {
		t.Fatal("source drift not detected", err)
	}
	for _, static := range []*evmbind.Static{nil, opt.Static} {
		func() {
			defer func() {
				if x := recover(); x == nil || !strings.Contains(x.(string), "stale") {
					t.Error("static-only mismatch did not panic")
				}
			}()
			opt.Mode = evmbind.StaticOnly
			opt.Static = static
			_, _ = evmbind.New("h", files, opt)
		}()
	}
	generated, err := evmbind.Generate("h", "github.com/andreyvit/edb/evm/evmbind/internal/fixture", "fixturebindings", os.DirFS("internal/fixture"))
	if err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile("internal/fixturebindings/bindings_gen.go")
	if err != nil || !bytes.Equal(generated, saved) {
		t.Fatal("regenerate fixture bindings", err)
	}
}

func (h *host) Enabled(c *evm.Call, n fixture.Count) bool { return n&1 != 0 }
func (h *host) Name(c *evm.Call, prefix fixture.Label, n fixture.Count) fixture.Label {
	if c.Context == nil || c.Position.Line == 0 || c.Argument(0).AsString() != string(prefix) || c.R[1] != uint64(n) || c.P[0] != c.Argument(0).P {
		panic("incorrect native argument window")
	}
	if prefix == "panic" {
		panic("from Name")
	}
	return fixture.Label(strings.Repeat(string(prefix), int(n)))
}
func (h *host) Lookup(c *evm.Call, id fixture.Count) *fixture.Row {
	return &fixture.Row{Count: id, Detail: fixture.Detail{Text: "member"}}
}
func (h *host) Ratio(c *evm.Call, x float32) float64 { return float64(x) * 1.5 }
func (h *host) Mark(_ *evm.Call, n fixture.Count)    { h.mark = n }
func (h *host) Marked(_ *evm.Call) fixture.Count     { return h.mark }

const generatedCalls = `import "h"
func Check() bool {
	a:=h.Lookup(9)
	b:=h.Lookup(4)
	var empty *h.Row
	return a.Choose(a)==a && a.Count==12 && a.Choose(b)==b && a.Count==13 &&
		b.Count==6 && empty.Choose(b)==b && b.Choose(nil)==b && b.Count==7 &&
		a.Tagged("member") && !empty.Tagged("member") &&
		h.Enabled(3) && !h.Enabled(2) && h.Name("ab",3)=="ababab" && h.Ratio(-1.25)==-1.875
}
func Bad() h.Label {return h.Name("panic",2)}
func StringBudget() h.Label {return h.Name("abc",2)}
func Predicates() int {
	count:=0
	for i:=h.Count(0);i<256;i++ {
		if h.Enabled(i) {count++}
	}
	return count
}
func Tags()int {
	r:=h.Row{Detail:h.Detail{Text:"member"}}
	count:=0
	for i:=0;i<256;i++ {if r.Tagged("member"){count++}}
	return count
}
func VoidFunction() h.Count {
 for i:=h.Count(0);i<256;i++ { h.Mark(i) }
 return h.Marked()
}
func VoidMethod() h.Count {
 r:=h.Row{}
 for i:=h.Count(0);i<256;i++ { r.Mark(i) }
 return r.Count
}
`

func generatedVM(t testing.TB, mode string) (*evm.VM, *evm.Module) {
	t.Helper()
	bindings := fixturebindings.Bindings
	opt := options()
	switch mode {
	case "Direct":
		opt.Static = &bindings
	case "General":
		bindings.Direct = nil
		opt.Static = &bindings
	case "Reflection":
	}
	s, err := evmbind.New("h", os.DirFS("internal/fixture"), opt)
	if err != nil {
		t.Fatal(err)
	}
	m, err := evmcompiler.Compile("bindings.go", generatedCalls, s.Package())
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, s.Bind(&host{}))
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	vm.Limits.Allocations = ^uint64(0)
	vm.Limits.AllocationBytes = ^uint64(0)
	return vm, m
}
func TestGeneratedDirect(t *testing.T) {
	for _, mode := range []string{"Direct", "General", "Reflection"} {
		t.Run(mode, func(t *testing.T) {
			vm, m := generatedVM(t, mode)
			if strings.Contains(m.Disassemble(), "call.host.method.pp") != (mode == "Direct") {
				t.Fatal(m.Disassemble())
			}
			for _, opcode := range []string{"call.host.uv", "call.host.method.uv"} {
				if strings.Contains(m.Disassemble(), opcode) != (mode == "Direct") {
					t.Fatalf("%s: missing/unexpected %s\n%s", mode, opcode, m.Disassemble())
				}
			}
			out := make([]values.Value, 1)
			for _, name := range []string{"VoidFunction", "VoidMethod"} {
				if err := vm.Run(context.Background(), name, nil, out); err != nil || out[0].R != 255 {
					t.Fatal(name, out, err)
				}
			}
			if err := vm.Run(context.Background(), "Check", nil, out); err != nil || out[0].R != 1 {
				t.Fatal(out, err)
			}
			if err := vm.Run(context.Background(), "Bad", nil, out); err == nil || !strings.Contains(err.Error(), "from Name") {
				t.Fatal(err)
			}
			vm.Reset()
			if err := vm.Run(context.Background(), "StringBudget", nil, out); err != nil {
				t.Fatal(err)
			}
			if err := vm.RunWithBudget(context.Background(), "StringBudget", nil, out, evm.Budget{AllocationBytes: 6, Allocations: 1}); err != nil || out[0].AsString() != "abcabc" {
				t.Fatal(out, err)
			}
			if err := vm.RunWithBudget(context.Background(), "StringBudget", nil, out, evm.Budget{AllocationBytes: 5}); !errors.Is(err, evm.ErrAllocation) {
				t.Fatal(err)
			}
		})
	}
}
func BenchmarkGeneratedHost(b *testing.B) {
	for _, work := range []struct {
		name string
		want uint64
	}{{"Predicates", 128}, {"Tags", 256}, {"VoidFunction", 255}, {"VoidMethod", 255}} {
		b.Run(work.name, func(b *testing.B) {
			for _, mode := range []string{"General", "Direct"} {
				b.Run(mode, func(b *testing.B) {
					vm, _ := generatedVM(b, mode)
					out := make([]values.Value, 1)
					if err := vm.Run(context.Background(), work.name, nil, out); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if err := vm.Run(context.Background(), work.name, nil, out); err != nil || out[0].R != work.want {
							b.Fatal(out, err)
						}
					}
				})
			}
		})
	}
}

func TestSharedNativeFields(t *testing.T) {
	for _, generated := range []bool{false, true} {
		opt := options()
		if generated {
			opt.Static = &fixturebindings.Bindings
			opt.Mode = evmbind.StaticOnly
		}
		s, err := evmbind.New("h", os.DirFS("internal/fixture"), opt)
		if err != nil {
			t.Fatal(err)
		}
		b := s.NativeFields(reflect.TypeFor[fixture.Row]())
		if b == nil || b.Generated != generated || s.NativeType(b.Type) == nil {
			t.Fatal("missing native binding")
		}
		row := fixture.Row{Count: 5, Detail: fixture.Detail{Text: "original"}}
		p := unsafe.Pointer(&row)
		b.Field("Count").Set(p, values.Int(9))
		child := b.Field("Detail").Get(p)
		s.NativeFields(reflect.TypeFor[fixture.Detail]()).Field("Text").Set(child.P, values.String("changed"))
		if row.Count != 9 || row.Detail.Text != "changed" {
			t.Fatal("native fields detached from owner")
		}
	}
}

func TestLiveNativeFields(t *testing.T) {
	for _, generated := range []bool{false, true} {
		for _, raw := range []bool{false, true} {
			name := fmt.Sprintf("generated=%v/raw=%v", generated, raw)
			t.Run(name, func(t *testing.T) {
				opt := options()
				if generated {
					opt.Static = &fixturebindings.Bindings
				}
				s, err := evmbind.New("h", os.DirFS("internal/fixture"), opt)
				if err != nil {
					t.Fatal(err)
				}
				m, err := (evmcompiler.Options{NoOptimization: raw}).Compile("live.go", `import "h"
import "fmt"
var saved *h.Row
var count h.Count
func increment(p *h.Count) { *p += 2 }
func Run() bool {
 x,y:=h.Pair()
 saved=x
 x.Count=10
 increment(&x.Count)
 x.Detail.Text="live"
 if y.Count!=12 || !y.Tagged("live") {return false}
 if x.Bump(3)!=15 || y.Choose(x)!=x || x.Count!=18 {return false}
 snapshot:=*x
 snapshot.Count=99
 *x=h.Row{Count:20,Detail:h.Detail{Text:"whole"}}
 if y.Count!=20 || snapshot.Count!=99 {return false}
 count=x.Count
 a:=[]h.Count{1,2,3}
 for i:=range a {a[i]+=count}
 return a[2]==23 && saved==y
}
func Later() h.Count {return saved.Count}
func VMCycles() bool {
 a:=[2]h.Row{{Count:2},{Count:3}}
 a[0].Next=&a[1];a[1].Next=&a[0]
 p:=a[0].Choose(&a[1])
 return p==&a[1] && a[0].Count==3 && a[1].Count==5 && a[0].Next.Next==&a[0]
}
func Display() string {
 x,_:=h.Pair(); x.Count=18; x.Detail.Text="native"
 return fmt.Sprintf("%+v",x)
}
func References() bool {
 x,y:=h.Pair()
 var dyn interface { Bump(h.Count) h.Count }=x
 if dyn.Bump(4)!=4{return false}
 x.Next=x
 x.Counter=&count
 *y.Counter=42
 if x.Next!=y || count!=42 {return false}
 x.Next=new(h.Row)
 x.Next.Count=3
 x.Next.Counter=new(h.Count)
 *x.Next.Counter=7
 if x.Next.Bump(2)!=5 {return false}
 copy:=*x
 if copy.Next!=x.Next || copy.Counter!=x.Counter {return false}
 x.Counter=nil
 return copy.Counter!=nil && copy.Next.Counter!=nil
}
`, s.Package())
				if err != nil {
					t.Fatal(err)
				}
				h := &host{}
				p, err := evm.Link(m, s.Bind(h))
				if err != nil {
					t.Fatal(err, m.Disassemble())
				}
				vm := evm.New(p)
				out := make([]values.Value, 1)
				if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].R != 1 {
					t.Fatalf("%v %v\n%s", out, err, m.Disassemble())
				}
				if h.row.Count != 20 || h.row.Detail.Text != "whole" {
					t.Fatalf("detached native object: %+v", h.row)
				}
				h.row.Count = 27
				if err := vm.Run(context.Background(), "Later", nil, out); err != nil || out[0].R != 27 {
					t.Fatalf("lost host identity: %v %v", out, err)
				}
				if err := vm.Run(context.Background(), "References", nil, out); err != nil || out[0].R != 1 {
					t.Fatalf("reference fields: %v %v", out, err)
				}
				if h.row.Next == nil || h.row.Next.Count != 5 || *h.row.Next.Counter != 7 || h.row.Counter != nil {
					t.Fatalf("detached native reference: %+v", h.row)
				}

				if err := vm.Run(context.Background(), "Display", nil, out); err != nil || !strings.Contains(out[0].AsString(), "Count:18") || !strings.Contains(out[0].AsString(), "Text:native") {
					t.Fatalf("native formatting: %v %v", out, err)
				}

				if err := vm.Run(context.Background(), "VMCycles", nil, out); err != nil || out[0].R != 1 {
					t.Fatalf("VM pointer cycle: %v %v", out, err)
				}

			})
		}
	}
}

func TestKVOUsesHostBindings(t *testing.T) {
	for _, generated := range []bool{false, true} {
		opt := options()
		if generated {
			opt.Static = &fixturebindings.Bindings
		}
		s, err := evmbind.New("h", os.DirFS("internal/fixture"), opt)
		if err != nil {
			t.Fatal(err)
		}
		schema := kvo.NewSchema()
		rt := kvo.NewEntityType(schema, "row")
		dt := kvo.NewEntityType(schema, "detail")
		kvo.NewProp(schema, 1, "count", kvo.NewIntType[fixture.Count]("count", nil), nil)
		kvo.NewProp(schema, 2, "detail", dt, nil)
		kvo.NewProp(schema, 3, "text", kvo.TMachineString, nil)
		kvo.NewModel(schema, dt, func(b *kvo.ModelBuilder) { b.Prop(schema.MustPropByCode(3)) })
		model := kvo.NewModel(schema, rt, func(b *kvo.ModelBuilder) { b.Prop(schema.MustPropByCode(1)); b.Prop(schema.MustPropByCode(2)) })
		projection := kvo.Project(reflect.TypeFor[fixture.Row](), model, kvo.FieldMap{
			reflect.TypeFor[fixture.Row](): {1: "Count", 2: "Detail"}, reflect.TypeFor[fixture.Detail](): {3: "Text"},
		}).WithNativeSchema(s)
		row := &fixture.Row{Count: 2, Detail: fixture.Detail{Text: "original"}}
		r := projection.Record(reflect.ValueOf(row))
		if r.Native() != row || r.NativeType() != s.Type("Row") {
			t.Fatal("lost native identity or type")
		}
		if err := r.Edit().TrySetValue(1, values.Int(5)); err != nil {
			t.Fatal(err)
		}
		if err := r.Edit().TryFastSetPathValue([]row2.Step{{Key: 2}, {Key: 3}}, values.Bytes([]byte("KVO"))); err != nil {
			t.Fatal(err)
		}
		m, err := evmcompiler.Compile("bound.go", `import "h"
func Run(row *h.Row) h.Count { row.Detail.Text="EVM"; return row.Bump(3) }
func Dynamic(row any) h.Count { return row.(*h.Row).Count }
func Pack(row *h.Row) any { return row }
`, s.Package())
		if err != nil {
			t.Fatal(err)
		}
		p, err := evm.Link(m, s.Bind(&host{}))
		if err != nil {
			t.Fatal(err)
		}
		arg, err := s.NativePointer(r.Native())
		if err != nil {
			t.Fatal(err)
		}
		packed, err := p.PointerInterface(s.NativeType(reflect.TypeFor[*fixture.Row]()), unsafe.Pointer(row))
		if err != nil {
			t.Fatal(err)
		}
		_, unpacked, err := p.UnpackInterface(packed)
		if err != nil || unpacked != arg {
			t.Fatal("native interface lost pointer tag", err)
		}

		out := make([]values.Value, 1)
		if err := evm.New(p).Run(context.Background(), "Run", []values.Value{arg}, out); err != nil {
			t.Fatal(err)
		}
		if out[0].R != 8 || row.Count != 8 || r.GetValue(1).AsInt() != 8 || string(r.Child(row2.Step{Key: 2}).GetValue(3).AsBytes()) != "EVM" {
			t.Fatalf("disconnected KVO/EVM/native object: %+v", row)
		}
		if err := evm.New(p).Run(context.Background(), "Dynamic", []values.Value{packed}, out); err != nil || out[0].R != 8 {
			t.Fatalf("native interface argument: %v %v", out, err)
		}
	}
}

func TestGeneratedNativeMethodDoesNotAllocate(t *testing.T) {
	call := fixturebindings.Bindings.Direct["*Row.Bump"](nil)
	row := new(fixture.Row)
	receiver := values.Value{R: values.NativeAddress, P: unsafe.Pointer(row)}
	if n := testing.AllocsPerRun(100, func() {
		if _, err := call(nil, receiver, values.Int(1)); err != nil {
			panic(err)
		}
	}); n != 0 {
		t.Fatalf("native method allocated %v", n)
	}
	if row.Count != 101 {
		t.Fatal("native method copied receiver")
	}
}
