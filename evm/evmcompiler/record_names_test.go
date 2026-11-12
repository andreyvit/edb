package evmcompiler_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestNamespacedRecordSelectors(t *testing.T) {
	s := kvo.NewSchema()
	key := kvo.NewProp(s, 7, "customer:points", kvo.TInt64, nil)
	m := kvo.NewModel(s, kvo.NewEntityType(s, "customer"), func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(key), kvo.PropName("legacy_points")) })
	pkg := evm.Package{Path: "m", Records: []*evm.RecordType{evm.NewRecordType("Customer", m, false), evm.NewRecordType("CustomerEdit", m, true)}}
	mod, err := evmcompiler.Compile("aliases.go", `import "m"
 func Read(r *m.Customer) int64 { return r.points + r.customer_points + r.legacy_points }
 func Edit(r *m.CustomerEdit) { r.customer_points = 10; r.points += 3 }
 `, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(mod, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	ctx, scope := evm.WithRecords(context.Background())
	defer scope.Close()
	edit := row2.Edit(row2.Empty(row2.Fast2))
	args := []values.Value{scope.Edit(m, edit)}
	if err := vm.Run(ctx, "Edit", args, nil); err != nil {
		t.Fatal(err)
	}
	if got := edit.Get([]row2.Step{{Key: key}}, row2.Signed).AsInt(); got != 13 {
		t.Fatal(got)
	}
	args[0] = scope.Read(m, edit.Snapshot())
	var out [1]values.Value
	if err := vm.Run(ctx, "Read", args, out[:]); err != nil {
		t.Fatal(err)
	}
	if out[0].AsInt() != 39 {
		t.Fatal(out)
	}
}

func TestRenamedUnqualifiedRecordSelectors(t *testing.T) {
	s := kvo.NewSchema()
	key := kvo.NewProp(s, 7, "points", kvo.TInt64, nil)
	m := kvo.NewModel(s, kvo.NewEntityType(s, "customer"), func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(key), kvo.PropName("balance")) })
	pkg := evm.Package{Path: "m", Records: []*evm.RecordType{evm.NewRecordType("Customer", m, false), evm.NewRecordType("CustomerEdit", m, true)}}
	mod, err := evmcompiler.Compile("aliases.go", `import "m"
func Read(r *m.Customer) int64 { return r.points + r.balance }
func Edit(r *m.CustomerEdit) { r.balance = 10; r.points += 3 }
`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(mod, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	ctx, scope := evm.WithRecords(context.Background())
	defer scope.Close()
	edit := row2.Edit(row2.Empty(row2.Fast2))
	if err := vm.Run(ctx, "Edit", []values.Value{scope.Edit(m, edit)}, nil); err != nil {
		t.Fatal(err)
	}
	var out [1]values.Value
	if err := vm.Run(ctx, "Read", []values.Value{scope.Read(m, edit.Snapshot())}, out[:]); err != nil || out[0].AsInt() != 26 {
		t.Fatal(out, err)
	}
}

func TestRecordMethodSelectorCollisions(t *testing.T) {
	s := kvo.NewSchema()
	key := kvo.NewProp(s, 7, "customer:points", kvo.TInt64, nil)
	m := kvo.NewModel(s, kvo.NewEntityType(s, "customer"), func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(key), kvo.PropName("balance")) })
	for _, mutable := range []bool{false, true} {
		for name := range m.Selectors() {
			t.Run(fmt.Sprintf("%s/mutable=%v", name, mutable), func(t *testing.T) {
				r := evm.NewRecordType("Customer", m, mutable)
				pkg := evm.Package{Path: "m", Records: []*evm.RecordType{r}, Methods: []evm.ExternalMethod{{
					Name: name, Receiver: r.Pointer, Signature: evm.Signature{Results: []*values.Type{values.Int64}},
					Call: func(c *evm.Call) error { c.SetResult(0, values.Int(42)); return nil },
				}}}
				// Direct package declarations must enforce the same rules as evmbind.
				_, err := evmcompiler.Compile("collision.go", `import "m"; func Read(r *m.Customer) int64 { return r.balance }`, pkg)
				if err == nil || !strings.Contains(err.Error(), "conflicts") {
					t.Fatalf("compile = %v, want selector conflict", err)
				}
			})
		}
	}
}
