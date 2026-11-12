package evmcompiler_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestKVOProperties(t *testing.T) {
	s := kvo.NewSchema()
	bar := kvo.NewEntityType(s, "bar")
	cust := kvo.NewEntityType(s, "customer")
	boz := kvo.NewProp(s, 5, "boz", kvo.TInt64, nil)
	child := kvo.NewProp(s, 41, "bar", bar, nil)
	total := kvo.NewProp(s, 99, "total", kvo.TInt64, nil)
	next := kvo.NewProp(s, 43, "next", bar, nil)
	units := kvo.NewProp(s, 17, "units", kvo.TInt64, nil)
	bm := kvo.NewModel(s, bar, func(b *kvo.ModelBuilder) {
		b.Prop(s.MustPropByCode(boz))
		b.Prop(s.MustPropByCode(next))
		b.Prop(s.MustPropByCode(units))
	})
	cm := kvo.NewModel(s, cust, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(total)); b.Prop(s.MustPropByCode(child)) })
	pkg := evm.Package{Path: "db", Records: []*evm.RecordType{evm.NewRecordType("Customer", cm, false), evm.NewRecordType("Bar", bm, false), evm.NewRecordType("Edit", cm, true), evm.NewRecordType("EditBar", bm, true)}}
	mod, err := evmcompiler.Compile("test.go", `import "db"
func Change(c *db.Edit) { b:=c.bar; b.boz += 3; c.total, b.boz = b.boz, c.total }
func Read(c *db.Customer) int64 { copy := *c; return copy.total * 10 + c.bar.boz }
func Missing(c *db.Edit) int64 { return c.bar.boz }
func Fused(c *db.Customer) int64 {
	return c.bar.boz
}
func Split(c *db.Customer) int64 {
	b := c.bar
	return b.boz
}
func Twice(c *db.Customer) int64 {
	b := c.bar
	return b.boz + b.boz
}
func Keep(a,b *db.Edit) int64 {
	var first *db.EditBar
	for _, row := range []*db.Edit{a,b} {
		child := row.bar
		if first == nil { first = child.next }
		child.boz++
	}
	first.boz = 73
	return a.bar.next.boz - b.bar.next.boz
}
func Pair(c *db.Customer)int64 {return c.bar.boz+c.bar.units}
`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	if listing := mod.Disassemble(); !strings.Contains(listing, "GetKey key5") || !strings.Contains(listing, "SetKey key99") {
		t.Fatal(listing)
	}
	p, err := evm.Link(mod, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	ctx, scope := evm.WithRecords(context.Background())
	defer scope.Close()
	edit := row2.Edit(row2.Empty(row2.Compact2))
	edit.Set([]row2.Step{{Key: total}}, values.Int(7), row2.Signed)
	var result [1]values.Value
	originalEdit := scope.Edit(cm, edit)
	args := []values.Value{originalEdit}
	if err := vm.Run(ctx, "Missing", args, result[:]); err != nil || result[0].R != 0 || !(edit.Get([]row2.Step{{Key: child}}, row2.Word).R == 0) {
		t.Fatalf("read created child: %v, %v", result, err)
	}
	if err := vm.Run(ctx, "Change", args, nil); err != nil {
		t.Fatal(err)
	}
	edit.Set([]row2.Step{{Key: child}, {Key: units}}, values.Int(-12), row2.Signed)
	row := edit.Snapshot()
	args[0] = scope.Read(cm, row)
	if err := vm.Run(ctx, "Read", args, result[:]); err != nil || result[0].R != 37 {
		t.Fatalf("read = %v, %v", result, err)
	}
	if err := vm.Run(ctx, "Pair", args, result[:]); err != nil || result[0].AsInt() != -5 {
		t.Fatalf("two child fields: %v %v", result, err)
	}
	// A branch may enter directly at the second word of a linked pair.
	for i, fn := range mod.Functions {
		if fn.Name != "Pair" {
			continue
		}
		first, second := fn.Code[0], fn.Code[1]
		if first.Op() != evm.OpExtended || second.Op() != evm.OpExtended || mod.ExtendedOps[second.BC()].Kind != evm.ExtendedGetChildKey {
			t.Fatal("expected two adjacent child reads")
		}
		fn.Code = []evm.Instruction{evm.WithBC(evm.OpJump, 0, 1), first, second, evm.ABC(evm.OpMoveR, 0, mod.ExtendedOps[second.BC()].Out[0].Slot, 0), evm.ABC(evm.OpReturn, 0, 0, 0)}
		fn.Positions = nil
		copy := *mod
		copy.Functions = slices.Clone(mod.Functions)
		copy.Functions[i] = fn
		test, err := evm.Link(&copy, pkg)
		if err != nil {
			t.Fatal(err)
		}
		if err = evm.New(test).Run(ctx, "Pair", args, result[:]); err != nil || result[0].AsInt() != -12 {
			t.Fatalf("branch to second read: %v %v", result, err)
		}
	}
	other := row2.Edit(row2.Empty(row2.Compact2))
	if err := vm.Run(ctx, "Keep", []values.Value{scope.Edit(cm, edit), scope.Edit(cm, other)}, result[:]); err != nil || result[0].AsInt() != 73 {
		t.Fatalf("retained grandchild: %v, %v", result, err)
	}
	for _, fn := range mod.Functions {
		fused := false
		for _, ins := range fn.Code {
			if ins.Op() == evm.OpExtended && mod.ExtendedOps[ins.BC()].Kind == evm.ExtendedGetChildKey {
				fused = true
			}
		}
		if ((fn.Name == "Fused" || fn.Name == "Split") && !fused) || (fn.Name == "Twice" && fused) {
			t.Fatalf("unexpected child read in %s: fused=%v", fn.Name, fused)
		}
	}
	empty := row2.Edit(row2.Empty(row2.Compact2))
	empty.Set([]row2.Step{{Key: total}}, values.Int(1), row2.Signed)
	missing := []values.Value{scope.Read(cm, empty.Snapshot())}
	if err := evm.New(p).Run(ctx, "Pair", missing, result[:]); err == nil || !strings.Contains(err.Error(), "missing Bar record") {
		t.Fatalf("missing common child: %v", err)
	}
	for _, tc := range []struct {
		name string
		line int
	}{{"Fused", 6}, {"Split", 9}, {"Twice", 14}} {
		err := evm.New(p).Run(ctx, tc.name, missing, result[:])
		var e *evm.ExecutionError
		if !errors.As(err, &e) || e.Position.Line != tc.line || e.Err.Error() != "missing Bar record" {
			t.Fatalf("%s: %v", tc.name, err)
		}
	}
	// A present child with no scalar is a zero word, not a missing child.
	empty.Set([]row2.Step{{Key: child}, {Key: next}, {Key: boz}}, values.Int(9), row2.Signed)
	withoutLeaf := []values.Value{scope.Read(cm, empty.Snapshot())}
	for _, name := range []string{"Fused", "Split"} {
		if err := vm.Run(ctx, name, withoutLeaf, result[:]); err != nil || result[0].AsInt() != 0 {
			t.Fatalf("%s missing word: %v, %v", name, result, err)
		}
	}
	// Feed the linker an existing word key belonging to the root, not the child.
	bad := *mod
	bad.ExtendedOps = slices.Clone(mod.ExtendedOps)
	for i := range bad.ExtendedOps {
		if bad.ExtendedOps[i].Kind == evm.ExtendedGetChildKey {
			bad.ExtendedOps[i].ChildKey = total
			break
		}
	}
	if _, err := evm.Link(&bad, pkg); err == nil {
		t.Fatal("linked a fused read of a key outside the child model")
	}
	_, foreign := evm.WithRecords(context.Background())
	defer foreign.Close()
	_ = foreign.Edit(cm, other) // Same index as the first handle in scope.
	if _, err := foreign.Mutable(originalEdit, cm); err == nil {
		t.Fatal("accepted a handle from another scope")
	}
	scope.Close()
	if err := vm.Run(ctx, "Read", args, result[:]); err != nil {
		t.Fatal("immutable reads must not depend on the editor scope", err)
	}
	for _, src := range []string{"c.total++", "_ = c.nope", "return &c.total"} {
		decl := "func Bad(c *db.Customer)"
		if strings.HasPrefix(src, "return") {
			decl += " *int64"
		}
		if _, err := evmcompiler.Compile("bad.go", "import \"db\";"+decl+"{"+src+"}", pkg); err == nil {
			t.Fatalf("accepted %s", src)
		}
	}
}

func TestKVOTypedDraftSelectors(t *testing.T) {
	s := kvo.NewSchema()
	child := kvo.NewEntityType(s, "child")
	root := kvo.NewEntityType(s, "row")
	kvo.NewProp(s, 1, "child", child, nil)
	kvo.NewProp(s, 2, "signed", kvo.TInt64, nil)
	kvo.NewProp(s, 3, "unsigned", kvo.TUint64, nil)
	kvo.NewProp(s, 4, "id", kvo.TUint64, nil)
	kvo.NewModel(s, child, func(b *kvo.ModelBuilder) {
		b.Prop(s.MustPropByCode(2), kvo.PresenceIsSignificant)
		b.Prop(s.MustPropByCode(3))
	})
	model := kvo.NewModel(s, root, func(b *kvo.ModelBuilder) {
		b.Prop(s.MustPropByCode(1), kvo.Compact)
		b.Prop(s.MustPropByCode(4), kvo.PrimaryKey)
	})
	pkg, err := evm.ModelPackage("m", s)
	if err != nil {
		t.Fatal(err)
	}
	program, err := evmcompiler.Compile("typed.go", `import "m"
func Change(r *m.RowEdit, a int64, b uint64) {
	r.id = 0
	r.child.signed = a
	r.child.unsigned = b
}
func Read(r *m.Row) (int64,uint64) {
	return r.child.signed, r.child.unsigned
}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	linked, err := evm.Link(program, pkg)
	if err != nil {
		t.Fatal(err)
	}
	for _, layout := range []row2.Layout{row2.Fast2, row2.Compact2} {
		vm := evm.New(linked)
		ctx, scope := evm.WithRecords(context.Background())
		root := row2.Edit(row2.Empty(layout))
		edit := scope.Edit(model, root)
		for _, n := range []int64{-129, -128, 0, 127, 128, 255, 1 << 48} {
			unsigned := uint64(n)
			if err := vm.Run(ctx, "Change", []values.Value{edit, values.Int(n), values.Uint(unsigned)}, nil); err != nil {
				t.Fatal(err)
			}
			v := root.Snapshot()
			c := v.Child(row2.Step{Key: 1})
			if !v.Has(4) || c.Layout() == row2.Fast2 || !c.Has(2) || c.Get(2, row2.Signed).AsInt() != n || c.Get(3, row2.Word).AsUint() != unsigned {
				t.Fatalf("stored value %d", n)
			}
			var out [2]values.Value
			if err := vm.Run(ctx, "Read", []values.Value{scope.Read(model, v)}, out[:]); err != nil || out[0].AsInt() != n || out[1].R != unsigned {
				t.Fatalf("read %d: %v %v", n, out, err)
			}
		}
		scope.Close()
	}
}

func TestDirectRecordScope(t *testing.T) {
	schema := kvo.NewSchema()
	balance := kvo.NewProp(schema, 5, "balance", kvo.TUint64, nil)
	model := kvo.NewModel(schema, kvo.NewEntityType(schema, "account"), func(b *kvo.ModelBuilder) { b.Prop(schema.MustPropByCode(balance)) })
	row := evm.NewRecordType("Account", model, false)
	signature := evm.Signature{Params: []*values.Type{values.Uint64}, Results: []*values.Type{row.Pointer}}
	var saved values.Value
	delta := row2.Edit(row2.Empty(row2.Fast2))
	var rows [2]kvo.Record
	for i := range rows {
		delta.Set([]row2.Step{{Key: balance}}, values.Uint(uint64(7+i)), row2.Word)
		rows[i] = kvo.Encoded(delta.Snapshot())
		delta = row2.Edit(row2.Empty(row2.Fast2))
	}
	pkg := evm.Package{Path: "m", Records: []*evm.RecordType{row}, Functions: []evm.ExternalFunction{
		{Name: "Get", Signature: signature, Direct: evm.DirectBinding{Op: evm.OpCallHostUS, Call: func(c *evm.Call, i, _ values.Value) (values.Value, error) { return c.RecordValue(model, rows[i.R%2]) }}},
		{Name: "Saved", Signature: evm.Signature{Results: signature.Results}, Direct: evm.DirectBinding{Op: evm.OpCallHostS, Call: func(*evm.Call, values.Value, values.Value) (values.Value, error) { return saved, nil }}},
	}}
	m, err := evmcompiler.Compile("scoped.go", `import "m"
func Sum() uint64 {
	var sum uint64
	for i:=uint64(0); i<64; i++ { sum+=m.Get(i).balance }
	return sum
}
func Capture(i uint64)*m.Account{return m.Get(i)}
func Saved()*m.Account{return m.Saved()}`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	ctx, scope := evm.WithRecords(context.Background())
	var out [1]values.Value
	if err := vm.Run(ctx, "Sum", nil, out[:]); err != nil || out[0].R != 480 {
		t.Fatal(out, err)
	}
	if err := vm.RunWithBudget(ctx, "Sum", nil, out[:], evm.Budget{AllocationBytes: 1}); err != nil {
		t.Fatal("steady-state cursor allocated", err)
	}
	if err := vm.Run(ctx, "Capture", []values.Value{values.Uint(0)}, out[:]); err != nil {
		t.Fatal(err)
	}
	saved = out[0]
	if err := vm.Run(ctx, "Capture", []values.Value{values.Uint(1)}, out[:]); err != nil || out[0].P == saved.P {
		t.Fatal("escaping token reused", err)
	}
	if err := vm.Run(ctx, "Saved", nil, out[:]); err != nil || out[0] != saved {
		t.Fatal(out, err)
	}
	other, another := evm.WithRecords(context.Background())
	defer another.Close()
	if err := vm.Run(other, "Saved", nil, out[:]); err != nil || out[0] != saved {
		t.Fatal("owned result failed across scopes", err)
	}
	vm.Reset()
	scope.Close()
	if err := vm.Run(ctx, "Saved", nil, out[:]); err != nil || out[0] != saved {
		t.Fatal("owned result failed after editor scope close", err)
	}
	vm.Reset()
	if err := vm.Run(context.Background(), "Capture", []values.Value{values.Uint(0)}, out[:]); err != nil {
		t.Fatal("immutable read required editor scope", err)
	}
}
