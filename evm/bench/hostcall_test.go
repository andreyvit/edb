package bench_test

import (
	"context"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

const hostCallScript = `import "svc"
func Predicate() int {
	n := 0
	for i:=0; i<256; i++ {
		if svc.Enabled(i) { n++ }
	}
	return n
}
func PointerGetter() int {
	sum:=0
	for i:=0;i<256;i++ {sum+=svc.Lookup(i).Balance()}
	return sum
}
func TagCheck() int {
	n:=0
	for i:=0;i<256;i++ {
		if svc.Lookup(i).HasTag("member") {n++}
	}
	return n
}
func CachedContext() int {
	sum:=0
	for i:=0;i<256;i++ {
		sum+=len(svc.Context().Name())
	}
	return sum
}
func Cursor() uint64 {
	cursor:=svc.Cursor()
	var sum uint64
	for row:=cursor.Next(); row!=nil; row=cursor.Next() {sum+=row.balance}
	return sum
}`

func smallHost(t testing.TB, direct bool) (*evm.VM, context.Context) {
	t.Helper()
	schema := kvo.NewSchema()
	prop := kvo.NewProp(schema, 5, "balance", kvo.TUint64, nil)
	model := kvo.NewModel(schema, kvo.NewEntityType(schema, "row"), func(m *kvo.ModelBuilder) { m.Prop(schema.MustPropByCode(prop)) })
	row := evm.NewRecordType("Row", model, false)
	account := values.NewStructShape("svc.Account", values.Field{Name: "Points", Shape: &values.Shape{Type: values.IntType, Kind: values.IntKind, Bits: 64, Cells: 1}})
	accountPtr := values.NewPointerShape(account)
	cursor := values.NewStructShape("svc.CursorState")
	cursorPtr := values.NewPointerShape(cursor)
	pkg := evm.Package{Path: "svc", Types: []evm.TypeDecl{{Name: "Account", Shape: account}, {Shape: accountPtr}, {Name: "CursorState", Shape: cursor}, {Shape: cursorPtr}}, Records: []*evm.RecordType{row}}
	word, str, acc, cur := values.IntType, values.StringType, accountPtr.Type, cursorPtr.Type
	var accounts [256]values.Value
	var rows [256]kvo.Record
	for i := range accounts {
		accounts[i] = values.Value{P: account.New()}
		account.SetField(accounts[i].P, 0, values.Int(int64(i)))
		edit := row2.Edit(row2.Empty(row2.Fast2))
		edit.Set([]row2.Step{{Key: prop}}, values.Uint(uint64(i)), row2.Word)
		rows[i] = kvo.Encoded(edit.Snapshot())
	}
	tok := values.Value{P: cursor.New()}
	index := 0
	add := func(name string, recv *values.Type, op evm.Opcode, params []*values.Type, res *values.Type, general evm.Native, fast evm.DirectFunc) {
		sig := evm.Signature{Params: params, Results: []*values.Type{res}}
		var d evm.DirectBinding
		if direct {
			d = evm.DirectBinding{Op: op, Call: fast}
		}
		if recv == nil {
			pkg.Functions = append(pkg.Functions, evm.ExternalFunction{Name: name, Signature: sig, Call: general, Direct: d})
		} else {
			pkg.Methods = append(pkg.Methods, evm.ExternalMethod{Name: name, Receiver: recv, Signature: sig, Call: general, Direct: d})
		}
	}
	add("Enabled", nil, evm.OpCallHostUU, []*values.Type{word}, values.Boolean,
		func(c *evm.Call) error { c.R[0] = c.R[0] & 1; return nil },
		func(_ *evm.Call, x, _ values.Value) (values.Value, error) { return values.Uint(x.R & 1), nil })
	add("Lookup", nil, evm.OpCallHostUP, []*values.Type{word}, acc,
		func(c *evm.Call) error { c.P[0] = accounts[c.R[0]].P; return nil },
		func(_ *evm.Call, x, _ values.Value) (values.Value, error) { return accounts[x.R], nil })
	add("Balance", acc, evm.OpCallHostMethodU, nil, word,
		func(c *evm.Call) error { c.R[0] = account.GetField(c.P[0], 0).R; return nil },
		func(_ *evm.Call, x, _ values.Value) (values.Value, error) { return account.GetField(x.P, 0), nil })
	hasTag := func(v values.Value, s string) values.Value {
		return values.Bool(account.GetField(v.P, 0).R&1 == 1 && s == "member")
	}
	add("HasTag", acc, evm.OpCallHostMethodSU, []*values.Type{str}, values.Boolean,
		func(c *evm.Call) error { c.R[0] = hasTag(values.Value{P: c.P[0]}, c.Value(1).AsString()).R; return nil },
		func(_ *evm.Call, x, y values.Value) (values.Value, error) { return hasTag(x, y.AsString()), nil })
	add("Context", nil, evm.OpCallHostP, nil, acc,
		func(c *evm.Call) error { c.P[0] = accounts[17].P; return nil },
		func(_ *evm.Call, _, _ values.Value) (values.Value, error) { return accounts[17], nil })
	name := values.String("cached")
	add("Name", acc, evm.OpCallHostMethodS, nil, str,
		func(c *evm.Call) error { c.SetValue(0, name); return c.ChargeAllocation(name.R) },
		func(_ *evm.Call, _, _ values.Value) (values.Value, error) { return name, nil })
	add("Cursor", nil, evm.OpCallHostP, nil, cur,
		func(c *evm.Call) error { index = 0; c.P[0] = tok.P; return nil },
		func(_ *evm.Call, _, _ values.Value) (values.Value, error) { index = 0; return tok, nil })
	next := func() kvo.Record {
		if index == len(rows) {
			return kvo.Record{}
		}
		v := rows[index]
		index++
		return v
	}
	add("Next", cur, evm.OpCallHostMethodP, nil, row.Pointer,
		func(c *evm.Call) error { return c.SetRecordResult(model, next()) },
		func(c *evm.Call, _, _ values.Value) (values.Value, error) { return c.RecordValue(model, next()) })
	m, err := evmcompiler.Compile("small.go", hostCallScript, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	vm.Limits.Allocations = ^uint64(0)
	vm.Limits.AllocationBytes = ^uint64(0)
	ctx, scope := evm.WithRecords(context.Background())
	t.Cleanup(scope.Close)
	return vm, ctx
}

func BenchmarkSmallHost(b *testing.B) {
	for _, tc := range []struct {
		name string
		want uint64
	}{{"Predicate", 128}, {"PointerGetter", 32640}, {"TagCheck", 128}, {"CachedContext", 1536}, {"Cursor", 32640}} {
		b.Run(tc.name, func(b *testing.B) {
			for _, mode := range []string{"General", "Direct"} {
				b.Run(mode, func(b *testing.B) {
					vm, ctx := smallHost(b, mode == "Direct")
					var out [1]values.Value
					for i := 0; i < 2; i++ {
						if err := vm.Run(ctx, tc.name, nil, out[:]); err != nil || out[0].R != tc.want {
							b.Fatalf("got %v, %v", out, err)
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if err := vm.Run(ctx, tc.name, nil, out[:]); err != nil || out[0].R != tc.want {
							b.Fatalf("got %v, %v", out, err)
						}
					}
				})
			}
		})
	}
}
