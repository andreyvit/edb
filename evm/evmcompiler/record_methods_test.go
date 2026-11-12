package evmcompiler_test

import (
	"context"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestRecordMethodFunctionABI(t *testing.T) {
	s := kvo.NewSchema()
	total := kvo.NewSignedProp[int64](s, 1, "total", kvo.TInt64)
	model := kvo.NewModel(s, kvo.NewEntityType(s, "Customer"), func(b *kvo.ModelBuilder) { b.Prop(total) })
	pkg, err := evm.ModelPackage("m", s)
	if err != nil {
		t.Fatal(err)
	}
	var seen int64
	pkg.Methods = append(pkg.Methods, evm.ExternalMethod{
		Name: "Balance", Receiver: pkg.Record(model, false).Pointer,
		Signature: evm.Signature{Params: []*values.Type{values.Int64}, Results: []*values.Type{values.Int64}},
		Ownership: &evm.OwnershipEffects{NoRetain: true, NoInvalidate: true},
		Direct: evm.DirectBinding{Op: evm.OpCallHostSUU, Call: func(c *evm.Call, recv, delta values.Value) (values.Value, error) {
			seen += delta.AsInt()
			return values.Int(kvo.FromValue(recv).Get(total) + delta.AsInt()), nil
		}},
	})
	m, err := evmcompiler.Compile("methods.go", `import "m"
 type Account interface { Balance(int64) int64 }
 func via(a Account) int64 { return a.Balance(2) }
 func Run(c *m.Customer) int64 {
  defer c.Balance(8)
  bound := c.Balance
  expr := (*m.Customer).Balance
  return c.Balance(1) + bound(3) + expr(c,4) + via(c) + c.total
 }`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(m.Disassemble(), "call.host.suu") {
		t.Fatal(m.Disassemble())
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	var w row2.Writer
	w.Begin(0, "", row2.Object, row2.Fast2)
	w.Put(total.Code(), "", values.Int(10), row2.Signed)
	w.End()
	r := kvo.Encoded(row2.Trusted(w.BytesWritten())).WithModel(model)
	var out [1]values.Value
	if err := evm.New(p).Run(context.Background(), "Run", []values.Value{r.Value()}, out[:]); err != nil || out[0].AsInt() != 60 || seen != 18 {
		t.Fatal(out, seen, err)
	}
	pkg.Methods[len(pkg.Methods)-1].Name = "total"
	if _, err := evmcompiler.Compile("collision.go", `import "m";func Run(c *m.Customer){}`, pkg); err == nil {
		t.Fatal("field/method collision accepted")
	}
}
