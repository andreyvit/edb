package evm

import (
	"context"
	"errors"
	"testing"

	"github.com/andreyvit/edb/values"
)

func TestDirectBranchCancellation(t *testing.T) {
	sig := Signature{Params: []*values.Type{values.Uint64}, Results: []*values.Type{values.Boolean}}
	m := &Module{Imports: []Import{{Package: "sensor", Name: "Ready", Signature: sig}}, Functions: []Function{{Name: "run", RCount: 1, Signature: sig, Code: []Instruction{
		ABC(OpCallHostUU, 0, 0, 0), WithBC(OpJz, 0, 1), ABC(OpReturn, 0, 0, 0), ABC(OpReturn, 0, 0, 0),
	}}}}
	var cancel context.CancelFunc
	pkg := Package{Path: "sensor", Functions: []ExternalFunction{{Name: "Ready", Signature: sig, Direct: DirectBinding{Op: OpCallHostUU, Call: func(_ *Call, x, _ values.Value) (values.Value, error) {
		if cancel != nil {
			cancel()
		}
		return values.Bool(x.R != 0), nil
	}}}}}
	vm := linked(t, m, pkg)
	if vm.program.functions[0].Code[0].Op() != opTestDirect {
		t.Fatal("expected linked predicate branch")
	}
	out := []values.Value{values.Uint(9)}
	args := []values.Value{values.Uint(1)}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	cancel = stop
	if err := vm.Run(ctx, "run", args, out); !errors.Is(err, context.Canceled) || out[0].R != 9 {
		t.Fatalf("cancelled predicate: %v, %v", out, err)
	}
}
