package evm

import (
	"context"
	"testing"

	"github.com/andreyvit/edb/values"
)

func TestRawFrameInitialization(t *testing.T) {
	ret := ABC(OpReturn, 0, 0, 0)
	for _, tc := range []struct {
		name    string
		code    []Instruction
		defined bool
		x, want uint64
	}{
		{"defined on both paths", []Instruction{WithBC(OpJz, 0, 2), WithBC(OpConst, 1, 0), WithBC(OpJump, 0, 1), WithBC(OpConst, 1, 0), ABC(OpMoveR, 0, 1, 0), ret}, true, 0, 42},
		{"missing on one path", []Instruction{WithBC(OpJz, 0, 1), WithBC(OpConst, 1, 0), ABC(OpMoveR, 0, 1, 0), ret}, false, 0, 0},
		{"loop reads old local", []Instruction{ABC(OpAddImmI64, 1, 1, 1), ABC(OpLtU64, 2, 1, 0), WithBC(OpJnz, 2, 0xfffffffd), ABC(OpMoveR, 0, 1, 0), ret}, false, 3, 3},
	} {
		for _, strict := range []bool{false, true} {
			vm := linked(t, &Module{StrictCallWindows: strict, Words: []uint64{42}, Functions: []Function{{Name: "f", RCount: 3, Signature: Signature{Params: []*values.Type{values.Uint64}, Results: []*values.Type{values.Uint64}}, Code: tc.code}}})
			if got := vm.program.functions[0].definedRaw; got != (strict && tc.defined) {
				t.Fatalf("%s strict=%v: defined=%v", tc.name, strict, got)
			}
			vm.r = []uint64{99, 99, 99}
			out := make([]values.Value, 1)
			if err := vm.Run(context.Background(), "f", []values.Value{values.Uint(tc.x)}, out); err != nil || out[0].R != tc.want {
				t.Fatalf("%s strict=%v: got %v, %v", tc.name, strict, out, err)
			}
		}
	}
}
