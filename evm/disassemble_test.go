package evm_test

import (
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
)

func TestDisassemble(t *testing.T) {
	m := &evm.Module{
		Sources: map[string]string{"a.go": "unused\r\n  first\r\n", "b.go": "\nsecond"},
		Functions: []evm.Function{{Name: "Run",
			Positions: []evm.SourcePosition{
				{File: "a.go", Line: 2}, {File: "a.go", Line: 2, Column: 4}, {File: "b.go", Line: 2}, {},
				{File: "a.go", Line: 2}, {File: "a.go", Line: 2}, {File: "a.go", Line: 2},
			},
			Code: []evm.Instruction{
				evm.ABC(evm.OpClearP, 0, 0, 0), evm.ABC(evm.OpMoveP, 0, 0, 0),
				evm.ABC(evm.OpAddI64, 0, 0, 0), evm.ABC(evm.OpReturn, 0, 0, 0),
				evm.ABC(evm.OpReturn, 0, 7, 0), evm.ABC(evm.OpReturn, 0, 0, 9), evm.ABC(evm.OpReturn, 0, 0, 0),
			},
		}},
	}
	var rows []string
	for _, line := range strings.Split(strings.TrimSpace(m.Disassemble()), "\n") {
		rows = append(rows, strings.Join(strings.Fields(line), " "))
	}
	want := "fun0 Run R=0 P=0\n; a.go:2 first\n0 clear.p p0\n1 move.p p0 p0\n; b.go:2 second\n2 add.i64 r0 r0 r0\n3 ret\n; a.go:2 first\n4 ret 0 7\n5 ret 0 0 9\n6 ret"
	if got := strings.Join(rows, "\n"); got != want {
		t.Fatalf("listing:\n%s\nwant:\n%s", got, want)
	}
}
