package evm

import "testing"

func TestLinkedLocalReadSites(t *testing.T) {
	marks := map[int]bool{0: true, 63: true, 64: true, 90: false, 129: true}
	fn := Function{Name: "sites", LocalRecordReads: marks, Code: make([]Instruction, 130)}
	for i := range fn.Code {
		fn.Code[i] = ABC(OpReturn, 0, 0, 0)
	}
	p, err := Link(&Module{Functions: []Function{fn}})
	if err != nil {
		t.Fatal(err)
	}
	clear(marks)
	for pc := range fn.Code {
		want := pc == 0 || pc == 63 || pc == 64 || pc == 129
		if p.functions[0].localRead(pc) != want {
			t.Fatalf("pc %d: expected %v", pc, want)
		}
	}
}
