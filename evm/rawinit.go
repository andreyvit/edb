package evm

import "github.com/andreyvit/edb/values"

// rawFrameDefined proves that no raw read can observe an old local on entry.
// Script calls share only their declared argument/result windows. Assemblers
// may use outgoing slots as scratch, so they retain zeroed frames by default.
func (p *Program) rawFrameDefined(f *linkedFunction, code []Instruction) bool {
	if f.rSize == 0 {
		return true
	}
	if f.rSize > 64 || f.RecoverPC != 0 || len(code) > 4096 {
		return false
	}
	bits := func(n int) uint64 { return (uint64(1) << n) - 1 }
	// Small functions fit on the Go stack during linking.
	var small [64]uint64
	live := small[:]
	if len(code)+1 > len(small) {
		live = make([]uint64, len(code)+1)
	}
	for pass := 0; pass < 8; pass++ {
		changed := false
		for pc := len(code) - 1; pc >= 0; pc-- {
			i := code[pc]
			a, b, c := uint64(1)<<i.A(), uint64(1)<<i.B(), uint64(1)<<i.C()
			var read, def uint64
			next := live[pc+1]
			switch i.Op() {
			case OpConst, OpString, OpFunction:
				def = a
			case OpMoveR, OpMoveV, OpAddImmI64, OpConvert:
				read, def = b, a
			case OpAddI64, OpSubI64, OpMulI64, OpDivI64, OpDivU64, OpRemI64, OpRemU64, OpAddF64, OpSubF64, OpMulF64, OpDivF64, OpAnd, OpOr, OpXor, OpShl, OpShrI64, OpShrU64, OpEqR, OpLtI64, OpLtU64, OpEqF64, OpLtF64, OpAddF32, OpSubF32, OpMulF32, OpDivF32, OpEqF32, OpLtF32, OpConcatStr, OpEqStr, OpLtStr, OpLeI64, OpLeU64, OpLoadIndexR, OpLoadIndexByte, OpLoadIndexWord:
				read, def = b|c, a
			case OpLoadIndexPtr, OpStoreIndexPtr:
				if i.Op() == OpLoadIndexPtr {
					read = b | c
				} else {
					read = a | b
				}
			case OpStoreIndexR, OpStoreIndexByte, OpStoreIndexWord:
				read = a | b | c
			case OpCheckShift:
				read = a
			case OpEqP:
				def = a
			case OpMoveP, OpClearP, OpAlloc, OpGlobal, OpCheckNil:
			case OpMulAddF64, OpAddMulF64, OpSubAddF64:
				dst, x, y, z := i.ABCD12()
				read, def = 1<<x|1<<y|1<<z, 1<<dst
			case OpLoopAddLtI64, OpLoopAddLeI64:
				x, y, z, _ := i.ABCD12()
				read, def = 1<<x|1<<y|1<<z, 1<<x
				next |= live[pc+1+i.SignedD12()]
			case OpLoopLtI64, OpLoopLeI64:
				read, def = a|b, a
				next |= live[pc+1+int(int16(i.C()))]
			case OpJump:
				next = live[pc+1+int(int32(i.BC()))]
			case OpJz, OpJnz, OpJNil, OpJNotNil:
				if i.Op() == OpJz || i.Op() == OpJnz {
					read = a
				}
				next |= live[pc+1+int(int32(i.BC()))]
			case OpCallFunc:
				g := &p.functions[i.BC()]
				read = bits(g.params.RCount) << f.RCount
				// Only declared results are writes. Padding may still contain an old word.
				for n, t := range g.Signature.Results {
					if t.Repr()&values.Raw != 0 {
						def |= 1 << (f.RCount + int(g.results.Slots[n]))
					}
				}
			case OpCallHostRR:
				read, def = b, a
			case OpCallHostRP:
				read = b
			case OpLoad:
				if p.shapes[i.C()].Type.Repr()&values.Raw != 0 {
					def = a
				}
			case OpStore:
				if p.shapes[i.C()].Type.Repr()&values.Raw != 0 {
					read = b
				}
			case OpExtended:
				desc := &p.extendedOps[i.BC()]
				for _, r := range desc.In {
					if r.Repr&values.Raw != 0 {
						read |= 1 << r.Slot
					}
				}
				for j, r := range desc.Out {
					// Exhaustion only writes the success flag.
					if desc.Kind == ExtendedNext && j > 0 {
						break
					}
					if r.Repr&values.Raw != 0 {
						def |= 1 << r.Slot
					}
				}
			case OpReturn:
				next = 0
				for j, t := range f.Signature.Results {
					if t.Repr()&values.Raw != 0 {
						read |= 1 << f.results.Slots[j]
					}
				}
			default:
				return false
			}
			incoming := read | (next &^ def)
			if incoming != live[pc] {
				live[pc] = incoming
				changed = true
			}
		}
		if !changed {
			var supplied uint64
			for j, t := range f.Signature.Params {
				if t.Repr()&values.Raw != 0 {
					supplied |= 1 << f.params.Slots[j]
				}
			}
			for _, c := range f.Captures {
				if c.Type.Repr()&values.Raw != 0 {
					supplied |= 1 << c.Slot
				}
			}
			return live[0]&^supplied == 0
		}
	}
	return false
}
