package evm

// Instruction is one 64-bit word with its opcode in the low 16 bits.
// Most opcodes have three 16-bit operands. ABCD12 uses four 12-bit operands.
// WithBC combines B and C into 32 bits. Branches use a signed displacement from
// the following instruction. Constants live in separate pools.
type Instruction uint64
type Opcode uint16

const (
	OpConst Opcode = iota + 1
	OpString
	OpMoveR
	OpMoveP
	OpMoveV
	OpClearP
	OpAddI64
	OpSubI64
	OpMulI64
	OpDivI64
	OpDivU64
	OpRemI64
	OpRemU64
	OpAddF64
	OpSubF64
	OpMulF64
	OpDivF64
	OpAnd
	OpOr
	OpXor
	OpShl
	OpShrI64
	OpShrU64
	OpEqR
	OpEqP
	OpLtI64
	OpLtU64
	OpEqF64
	OpLtF64
	OpJump
	OpJz
	OpJnz
	OpCallFunc
	OpCallHost
	OpReturn
	_ // retired new.struct; preserve opcode numbers
	_ // retired store.field.r
	_ // retired store.field.p
	_ // retired store.field.v
	OpConvert
	OpCheckShift
	OpAddF32
	OpSubF32
	OpMulF32
	OpDivF32
	OpEqF32
	OpLtF32
	OpConcatStr
	OpEqStr
	OpLtStr
	OpAlloc
	OpLoad
	OpStore
	OpExtended
	OpGlobal
	OpFunction
	OpClosure
	OpCallVirt
	OpCallIndirect
	OpCheckNil
	OpDeferStack
	OpDefer
	OpRunDefers
	OpPanic
	OpRecover
	OpLoopLtI64
	OpCallHostRR
	OpCallHostRP
	OpJNil
	OpJNotNil
	OpLoadIndexR
	OpStoreIndexR
	OpLeI64
	OpLeU64
	OpMulAddF64
	OpAddMulF64
	OpSubAddF64
	OpLoopAddLtI64
	OpLoopAddLeI64
	OpAddImmI64
	OpLoopLeI64
	// These opcodes occur only in the private linked instruction stream.
	opCallRawUnary
	opBranchLtI64
	opBranchLeI64
	opBranchLtF64
	opBranchEqR
	opCallDirect
	// Direct external calls. Each suffix names its explicit arguments, then its result.
	OpCallHostU
	OpCallHostS
	OpCallHostP
	OpCallHostUU
	OpCallHostUS
	OpCallHostUP
	OpCallHostSU
	OpCallHostSS
	OpCallHostSP
	OpCallHostPU
	OpCallHostPS
	OpCallHostPP
	OpCallHostUUU
	OpCallHostUUS
	OpCallHostUUP
	OpCallHostUSU
	OpCallHostUSS
	OpCallHostUSP
	OpCallHostUPU
	OpCallHostUPS
	OpCallHostUPP
	OpCallHostSUU
	OpCallHostSUS
	OpCallHostSUP
	OpCallHostSSU
	OpCallHostSSS
	OpCallHostSSP
	OpCallHostSPU
	OpCallHostSPS
	OpCallHostSPP
	OpCallHostPUU
	OpCallHostPUS
	OpCallHostPUP
	OpCallHostPSU
	OpCallHostPSS
	OpCallHostPSP
	OpCallHostPPU
	OpCallHostPPS
	OpCallHostPPP
	OpCallHostMethodU
	OpCallHostMethodS
	OpCallHostMethodP
	OpCallHostMethodUU
	OpCallHostMethodUS
	OpCallHostMethodUP
	OpCallHostMethodSU
	OpCallHostMethodSS
	OpCallHostMethodSP
	OpCallHostMethodPU
	OpCallHostMethodPS
	OpCallHostMethodPP
	OpLoadIndexByte
	OpStoreIndexByte
	OpLoadIndexWord
	OpStoreIndexWord
	OpLoadIndexPtr
	OpStoreIndexPtr
	opTestDirect    // A direct call followed by a branch on its result.
	opReadChildPair // Private linked form. The following word supplies the second key.
	// Side-effecting direct calls; V means no result. A and B are read-only.
	OpCallHostV
	OpCallHostUV
	OpCallHostSV
	OpCallHostPV
	OpCallHostUUV
	OpCallHostUSV
	OpCallHostUPV
	OpCallHostSUV
	OpCallHostSSV
	OpCallHostSPV
	OpCallHostPUV
	OpCallHostPSV
	OpCallHostPPV
	OpCallHostMethodV
	OpCallHostMethodUV
	OpCallHostMethodSV
	OpCallHostMethodPV
)

func ABC(op Opcode, a, b, c uint16) Instruction {
	return Instruction(op) | Instruction(a)<<16 | Instruction(b)<<32 | Instruction(c)<<48
}

// ABCD12 packs four 12-bit operands above the ordinary opcode.
func ABCD12(op Opcode, a, b, c, d uint16) Instruction {
	if (a | b | c | d) > 0xfff {
		panic("compact operand exceeds 12 bits")
	}
	return Instruction(op) | Instruction(a)<<16 | Instruction(b)<<28 | Instruction(c)<<40 | Instruction(d)<<52
}
func (i Instruction) ABCD12() (a, b, c, d uint16) {
	return uint16(i>>16) & 0xfff, uint16(i>>28) & 0xfff, uint16(i>>40) & 0xfff, uint16(i >> 52)
}
func (i Instruction) SignedD12() int { return int(int64(i) >> 52) }
func WithBC(op Opcode, a uint16, bc uint32) Instruction {
	return Instruction(op) | Instruction(a)<<16 | Instruction(bc)<<32
}
func (i Instruction) Op() Opcode { return Opcode(i) }
func (i Instruction) A() uint16  { return uint16(i >> 16) }
func (i Instruction) B() uint16  { return uint16(i >> 32) }
func (i Instruction) C() uint16  { return uint16(i >> 48) }
func (i Instruction) BC() uint32 { return uint32(i >> 32) }

var opcodeNames = [...]string{
	OpLoadIndexByte:  "load.index.byte",
	OpStoreIndexByte: "store.index.byte",
	OpLoadIndexWord:  "load.index.word",
	OpStoreIndexWord: "store.index.word",
	OpLoadIndexPtr:   "load.index.ptr",
	OpStoreIndexPtr:  "store.index.ptr",
	OpLoopLeI64:      "loop.le.i64", OpAddImmI64: "add.imm.i64",
	OpLoopAddLtI64: "loop.add.lt.i64", OpLoopAddLeI64: "loop.add.le.i64",
	OpMulAddF64: "mul.add.f64", OpAddMulF64: "add.mul.f64", OpSubAddF64: "sub.add.f64",
	OpLoadIndexR: "load.index.r", OpStoreIndexR: "store.index.r", OpLeI64: "le.i64", OpLeU64: "le.u64",
	OpDeferStack: "defer.stack", OpDefer: "defer", OpRunDefers: "run.defers", OpPanic: "panic", OpRecover: "recover",
	OpJNil: "jnil", OpJNotNil: "jnotnil",
	OpLoopLtI64: "loop.lt.i64", OpCallHostRR: "call.host.rr", OpCallHostRP: "call.host.rp",
	OpCallVirt: "call.virt",
	OpFunction: "function", OpClosure: "closure", OpCallIndirect: "call.indirect", OpCheckNil: "check.nil",
	OpAlloc: "alloc", OpLoad: "load", OpStore: "store", OpExtended: "extended", OpGlobal: "global",
	OpConvert: "convert", OpCheckShift: "check.shift", OpAddF32: "add.f32", OpSubF32: "sub.f32", OpMulF32: "mul.f32", OpDivF32: "div.f32", OpEqF32: "eq.f32", OpLtF32: "lt.f32", OpConcatStr: "concat.str", OpEqStr: "eq.str", OpLtStr: "lt.str",
	OpConst: "const", OpString: "string", OpMoveR: "move.r", OpMoveP: "move.p", OpMoveV: "move.v", OpClearP: "clear.p",
	OpAddI64: "add.i64", OpSubI64: "sub.i64", OpMulI64: "mul.i64", OpDivI64: "div.i64", OpDivU64: "div.u64",
	OpRemI64: "rem.i64", OpRemU64: "rem.u64", OpAddF64: "add.f64", OpSubF64: "sub.f64", OpMulF64: "mul.f64", OpDivF64: "div.f64",
	OpAnd: "and", OpOr: "or", OpXor: "xor", OpShl: "shl", OpShrI64: "shr.i64", OpShrU64: "shr.u64",
	OpEqR: "eq.r", OpEqP: "eq.p", OpLtI64: "lt.i64", OpLtU64: "lt.u64", OpEqF64: "eq.f64", OpLtF64: "lt.f64",
	OpJump: "jump", OpJz: "jz", OpJnz: "jnz", OpCallFunc: "call.func", OpCallHost: "call.host", OpReturn: "ret",
}

func (op Opcode) String() string {
	if form := op.hostForm(); form != nil {
		return form.name()
	}
	if int(op) < len(opcodeNames) {
		return opcodeNames[op]
	}
	return ""
}
