package evm

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

func referenceFloatProgram(t *testing.T, m *Module) (*Program, *Program) {
	t.Helper()
	fast, err := Link(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(fast.floatLoops) == 0 {
		t.Fatal("fixture did not enter the float evaluator")
	}
	ref, err := Link(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range ref.functions {
		for pc, i := range f.Code {
			if i.isFloatLoop() {
				f.Code[pc] = ref.floatLoops[i.BC()].branch
			}
		}
	}
	return fast, ref
}

// Calls with a small budget stop in different parts of the same loop. Compare
// every frame word, including dead temporaries, and the PC of the next trap.
func TestFloatLoopDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(529))
	// This test compares every bit, including integer reinterpretations. Use
	// the same NaN that invalid arithmetic produces on this architecture.
	// A separate test exercises several distinct payloads.
	var zero float64
	words := []uint64{0, 555, math.Float64bits(1.0 + 1.0/(1<<27)), math.Float64bits(1.0 - 1.0/(1<<27)), math.Float64bits(-1), 1 << 63, math.Float64bits(math.Inf(1)), math.Float64bits(zero / zero)}
	ops := []Opcode{OpAddF64, OpSubF64, OpMulF64, OpDivF64, OpMulAddF64, OpAddMulF64, OpSubAddF64, OpMoveR, OpConvert}
	for example := range 35 {
		code := make([]Instruction, len(words))
		for i := range words {
			code[i] = WithBC(OpConst, uint16(i), uint32(i))
		}
		head := len(code)
		code = append(code, ABCD12(OpMulAddF64, 8, 2, 3, 4), ABC(OpSubF64, 9, 8, 3))
		slot := func() uint16 { return uint16(2 + rng.Intn(12)) }
		for range 3 + example%14 {
			op := ops[rng.Intn(len(ops))]
			a, b, c := slot(), slot(), slot()
			switch op {
			case OpMoveR:
				code = append(code, ABC(op, a, b, 0))
			case OpConvert:
				code = append(code, ABC(op, a, b, uint16(example%2)))
			case OpMulAddF64, OpAddMulF64, OpSubAddF64:
				code = append(code, ABCD12(op, a, b, c, slot()))
			default:
				code = append(code, ABC(op, a, b, c))
			}
		}
		if example%3 != 0 {
			// One guard jumps over an ordinary return to the loop latch.
			j := OpJz
			if example%3 == 2 {
				j = OpJnz
			}
			code = append(code, ABC(OpLtF64, 15, slot(), slot()), WithBC(j, 15, 1), ABC(OpReturn, 0, 0, 0))
		}
		latch := OpLoopLtI64
		if example%2 == 1 {
			latch = OpLoopLeI64
		}
		code = append(code, ABC(latch, 0, 1, uint16(head-len(code)-1)), ABC(OpReturn, 0, 0, 0))
		f := Function{Name: "run", RCount: 16, Code: code}
		m := &Module{Functions: []Function{f}, Words: words, Conversions: []Conversion{
			{NumericType{NumericSigned, 64}, NumericType{NumericFloat, 64}}, {NumericType{NumericUnsigned, 64}, NumericType{NumericFloat, 64}},
		}}
		fast, ref := referenceFloatProgram(t, m)
		for _, fuel := range []uint64{1, 13, 23, 254, 255, 256, 257, 258, 499, 767, 31000} {
			a, b := New(fast), New(ref)
			ea := a.RunWithBudget(context.Background(), "run", nil, nil, Budget{Steps: fuel})
			eb := b.RunWithBudget(context.Background(), "run", nil, nil, Budget{Steps: fuel})
			if fmt.Sprint(ea) != fmt.Sprint(eb) || !slices.Equal(a.r, b.r) {
				t.Fatalf("example %d fuel %d: %v %x vs %v %x\n%s", example, fuel, ea, a.r, eb, b.r, fast.DisassembleFloatLoops())
			}
		}
	}
}

func TestFloatLoopCopiesExactBits(t *testing.T) {
	s := values.NewType("[]float64", values.Pair)
	f := Function{Name: "copy", Signature: Signature{Params: []*values.Type{s, s, values.IntType}}, RCount: 8, PCount: 2}
	f.Code = []Instruction{
		WithBC(OpConst, 3, 0), WithBC(OpConst, 7, 1),
		ABC(OpLoadIndexR, 4, 0, 3), ABC(OpMoveR, 5, 4, 0), ABC(OpStoreIndexR, 1, 3, 5),
		ABC(OpAddF64, 6, 7, 5), ABC(OpLoopLtI64, 3, 2, 0xfffb), ABC(OpReturn, 0, 0, 0),
	}
	fast, ref := referenceFloatProgram(t, &Module{Functions: []Function{f}, Words: []uint64{0, 0x7ff8000000000042}})
	patterns := []uint64{0, 1 << 63, 1, 0x7ff0000000000000, 0xfff0000000000000, 0xfff0000000000034, 0x7ff8000000000051}
	src := make([]values.Value, 259)
	for i := range src {
		src[i].R = patterns[i%len(patterns)]
	}
	for _, p := range []*Program{fast, ref} {
		dst := make([]values.Value, len(src))
		asSlice := func(s []values.Value) values.Value {
			return values.SliceValue(unsafe.Pointer(&s[0]), uint32(len(s)), uint32(len(s)))
		}
		vm := New(p)
		if err := vm.Run(context.Background(), "copy", []values.Value{asSlice(src), asSlice(dst), values.Int(int64(len(src)))}, nil); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(src, dst) || vm.r[5] != src[len(src)-1].R {
			t.Fatal("load, move, store or spill changed the input bits")
		}
		// Both inputs to the last add are NaNs with different payloads.
		if !math.IsNaN(math.Float64frombits(vm.r[6])) {
			t.Fatalf("NaN + NaN produced %x", vm.r[6])
		}
	}
}

func TestFloatLoopIndexedFault(t *testing.T) {
	slice := values.NewType("[]float64", values.Pair)
	// Two potentially aliasing slices arrive in V0 and V1, the loop bound in R2.
	f := Function{Name: "run", Signature: Signature{Params: []*values.Type{slice, slice, values.IntType}}, RCount: 8, PCount: 2}
	f.Code = []Instruction{
		WithBC(OpConst, 3, 0), WithBC(OpConst, 6, 1),
		ABC(OpLoadIndexR, 4, 0, 3), ABC(OpAddF64, 5, 4, 6), ABC(OpStoreIndexR, 1, 3, 5), ABC(OpMulF64, 7, 5, 5),
		ABC(OpLoopLtI64, 3, 2, 0xfffb), ABC(OpReturn, 0, 0, 0),
	}
	m := &Module{Functions: []Function{f}, Words: []uint64{0, math.Float64bits(0.5)}}
	// A slice range has separate registers for the previous index and test.
	rangeBody := []Instruction{
		WithBC(OpConst, 8, 2), WithBC(OpConst, 6, 1),
		ABC(OpAddImmI64, 3, 8, 1), ABC(OpLtI64, 9, 3, 2), WithBC(OpJz, 9, 6),
	}
	rangeBody = append(rangeBody, f.Code[2:6]...)
	rangeBody = append(rangeBody, ABC(OpMoveR, 8, 3, 0), WithBC(OpJump, 0, 0xfffffff7), ABC(OpReturn, 0, 0, 0))
	rf := Function{Name: f.Name, Signature: f.Signature, RCount: 10, PCount: f.PCount, Code: rangeBody}
	for _, fixture := range []*Module{m, {Functions: []Function{rf}, Words: []uint64{0, math.Float64bits(0.5), ^uint64(0)}}} {
		fast, ref := referenceFloatProgram(t, fixture)
		for _, sizes := range [][2]int{{0, 0}, {4, 3}, {3, 4}, {255, 256}, {257, 256}, {300, 300}} {
			for _, nilData := range []bool{false, true} {
				run := func(p *Program) ([]values.Value, string) {
					mem := make([]values.Value, 301)
					for i := range mem {
						mem[i] = values.Float(float64(i))
					}
					ptr := unsafe.Pointer(&mem[0])
					if nilData {
						ptr = nil
					}
					arg := []values.Value{
						values.SliceValue(ptr, uint32(sizes[0]), uint32(sizes[0])),
						values.SliceValue(unsafe.Pointer(&mem[1]), uint32(sizes[1]), uint32(sizes[1])),
						values.Int(300),
					}
					return mem, fmt.Sprint(New(p).Run(context.Background(), "run", arg, nil))
				}
				a, ae := run(fast)
				b, be := run(ref)
				if ae != be || !slices.Equal(a, b) {
					t.Fatalf("sizes %v nil %v: %s vs %s", sizes, nilData, ae, be)
				}
			}
		}
	}
	for _, bad := range []Instruction{ABC(OpAddF64, 0, 5, 6), ABC(OpAddF64, 3, 5, 6), ABC(OpLoadIndexR, 4, 0, 2)} {
		alt := f
		alt.Code = slices.Clone(f.Code)
		alt.Code[3] = bad
		p, err := Link(&Module{Functions: []Function{alt}, Words: m.Words})
		if err != nil || len(p.floatLoops) != 0 {
			t.Fatalf("unsafe header/index %#x: %v", bad, err)
		}
	}
}

type floatPoll struct {
	context.Context
	cancel context.CancelFunc
	polls  int
}

func (c *floatPoll) Err() error {
	c.polls++
	if c.polls == 4 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestFloatLoopPollAndInteriorEntry(t *testing.T) {
	code := []Instruction{
		WithBC(OpConst, 0, 0), WithBC(OpConst, 1, 1), WithBC(OpConst, 2, 2), WithBC(OpJump, 0, 1),
		ABC(OpAddF64, 3, 3, 2), ABC(OpMulF64, 4, 3, 2), ABC(OpLoopLtI64, 0, 1, 0xfffd), ABC(OpReturn, 0, 0, 0),
	}
	m := &Module{Functions: []Function{{Name: "run", Code: code, RCount: 5}}, Words: []uint64{0, 8000, math.Float64bits(1.5)}}
	fast, ref := referenceFloatProgram(t, m)
	var state []uint64
	var previous string
	for _, p := range []*Program{fast, ref} {
		ctx, cancel := context.WithCancel(context.Background())
		c := &floatPoll{Context: ctx, cancel: cancel}
		vm := New(p)
		err := vm.RunWithBudget(c, "run", nil, nil, Budget{Steps: 15000})
		cancel()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v after %d polls", err, c.polls)
		}
		if state != nil && (!slices.Equal(vm.r, state) || fmt.Sprint(err) != previous) {
			t.Fatalf("cancellation state differs: %v %v vs %v %v", vm.r, err, state, previous)
		}
		state = slices.Clone(vm.r)
		previous = fmt.Sprint(err)
	}
	if d := fast.DisassembleFloatLoops(); !strings.Contains(d, "run pc 4..6") || !strings.Contains(d, "load.f") || !strings.Contains(d, "mul.") || !strings.Contains(d, "loop.lt.i64 r0 r1 -> 4") {
		t.Fatalf("bad loop listing:\n%s", d)
	}
	// A borrower cannot forge a private entry PC in a public module.
	m.Functions[0].Code[4] = WithBC(OpExtended, floatLoopMarker, 0)
	if _, err := Link(m); err == nil {
		t.Fatal("linked forged scratch loop")
	}
}

func TestFloatRangeLoopMeter(t *testing.T) {
	// The header's index, predicate and saved previous index all remain
	// observable, including when fuel stops between the last copy and jump.
	f := Function{Name: "range", RCount: 9, Code: []Instruction{
		WithBC(OpConst, 1, 0), WithBC(OpConst, 3, 1), WithBC(OpConst, 4, 2),
		ABC(OpAddImmI64, 2, 1, 1), ABC(OpLtI64, 5, 2, 3), WithBC(OpJz, 5, 5),
		ABC(OpMulF64, 6, 4, 4), ABC(OpAddF64, 7, 7, 6), ABC(OpAddF64, 8, 7, 4),
		ABC(OpMoveR, 1, 2, 0), WithBC(OpJump, 0, 0xfffffff8), ABC(OpReturn, 0, 0, 0),
	}}
	for _, initial := range []uint64{^uint64(0), 0, 270, uint64(1)<<63 - 1} {
		m := &Module{Functions: []Function{f}, Words: []uint64{initial, 270, math.Float64bits(1.125)}}
		fast, ref := referenceFloatProgram(t, m)
		if !fast.floatLoops[0].rangeLoop {
			t.Fatal("range loop not recognized")
		}
		for _, fuel := range []uint64{1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 19, 254, 255, 256, 257, 511, 700, 2200} {
			a, b := New(fast), New(ref)
			ea := a.RunWithBudget(context.Background(), "range", nil, nil, Budget{Steps: fuel})
			eb := b.RunWithBudget(context.Background(), "range", nil, nil, Budget{Steps: fuel})
			if fmt.Sprint(ea) != fmt.Sprint(eb) || !slices.Equal(a.r, b.r) {
				t.Fatalf("start %d fuel %d: %v %x vs %v %x", initial, fuel, ea, a.r, eb, b.r)
			}
		}
	}
}
