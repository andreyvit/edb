package evm

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"unsafe"
)

//go:generate go run gen_floatloop.go

// Public extended instructions require A=0. The private form enters a loop
// by index in BC, after Link has validated every original instruction.
const floatLoopMarker = 1

func (i Instruction) isFloatLoop() bool { return i.Op() == OpExtended && i.A() == floatLoopMarker }

const floatFrame = 2 // after f0 and f1 in the generated operand encoding

type floatKind uint8
type floatExpr struct {
	kind floatKind
	slot [4]uint16
}
type floatStep struct {
	form uint16
	slot [4]uint16
}
type floatLoop struct {
	steps                   []floatStep
	slices                  []uint16 // stable slice headers indexed by the induction variable
	branch                  Instruction
	head, latch, exit, cost int
	counter, limit          uint16
	rangeLoop               bool
	rangeHeader             int
	previous, test          uint16
	inclusive, escapeOnTrue bool
	slots                   [2]uint16
	loadMask, writeMask     byte
}

func (p *Program) floatExpr(i Instruction) floatExpr {
	e := floatExpr{slot: [4]uint16{i.A(), i.B(), i.C()}}
	switch i.Op() {
	case OpAddF64:
		e.kind = floatAdd
	case OpSubF64:
		e.kind = floatSub
	case OpMulF64:
		e.kind = floatMul
	case OpDivF64:
		e.kind = floatDiv
	case OpMulAddF64:
		e.kind = floatMulAdd
	case OpAddMulF64:
		e.kind = floatAddMul
	case OpSubAddF64:
		e.kind = floatSubAdd
	case OpMoveR:
		e.kind = floatMove
	case OpLoadIndexR:
		e.kind = floatLoadIndex
	case OpStoreIndexR:
		e.kind = floatStoreIndex
	case OpConvert:
		c := p.conversions[i.C()]
		if c.To == (NumericType{NumericFloat, 64}) && c.From.Bits == 64 {
			switch c.From.Kind {
			case NumericSigned:
				e.kind = floatFromInt
			case NumericUnsigned:
				e.kind = floatFromUint
			}
		}
	}
	if e.kind >= floatMulAdd && e.kind <= floatSubAdd {
		e.slot[0], e.slot[1], e.slot[2], e.slot[3] = i.ABCD12()
	}
	return e
}

// A region has one back edge, up to 32 straight-line operations, and at most
// one escape comparison. Only its back edge changes in the linked code.
// A branch or recovery entry into its middle can still run the original words.
func (p *Program) linkFloatLoops(f *linkedFunction) {
	for latch, ins := range f.Code {
		l := floatLoop{branch: ins, latch: latch}
		end := latch
		switch ins.Op() {
		case OpLoopLtI64, OpLoopLeI64:
			l.head = latch + 1 + int(int16(ins.C()))
			l.counter, l.limit, l.inclusive = ins.A(), ins.B(), ins.Op() == OpLoopLeI64
		case OpJump:
			// A Go slice range increments its saved index in the header.
			// The back edge copies the current index, then jumps to the add.
			h := latch + 1 + int(int32(ins.BC()))
			if h < 0 || h+4 >= latch || latch-h > 36 {
				continue
			}
			add, cmp, br, copy := f.Code[h], f.Code[h+1], f.Code[h+2], f.Code[latch-1]
			if add.Op() != OpAddImmI64 || add.C() != 1 ||
				cmp.Op() != opBranchLtI64 || cmp.B() != add.A() ||
				br.Op() != OpJz || br.A() != cmp.A() ||
				copy.Op() != OpMoveR || copy.A() != add.B() || copy.B() != add.A() {
				continue
			}
			l.rangeLoop, l.rangeHeader = true, h
			l.head, end = h+3, latch-1
			l.counter, l.previous, l.limit, l.test = add.A(), add.B(), cmp.C(), cmp.A()
			if l.counter == l.previous || l.counter == l.limit || l.counter == l.test ||
				l.previous == l.limit || l.previous == l.test || l.limit == l.test {
				continue
			}
		default:
			continue
		}
		head := l.head
		if head < 0 || head >= end {
			continue
		}
		var buf [33]floatExpr
		body := buf[:0]
		arithmetic := 0
		for pc := head; pc < end && len(body) < 32; pc++ {
			e := p.floatExpr(f.Code[pc])
			if e.kind == 0 {
				break
			}
			if e.kind >= floatAdd && e.kind <= floatSubAdd {
				arithmetic++
			}
			body = append(body, e)
		}
		if arithmetic == 0 || len(body) < 2 {
			continue
		}
		pc := head + len(body)
		if pc != end {
			if l.rangeLoop || pc+1 >= len(f.Code) || f.Code[pc].Op() != opBranchLtF64 {
				continue
			}
			br := f.Code[pc+1]
			if br.Op() != OpJz && br.Op() != OpJnz {
				continue
			}
			taken, fall := pc+2+int(int32(br.BC())), pc+2
			switch {
			case taken == latch:
				l.exit, l.escapeOnTrue = fall, br.Op() == OpJz
			case fall == latch:
				l.exit, l.escapeOnTrue = taken, br.Op() == OpJnz
			default:
				continue
			}
			i := f.Code[pc]
			body = append(body, floatExpr{floatLess, [4]uint16{i.A(), i.B(), i.C()}})
		}
		// A uint32 slice length and a nonnegative consecutive index let us
		// check the whole batch once. Unusual index expressions use the VM.
		if !l.checkHeaders(body) {
			continue
		}
		l.cost = len(body) + 1
		if l.rangeLoop {
			if !l.stableRange(body) {
				continue
			}
			l.cost += 3 // header add, linked compare and copy, in addition to the back jump
		}
		l.allocate(body)
		f.Code[latch] = WithBC(OpExtended, floatLoopMarker, uint32(len(p.floatLoops)))
		p.floatLoops = append(p.floatLoops, l)
	}
}

// The ordinary header recreates these registers on leaving a batch. A
// hand-written body may observe or overwrite them; keep that bytecode as is.
func (l *floatLoop) stableRange(body []floatExpr) bool {
	for _, e := range body {
		for j := range len(floatKinds[e.kind].operands) {
			slot := e.slot[j]
			if slot == l.previous || slot == l.test {
				return false
			}
			if j == 0 && e.kind != floatStoreIndex && (slot == l.counter || slot == l.limit) {
				return false
			}
		}
	}
	return true
}

func (l *floatLoop) checkHeaders(body []floatExpr) bool {
	for _, e := range body {
		var head, idx uint16
		switch e.kind {
		case floatLoadIndex:
			head, idx = e.slot[1], e.slot[2]
		case floatStoreIndex:
			head, idx = e.slot[0], e.slot[1]
		default:
			continue
		}
		if idx != l.counter || head == l.counter {
			return false
		}
		if !slices.Contains(l.slices, head) {
			l.slices = append(l.slices, head)
		}
	}
	if len(l.slices) == 0 {
		return true
	}
	for _, e := range body {
		if e.kind != floatStoreIndex && (e.slot[0] == l.counter || slices.Contains(l.slices, e.slot[0])) {
			return false
		}
	}
	return true
}

func (vm *VM) floatBackedge(cur *frame, index uint32, remaining uint64) uint64 {
	l := &vm.program.floatLoops[index]
	r, p := vm.r[cur.rb:], vm.p[cur.pb:]
	if l.rangeLoop {
		cur.pc, remaining = l.rangeBackedge(r, p, remaining)
	} else {
		cur.pc, remaining = l.backedge(r, p, remaining)
	}
	return remaining
}

// Enter just after a saved-index copy and a jump. Charge each full original
// iteration. The header remains responsible for the final failed comparison.
func (l *floatLoop) rangeBackedge(r []uint64, p []unsafe.Pointer, remaining uint64) (int, uint64) {
	next, bound := r[l.previous]+1, r[l.limit]
	if int64(next) < 0 || int64(next) >= int64(bound) {
		return l.rangeHeader, remaining
	}
	n := l.iterationsFrom(r, p, remaining&255, next)
	if left := bound - next; left < uint64(n) {
		n = int(left)
	}
	if n == 0 {
		return l.rangeHeader, remaining
	}
	r[l.counter], r[l.test] = next, 1
	_, spent := l.run(r, p, n)
	last := r[l.counter] - 1
	r[l.previous], r[l.counter] = last, last
	return l.rangeHeader, remaining - uint64(spent)
}

// The caller has already charged this latch. Back edges that reach a fuel
// checkpoint return to the original loop head for the ordinary meter.
func (l *floatLoop) backedge(r []uint64, p []unsafe.Pointer, remaining uint64) (int, uint64) {
	r[l.counter]++
	x, y := int64(r[l.counter]), int64(r[l.limit])
	if x > y || x == y && !l.inclusive {
		return l.latch + 1, remaining
	}
	n := l.iterationsFrom(r, p, remaining&255, r[l.counter])
	if n == 0 {
		return l.head, remaining
	}
	pc, spent := l.run(r, p, n)
	return pc, remaining - uint64(spent)
}

// A nil header or an index beyond the end falls through to ordinary execution
// at the original PC. Earlier stores and the resulting panic keep their order.
func (l *floatLoop) iterationsFrom(r []uint64, p []unsafe.Pointer, fuel, start uint64) int {
	n := int(fuel) / l.cost
	for _, s := range l.slices {
		size := uint64(uint32(r[s]))
		if start >= size || p[s] == nil {
			return 0
		}
		if left := size - start; left < uint64(n) {
			n = int(left)
		}
	}
	return n
}

type floatRange struct {
	slot                          uint16
	first, last, lastWrite, score int
	liveIn, written               bool
	lane                          int
}

func (l *floatLoop) allocate(body []floatExpr) {
	// Inputs precede the output at each word. Two disjoint temporary
	// ranges may meet at the same instruction and use the same lane.
	var ranges []floatRange
	for pc, e := range body {
		roles := floatKinds[e.kind].operands
		for pass := 0; pass < 2; pass++ {
			for j, role := range roles {
				if role == '.' || (role == 'd') != (pass == 1) {
					continue
				}
				slot := e.slot[j]
				if slot == l.counter || slot == l.limit || slices.Contains(l.slices, slot) ||
					body[len(body)-1].kind == floatLess && slot == body[len(body)-1].slot[0] {
					continue
				}
				k := slices.IndexFunc(ranges, func(v floatRange) bool { return v.slot == slot })
				pos := pc*2 + pass
				if k < 0 {
					k = len(ranges)
					ranges = append(ranges, floatRange{slot: slot, first: pos, liveIn: pass == 0, lane: -1})
				}
				t := &ranges[k]
				t.last = pos
				t.score++
				if pass == 1 {
					t.written = true
					t.lastWrite = pc
					t.score++
				}
			}
		}
	}
	// Loop-carried values and invariant inputs need a stable home across
	// the back edge. A write-first temporary can share a disjoint lifetime.
	slices.SortStableFunc(ranges, func(a, b floatRange) int { return cmp.Compare(b.score, a.score) })
	var owners [2][]*floatRange
	for i := range ranges {
		t := &ranges[i]
		for k := range owners {
			if len(owners[k]) == 0 {
				t.lane = k
				break
			}
		}
		if t.lane < 0 && !t.liveIn && t.score > 2 {
			for k, users := range owners {
				if !slices.ContainsFunc(users, func(v *floatRange) bool { return v.liveIn || t.first <= v.last && v.first <= t.last }) {
					t.lane = k
					break
				}
			}
		}
		if t.lane >= 0 {
			owners[t.lane] = append(owners[t.lane], t)
		}
	}
	for k, users := range owners {
		if len(users) == 1 {
			v := users[0]
			l.slots[k] = v.slot
			if v.liveIn {
				l.loadMask |= 1 << k
			}
			if v.written {
				l.writeMask |= 1 << k
			}
		}
	}
	l.steps = make([]floatStep, 0, len(body))
	for pc, e := range body {
		kind := floatKinds[e.kind]
		form := uint16(0)
		for j, role := range kind.operands {
			if role == '.' {
				continue
			}
			lane := floatFrame
			for k, users := range owners {
				if slices.ContainsFunc(users, func(v *floatRange) bool { return v.slot == e.slot[j] }) {
					lane = k
					break
				}
			}
			form = form*3 + uint16(lane)
		}
		l.steps = append(l.steps, floatStep{kind.base + form, e.slot})
		for k, users := range owners {
			if len(users) > 1 {
				for _, v := range users {
					if v.lastWrite == pc {
						l.steps = append(l.steps, floatStep{floatKinds[floatSpill].base + uint16(k), [4]uint16{v.slot, v.slot}})
					}
				}
			}
		}
	}
}

// DisassembleFloatLoops lists the private execution plans chosen by Link.
// The module's source listing and instruction PCs remain valid.
func (p *Program) DisassembleFloatLoops() string {
	var out strings.Builder
	for _, fn := range p.functions {
		for _, i := range fn.Code {
			if !i.isFloatLoop() {
				continue
			}
			l := &p.floatLoops[i.BC()]
			fmt.Fprintf(&out, "%s pc %d..%d\n", fn.Name, l.head, l.latch)
			for k, slot := range l.slots {
				if l.loadMask&(1<<k) != 0 {
					fmt.Fprintf(&out, "  load.f%d.r r%d\n", k, slot)
				}
			}
			for _, s := range l.steps {
				fmt.Fprintf(&out, "  %s", s)
				if s.form >= floatKinds[floatLess].base && s.form < floatKinds[floatLoadIndex].base {
					fmt.Fprintf(&out, " ; exit at %d if %t", l.exit, l.escapeOnTrue)
				}
				out.WriteByte('\n')
			}
			if l.rangeLoop {
				fmt.Fprintf(&out, "  range r%d = r%d + 1, while r%d < r%d ; header %d\n", l.counter, l.previous, l.counter, l.limit, l.rangeHeader)
			} else {
				fmt.Fprintf(&out, "  %s r%d r%d -> %d\n", l.branch.Op(), l.counter, l.limit, l.head)
			}
			for k, slot := range l.slots {
				if l.writeMask&(1<<k) != 0 {
					fmt.Fprintf(&out, "  store.r.f%d r%d\n", k, slot)
				}
			}
		}
	}
	return out.String()
}
func (s floatStep) String() string {
	i := len(floatKinds) - 1
	for s.form < floatKinds[i].base {
		i--
	}
	kind := floatKinds[i]
	form := s.form - kind.base
	var suffix, args [4]string
	for j := len(kind.operands) - 1; j >= 0; j-- {
		if kind.operands[j] == '.' {
			bank := "r"
			if floatKind(i) == floatLoadIndex && j == 1 || floatKind(i) == floatStoreIndex && j == 0 {
				bank = "v"
			}
			if floatKind(i) == floatLess || floatKind(i) == floatSpill {
				suffix[j] = ".r"
			}
			args[j] = fmt.Sprintf("%s%d", bank, s.slot[j])
			continue
		}
		lane := form % 3
		form /= 3
		if lane == floatFrame {
			suffix[j] = ".r"
			args[j] = fmt.Sprintf("r%d", s.slot[j])
		} else {
			suffix[j] = fmt.Sprintf(".f%d", lane)
		}
	}
	return strings.TrimSpace(kind.name + strings.Join(suffix[:], "") + " " + strings.Join(strings.Fields(strings.Join(args[:], " ")), " "))
}
