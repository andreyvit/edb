package evm

import (
	"fmt"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

// DirectBinding binds a small external operation with zero or one result. Call receives
// arguments in declaration order, receiver first. Missing arguments are zero.
// Its value result is ignored for a void operation.
// Its error result reports VM or conversion failures. Fallible APIs use Native.
// Op alone is sufficient in the package supplied to the compiler.
// Methods may use a function opcode with their receiver as argument zero;
// this supports pair-valued receivers such as immutable KVO records.
type DirectBinding struct {
	word func(uint64) uint64 // optional specialized implementation from DirectUnary
	Op   Opcode
	Call DirectFunc
	// Window also exposes the ordinary R/P argument window through Call.
	// Generated package functions use it for their leading *evm.Call.
	Window bool
}
type DirectFunc func(*Call, values.Value, values.Value) (values.Value, error)

// DirectUnary binds an infallible word-to-word operation through DirectBinding.
// Direct calls use a specialized instruction; indirect and deferred calls use
// the same declaration and ordinary ABI. The returned binding is immutable
// registration metadata. Use DirectBinding directly for Call/context access.
func DirectUnary(fn func(uint64) uint64) DirectBinding {
	if fn == nil {
		panic("evm: nil direct unary function")
	}
	return DirectBinding{Op: OpCallHostUU, word: fn, Call: func(_ *Call, x, _ values.Value) (values.Value, error) {
		return values.Uint(fn(x.R)), nil
	}}
}

// first and second are explicit arguments. A two-argument function or a
// one-argument method reads its first explicit argument from its destination.
type hostForm struct {
	first, second, result values.Repr
	method                bool
	receiver              values.Repr
}

func (h *hostForm) tied() bool { return h.second != 0 || h.method && h.first != 0 }

var hostForms = func() [OpCallHostMethodPP - OpCallHostU + 1]hostForm {
	var forms [OpCallHostMethodPP - OpCallHostU + 1]hostForm
	banks := [...]values.Repr{values.Raw, values.Pair, values.Pointer}
	i := 0
	add := func(method bool, first, second values.Repr) {
		for _, result := range banks {
			forms[i] = hostForm{first: first, second: second, result: result, method: method, receiver: values.Pointer}
			i++
		}
	}
	for _, method := range []bool{false, true} {
		add(method, 0, 0)
		for _, first := range banks {
			add(method, first, 0)
		}
		if !method {
			for _, first := range banks {
				for _, second := range banks {
					add(false, first, second)
				}
			}
		}
	}
	return forms
}()

var voidHostForms = func() [OpCallHostMethodPV - OpCallHostV + 1]hostForm {
	var forms [OpCallHostMethodPV - OpCallHostV + 1]hostForm
	j := 0
	for i := 0; i < len(hostForms); i += 3 {
		forms[j] = hostForms[i]
		forms[j].result = 0
		j++
	}
	return forms
}()

func (h *hostForm) name() string {
	prefix := "call.host."
	if h.method {
		prefix += "method."
	}
	const letters = "\x00ups" // indexed by values.Repr
	var suffix [3]byte
	n := 0
	for _, bank := range [...]values.Repr{h.first, h.second, h.result} {
		if bank != 0 {
			suffix[n] = letters[bank]
			n++
		}
	}
	if h.result == 0 {
		suffix[n] = 'v'
		n++
	}
	return prefix + string(suffix[:n])
}

func (op Opcode) hostForm() *hostForm {
	if op >= OpCallHostV && op <= OpCallHostMethodPV {
		return &voidHostForms[op-OpCallHostV]
	}
	if op < OpCallHostU || op > OpCallHostMethodPP {
		return nil
	}
	return &hostForms[op-OpCallHostU]
}

// HostRegisters supplies the disassembly and assembler prefixes. When a tied
// operand crosses R/P, print the result bank, except that a string always prints V.
func (op Opcode) HostRegisters() (a, b byte, ok bool) {
	h := op.hostForm()
	if h == nil {
		return
	}
	const prefix = "\x00rpv"
	a = prefix[h.result]
	if h.result == 0 && h.tied() {
		a = prefix[h.first]
	}
	if h.tied() && h.first == values.Pair {
		a = 'v'
	}
	switch {
	case h.method:
		b = 'p'
	case h.second != 0:
		b = prefix[h.second]
	default:
		b = prefix[h.first]
	}
	return a, b, true
}

// TiedHostArgument returns the source argument that must occupy A, counting
// the receiver as argument zero. Void calls read A without overwriting it.
func (op Opcode) TiedHostArgument() (int, bool) {
	h := op.hostForm()
	if h == nil || !h.tied() {
		return 0, false
	}
	if h.method {
		return 1, true
	}
	return 0, true
}

// True pointers and mutable record tokens use p. Immutable record cells and
// strings use both banks. Struct snapshots need their own copying convention.
func hostTypes(packages []Package) map[*values.Type]values.Repr {
	kinds := map[*values.Type]values.Repr{values.StringType: values.Pair}
	for _, pkg := range packages {
		for _, d := range pkg.Types {
			if s := d.Shape; s != nil && s.Type != nil {
				switch s.Kind {
				case values.StringKind:
					kinds[s.Type] = values.Pair
				case values.PointerKind:
					kinds[s.Type] = values.Pointer
				}
			}
		}
		for _, r := range pkg.Records {
			if r != nil && r.Pointer != nil && r.Model != nil {
				kinds[r.Pointer] = r.Pointer.Repr()
			}
		}
	}
	return kinds
}

func (h *hostForm) matches(sig Signature, method, variadic bool, kinds map[*values.Type]values.Repr) bool {
	is := func(t *values.Type, want values.Repr) bool {
		return t != nil && (t.Repr() == want || want == values.Pointer && t.Repr() == values.Pair && kinds[t] == values.Pointer) && (want == values.Raw || kinds[t] == want)
	}
	if h == nil || h.method && !method || variadic {
		return false
	}
	if h.result == 0 {
		if len(sig.Results) != 0 {
			return false
		}
	} else if len(sig.Results) != 1 || !is(sig.Results[0], h.result) {
		return false
	}
	params := sig.Params
	if h.method {
		if len(params) == 0 || !is(params[0], values.Pointer) {
			return false
		}
		params = params[1:]
	}
	for _, bank := range [...]values.Repr{h.first, h.second} {
		if bank == 0 {
			break
		}
		if len(params) == 0 || !is(params[0], bank) {
			return false
		}
		params = params[1:]
	}
	return len(params) == 0
}

func (h *hostForm) validSlots(a, b int, f *linkedFunction) bool {
	valid := func(slot int, bank values.Repr) bool {
		return (bank&values.Raw == 0 || slot < f.RCount) && (bank&values.Pointer == 0 || slot < f.PCount)
	}
	dst := h.result
	if h.tied() {
		dst |= h.first
	}
	if dst == 0 && a != 0 || !valid(a, dst) {
		return false
	}
	switch {
	case h.method:
		return valid(b, h.receiver)
	case h.second != 0:
		return valid(b, h.second)
	case h.first != 0:
		return valid(b, h.first)
	default:
		return b == 0
	}
}

func hostArg(bank values.Repr, slot int, r []uint64, p []unsafe.Pointer) (v values.Value) {
	if bank&values.Raw != 0 {
		v.R = r[slot]
	}
	if bank&values.Pointer != 0 {
		v.P = p[slot]
	}
	return
}

// ValidateDirectBindings checks register conventions against the declared packages.
// A compiler schema may omit implementations until Link binds it to a host.
func ValidateDirectBindings(packages []Package) error {
	var kinds map[*values.Type]values.Repr
	check := func(d DirectBinding, sig Signature, method, variadic bool) bool {
		if d.Op == 0 {
			return d.Call == nil && !d.Window && d.word == nil
		}
		if d.word != nil && (d.Op != OpCallHostUU || d.Call == nil || d.Window) {
			return false
		}
		if kinds == nil {
			kinds = hostTypes(packages)
		}
		return d.Op.hostForm().matches(sig, method, variadic, kinds)
	}
	for _, pkg := range packages {
		for _, fn := range pkg.Functions {
			if !check(fn.Direct, fn.Signature, false, fn.Variadic) {
				return fmt.Errorf("%s.%s: invalid direct binding %s for signature", pkg.Path, fn.Name, fn.Direct.Op)
			}
		}
		for _, m := range pkg.Methods {
			if m.Direct.Op == 0 && m.Direct.Call == nil && !m.Direct.Window {
				continue
			}
			sig := Signature{Params: append([]*values.Type{m.Receiver}, m.Signature.Params...), Results: m.Signature.Results}
			if !check(m.Direct, sig, true, m.Variadic) {
				return fmt.Errorf("%s.%s: invalid direct method binding %s for signature", pkg.Path, m.Name, m.Direct.Op)
			}
		}
	}
	return nil
}

// Indirect, deferred and wide-index calls enter with the ordinary ABI.
func directWindowCall(c *Call) error {
	var x, y values.Value
	switch len(c.native.Signature.Params) {
	case 2:
		y = c.Argument(1)
		fallthrough
	case 1:
		x = c.Argument(0)
	}
	v, err := c.directResult(x, y)
	if err == nil && len(c.native.Signature.Results) != 0 {
		c.SetResult(0, v)
	}
	return err
}

func (c *Call) directResult(x, y values.Value) (values.Value, error) {
	g := c.native
	v, err := g.Direct.Call(c, x, y)
	if err == nil && g.record != nil && !g.record.Mutable {
		_, err = readRecordCell(v, g.record.Model)
	}
	if err == nil && g.record != nil && g.record.Mutable {
		scope := c.budget.recordScope()
		if scope == nil || scope.closed {
			return values.Value{}, fmt.Errorf("KVO access outside record scope")
		}
		if v.P != nil {
			r := scope.lookup(v)
			if r == nil || r.model != g.record.Model || (r.edit != nil || r.parent != nil) != g.record.Mutable {
				return values.Value{}, fmt.Errorf("invalid or expired %s record", g.record.Name)
			}
		}
	}
	if err == nil && g.resultString {
		err = c.ChargeAllocation(v.R)
	}
	return v, err
}

func (vm *VM) invokeDirect(cur *frame, ins Instruction, budget *executionBudget) error {
	if err := budget.contextErr(); err != nil {
		return err
	}
	a, b := int(ins.A()), int(ins.B())
	g := &vm.program.natives[ins.C()]
	f := &vm.program.functions[cur.function]
	r, p := vm.r[cur.rb:], vm.p[cur.pb:]
	h := g.form
	var x, y values.Value
	switch {
	case h.method:
		x = hostArg(h.receiver, b, r, p)
		y = hostArg(h.first, a, r, p)
	case h.second != 0:
		x = hostArg(h.first, a, r, p)
		y = hostArg(h.second, b, r, p)
	default:
		x = hostArg(h.first, b, r, p)
	}
	c := &vm.call
	c.native, c.Position, c.Context = g, f.position(cur.pc-1), budget.ctx
	// A generated package function receives a public *Call. Preserve its
	// complete window, including independently packed raw and pointer banks.
	if g.Direct.Window {
		c.R = vm.directR[:g.rSize:g.rSize]
		c.P = vm.directP[:g.pSize:g.pSize]
		clear(c.R)
		clear(c.P)
		for i, slot := range g.params.Slots {
			arg := x
			if i == 1 {
				arg = y
			}
			repr := g.Signature.Params[i].Repr()
			if repr&values.Raw != 0 {
				c.R[slot] = arg.R
			}
			if repr&values.Pointer != 0 {
				c.P[slot] = arg.P
			}
		}
	}
	v, err := c.directResult(x, y)
	if g.Direct.Window {
		clear(c.P)
		c.R, c.P = nil, nil
	}
	if err != nil {
		return err
	}
	if budget.err != nil {
		return budget.err
	}
	if err := budget.contextErr(); err != nil {
		return err
	}
	if h.result == 0 {
		return nil
	}
	write := h.result
	if h.tied() {
		write |= h.first
	}
	if write&values.Raw != 0 {
		if h.result == values.Pointer {
			v.R = 0
		}
		r[a] = v.R
	}
	if write&values.Pointer != 0 {
		if h.result == values.Raw {
			v.P = nil
		}
		p[a] = v.P
	}
	return nil
}

// HostCallOpcode selects a direct instruction from its explicit argument and
// result banks. Pair means string; Pointer means a true pointer or a record token.
// A zero result selects a void operation.
func HostCallOpcode(method bool, args []values.Repr, result values.Repr) Opcode {
	if len(args) > 2 || method && len(args) > 1 {
		return 0
	}
	var first, second values.Repr
	if len(args) > 0 {
		first = args[0]
		if first == 0 {
			return 0
		}
	}
	if len(args) > 1 {
		second = args[1]
		if second == 0 {
			return 0
		}
	}
	forms, base := hostForms[:], OpCallHostU
	if result == 0 {
		forms, base = voidHostForms[:], OpCallHostV
	}
	for i := range forms {
		h := &forms[i]
		if h.method == method && h.first == first && h.second == second && h.result == result {
			return base + Opcode(i)
		}
	}
	return 0
}
