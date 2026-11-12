package evmcompiler

import (
	"go/token"
	"go/types"
	"slices"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
	"golang.org/x/tools/go/ssa"
)

// Analyze typed SSA, before virtual registers are reused. Lowering emits explicit
// Own bytecode; runtime property access needs no taint bit or scope lookup.
// Stored leaves are owned at insertion, so aggregate aliases cannot conceal a
// borrowed leaf. Opaque borrowed aggregates use the declared acquisition clone.
type borrowOrigin struct {
	domain string
	scope  ssa.Value
}
type borrowFacts map[borrowOrigin]bool
type ownershipClone struct {
	value  ssa.Value
	reason string
}
type ownershipPlan struct {
	before map[ssa.Instruction][]ownershipClone
	after  map[ssa.Instruction][]ownershipClone
}
type liveValues map[ssa.Value]bool

func (f *lowerer) ownershipBinding(call *ssa.CallCommon) *evm.ExternalFunction {
	fn := call.StaticCallee()
	if fn == nil {
		return nil
	}
	if forwarded := f.forwardedHost(fn); forwarded != nil {
		fn = forwarded
	}
	obj, _ := fn.Object().(*types.Func)
	return f.c.env.natives[obj].function
}
func (f *lowerer) ownershipType(v ssa.Value) *values.Type {
	if v == nil {
		return nil
	}
	if f.c.deferValues[v] {
		return nil
	}
	if call, ok := v.(*ssa.Call); ok {
		if builtin, ok := call.Call.Value.(*ssa.Builtin); ok && builtin.Name() == "ssa:deferstack" {
			return nil
		}
	}
	if _, tuple := v.Type().(*types.Tuple); tuple {
		return nil
	}
	t, _ := f.c.env.valueType(v.Type())
	return t
}
func (f *lowerer) ownershipIntrinsic(call *ssa.CallCommon) bool {
	fn := call.StaticCallee()
	if fn == nil {
		return false
	}
	obj, _ := fn.Object().(*types.Func)
	_, ok := f.c.env.recordMethods[obj]
	return ok
}

// This summary proves ordinary read-only helpers safe without cloning their
// arguments. Recursive/indirect calls and possible reference returns are kept
// conservative. Stores in a helper are handled by owning arguments at its caller.
func (f *lowerer) ownershipSafeCall(call *ssa.CallCommon, retaining bool, seen map[*ssa.Function]bool) bool {
	if f.ownershipIntrinsic(call) {
		return true
	}
	if builtin, ok := call.Value.(*ssa.Builtin); ok {
		switch builtin.Name() {
		case "len", "cap", "real", "imag", "complex", "min", "max", "delete", "clear", "ssa:wrapnilchk":
			return true
		}
		return !retaining && (builtin.Name() == "append" || builtin.Name() == "copy")
	}
	if b := f.ownershipBinding(call); b != nil {
		if b.Ownership == nil {
			return false
		}
		if retaining {
			return b.Ownership.NoRetain
		}
		return b.Ownership.NoInvalidate
	}
	fn := call.StaticCallee()
	if fn == nil || len(fn.Blocks) == 0 || seen[fn] {
		return false
	}
	seen[fn] = true
	defer delete(seen, fn)
	for _, block := range fn.Blocks {
		for _, ins := range block.Instrs {
			switch i := ins.(type) {
			case *ssa.Store, *ssa.MapUpdate, *ssa.MakeClosure:
				if retaining {
					return false
				}
			case *ssa.Return:
				if retaining {
					for _, v := range i.Results {
						if t := f.ownershipType(v); t == nil || t.Repr() != values.Raw {
							return false
						}
					}
				}
			case *ssa.Call:
				if !f.ownershipSafeCall(&i.Call, retaining, seen) {
					return false
				}
			case *ssa.Defer, *ssa.RunDefers, *ssa.Panic, *ssa.Go:
				return false
			}
		}
	}
	return true
}

func (f *lowerer) planOwnership() *ownershipPlan {
	plan := &ownershipPlan{before: make(map[ssa.Instruction][]ownershipClone), after: make(map[ssa.Instruction][]ownershipClone)}
	facts := make(map[ssa.Value]borrowFacts)
	localContainers := f.localBorrowContainers()
	var order []ssa.Value
	positions := make(map[ssa.Instruction]int)
	add := func(v ssa.Value, origin borrowOrigin) {
		if facts[v] == nil {
			facts[v] = make(borrowFacts)
		}
		facts[v][origin] = true
	}
	addClone := func(ins ssa.Instruction, v ssa.Value, after bool, reason string) {
		where := plan.before
		if after {
			where = plan.after
		}
		for _, c := range where[ins] {
			if c.value == v {
				return
			}
		}
		where[ins] = append(where[ins], ownershipClone{v, reason})
	}
	for _, param := range f.fn.Params {
		order = append(order, param)
		if t := f.ownershipType(param); t != nil && t.CanOwn() && f.isReadRecord(t) {
			add(param, borrowOrigin{"evm.record", nil})
		}
	}
	for _, block := range f.fn.Blocks {
		for j, ins := range block.Instrs {
			positions[ins] = j
			if v, ok := ins.(ssa.Value); ok {
				order = append(order, v)
				if _, alloc := v.(*ssa.Alloc); alloc && f.isReadRecord(f.ownershipType(v)) {
					add(v, borrowOrigin{"evm.record", nil})
				}
			}
			call, ok := ins.(*ssa.Call)
			if !ok || f.ownershipIntrinsic(&call.Call) {
				continue
			}
			binding := f.ownershipBinding(&call.Call)
			var sources []evm.BorrowSource
			if binding != nil && binding.Ownership != nil {
				if err := binding.Ownership.Validate(binding.Signature); err != nil {
					f.c.fail(ins.Pos(), "%v", err)
					continue
				}
				sources = binding.Ownership.Borrowed
				for _, cb := range binding.Ownership.Callbacks {
					if cb.Argument >= len(call.Call.Args) {
						continue
					}
					if _, ok := call.Call.Args[cb.Argument].Type().Underlying().(*types.Signature); !ok {
						f.c.fail(ins.Pos(), "scoped callback argument is not a function")
					}
				}
			}
			// Immutable KVO result types have an ownership operation even for old
			// bindings that have not yet supplied finer scope/effect declarations.
			if results, ok := call.Type().(*types.Tuple); ok {
				for i := 0; i < results.Len(); i++ {
					t, _ := f.c.env.valueType(results.At(i).Type())
					if t.CanOwn() && f.isReadRecord(t) && !slices.ContainsFunc(sources, func(s evm.BorrowSource) bool { return s.Result == i }) {
						sources = append(slices.Clone(sources), evm.BorrowSource{Result: i, Domain: "evm.record"})
					}
				}
			} else if t := f.ownershipType(call); t != nil && t.CanOwn() && f.isReadRecord(t) && len(sources) == 0 {
				sources = []evm.BorrowSource{{Domain: "evm.record"}}
			}
			for _, source := range sources {
				origin := borrowOrigin{domain: source.Domain}
				if source.ScopeParameter > 0 {
					origin.scope = call.Call.Args[source.ScopeParameter-1]
				}
				var results []ssa.Value
				if _, tuple := call.Type().(*types.Tuple); tuple {
					for _, use := range *call.Referrers() {
						if x, ok := use.(*ssa.Extract); ok && x.Index == source.Result {
							results = append(results, x)
						}
					}
				} else if source.Result == 0 {
					results = []ssa.Value{call}
				}
				for _, v := range results {
					t := f.ownershipType(v)
					if t == nil || !t.CanOwn() {
						f.c.fail(ins.Pos(), "borrowed result has no ownership operation")
						continue
					}
					if t == values.StringType || f.isReadRecord(t) {
						add(v, origin)
					} else {
						addClone(v.(ssa.Instruction), v, true, "acquisition fallback: opaque borrowed value")
					}
				}
			}
		}
	}
	if len(facts) == 0 {
		return plan
	}
	merge := func(dst ssa.Value, src ssa.Value) bool {
		if len(facts[src]) == 0 {
			return false
		}
		t := f.ownershipType(dst)
		if t == nil {
			return false
		}
		if !t.CanOwn() && localContainers[dst] == nil {
			s, err := f.c.env.shape(dst.Type())
			if err != nil || s.Kind != values.InterfaceKind && s.Kind != values.StringKind {
				return false
			}
		}
		changed := false
		for origin := range facts[src] {
			if !facts[dst][origin] {
				add(dst, origin)
				changed = true
			}
		}
		return changed
	}
	for changed := true; changed; {
		changed = false
		for _, block := range f.fn.Blocks {
			for _, ins := range block.Instrs {
				switch i := ins.(type) {
				case *ssa.Store:
					for _, alias := range localContainers[i.Addr] {
						changed = merge(alias, i.Val) || changed
					}
				case *ssa.MapUpdate:
					for _, alias := range localContainers[i.Map] {
						changed = merge(alias, i.Key) || changed
						changed = merge(alias, i.Value) || changed
					}
				case *ssa.Index:
					changed = merge(i, i.X) || changed
				case *ssa.Field:
					changed = merge(i, i.X) || changed
				case *ssa.Lookup:
					if !i.CommaOk {
						changed = merge(i, i.X) || changed
					}

				case *ssa.Phi:
					for _, v := range i.Edges {
						changed = merge(i, v) || changed
					}
				case *ssa.ChangeType:
					changed = merge(i, i.X) || changed
				case *ssa.ChangeInterface:
					changed = merge(i, i.X) || changed
				case *ssa.MakeInterface:
					changed = merge(i, i.X) || changed
				case *ssa.TypeAssert:
					if !i.CommaOk {
						changed = merge(i, i.X) || changed
					}
				case *ssa.Extract:
					if lookup, ok := i.Tuple.(*ssa.Lookup); ok && i.Index == 0 {
						changed = merge(i, lookup.X) || changed
					}
					if next, ok := i.Tuple.(*ssa.Next); ok && i.Index > 0 {
						if rng, ok := next.Iter.(*ssa.Range); ok {
							changed = merge(i, rng.X) || changed
						}
					}
					if assertion, ok := i.Tuple.(*ssa.TypeAssert); ok && i.Index == 0 {
						changed = merge(i, assertion.X) || changed
					}
				case *ssa.Slice:
					changed = merge(i, i.X) || changed
				case *ssa.Convert:
					if a, ok := i.X.Type().Underlying().(*types.Basic); ok && a.Kind() == types.String {
						if b, ok := i.Type().Underlying().(*types.Basic); ok && b.Kind() == types.String {
							changed = merge(i, i.X) || changed
						}
					}
				case *ssa.UnOp:
					if i.Op == token.MUL {
						changed = merge(i, i.X) || changed
						if ref, _, ok := f.recordRef(i.X); ok {
							changed = merge(i, ref.Call.Args[0]) || changed
						}
					}
				}
			}
		}
	}
	liveAfter := ownershipLiveness(f.fn)
	for _, block := range f.fn.Blocks {
		for _, ins := range block.Instrs {
			own := func(v ssa.Value, reason string) {
				if len(facts[v]) != 0 {
					addClone(ins, v, false, reason)
				}
			}
			var invalidates bool
			var ends []evm.ScopeEnd
			switch i := ins.(type) {
			case *ssa.UnOp:
				if i.Op == token.MUL && f.isReadRecord(f.ownershipType(i.X)) {
					own(i.X, "escape: explicit record struct copy")
				}
			case *ssa.Return:
				for _, v := range i.Results {
					own(v, "escape: return")
				}
			case *ssa.Store:
				if localContainers[i.Addr] == nil {
					own(i.Val, "escape: store borrowed leaf")
				}
			case *ssa.MapUpdate:
				if localContainers[i.Map] == nil {
					own(i.Key, "escape: map key")
					own(i.Value, "escape: map value")
				}
			case *ssa.MakeClosure:
				for _, v := range i.Bindings {
					own(v, "escape: closure capture")
				}
			case *ssa.Call:
				if !f.ownershipSafeCall(&i.Call, true, make(map[*ssa.Function]bool)) {
					for _, v := range i.Call.Args {
						own(v, "escape: retaining call")
					}
				}
				invalidates = !f.ownershipSafeCall(&i.Call, false, make(map[*ssa.Function]bool))
				if b := f.ownershipBinding(&i.Call); b != nil && b.Ownership != nil {
					ends = b.Ownership.Ends
				}
			case *ssa.Defer:
				for _, v := range i.Call.Args {
					own(v, "escape: deferred call")
				}
				own(i.Call.Value, "escape: deferred closure")
			case *ssa.Panic:
				own(i.X, "escape: panic")
			case *ssa.RunDefers:
				invalidates = true
			}
			if invalidates {
				for _, v := range order {
					if !liveAfter[ins][v] || len(facts[v]) == 0 {
						continue
					}
					if definition, ok := v.(ssa.Instruction); ok && !instructionBefore(definition, ins, positions) {
						continue
					}
					matches := len(ends) == 0
					for origin := range facts[v] {
						for _, end := range ends {
							matches = matches || origin.domain == "evm.record" || origin.domain == end.Domain
						}
					}
					if matches {
						own(v, "scope boundary: live borrowed value")
					}
				}
			}
		}
	}
	// Index by value rather than scanning the whole function per copy site.
	// A dominating clone discharges subsequent copies of the same SSA value.
	clonesByValue := make(map[ssa.Value][]ssa.Instruction)
	for ins, clones := range plan.before {
		for _, clone := range clones {
			clonesByValue[clone.value] = append(clonesByValue[clone.value], ins)
		}
	}
	for ins, clones := range plan.before {
		plan.before[ins] = slices.DeleteFunc(clones, func(clone ownershipClone) bool {
			for _, prior := range clonesByValue[clone.value] {
				if prior != ins && instructionBefore(prior, ins, positions) {
					return true
				}
			}
			return false
		})
	}
	return plan
}

func instructionBefore(a, b ssa.Instruction, positions map[ssa.Instruction]int) bool {
	if a.Block() == b.Block() {
		return positions[a] < positions[b]
	}
	return a.Block().Dominates(b.Block())
}

func cloneLive(src liveValues) liveValues {
	dst := make(liveValues, len(src))
	for v := range src {
		dst[v] = true
	}
	return dst
}
func sameLive(a, b liveValues) bool {
	if len(a) != len(b) {
		return false
	}
	for v := range a {
		if !b[v] {
			return false
		}
	}
	return true
}

// Phi uses belong to incoming edges, not the successor body. This matters for
// Next loops: a consumed row must not appear live across acquisition of the next.
func ownershipLiveness(fn *ssa.Function) map[ssa.Instruction]liveValues {
	in, out := make(map[*ssa.BasicBlock]liveValues), make(map[*ssa.BasicBlock]liveValues)
	for changed := true; changed; {
		changed = false
		for bi := len(fn.Blocks) - 1; bi >= 0; bi-- {
			block := fn.Blocks[bi]
			live := make(liveValues)
			for _, succ := range block.Succs {
				for v := range in[succ] {
					live[v] = true
				}
				for _, ins := range succ.Instrs {
					phi, ok := ins.(*ssa.Phi)
					if !ok {
						break
					}
					delete(live, phi)
					for i, pred := range succ.Preds {
						if pred == block {
							live[phi.Edges[i]] = true
						}
					}
				}
			}
			out[block] = cloneLive(live)
			for j := len(block.Instrs) - 1; j >= 0; j-- {
				ownershipLiveBefore(block.Instrs[j], live)
			}
			if !sameLive(live, in[block]) {
				in[block] = live
				changed = true
			}
		}
	}
	result := make(map[ssa.Instruction]liveValues)
	for _, block := range fn.Blocks {
		live := cloneLive(out[block])
		for j := len(block.Instrs) - 1; j >= 0; j-- {
			ins := block.Instrs[j]
			result[ins] = cloneLive(live)
			ownershipLiveBefore(ins, live)
		}
	}
	return result
}
func ownershipLiveBefore(ins ssa.Instruction, live liveValues) {
	if v, ok := ins.(ssa.Value); ok {
		delete(live, v)
	}
	if _, phi := ins.(*ssa.Phi); phi {
		return
	}
	for _, p := range ins.Operands(nil) {
		if p != nil && *p != nil {
			live[*p] = true
		}
	}
}

func (f *lowerer) emitOwnership(clones []ownershipClone) {
	for _, c := range clones {
		r := f.regs[c.value][0]
		f.code.OwnershipSites = append(f.code.OwnershipSites, evm.OwnershipSite{PC: len(f.code.Code), Reason: c.reason})
		f.extended(evm.ExtendedOwn, f.shape(c.value.Type()), []vreg{r}, []vreg{r}, 0, 0, nil)
	}
}
