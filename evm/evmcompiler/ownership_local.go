package evmcompiler

import (
	"go/token"
	"go/types"

	"golang.org/x/tools/go/ssa"
)

// Prove a local aggregate's entire alias graph stays in this function. In this
// first pass, any possible scope invalidation disables the optimization. This
// deliberately favors insertion-time leaf copies over runtime container walks.
func (f *lowerer) localBorrowContainers() map[ssa.Value][]ssa.Value {
	for _, block := range f.fn.Blocks {
		for _, ins := range block.Instrs {
			switch i := ins.(type) {
			case *ssa.Call:
				if !f.ownershipSafeCall(&i.Call, false, make(map[*ssa.Function]bool)) {
					return nil
				}
			case *ssa.Defer, *ssa.RunDefers, *ssa.Panic, *ssa.Go:
				return nil
			}
		}
	}
	result := make(map[ssa.Value][]ssa.Value)
	for _, block := range f.fn.Blocks {
		for _, ins := range block.Instrs {
			var root ssa.Value
			switch i := ins.(type) {
			case *ssa.Alloc:
				root = i
			case *ssa.MakeSlice:
				root = i
			case *ssa.MakeMap:
				root = i
			default:
				continue
			}
			if !ownershipAggregate(root.Type()) || f.isReadRecord(f.ownershipType(root)) {
				continue
			}
			seen := make(map[ssa.Value]bool)
			var aliases []ssa.Value
			var visit func(ssa.Value) bool
			visit = func(v ssa.Value) bool {
				if seen[v] {
					return true
				}
				seen[v] = true
				aliases = append(aliases, v)
				if v.Referrers() == nil {
					return false
				}
				for _, use := range *v.Referrers() {
					switch u := use.(type) {
					case *ssa.Store:
						if u.Addr != v || u.Val == v {
							return false
						}
					case *ssa.MapUpdate:
						if u.Map != v || u.Key == v || u.Value == v {
							return false
						}
					case *ssa.IndexAddr:
						if u.X != v || !visit(u) {
							return false
						}
					case *ssa.FieldAddr:
						if u.X != v || !visit(u) {
							return false
						}
					case *ssa.Slice:
						if u.X != v || !visit(u) {
							return false
						}
					case *ssa.Phi:
						// A join may also alias externally owned storage.
						return false
					case *ssa.ChangeType:
						if !visit(u) {
							return false
						}
					case *ssa.UnOp:
						if u.Op != token.MUL || (ownershipAggregate(u.Type()) && !f.isReadRecord(f.ownershipType(u))) {
							return false
						}
					case *ssa.Index:
						if u.X != v || (ownershipAggregate(u.Type()) && !f.isReadRecord(f.ownershipType(u))) {
							return false
						}
					case *ssa.Lookup:
						if u.X != v || (ownershipAggregate(u.Type()) && !f.isReadRecord(f.ownershipType(u))) {
							return false
						}
					case *ssa.Range:
						if u.X != v {
							return false
						}
					case *ssa.Call:
						b, ok := u.Call.Value.(*ssa.Builtin)
						if !ok || (b.Name() != "len" && b.Name() != "cap") {
							return false
						}
					case *ssa.BinOp:
						if u.Op != token.EQL && u.Op != token.NEQ {
							return false
						}
					case *ssa.DebugRef:
					default:
						return false
					}
				}
				return true
			}
			if visit(root) {
				for _, v := range aliases {
					result[v] = aliases
				}
			}
		}
	}
	return result
}
func ownershipAggregate(t types.Type) bool {
	if p, ok := t.Underlying().(*types.Pointer); ok {
		t = p.Elem()
	}
	switch t.Underlying().(type) {
	case *types.Struct, *types.Array, *types.Slice, *types.Map:
		return true
	}
	return false
}
