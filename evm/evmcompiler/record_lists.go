package evmcompiler

import (
	"go/token"
	"go/types"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/kvo"
	"golang.org/x/tools/go/ssa"
)

// A local read-only list uses the same compact ListCell as native KVO access.
// All consumers are specialized; it never materializes a guest slice or crosses
// an ABI. Unproven uses keep normal materialized slice semantics.
func (f *lowerer) findRecordLists() map[ssa.Value]recordProperty {
	result := make(map[ssa.Value]recordProperty)
	live := ownershipLiveness(f.fn)
	for _, block := range f.fn.Blocks {
		for _, ins := range block.Instrs {
			load, ok := ins.(*ssa.UnOp)
			if !ok || load.Op != token.MUL {
				continue
			}
			_, prop, ok := f.recordRef(load.X)
			if !ok || prop.owner.Mutable {
				continue
			}
			list, ok := prop.prop.AnyType().(*kvo.ListType)
			if !ok || evm.RecordScalarType(list.ItemType()) == nil {
				continue
			}
			// Keep aggregate/time/interface values on the regular converter path.
			elem, ok := load.Type().Underlying().(*types.Slice).Elem().Underlying().(*types.Basic)
			if !ok || elem.Info()&(types.IsBoolean|types.IsNumeric|types.IsString) == 0 {
				continue
			}
			uses := []ssa.Value{load}
			valid := true
			for _, use := range *load.Referrers() {
				switch u := use.(type) {
				case *ssa.Call:
					b, ok := u.Call.Value.(*ssa.Builtin)
					if !ok || (b.Name() != "len" && b.Name() != "cap") {
						valid = false
					}
				case *ssa.IndexAddr:
					refs := u.Referrers()
					if refs == nil || len(*refs) != 1 {
						valid = false
						continue
					}
					x, ok := (*refs)[0].(*ssa.UnOp)
					if !ok || x.Op != token.MUL || x.Block() != u.Block() {
						valid = false
						continue
					}
					adjacent := false
					for j, i := range u.Block().Instrs {
						if i == u && j+1 < len(u.Block().Instrs) && u.Block().Instrs[j+1] == x {
							adjacent = true
						}
					}
					if !adjacent {
						valid = false
						continue
					}
					uses = append(uses, u)
					if elem.Info()&types.IsString != 0 {
						// Borrowed strings may only feed local comparisons or len.
						uses = append(uses, x)
						for _, ref := range *x.Referrers() {
							switch v := ref.(type) {
							case *ssa.BinOp:
								if v.Op != token.EQL && v.Op != token.NEQ && v.Op != token.LSS && v.Op != token.LEQ && v.Op != token.GTR && v.Op != token.GEQ {
									valid = false
								}
							case *ssa.Call:
								b, ok := v.Call.Value.(*ssa.Builtin)
								if !ok || b.Name() != "len" {
									valid = false
								}
							default:
								valid = false
							}
						}
					}
				default:
					valid = false
				}
			}
			if !valid {
				continue
			}
			// Reject any possible mutation or scope boundary while the list or a
			// borrowed element is live. Loop liveness excludes the next acquisition.
			for _, b := range f.fn.Blocks {
				for _, i := range b.Instrs {
					unsafe := false
					switch x := i.(type) {
					case *ssa.Call:
						if _, p, ok := f.recordRef(x); ok {
							unsafe = p.owner.Mutable
						} else if builtin, ok := x.Call.Value.(*ssa.Builtin); ok {
							unsafe = builtin.Name() != "len" && builtin.Name() != "cap"
						} else {
							unsafe = true
						}
					case *ssa.Store, *ssa.MapUpdate, *ssa.Defer, *ssa.RunDefers, *ssa.Go:
						unsafe = true
					}
					if unsafe {
						for _, v := range uses {
							if live[i][v] {
								valid = false
							}
						}
					}
				}
			}
			if valid {
				result[load] = prop
			}
		}
	}
	return result
}

func (f *lowerer) recordListOp(kind evm.ExtendedKind, list ssa.Value, in, out []vreg) {
	p := f.recordLists[list]
	flags := uint8(0)
	if parent := f.directRecordList(list); parent != nil {
		flags = evm.RecordListDirect
		in[0] = f.regs[parent.Call.Args[0]][0]
	}
	f.extended(kind, f.shape(list.Type()), in, out, p.prop.Code(), flags, nil)
	f.c.module.ExtendedOps[len(f.c.module.ExtendedOps)-1].Record = p.owner
}
func (f *lowerer) fuseRecordList(addr *ssa.IndexAddr, next ssa.Instruction) bool {
	if _, ok := f.recordLists[addr.X]; !ok {
		return false
	}
	load, ok := next.(*ssa.UnOp)
	if !ok || load.Op != token.MUL || load.X != addr {
		return false
	}
	f.recordListOp(evm.ExtendedRecordListIndex, addr.X, []vreg{f.regs[addr.X][0], f.regs[addr.Index][0]}, f.regs[load])
	return true
}

// Fuse only adjacent, single-consumer reads to preserve evaluation order and
// retain first-class cells for shared locals and ordinary range loops.
func (f *lowerer) directRecordList(value ssa.Value) *ssa.Call {
	load, ok := value.(*ssa.UnOp)
	if !ok || load.Referrers() == nil || len(*load.Referrers()) != 1 {
		return nil
	}
	use := (*load.Referrers())[0]
	switch x := use.(type) {
	case *ssa.Call:
		b, ok := x.Call.Value.(*ssa.Builtin)
		if !ok || b.Name() != "len" && b.Name() != "cap" {
			return nil
		}
	case *ssa.IndexAddr:
	default:
		return nil
	}
	if use.Block() != load.Block() {
		return nil
	}
	for i, ins := range load.Block().Instrs {
		if ins == load && i+1 < len(load.Block().Instrs) && load.Block().Instrs[i+1] == use {
			ref, _, ok := f.recordRef(load.X)
			if ok {
				return ref
			}
		}
	}
	return nil
}
