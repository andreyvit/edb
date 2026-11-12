package evmcompiler

import (
	"go/token"
	"go/types"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
	"golang.org/x/tools/go/ssa"
)

// Each connected group of slice headers and array pointers must agree on its
// stride. Start at allocations and follow aliases in both directions. A guest
// result or external argument can share storage with the caller's []Value
// cells, so an unknown producer or consumer keeps the whole group cell-backed.
func (f *lowerer) findPackedSlices() map[ssa.Value]bool {
	var packed, done map[ssa.Value]bool
	for _, block := range f.fn.Blocks {
		for _, i := range block.Instrs {
			var root ssa.Value
			var elem types.Type
			switch v := i.(type) {
			case *ssa.Call:
				if b, ok := v.Call.Value.(*ssa.Builtin); ok && b.Name() == "append" {
					root, elem = v, v.Type().Underlying().(*types.Slice).Elem()
				}
			case *ssa.MakeSlice:
				root, elem = v, v.Type().Underlying().(*types.Slice).Elem()
			case *ssa.Alloc:
				if a, ok := v.Type().Underlying().(*types.Pointer).Elem().Underlying().(*types.Array); ok {
					root, elem = v, a.Elem()
				}
			}
			if root == nil || done[root] {
				continue
			}
			if f.shape(elem).Type.Repr() == values.Pair {
				continue // Tagged native pointers need both banks in each element.
			}
			switch t := elem.Underlying().(type) {
			case *types.Pointer:
			case *types.Basic:
				if t.Info()&(types.IsBoolean|types.IsInteger|types.IsFloat) == 0 {
					continue
				}
			default:
				continue
			}
			if done == nil {
				done = make(map[ssa.Value]bool)
			}
			var group, addrs []ssa.Value
			add := func(v ssa.Value) {
				if k, ok := v.(*ssa.Const); ok && k.IsNil() {
					return
				}
				if !done[v] {
					done[v] = true
					group = append(group, v)
				}
			}
			add(root)
			ok := true
			for n := 0; n < len(group); n++ {
				v := group[n]
				switch x := v.(type) {
				case *ssa.MakeSlice, *ssa.Alloc:
				case *ssa.Phi:
					for _, e := range x.Edges {
						add(e)
					}
				case *ssa.ChangeType:
					add(x.X)
				case *ssa.Slice:
					add(x.X)
				case *ssa.SliceToArrayPointer:
					add(x.X)
				case *ssa.Call:
					if builtin, yes := x.Call.Value.(*ssa.Builtin); yes && builtin.Name() == "append" {
						add(x.Call.Args[0])
						if _, s := x.Call.Args[1].Type().Underlying().(*types.Slice); s {
							add(x.Call.Args[1])
						}
					} else {
						ok = false
					}
				default:
					ok = false
				}
				refs := v.Referrers()
				if refs == nil {
					ok = false
					continue
				}
				for _, u := range *refs {
					switch x := u.(type) {
					case *ssa.Phi:
						add(x)
					case *ssa.ChangeType:
						add(x)
					case *ssa.Slice:
						add(x)
					case *ssa.SliceToArrayPointer:
						add(x)
					case *ssa.Index: // A direct value read creates no alias.
					case *ssa.IndexAddr:
						addrs = append(addrs, x)
						for _, use := range *x.Referrers() {
							switch a := use.(type) {
							case *ssa.Store:
								if a.Addr != x || a.Val == x {
									ok = false
								}
							case *ssa.UnOp:
								if a.Op != token.MUL {
									ok = false
								}
							default:
								ok = false
							}
						}
					case *ssa.BinOp:
						if x.Op != token.EQL && x.Op != token.NEQ {
							ok = false
						}
					case *ssa.Call:
						b, yes := x.Call.Value.(*ssa.Builtin)
						if !yes {
							ok = false
							continue
						}
						switch b.Name() {
						case "len", "cap", "clear":
						case "append", "copy":
							add(x.Call.Args[0])
							if _, s := x.Call.Args[1].Type().Underlying().(*types.Slice); s {
								add(x.Call.Args[1])
							}
							if b.Name() == "append" {
								add(x)
							}
						default:
							ok = false
						}
					default:
						ok = false
					}
				}
			}
			if ok {
				if packed == nil {
					packed = make(map[ssa.Value]bool)
				}
				for _, v := range group {
					packed[v] = true
				}
				for _, v := range addrs {
					packed[v] = true
				}
			}
		}
	}
	return packed
}

func (f *lowerer) markPacked() { f.c.module.ExtendedOps[len(f.c.module.ExtendedOps)-1].Packed = true }

func (f *lowerer) packedIndex(addr *ssa.IndexAddr, next ssa.Instruction) bool {
	if !f.packed[addr] || len(*addr.Referrers()) != 1 {
		return false
	}
	// An array pointer carries no length. Its descriptor supplies the bound.
	if _, ok := addr.X.Type().Underlying().(*types.Slice); !ok {
		return false
	}
	s := f.shape(addr.Type().Underlying().(*types.Pointer).Elem())
	load, store := evm.OpLoadIndexWord, evm.OpStoreIndexWord
	if s.Type.Repr() == values.Pointer {
		load, store = evm.OpLoadIndexPtr, evm.OpStoreIndexPtr
	} else if s.Kind == values.BoolKind || s.Kind == values.UintKind && s.Bits == 8 {
		load, store = evm.OpLoadIndexByte, evm.OpStoreIndexByte
	}
	base, index := f.regs[addr.X][0].slot, f.regs[addr.Index][0].slot
	switch x := next.(type) {
	case *ssa.UnOp:
		if x.Op == token.MUL && x.X == addr {
			f.emit(evm.ABC(load, f.regs[x][0].slot, base, index))
			return true
		}
	case *ssa.Store:
		if x.Addr == addr {
			f.emit(evm.ABC(store, base, index, f.regs[x.Val][0].slot))
			return true
		}
	}
	return false
}
