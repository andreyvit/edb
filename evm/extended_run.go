package evm

import (
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

var (
	ErrNil    = errors.New("nil pointer dereference")
	ErrBounds = errors.New("index or slice bounds out of range")
	ErrNilMap = errors.New("assignment to nil map")
)

type allocationUsage struct{ bytes, count uint64 }

// One reservation represents a logical allocation, independent of Go's allocator.
// Empty reservations (e.g. a program without globals) consume neither resource.
func charge(n uint64, used *allocationUsage, limits Limits) error {
	if n == 0 {
		return nil
	}
	if n > limits.AllocationBytes-used.bytes {
		return ErrAllocation
	}
	if used.count >= limits.Allocations {
		return ErrAllocations
	}
	used.bytes += n
	used.count++
	return nil
}
func allocCells(n uint64, allocated *allocationUsage, limits Limits) (unsafe.Pointer, error) {
	if n == math.MaxUint64 {
		return nil, ErrAllocation
	}
	n = max(n+1, 1)
	if n > uint64(math.MaxInt)/uint64(unsafe.Sizeof(values.Value{})) {
		return nil, ErrAllocation
	}
	if err := charge(n*uint64(unsafe.Sizeof(values.Value{})), allocated, limits); err != nil {
		return nil, err
	}
	data := make([]values.Value, int(n))
	return unsafe.Pointer(&data[0]), nil
}
func snapshot(s *values.Shape, v values.Value, allocated *allocationUsage, limits Limits) (values.Value, error) {
	if !s.Aggregate() || v.P == nil {
		return v, nil
	}
	p, err := allocCells(s.Cells, allocated, limits)
	if err != nil {
		return values.Value{}, err
	}
	s.Store(p, v)
	return values.Value{P: p}, nil
}
func (vm *VM) read(f frame, r Operand) values.Value {
	var v values.Value
	if r.Repr != values.Pointer {
		v.R = vm.r[f.rb+int(r.Slot)]
	}
	if r.Repr != values.Raw {
		v.P = vm.p[f.pb+int(r.Slot)]
	}
	return v
}
func (vm *VM) write(f frame, r Operand, v values.Value) {
	if r.Repr != values.Pointer {
		vm.r[f.rb+int(r.Slot)] = v.R
	}
	if r.Repr != values.Raw {
		vm.p[f.pb+int(r.Slot)] = v.P
	}
}

type iterator struct {
	text    string
	entries []*values.MapEntry
	index   int
}

func (vm *VM) extended(f frame, op *ExtendedOp, budget *executionBudget) error {
	allocated, limits := &budget.allocated, budget.limits
	get := func(i int) values.Value { return vm.read(f, op.In[i]) }
	put := func(i int, v values.Value) { vm.write(f, op.Out[i], v) }
	s := op.Shape
	var keyHash uint64
	switch op.Kind {
	case ExtendedMapUpdate, ExtendedLookup, ExtendedDelete:
		var err error
		keyHash, err = s.Key.HashContext(budget.ctx, get(1), vm.program.typeShapes)
		if err != nil {
			return err
		}
	}
	switch op.Kind {
	case ExtendedRecordCopy:
		v := recordCell(get(0))
		if v.P == nil {
			return fmt.Errorf("nil record dereference")
		}
		p, err := allocCells(1, allocated, limits)
		if err != nil {
			return err
		}
		*(*values.Value)(p) = v
		put(0, values.Value{P: p})
		return nil
	case ExtendedOwn:
		v, err := vm.program.ownValue(s, get(0), budget)
		if err != nil {
			return err
		}
		put(0, v)
		return nil
	case ExtendedMakeInterface:
		if s.Type.Repr() == values.Pointer {
			put(0, values.Value{R: op.Index, P: get(0).P})
			break
		}
		if s.Kind == values.FunctionKind {
			p, err := allocCells(1, allocated, limits)
			if err != nil {
				return err
			}
			*(*values.Value)(p) = get(0)
			put(0, values.Value{R: op.Index, P: p})
			break
		}
		v, err := vm.program.packInterface(op.Index, get(0), allocated, limits)
		if err != nil {
			return err
		}
		put(0, v)
	case ExtendedTypeAssert:
		v, ok, err := vm.program.assert(get(0), s, op.Index)
		if err != nil {
			return err
		}
		if !ok {
			v = values.Value{}
		}
		if len(op.Out) == 1 && !ok {
			return fmt.Errorf("%w to %s", ErrTypeAssertion, s.Type.Name())
		}
		put(0, v)
		if len(op.Out) == 2 {
			put(1, values.Bool(ok))
		}
	case ExtendedNativeAlloc:
		if err := charge(s.Native.Size, allocated, limits); err != nil {
			return err
		}
		put(0, values.Value{R: values.NativeAddress, P: s.Native.New()})
	case ExtendedAddressLoad:
		x := get(0)
		if x.P == nil {
			return ErrNil
		}
		var v values.Value
		var err error
		if x.R == values.NativeAddress {
			v, err = loadNative(s, x.P, allocated, limits)
		} else if x.R == 0 {
			v, err = snapshot(s, s.Stored(x.P), allocated, limits)
		} else {
			return ErrInvalidState
		}
		if err != nil {
			return err
		}
		put(0, v)
	case ExtendedAddressStore:
		x := get(0)
		if x.P == nil {
			return ErrNil
		}
		if x.R == values.NativeAddress {
			return storeNative(s, x.P, get(1), allocated, limits)
		}
		if x.R != 0 {
			return ErrInvalidState
		}
		s.Store(x.P, get(1))
	case ExtendedFieldAddr, ExtendedField, ExtendedFieldStore:
		x := get(0)
		index := op.Index &^ VMFieldBase
		field := s.Fields[index]
		var p unsafe.Pointer
		if x.R == values.NativeAddress {
			if x.P == nil {
				return ErrNil
			}
			if s.Native == nil || int(index) >= len(s.Native.Fields) {
				return ErrInvalidState
			}
			p = unsafe.Add(x.P, s.Native.Fields[index].Offset)
		} else if x.R == 0 {
			if x.P == nil {
				return ErrNil
			}
			p = values.CellAt(x.P, field.Offset)

		} else {
			return ErrInvalidState
		}
		if op.Kind == ExtendedFieldAddr {
			if x.P == nil {
				return ErrNil
			}
			put(0, values.Value{R: x.R, P: p})
		} else if op.Kind == ExtendedFieldStore {
			if x.R == values.NativeAddress {
				v, err := nativeStoredValue(field.Shape, get(1), allocated, limits)
				if err != nil {
					return err
				}
				s.Native.Fields[index].Set(x.P, v)
			} else {
				field.Shape.Store(p, get(1))
			}
		} else {
			var v values.Value
			var err error
			if x.R == values.NativeAddress {
				v, err = nativeLoadedValue(field.Shape, s.Native.Fields[index].Get(x.P), allocated, limits)
			} else {
				v, err = snapshot(field.Shape, field.Shape.Stored(p), allocated, limits)
			}
			if err != nil {
				return err
			}
			put(0, v)
		}
	case ExtendedPackedArray:
		p, err := storageFor(s.Elem, true).allocate(s.Len, s.Elem.Cells, allocated, limits)
		if err != nil {
			return err
		}
		put(0, values.Value{P: p})
	case ExtendedPackedLoad, ExtendedPackedStore:
		p := get(0).P
		if p == nil {
			return ErrNil
		}
		st := storageFor(s, true)
		if op.Kind == ExtendedPackedLoad {
			put(0, st.load(p))
		} else {
			st.store(p, get(1))
		}
	case ExtendedIndexAddr, ExtendedIndex:
		x, n := get(0), get(1).R
		if s.Kind == values.StringKind {
			if n >= x.R {
				return ErrBounds
			}
			put(0, values.Uint(uint64(x.AsString()[n])))
			break
		}
		length := s.Len
		elem := s.Elem
		if s.Kind == values.SliceKind {
			length = uint64(x.SliceLen())
		}
		if n >= length {
			return ErrBounds
		}
		if op.Kind == ExtendedIndexAddr && x.P == nil {
			return ErrNil
		}
		st := storageFor(elem, op.Packed)
		p := st.at(x.P, n, elem.Cells)
		if op.Kind == ExtendedIndexAddr {
			put(0, values.Value{P: p})
		} else {
			if op.Packed {
				put(0, st.load(p))
				break
			}
			v, err := snapshot(elem, elem.Stored(p), allocated, limits)
			if err != nil {
				return err
			}
			put(0, v)
		}
	case ExtendedMakeSlice:
		length, capacity := get(0).R, get(1).R
		if length > capacity || capacity > math.MaxUint32 {
			return ErrBounds
		}
		p, err := storageFor(s.Elem, op.Packed).allocate(capacity, s.Elem.Cells, allocated, limits)
		if err != nil {
			return err
		}
		put(0, values.SliceValue(p, uint32(length), uint32(capacity)))
	case ExtendedSlice:
		x := get(0)
		lo, hi, capBound := get(1).R, get(2).R, get(3).R
		length, capacity, stride := x.R, x.R, uint64(1)
		base := x.P
		if s.Kind == values.SliceKind {
			length, capacity, stride = uint64(x.SliceLen()), uint64(x.SliceCap()), s.Elem.Cells
		}
		if s.Kind == values.ArrayKind {
			length, capacity, stride = s.Len, s.Len, s.Elem.Cells
			if base == nil {
				return ErrNil
			}
		}
		if op.Flags&1 != 0 {
			hi = length
		}
		if op.Flags&2 != 0 {
			capBound = capacity
		}
		if lo > hi || hi > capBound || capBound > capacity {
			return ErrBounds
		}
		if s.Kind == values.StringKind {
			put(0, values.String(x.AsString()[lo:hi]))
			break
		}
		if capBound > math.MaxUint32 {
			return ErrBounds
		}
		// Avoid producing an out-of-allocation pointer for a zero-capacity tail.
		if capBound > lo {
			base = storageFor(s.Elem, op.Packed).at(base, lo, stride)
		}
		put(0, values.SliceValue(base, uint32(hi-lo), uint32(capBound-lo)))
	case ExtendedSliceToArray:
		x := get(0)
		if s.Elem.Len > uint64(x.SliceLen()) {
			return ErrBounds
		}
		put(0, values.Value{P: x.P})
	case ExtendedMakeMap:
		if err := charge(128, allocated, limits); err != nil {
			return err
		}
		put(0, values.Value{P: unsafe.Pointer(values.NewMap(s.Key, vm.program.typeShapes))})
	case ExtendedLookup:
		m := (*values.Map)(get(0).P)
		e, ok, err := m.FindContext(budget.ctx, get(1), keyHash)
		if err != nil {
			return err
		}
		v := values.Value{}
		if ok {
			v = e.Value
		}
		put(0, v)
		if len(op.Out) == 2 {
			put(1, values.Bool(ok))
		}
	case ExtendedMapUpdate:
		m := (*values.Map)(get(0).P)
		if m == nil {
			return ErrNilMap
		}
		k := get(1)
		if _, ok, err := m.FindContext(budget.ctx, k, keyHash); err != nil {
			return err
		} else if !ok {
			if err := charge(128, allocated, limits); err != nil {
				return err
			}
		}
		if err := m.PutContext(budget.ctx, k, get(2), keyHash); err != nil {
			return err
		}
	case ExtendedDelete:
		if err := (*values.Map)(get(0).P).DeleteContext(budget.ctx, get(1), keyHash); err != nil {
			return err
		}
	case ExtendedClear:
		x := get(0)
		if s.Kind == values.MapKind {
			(*values.Map)(x.P).Clear()
		} else {
			storageFor(s.Elem, op.Packed).clear(x.P, uint64(x.SliceLen()), s.Elem.Cells)
		}
	case ExtendedLen, ExtendedCap:
		x := get(0)
		var n uint64
		switch s.Kind {
		case values.StringKind:
			n = x.R
		case values.SliceKind:
			n = uint64(x.SliceLen())
			if op.Kind == ExtendedCap {
				n = uint64(x.SliceCap())
			}
		case values.MapKind:
			n = uint64((*values.Map)(x.P).Len())
		case values.ArrayKind:
			n = s.Len
		}
		put(0, values.Uint(n))
	case ExtendedAppend, ExtendedCopy:
		st := storageFor(s.Elem, op.Packed)
		dst, src := get(0), get(1)
		srcLen := uint64(src.SliceLen())
		if op.Flags&1 != 0 {
			srcLen = src.R
		}
		length, capacity := uint64(dst.SliceLen()), uint64(dst.SliceCap())
		start := uint64(0)
		n := min(length, srcLen)
		if op.Kind == ExtendedAppend {
			start = length
			n = srcLen
			if srcLen > math.MaxUint32-length {
				return ErrBounds
			}
			length += srcLen
			if length > capacity {
				capacity = min(uint64(math.MaxUint32), max(length, capacity*2))
				p, err := st.allocate(capacity, s.Elem.Cells, allocated, limits)
				if err != nil {
					return err
				}
				st.copy(p, dst.P, start, s.Elem.Cells)
				dst.P = p
			}
		}
		var target unsafe.Pointer
		if n > 0 {
			target = st.at(dst.P, start, s.Elem.Cells)
		}
		if op.Flags&1 != 0 && op.Packed {
			copy(unsafe.Slice((*byte)(target), n), src.AsString())
		} else if op.Flags&1 != 0 {
			text := src.AsString()
			for i := uint64(0); i < n; i++ {
				if i&255 == 255 {
					if err := budget.contextErr(); err != nil {
						return err
					}
				}
				s.Elem.Store(values.CellAt(target, i*s.Elem.Cells), values.Uint(uint64(text[i])))
			}
		} else {
			st.copy(target, src.P, n, s.Elem.Cells)
		}
		if op.Kind == ExtendedAppend {
			put(0, values.SliceValue(dst.P, uint32(length), uint32(capacity)))
		} else {
			put(0, values.Uint(n))
		}
	case ExtendedRange:
		if err := charge(64, allocated, limits); err != nil {
			return err
		}
		it := &iterator{}
		if s.Kind == values.StringKind {
			it.text = get(0).AsString()
		} else {
			it.entries = (*values.Map)(get(0).P).Entries()
		}
		put(0, values.Value{P: unsafe.Pointer(it)})
	case ExtendedNext:
		it := (*iterator)(get(0).P)
		put(0, values.Bool(false))
		if op.Flags&1 != 0 {
			if it.index < len(it.text) {
				r, n := utf8.DecodeRuneInString(it.text[it.index:])
				put(0, values.Bool(true))
				put(1, values.Int(int64(it.index)))
				put(2, values.Int(int64(r)))
				it.index += n
			}
		} else {
			for it.index < len(it.entries) {
				e := it.entries[it.index]
				if it.index&255 == 255 {
					if err := budget.contextErr(); err != nil {
						return err
					}
				}
				it.index++
				if e != nil && e.Live {
					put(0, values.Bool(true))
					put(1, e.Key)
					put(2, e.Value)
					break
				}
			}
		}
	case ExtendedEqual:
		eq, err := s.EqualContext(budget.ctx, get(0), get(1), vm.program.typeShapes)
		if err != nil {
			return err
		}
		put(0, values.Bool(eq))
	case ExtendedConvert:
		v, err := convertExtended(s, op.Source, get(0), budget)
		if err != nil {
			return err
		}
		put(0, v)
	case ExtendedMinMax:
		v := get(0)
		for i := 1; i < len(op.In); i++ {
			y := get(i)
			switch s.Kind {
			case values.StringKind:
				if op.Flags == 0 && y.AsString() < v.AsString() || op.Flags != 0 && y.AsString() > v.AsString() {
					v = y
				}
			case values.IntKind:
				if op.Flags == 0 && int64(y.R) < int64(v.R) || op.Flags != 0 && int64(y.R) > int64(v.R) {
					v = y
				}
			case values.UintKind:
				if op.Flags == 0 && y.R < v.R || op.Flags != 0 && y.R > v.R {
					v = y
				}
			case values.FloatKind:
				if s.Bits == 32 {
					a, b := math.Float32frombits(uint32(v.R)), math.Float32frombits(uint32(y.R))
					if op.Flags == 0 {
						v.R = uint64(math.Float32bits(min(a, b)))
					} else {
						v.R = uint64(math.Float32bits(max(a, b)))
					}
				} else {
					a, b := v.AsFloat(), y.AsFloat()
					if op.Flags == 0 {
						v = values.Float(min(a, b))
					} else {
						v = values.Float(max(a, b))
					}
				}
			}
		}
		put(0, v)
	case ExtendedComplex, ExtendedComplexBinary, ExtendedComplexNegate, ExtendedReal, ExtendedImag:
		var z complex128
		if op.Kind == ExtendedComplex {
			x, y := get(0), get(1)
			if s.Bits == 64 {
				z = complex(float64(math.Float32frombits(uint32(x.R))), float64(math.Float32frombits(uint32(y.R))))
			} else {
				z = complex(x.AsFloat(), y.AsFloat())
			}
		} else {
			z = complexValue(s, get(0))
		}
		switch op.Kind {
		case ExtendedComplexNegate:
			z = -z
		case ExtendedComplexBinary:
			other := complexValue(s, get(1))
			if s.Bits == 64 {
				a, b := complex64(z), complex64(other)
				switch op.Flags {
				case '+':
					z = complex128(a + b)
				case '-':
					z = complex128(a - b)
				case '*':
					z = complex128(a * b)
				case '/':
					z = complex128(a / b)
				}
			} else {
				switch op.Flags {
				case '+':
					z += other
				case '-':
					z -= other
				case '*':
					z *= other
				case '/':
					z /= other
				}
			}
		case ExtendedReal, ExtendedImag:
			x := real(z)
			if op.Kind == ExtendedImag {
				x = imag(z)
			}
			v := values.Float(x)
			if s.Bits == 64 {
				v.R = uint64(math.Float32bits(float32(x)))
			}
			put(0, v)
			return nil
		}
		v, err := makeComplex(s, z, allocated, limits)
		if err != nil {
			return err
		}
		put(0, v)
	}
	return nil
}
func complexValue(s *values.Shape, v values.Value) complex128 {
	if s.Bits == 64 {
		return complex(float64(math.Float32frombits(uint32(v.R))), float64(math.Float32frombits(uint32(v.R>>32))))
	}
	return complex(s.Elem.Stored(v.P).AsFloat(), s.Elem.Stored(values.CellAt(v.P, 1)).AsFloat())
}
func makeComplex(s *values.Shape, z complex128, a *allocationUsage, l Limits) (values.Value, error) {
	if s.Bits == 64 {
		return values.Uint(uint64(math.Float32bits(float32(real(z)))) | uint64(math.Float32bits(float32(imag(z))))<<32), nil
	}
	p, err := allocCells(2, a, l)
	if err != nil {
		return values.Value{}, err
	}
	s.Elem.Store(p, values.Float(real(z)))
	s.Elem.Store(values.CellAt(p, 1), values.Float(imag(z)))
	return values.Value{P: p}, nil
}
func convertExtended(to, from *values.Shape, x values.Value, budget *executionBudget) (values.Value, error) {
	a, l := &budget.allocated, budget.limits
	if to.Kind == values.ComplexKind && from.Kind == values.ComplexKind {
		return makeComplex(to, complexValue(from, x), a, l)
	}
	if to.Kind == values.StringKind {
		if from.Kind == values.IntKind || from.Kind == values.UintKind {
			r := rune(utf8.RuneError)
			if x.R <= utf8.MaxRune {
				r = rune(x.R)
			}
			if err := charge(4, a, l); err != nil {
				return values.Value{}, err
			}
			return values.String(string(r)), nil
		}
		n := uint64(x.SliceLen())
		if err := charge(n*8, a, l); err != nil {
			return values.Value{}, err
		}
		if from.Elem.Bits == 8 {
			b := make([]byte, n)
			for i := range b {
				if i&255 == 255 {
					if err := budget.contextErr(); err != nil {
						return values.Value{}, err
					}
				}
				b[i] = byte(from.Elem.Stored(values.CellAt(x.P, uint64(i)*from.Elem.Cells)).R)
			}
			return values.String(string(b)), nil
		}
		r := make([]rune, n)
		for i := range r {
			if i&255 == 255 {
				if err := budget.contextErr(); err != nil {
					return values.Value{}, err
				}
			}
			r[i] = rune(from.Elem.Stored(values.CellAt(x.P, uint64(i)*from.Elem.Cells)).R)
		}
		return values.String(string(r)), nil
	}
	if to.Kind == values.SliceKind && from.Kind == values.StringKind {
		text := x.AsString()
		n := len(text)
		if to.Elem.Bits != 8 {
			n = utf8.RuneCountInString(text)
		}
		if uint64(n) > math.MaxUint32 {
			return values.Value{}, ErrBounds
		}
		p, err := allocCells(uint64(n)*to.Elem.Cells, a, l)
		if err != nil {
			return values.Value{}, err
		}
		if to.Elem.Bits == 8 {
			for i, b := range []byte(text) {
				if i&255 == 255 {
					if err := budget.contextErr(); err != nil {
						return values.Value{}, err
					}
				}
				to.Elem.Store(values.CellAt(p, uint64(i)*to.Elem.Cells), values.Uint(uint64(b)))
			}
		} else {
			i := uint64(0)
			for _, r := range text {
				if i&255 == 255 {
					if err := budget.contextErr(); err != nil {
						return values.Value{}, err
					}
				}
				to.Elem.Store(values.CellAt(p, i*to.Elem.Cells), values.Int(int64(r)))
				i++
			}
		}
		return values.SliceValue(p, uint32(n), uint32(n)), nil
	}
	return values.Value{}, errors.New("invalid extended conversion")
}
