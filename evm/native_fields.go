package evm

import (
	"unsafe"

	"github.com/andreyvit/edb/values"
)

func nativeLoadedValue(s *values.Shape, v values.Value, used *allocationUsage, limits Limits) (values.Value, error) {
	if s.Kind == values.StructKind {
		return loadNative(s, v.P, used, limits)
	}
	if s.Kind == values.PointerKind && v.P != nil {
		v.R = values.NativeAddress
	}
	return v, nil
}

func nativeStoredValue(s *values.Shape, v values.Value, used *allocationUsage, limits Limits) (values.Value, error) {
	if s.Kind == values.StructKind {
		if s.Native == nil {
			return values.Value{}, ErrInvalidState
		}
		if err := charge(s.Native.Size, used, limits); err != nil {
			return values.Value{}, err
		}
		p := s.Native.New()
		if err := storeNative(s, p, v, used, limits); err != nil {
			return values.Value{}, err
		}
		return values.Value{P: p}, nil
	}
	if s.Kind == values.PointerKind && v.P != nil && v.R != values.NativeAddress {
		return values.Value{}, ErrInvalidState
	}
	return v, nil
}

func loadNative(s *values.Shape, p unsafe.Pointer, used *allocationUsage, limits Limits) (values.Value, error) {
	if s.Native == nil {
		return values.Value{}, ErrInvalidState
	}
	if s.Kind != values.StructKind {
		v := s.Native.Load(p)
		if s.Kind == values.PointerKind && v.P != nil {
			v.R = values.NativeAddress
		}
		return v, nil
	}
	dst, err := allocCells(s.Cells, used, limits)
	if err != nil {
		return values.Value{}, err
	}
	if len(s.Native.Fields) != len(s.Fields) {
		return values.Value{}, ErrInvalidState
	}
	for i, f := range s.Fields {
		v := s.Native.Fields[i].Get(p)
		if f.Shape.Kind == values.StructKind {
			v, err = loadNative(f.Shape, v.P, used, limits)
			if err != nil {
				return values.Value{}, err
			}
		} else if f.Shape.Kind == values.PointerKind && v.P != nil {
			v.R = values.NativeAddress
		}
		f.Shape.Store(values.CellAt(dst, f.Offset), v)
	}
	return values.Value{P: dst}, nil
}
func storeNative(s *values.Shape, p unsafe.Pointer, v values.Value, used *allocationUsage, limits Limits) error {
	if s.Native == nil {
		return ErrInvalidState
	}
	if s.Kind != values.StructKind {
		if s.Kind == values.PointerKind && v.P != nil && v.R != values.NativeAddress {
			return ErrInvalidState
		}
		s.Native.Store(p, v)
		return nil
	}
	if len(s.Native.Fields) != len(s.Fields) {
		return ErrInvalidState
	}
	for i, f := range s.Fields {
		x := f.Shape.Stored(values.CellAt(v.P, f.Offset))
		if f.Shape.Kind == values.StructKind {
			if f.Shape.Native == nil {
				return ErrInvalidState
			}
			if err := charge(f.Shape.Native.Size, used, limits); err != nil {
				return err
			}
			native := f.Shape.Native.New()
			if err := storeNative(f.Shape, native, x, used, limits); err != nil {
				return err
			}
			x = values.Value{P: native}
		} else if f.Shape.Kind == values.PointerKind && x.P != nil && x.R != values.NativeAddress {
			return ErrInvalidState
		}
		s.Native.Fields[i].Set(p, x)
	}
	return nil
}
