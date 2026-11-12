package evm

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func recordShapeMatches(k kvo.AnyType, s *values.Shape) bool {
	if s == nil {
		return false
	}
	if typ := RecordScalarType(k); typ != nil {
		if s.Type == typ {
			return true
		}
		nativeType, ok := k.(interface{ NativeType() reflect.Type })
		if !ok {
			return false
		}
		native := nativeType.NativeType()
		switch native.Kind() {
		case reflect.String:
			return s.Kind == values.StringKind
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return s.Kind == values.UintKind && s.Bits == uint8(native.Size()*8)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return s.Kind == values.IntKind && s.Bits == uint8(native.Size()*8)
		}
		return false
	}
	switch t := k.(type) {
	case *kvo.EntityType:
		return s.Kind == values.PointerKind || s.Kind == values.TypedPairKind
	case *kvo.ScalarDataType:
		return t.NativeType().Kind() == reflect.Slice && s.Kind == values.SliceKind && s.Elem.Type == values.Uint8
	case *kvo.MapType:
		return s.Kind == values.MapKind && recordShapeMatches(t.MapKeyType(), s.Key) && recordShapeMatches(t.ItemType(), s.Elem)
	case *kvo.ListType:
		return s.Kind == values.SliceKind && recordShapeMatches(t.ItemType(), s.Elem)
	}
	return false
}

type recordFieldValue struct {
	path     []row2.Step
	s        *values.Shape
	k        kvo.AnyType
	value    values.Value
	baseline []byte
	assigned bool
}

// Collections have normal Go reference semantics inside an edit. A second
// selector returns the same map or slice. Put writes its current contents.
func (s *RecordScope) field(e *kvo.Editor, path []row2.Step) *recordFieldValue {
	for _, p := range s.fields[e] {
		if slices.Equal(p.path, path) {
			return p
		}
	}
	return nil
}

type recordConverter struct {
	p *Program
	b *executionBudget
}

func (vm *VM) recordField(f frame, op *ExtendedOp, r *borrowedRecord, mutable bool, b *executionBudget) error {
	k := op.Record.Model.MustPropByCode(op.Index).AnyType()
	cv := recordConverter{vm.program, b}
	var scope *RecordScope
	var path []row2.Step
	var edit *kvo.Editor
	var cached *recordFieldValue
	composite := op.Shape.Kind == values.MapKind || op.Shape.Kind == values.SliceKind || op.Shape.Kind == values.InterfaceKind
	var scratch [8]row2.Step
	if mutable {
		scope = b.recordScope()
		scratch[0] = op.step
		var err error
		edit, path, err = r.editPath(scratch[:1], b)
		if err != nil {
			return err
		}
		if composite {
			cached = scope.field(edit, path)
		}
	}
	var v values.Value
	if op.Kind == ExtendedSetKey {
		v = vm.read(f, op.In[1])
		if !composite {
			raw, err := cv.encode(k, op.Shape, v, 0)
			if err != nil {
				return err
			}
			return edit.TryFastSetPathValueWithWriter(path, raw, &scope.writer)
		}
	} else if cached != nil {
		vm.write(f, op.Out[0], cached.value)
		return nil
	} else {
		var raw values.Value
		if mutable {
			raw = edit.GetPathValue(path)
		} else {
			raw = r.read.GetValue(op.Index)
		}
		var err error
		if op.Shape.Kind == values.StringKind && (!mutable || edit.OwnsData()) {
			v = raw
		} else {
			v, err = cv.decode(k, op.Shape, raw, mutable, 0)
		}
		if err != nil {
			return err
		}
		vm.write(f, op.Out[0], v)
	}
	if composite && mutable {
		if cached == nil {
			if err := cv.charge(128 + uint64(len(path))*32); err != nil {
				return err
			}
			if scope.fields == nil {
				scope.fields = make(map[*kvo.Editor][]*recordFieldValue)
			}
			cached = &recordFieldValue{path: slices.Clone(path), s: op.Shape, k: k}
			scope.fields[edit] = append(scope.fields[edit], cached)
		}
		if op.Kind == ExtendedSetKey {
			cached.assigned = true
		} else if cached.baseline == nil {
			raw, err := cv.encode(k, op.Shape, v, 0)
			if err != nil {
				return err
			}
			cached.baseline = raw.AsBytes()
		}
		cached.value = v
	}
	return nil
}

func (cv recordConverter) charge(n uint64) error { return charge(n, &cv.b.allocated, cv.b.limits) }

// FlushRecord applies edits made through Go map and slice aliases before the
// external database saves this one row. Repeated saves see subsequent edits.
func (c *Call) FlushRecord(editor *kvo.Editor) error {
	return (recordConverter{c.program, c.budget}).flush(editor, nil, 0)
}
func (cv recordConverter) flush(e *kvo.Editor, prefix []row2.Step, depth int) error {
	if depth > 128 {
		return errors.New("cyclic or deeply nested KVO record")
	}
	scope := cv.b.recordScope()
	for _, p := range scope.fields[e] {
		if len(p.path) < len(prefix) || !slices.Equal(p.path[:len(prefix)], prefix) {
			continue
		}
		raw, err := cv.encode(p.k, p.s, p.value, depth+1)
		if err != nil {
			return err
		}
		if p.assigned || !bytes.Equal(raw.AsBytes(), p.baseline) {
			if err := e.TryFastSetPathValueWithWriter(p.path, raw, &scope.writer); err != nil {
				return err
			}
			p.baseline, p.assigned = raw.AsBytes(), false
		}
	}
	return nil
}

func (cv recordConverter) decode(k kvo.AnyType, s *values.Shape, x values.Value, mutable bool, depth int) (values.Value, error) {
	var zero values.Value
	if depth > 128 {
		return zero, errors.New("KVO nesting exceeds 128")
	}
	if err := cv.b.contextErr(); err != nil {
		return zero, err
	}
	switch t := k.(type) {
	case *kvo.WordType:
		return x, nil
	case *kvo.JSONType:
		return cv.decodeJSON(x.AsBytes(), depth)
	case *kvo.ScalarDataType:
		if t == kvo.TTimestamp {
			return x, nil
		}
		data := x.AsBytes()
		if s.Kind == values.StringKind {
			if err := cv.charge(uint64(len(data))); err != nil {
				return zero, err
			}
			return values.String(string(data)), nil
		}
		if data == nil {
			return zero, nil
		}
		p, err := allocCells(uint64(len(data)), &cv.b.allocated, cv.b.limits)
		if err != nil {
			return zero, err
		}
		for i, n := range data {
			values.CellSlice(p, uint64(len(data)))[i] = values.Uint(uint64(n))
		}
		return values.SliceValue(p, uint32(len(data)), uint32(len(data))), nil
	case *kvo.EntityType:
		view := row2.Trusted(x.AsBytes())
		if view.Missing() {
			return zero, nil
		}
		if mutable {
			scope := cv.b.recordScope()
			if err := cv.charge(recordHandleBytes); err != nil {
				return zero, err
			}
			return scope.Edit(t.Model(), row2.Edit(view)), nil
		}
		// Materialized collections own their leaves before crossing any scope.
		return kvo.Type(t.Model()).Own(kvo.Encoded(view).WithModel(t.Model()).Value(), cv.b)
	}
	view := row2.Trusted(x.AsBytes())
	if view.Missing() {
		return zero, nil
	}
	n := uint64(view.Len())
	if n > math.MaxUint32 {
		return zero, ErrAllocation
	}
	it := view.Iter()
	if s.Kind == values.SliceKind {
		p, err := allocCells(n, &cv.b.allocated, cv.b.limits)
		if err != nil {
			return zero, err
		}
		for {
			i, _, item, ok := it.Next()
			if !ok {
				break
			}
			v, err := cv.decode(k.ItemType(), s.Elem, item.Decode(kvo.Encoding(k.ItemType())), mutable, depth+1)
			if err != nil {
				return zero, err
			}
			s.Elem.Store(values.CellAt(p, i), v)
		}
		return values.SliceValue(p, uint32(n), uint32(n)), nil
	}
	if err := cv.charge(64 + n*128); err != nil {
		return zero, err
	}
	m := values.NewMap(s.Key, cv.p.typeShapes)
	for {
		num, text, item, ok := it.Next()
		if !ok {
			break
		}
		key := values.Uint(num)
		if s.Key.Kind == values.StringKind {
			if err := cv.charge(uint64(len(text))); err != nil {
				return zero, err
			}
			key = values.String(string(text))
		}
		v, err := cv.decode(k.ItemType(), s.Elem, item.Decode(kvo.Encoding(k.ItemType())), mutable, depth+1)
		if err != nil {
			return zero, err
		}
		h, err := s.Key.HashContext(cv.b.ctx, key, cv.p.typeShapes)
		if err != nil {
			return zero, err
		}
		if err = m.PutContext(cv.b.ctx, key, v, h); err != nil {
			return zero, err
		}
	}
	return values.Value{P: unsafe.Pointer(m)}, nil
}

func (cv recordConverter) encode(k kvo.AnyType, s *values.Shape, v values.Value, depth int) (values.Value, error) {
	var zero values.Value
	if depth > 128 {
		return zero, errors.New("cyclic or deeply nested KVO field")
	}
	if err := cv.b.contextErr(); err != nil {
		return zero, err
	}
	switch t := k.(type) {
	case *kvo.WordType:
		return v, nil
	case *kvo.JSONType:
		limit := min(cv.b.limits.AllocationBytes-cv.b.allocated.bytes, math.MaxInt)
		raw, err := cv.p.EncodeJSON(cv.b.ctx, v, int(limit))
		if err != nil {
			return zero, err
		}
		if err := cv.charge(uint64(len(raw))); err != nil {
			return zero, err
		}
		return values.Bytes(raw), nil
	case *kvo.ScalarDataType:
		if t == kvo.TTimestamp {
			v.AsTime()
			return v, nil
		}
		if s.Kind == values.StringKind {
			return v, nil
		}
		if v.P == nil {
			return zero, nil
		}
		if err := cv.charge(uint64(v.SliceLen())); err != nil {
			return zero, err
		}
		data := make([]byte, v.SliceLen())
		for i := range data {
			data[i] = byte(s.Elem.Stored(values.CellAt(v.P, uint64(i))).R)
		}
		return values.Bytes(data), nil
	case *kvo.EntityType:
		if v.P == nil {
			return zero, nil
		}
		if s.Kind == values.TypedPairKind {
			r, err := readRecordCell(v, t.Model())
			if err != nil {
				return zero, err
			}
			return values.Bytes(r.Snapshot().Raw()), nil
		}
		row := cv.b.recordScope().lookup(v)
		if row == nil || row.model != t.Model() {
			return zero, fmt.Errorf("invalid or expired %s record", t.Name())
		}
		if row.edit == nil && row.parent == nil {
			return values.Bytes(row.read.Snapshot().Raw()), nil
		}
		var scratch [8]row2.Step
		edit, path, err := row.editPath(scratch[:0], cv.b)
		if err != nil {
			return zero, err
		}
		if err = cv.flush(edit, path, depth+1); err != nil {
			return zero, err
		}
		if len(path) == 0 {
			return values.Bytes(edit.Snapshot().Raw()), nil
		}
		return edit.GetPathValue(path), nil
	}
	if v.P == nil {
		return zero, nil
	}
	var w row2.Writer
	if s.Kind == values.SliceKind {
		w.Begin(0, "", row2.List, row2.Compact2)
		for i := range v.SliceLen() {
			x, err := cv.encode(k.ItemType(), s.Elem, s.Elem.Stored(values.CellAt(v.P, uint64(i))), depth+1)
			if err != nil {
				return zero, err
			}
			if err := cv.charge(16 + row2.PayloadSize(x)); err != nil {
				return zero, err
			}
			w.PutPresent(0, "", x, kvo.Encoding(k.ItemType()))
		}
	} else {
		m := (*values.Map)(v.P)
		if err := cv.charge(64 + uint64(m.Len())*32); err != nil {
			return zero, err
		}
		items := make([]*values.MapEntry, 0, m.Len())
		for _, a := range m.Entries() {
			if a.Live {
				items = append(items, a)
			}
		}
		if s.Key.Kind == values.StringKind {
			w.Begin(0, "", row2.Dictionary, row2.Compact2)
			slices.SortFunc(items, func(a, b *values.MapEntry) int { return strings.Compare(a.Key.AsString(), b.Key.AsString()) })
		} else {
			w.Begin(0, "", row2.Object, row2.Compact2)
			slices.SortFunc(items, func(a, b *values.MapEntry) int { return cmp.Compare(a.Key.R, b.Key.R) })
		}
		for _, a := range items {
			x, err := cv.encode(k.ItemType(), s.Elem, a.Value, depth+1)
			if err != nil {
				return zero, err
			}
			n, text := a.Key.R, ""
			if s.Key.Kind == values.StringKind {
				n, text = 0, a.Key.AsString()
			}
			if err := cv.charge(16 + uint64(len(text)) + row2.PayloadSize(x)); err != nil {
				return zero, err
			}
			w.PutPresent(n, text, x, kvo.Encoding(k.ItemType()))
		}
	}
	w.End()
	return values.Bytes(w.BytesWritten()), nil
}
