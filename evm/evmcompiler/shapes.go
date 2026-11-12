package evmcompiler

import (
	"fmt"
	"go/types"
	"math"
	"strconv"
	"strings"

	"github.com/andreyvit/edb/values"
)

func (e *typeEnvironment) shape(t types.Type) (result *values.Shape, err error) {
	t = types.Default(types.Unalias(t))
	if s, ok := e.shapes.At(t).(*values.Shape); ok {
		return s, nil
	}
	s := &values.Shape{Cells: 1, DisplayName: displayTypeName(t)}
	e.shapes.Set(t, s)
	e.shapeOrder = append(e.shapeOrder, s)
	defer func() {
		if err != nil {
			e.shapes.Delete(t)
		}
	}()
	vt, _ := e.fromGo.At(t).(*values.Type)
	if vt == values.TimeType {
		s.Kind = values.TimestampKind
		s.Type = vt
		return s, nil
	}
	repr := values.Raw
	switch u := t.Underlying().(type) {
	case *types.Basic:
		s.Bits = 64
		switch u.Kind() {
		case types.Bool:
			s.Kind = values.BoolKind
		case types.Int, types.Int8, types.Int16, types.Int32, types.Int64:
			s.Kind = values.IntKind
		case types.Uint, types.Uint8, types.Uint16, types.Uint32, types.Uint64, types.Uintptr:
			s.Kind = values.UintKind
		case types.Float32, types.Float64:
			s.Kind = values.FloatKind
		case types.String:
			s.Kind = values.StringKind
			repr = values.Pair
		case types.Complex64, types.Complex128:
			s.Kind = values.ComplexKind
			s.Bits = 64
			elem := types.Typ[types.Float32]
			if u.Kind() == types.Complex128 {
				s.Bits = 128
				s.Cells = 2
				repr = values.Pointer
				elem = types.Typ[types.Float64]
			}
			s.Elem, _ = e.shape(elem)
		default:
			return nil, fmt.Errorf("type %s is not implemented yet", t)
		}
		switch u.Kind() {
		case types.Int8, types.Uint8:
			s.Bits = 8
		case types.Int16, types.Uint16:
			s.Bits = 16
		case types.Int32, types.Uint32, types.Float32:
			s.Bits = 32
		}
	case *types.Interface:
		s.Kind = values.InterfaceKind
		repr = values.Pair
	case *types.Signature:
		s.Kind = values.FunctionKind
		repr = values.Pair
	case *types.Pointer:
		s.Kind = values.PointerKind
		repr = values.Pointer
		elem, _ := e.fromGo.At(u.Elem()).(*values.Type)
		if e.nativeLayouts[elem] != nil {
			repr = values.Pair
		}
		if vt != nil {
			repr = vt.Repr()
			if e.isReadRecord(vt) {
				s.Kind = values.TypedPairKind
				s.PointerLike = true
			}
		}
	case *types.Struct:
		s.Kind = values.StructKind
		repr = values.Pointer
	case *types.Array:
		s.Kind = values.ArrayKind
		repr = values.Pointer
		s.Len = uint64(u.Len())
	case *types.Slice:
		s.Kind = values.SliceKind
		repr = values.Pair
	case *types.Map:
		s.Kind = values.MapKind
		repr = values.Pointer
	default:
		return nil, fmt.Errorf("type %s is not implemented yet", t)
	}
	if vt != nil && vt.Repr() != repr {
		return nil, fmt.Errorf("native representation mismatch for %s", t)
	}
	if vt == nil {
		if s.Kind == values.StringKind {
			vt = values.NewOwnedType(types.TypeString(t, nil), repr, values.StringType.Own)
		} else {
			vt = values.NewType(types.TypeString(t, nil), repr)
		}
		e.fromGo.Set(t, vt)
	}
	s.Type = vt
	s.Native = e.nativeLayouts[vt]
	switch u := t.Underlying().(type) {
	case *types.Interface:
		for i := 0; i < u.NumMethods(); i++ {
			m := u.Method(i)
			ms, e2 := e.shape(withoutReceiver(m.Type().(*types.Signature)))
			if e2 != nil {
				return nil, e2
			}
			s.Methods = append(s.Methods, values.Method{ID: m.Id(), Signature: ms})
		}
	case *types.Signature:
		s.Variadic = u.Variadic()
		if u.Recv() != nil {
			r, e2 := e.shape(u.Recv().Type())
			if e2 != nil {
				return nil, e2
			}
			s.Params = append(s.Params, r)
		}
		for i := 0; i < u.Params().Len(); i++ {
			p, e2 := e.shape(u.Params().At(i).Type())
			if e2 != nil {
				return nil, e2
			}
			s.Params = append(s.Params, p)
		}
		for i := 0; i < u.Results().Len(); i++ {
			r, e2 := e.shape(u.Results().At(i).Type())
			if e2 != nil {
				return nil, e2
			}
			s.Results = append(s.Results, r)
		}
	case *types.Pointer:
		s.Elem, err = e.shape(u.Elem())
	case *types.Slice:
		s.Elem, err = e.shape(u.Elem())
	case *types.Map:
		s.Key, err = e.shape(u.Key())
		if err == nil {
			s.Elem, err = e.shape(u.Elem())
		}
	case *types.Struct:
		s.Cells = 0
		for i := 0; i < u.NumFields(); i++ {
			field := u.Field(i)
			fs, e2 := e.shape(field.Type())
			if e2 != nil {
				return nil, e2
			}
			s.Fields = append(s.Fields, values.Field{Name: field.Name(), Shape: fs, Offset: s.Cells, Embedded: field.Embedded(), Tag: u.Tag(i)})
			s.Cells += fs.Cells
			if s.Cells > math.MaxUint32 {
				return nil, fmt.Errorf("type %s exceeds storage limit", t)
			}
		}
	case *types.Array:
		s.Elem, err = e.shape(u.Elem())
		if err == nil {
			if s.Elem.Cells != 0 && s.Len > math.MaxUint32/s.Elem.Cells {
				return nil, fmt.Errorf("type %s exceeds storage limit", t)
			}
			s.Cells = s.Len * s.Elem.Cells
		}
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}

// Match the source type spelling used by Go's formatter. go/types uses compact
// struct/interface spelling; reflect (and therefore fmt) includes inner spaces.
func displayTypeName(t types.Type) string {
	t = types.Unalias(t)
	switch u := t.(type) {
	case *types.Basic:
		return types.Typ[u.Kind()].Name()
	case *types.Named:
		if u.Obj().Pkg() == nil {
			return u.Obj().Name()
		}
		name := u.Obj().Pkg().Name() + "." + u.Obj().Name()
		if u.TypeArgs().Len() > 0 {
			args := make([]string, u.TypeArgs().Len())
			for i := range args {
				args[i] = displayTypeName(u.TypeArgs().At(i))
			}
			name += "[" + strings.Join(args, ",") + "]"
		}
		return name
	case *types.Pointer:
		return "*" + displayTypeName(u.Elem())
	case *types.Slice:
		return "[]" + displayTypeName(u.Elem())
	case *types.Array:
		return fmt.Sprintf("[%d]%s", u.Len(), displayTypeName(u.Elem()))
	case *types.Map:
		return "map[" + displayTypeName(u.Key()) + "]" + displayTypeName(u.Elem())
	case *types.Struct:
		if u.NumFields() == 0 {
			return "struct {}"
		}
		fields := make([]string, u.NumFields())
		for i := range fields {
			f := u.Field(i)
			fields[i] = displayTypeName(f.Type())
			if !f.Embedded() {
				fields[i] = f.Name() + " " + fields[i]
			}
			if u.Tag(i) != "" {
				fields[i] += " " + strconv.Quote(u.Tag(i))
			}
		}
		return "struct { " + strings.Join(fields, "; ") + " }"
	case *types.Interface:
		if u.NumMethods() == 0 {
			return "interface {}"
		}
		methods := make([]string, u.NumMethods())
		for i := range methods {
			m := u.Method(i)
			methods[i] = m.Name() + strings.TrimPrefix(displayTypeName(m.Type()), "func")
		}
		return "interface { " + strings.Join(methods, "; ") + " }"
	case *types.Signature:
		args := make([]string, u.Params().Len())
		for i := range args {
			args[i] = displayTypeName(u.Params().At(i).Type())
			if u.Variadic() && i == len(args)-1 {
				args[i] = "..." + strings.TrimPrefix(args[i], "[]")
			}
		}
		text := "func(" + strings.Join(args, ", ") + ")"
		res := make([]string, u.Results().Len())
		for i := range res {
			res[i] = displayTypeName(u.Results().At(i).Type())
		}
		if len(res) == 1 {
			text += " " + res[0]
		} else if len(res) > 1 {
			text += " (" + strings.Join(res, ", ") + ")"
		}
		return text
	}
	return types.TypeString(t, nil)
}
