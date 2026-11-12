package evm

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

var ErrOutput = errors.New("output limit exceeded")

type standardPanic struct{ message string }

func (e *standardPanic) Error() string { return e.message }

func standardComparable(c *Call) error {
	v := c.argument(0)
	id := c.program.typeShapes.Index(v)
	if id >= uint64(len(c.program.types)) {
		return ErrFunction
	}
	c.result(0, values.Bool(c.program.typeShapes.Comparable(id)))
	return nil
}
func standardFormat(c *Call) error {
	name := c.native.Name
	switch name {
	case "decodeRune":
		r, n := utf8.DecodeRuneInString(c.argument(0).AsString())
		c.result(0, values.Int(int64(r)))
		c.result(1, values.Int(int64(n)))
		return nil
	case "comparable":
		return standardComparable(c)
	case "checkSize":
		if c.argument(0).R > c.budget.limits.OutputBytes {
			return ErrOutput
		}
		return nil
	case "output":
		text := c.argument(0).AsString()
		if uint64(len(text)) > c.budget.limits.OutputBytes-c.budget.output {
			return ErrOutput
		}
		c.budget.output += uint64(len(text))
		sink := c.output
		if sink == nil {
			sink = defaultOutput
		}
		if err := sink(text); err != nil {
			return err
		}
		c.result(0, values.Int(int64(len(text))))
		c.result(1, values.Value{})
		return nil
	}
	shape, v, err := c.program.typeShapes.Unpack(c.argument(0))
	if err != nil {
		return err
	}
	switch name {
	case "describe":
		if shape == nil {
			c.result(0, values.Value{})
			c.result(1, values.String("<nil>"))
			c.result(2, values.Value{})
			c.result(3, values.Bool(true))
			return nil
		}
		n := uint64(0)
		switch shape.Kind {
		case values.ArrayKind:
			n = shape.Len
		case values.StructKind:
			n = uint64(len(shape.Fields))
		case values.SliceKind:
			n = uint64(v.SliceLen())
		case values.MapKind:
			n = uint64((*values.Map)(v.P).Len())
		}
		c.result(0, values.Uint(uint64(shape.Kind)))
		c.result(1, values.String(goTypeName(shape)))
		c.result(2, values.Uint(n))
		c.result(3, values.Bool(nilValue(shape, v)))
		return nil
	case "byteSlice":
		c.result(0, values.Bool(shape != nil && shape.Kind == values.SliceKind && shape.Elem.Kind == values.UintKind && shape.Elem.Bits == 8))
		return nil
	case "integer":
		ok := shape != nil && (shape.Kind == values.IntKind || shape.Kind == values.UintKind) && (shape.Kind == values.IntKind || v.R <= math.MaxInt64)
		c.result(0, values.Int(int64(v.R)))
		c.result(1, values.Bool(ok))
		return nil
	case "children":
		return c.formatChildren(shape, v)
	case "scalar":
		return c.formatScalar(shape, v)
	}
	return fmt.Errorf("unknown fmt primitive")
}
func nilValue(s *values.Shape, v values.Value) bool {
	switch s.Kind {
	case values.PointerKind, values.TypedPairKind, values.MapKind, values.SliceKind:
		return v.P == nil
	case values.FunctionKind:
		return v.R == 0
	}
	return false
}
func goTypeName(s *values.Shape) string {
	if s.DisplayName != "" {
		return s.DisplayName
	}
	switch s.Type {
	case values.Int64:
		return "int64"
	case values.Uint64:
		return "uint64"
	case values.Float64:
		return "float64"
	}
	return s.Type.Name()
}
func (c *Call) formatScalar(s *values.Shape, v values.Value) error {
	flags := c.argument(1).AsString()
	width, precision := int(c.argument(2).R), int(c.argument(3).R)
	hasWidth, hasPrecision := c.argument(4).R != 0, c.argument(5).R != 0
	verb := rune(c.argument(6).R)
	diagnostic := c.argument(7).R != 0
	content := verb != 'T' && !(verb == 'p' && s != nil && s.Kind == values.SliceKind)
	var arg any
	if s != nil && verb != 'T' {
		switch s.Kind {
		case values.BoolKind:
			arg = v.R != 0
		case values.IntKind:
			switch s.Bits {
			case 8:
				arg = int8(v.R)
			case 16:
				arg = int16(v.R)
			case 32:
				arg = int32(v.R)
			default:
				if s.Type == values.IntType {
					arg = int(v.R)
				} else {
					arg = int64(v.R)
				}
			}
		case values.UintKind:
			switch s.Bits {
			case 8:
				arg = uint8(v.R)
			case 16:
				arg = uint16(v.R)
			case 32:
				arg = uint32(v.R)
			default:
				if s.Type == values.UintType {
					arg = uint(v.R)
				} else if s.Type == values.UintptrType {
					arg = uintptr(v.R)
				} else {
					arg = v.R
				}
			}
		case values.FloatKind:
			if s.Bits == 32 {
				arg = math.Float32frombits(uint32(v.R))
			} else {
				arg = v.AsFloat()
			}
		case values.TimestampKind:
			arg = v.AsTime()
		case values.StringKind:
			arg = v.AsString()
		case values.ComplexKind:
			if s.Bits == 64 {
				arg = complex(math.Float32frombits(uint32(v.R)), math.Float32frombits(uint32(v.R>>32)))
			} else {
				arg = complex(s.Elem.Stored(v.P).AsFloat(), s.Elem.Stored(values.CellAt(v.P, 1)).AsFloat())
			}
		case values.PointerKind, values.TypedPairKind, values.MapKind:
			arg = v.P
		case values.FunctionKind:
			var ptr unsafe.Pointer
			if v.R != 0 {
				if v.R > uint64(len(c.program.functionRefs)) {
					return ErrFunction
				}
				ref := c.program.functionRefs[v.R-1]
				if ref.Native {
					ptr = unsafe.Pointer(&c.program.natives[ref.Index])
				} else {
					ptr = unsafe.Pointer(&c.program.functions[ref.Index])
				}
			}
			arg = ptr
		case values.SliceKind:
			if (verb == 's' || verb == 'q' || verb == 'x' || verb == 'X') && s.Elem.Kind == values.UintKind && s.Elem.Bits == 8 {
				if err := c.reserve(uint64(v.SliceLen())); err != nil {
					return err
				}
				data := make([]byte, v.SliceLen())
				for i := range data {
					data[i] = byte(s.Elem.Stored(values.CellAt(v.P, uint64(i)*s.Elem.Cells)).R)
				}
				arg = data
			} else {
				arg = v.P
			}
		}
	}
	inputBytes := uint64(0)
	if s != nil && content {
		if s.Kind == values.StringKind {
			inputBytes = v.R
		}
		if s.Kind == values.SliceKind {
			inputBytes = uint64(v.SliceLen())
		}
	}
	precisionCost := uint64(max(precision, 0))
	if s == nil || s.Kind == values.StringKind || s.Kind == values.SliceKind || verb == 'T' {
		precisionCost = min(precisionCost, inputBytes)
	}
	size := uint64(1024) + uint64(max(width, 0)) + precisionCost*2 + inputBytes*8
	if s != nil {
		size += uint64(len(goTypeName(s)))
	}
	if hasWidth && uint64(max(width, 0)) > c.budget.limits.OutputBytes {
		return ErrOutput
	}
	if err := c.reserve(size * 4); err != nil {
		return err
	}

	format := "%" + flags
	largeWidth := hasWidth && width > 1000000
	if !hasWidth {
		width = 0
	}
	if largeWidth {
		format += strconv.Itoa(width)
	} else {
		format += "*"
	}
	if hasPrecision {
		format += "." + strconv.Itoa(precision)
	}
	if largeWidth {
		format += "[1]"
	} else {
		format += "[2]"
	}
	render := func(verb rune, arg any) string {
		if largeWidth {
			return fmt.Sprintf(format+string(verb), arg)
		}
		return fmt.Sprintf(format+string(verb), width, arg)
	}

	if diagnostic && verb == 'v' && s != nil {
		switch s.Kind {
		case values.BoolKind:
			verb = 't'
		case values.IntKind, values.UintKind:
			verb = 'd'
		case values.FloatKind, values.ComplexKind:
			verb = 'g'
		case values.StringKind:
			verb = 's'
		case values.PointerKind, values.TypedPairKind, values.FunctionKind:
			if nilValue(s, v) {
				verb = 's'
				arg = "<nil>"
			} else {
				verb = 'p'
			}
		}
	}
	var text string
	if verb == 'T' && s == nil {
		text = render(verb, nil)
	} else if verb == 'T' {
		typename := "<nil>"
		if s != nil {
			typename = goTypeName(s)
		}
		text = render('s', typename)
	} else if s != nil && verb == 'v' && strings.Contains(flags, "#") && (s.Kind == values.PointerKind || s.Kind == values.TypedPairKind || s.Kind == values.FunctionKind || s.Kind == values.MapKind) {
		if nilValue(s, v) {
			text = "(" + goTypeName(s) + ")(nil)"
		} else {
			text = "(" + goTypeName(s) + ")(" + fmt.Sprintf("%p", arg) + ")"
		}
	} else {
		text = render(verb, arg)
	}
	if s != nil && strings.HasPrefix(text, "%!"+string(verb)+"(") {
		if start := len("%!" + string(verb)); start < len(text) {
			if end := strings.IndexByte(text[start:], '='); end >= 0 {
				text = text[:start+1] + goTypeName(s) + text[start+end:]
			}
		}
	}
	if uint64(len(text)) > c.budget.limits.OutputBytes {
		return ErrOutput
	}
	c.result(0, values.String(text))
	return nil
}
func (c *Call) packInterface(s *values.Shape, v values.Value) (values.Value, error) {
	if s.Kind == values.InterfaceKind {
		return v, nil
	}
	id, ok := c.program.typeIDs[s.Type]
	if !ok {
		return values.Value{}, fmt.Errorf("format child type %s is not linked", s.Type.Name())
	}
	v, err := snapshot(s, v, &c.budget.allocated, c.budget.limits)
	if err != nil {
		return values.Value{}, err
	}
	return c.program.packInterface(uint64(id), v, &c.budget.allocated, c.budget.limits)
}
func (c *Call) formatChildren(s *values.Shape, v values.Value) error {
	n := uint64(0)
	if s != nil {
		switch s.Kind {
		case values.StructKind:
			n = uint64(len(s.Fields))
		case values.ArrayKind:
			n = s.Len
		case values.SliceKind:
			n = uint64(v.SliceLen())
		case values.MapKind:
			n = uint64((*values.Map)(v.P).Len()) * 2
		case values.PointerKind:
			if v.P != nil && (s.Elem.Kind == values.StructKind || s.Elem.Kind == values.ArrayKind || s.Elem.Kind == values.SliceKind || s.Elem.Kind == values.MapKind) {
				n = 1
			}
		}
	}
	if n > math.MaxUint32 {
		return ErrAllocation
	}
	if err := c.reserve(n * 64); err != nil {
		return err
	}
	data := make([]values.Value, n)
	names := make([]values.Value, n)
	allowed := make([]values.Value, n)
	for i := range allowed {
		allowed[i] = values.Bool(true)
	}
	set := func(i int, shape *values.Shape, value values.Value) error {
		if i&255 == 255 {
			if err := c.budget.contextErr(); err != nil {
				return err
			}
		}
		packed, err := c.packInterface(shape, value)
		data[i] = packed
		return err
	}
	if n > 0 {
		switch s.Kind {
		case values.StructKind:
			for i := range s.Fields {
				f := &s.Fields[i]
				if err := set(i, f.Shape, f.Shape.Stored(values.CellAt(v.P, f.Offset))); err != nil {
					return err
				}
				names[i] = values.String(f.Name)
				r, _ := utf8.DecodeRuneInString(f.Name)
				allowed[i] = values.Bool(unicode.IsUpper(r))
			}
		case values.ArrayKind, values.SliceKind:
			for i := range data {
				if err := set(i, s.Elem, s.Elem.Stored(values.CellAt(v.P, uint64(i)*s.Elem.Cells))); err != nil {
					return err
				}
			}
		case values.PointerKind:
			var child values.Value
			if v.R == values.NativeAddress {
				var err error
				child, err = loadNative(s.Elem, v.P, &c.budget.allocated, c.budget.limits)
				if err != nil {
					return err
				}
			} else if v.R == 0 {
				child = s.Elem.Stored(v.P)
			} else {
				return ErrInvalidState
			}
			if err := set(0, s.Elem, child); err != nil {
				return err
			}
		case values.MapKind:
			m := (*values.Map)(v.P)
			entries := make([]*values.MapEntry, 0, m.Len())
			for _, e := range m.Entries() {
				if e.Live {
					entries = append(entries, e)
				}
			}
			var sortErr error
			comparisons := 0
			slices.SortStableFunc(entries, func(a, b *values.MapEntry) int {
				comparisons++
				if comparisons&255 == 0 && sortErr == nil {
					sortErr = c.budget.contextErr()
				}
				if sortErr != nil {
					return 0
				}
				var n int
				n, sortErr = compareFormatKey(c.Context, s.Key, a.Key, b.Key, c.program.typeShapes)
				return n
			})
			if sortErr != nil {
				return sortErr
			}
			for i, e := range entries {
				if err := set(i*2, s.Key, e.Key); err != nil {
					return err
				}
				if err := set(i*2+1, s.Elem, e.Value); err != nil {
					return err
				}
			}
		}
	}
	if err := c.sliceResult(0, data); err != nil {
		return err
	}
	if err := c.sliceResult(1, names); err != nil {
		return err
	}
	return c.sliceResult(2, allowed)
}
func compareFormatKey(ctx context.Context, s *values.Shape, a, b values.Value, ts *values.TypeTable) (int, error) {
	switch s.Kind {
	case values.IntKind:
		return cmp.Compare(int64(a.R), int64(b.R)), nil
	case values.FloatKind:
		if s.Bits == 32 {
			return cmp.Compare(math.Float32frombits(uint32(a.R)), math.Float32frombits(uint32(b.R))), nil
		}
		return cmp.Compare(a.AsFloat(), b.AsFloat()), nil
	case values.ComplexKind:
		var ar, ai, br, bi float64
		if s.Bits == 64 {
			ar = float64(math.Float32frombits(uint32(a.R)))
			ai = float64(math.Float32frombits(uint32(a.R >> 32)))
			br = float64(math.Float32frombits(uint32(b.R)))
			bi = float64(math.Float32frombits(uint32(b.R >> 32)))
		} else {
			ar = s.Elem.Stored(a.P).AsFloat()
			ai = s.Elem.Stored(values.CellAt(a.P, 1)).AsFloat()
			br = s.Elem.Stored(b.P).AsFloat()
			bi = s.Elem.Stored(values.CellAt(b.P, 1)).AsFloat()
		}
		if n := cmp.Compare(ar, br); n != 0 {
			return n, nil
		}
		return cmp.Compare(ai, bi), nil
	case values.TimestampKind:
		return a.AsTime().Compare(b.AsTime()), nil
	case values.StringKind:
		return strings.Compare(a.AsString(), b.AsString()), nil
	case values.PointerKind:
		return cmp.Compare(uintptr(a.P), uintptr(b.P)), nil
	case values.TypedPairKind:
		if n := cmp.Compare(uintptr(a.P), uintptr(b.P)); n != 0 {
			return n, nil
		}
		return cmp.Compare(a.R, b.R), nil
	case values.InterfaceKind:
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		aID, bID := ts.Index(a), ts.Index(b)
		if aID == 0 || bID == 0 {
			return cmp.Compare(aID, bID), nil
		}
		sa, va, err := ts.Unpack(a)
		if err != nil {
			return 0, err
		}
		sb, vb, err := ts.Unpack(b)
		if err != nil {
			return 0, err
		}
		if sa.Type != sb.Type {
			if n := strings.Compare(goTypeName(sa), goTypeName(sb)); n != 0 {
				return n, nil
			}
			return cmp.Compare(aID, bID), nil
		}
		return compareFormatKey(ctx, sa, va, vb, ts)
	case values.StructKind:
		for i := range s.Fields {
			f := &s.Fields[i]
			n, err := compareFormatKey(ctx, f.Shape, f.Shape.Stored(values.CellAt(a.P, f.Offset)), f.Shape.Stored(values.CellAt(b.P, f.Offset)), ts)
			if err != nil || n != 0 {
				return n, err
			}
		}
		return 0, nil
	case values.ArrayKind:
		if s.Elem.Cells == 0 {
			return 0, nil
		}
		for i := uint64(0); i < s.Len; i++ {
			if i&255 == 255 {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
			}
			n, err := compareFormatKey(ctx, s.Elem, s.Elem.Stored(values.CellAt(a.P, i*s.Elem.Cells)), s.Elem.Stored(values.CellAt(b.P, i*s.Elem.Cells)), ts)
			if err != nil || n != 0 {
				return n, err
			}
		}
		return 0, nil
	}
	return cmp.Compare(a.R, b.R), nil
}

func standardPrint(c *Call) error {
	g := c.native
	newline := g.Name == "println"
	newlineBytes := uint64(0)
	if newline {
		newlineBytes = 1
	}
	if err := c.reserve(32); err != nil {
		return err
	}
	// strings.Builder writes cannot fail.
	var text strings.Builder
	for i, t := range g.Signature.Params {
		s := c.program.shapeByType[t]
		v := c.argument(i)
		if s.Kind == values.InterfaceKind {
			var err error
			s, v, err = c.program.typeShapes.Unpack(v)
			if err != nil {
				return err
			}
		}
		if i > 0 && newline {
			_ = text.WriteByte(' ')
		}
		var part string
		if s == nil {
			part = "<nil>"
		} else {
			switch s.Kind {
			case values.StringKind:
				part = v.AsString()
			case values.IntKind:
				part = strconv.FormatInt(int64(v.R), 10)
			case values.UintKind:
				part = strconv.FormatUint(v.R, 10)
			case values.BoolKind:
				part = strconv.FormatBool(v.R != 0)
			case values.FloatKind:
				x := v.AsFloat()
				if s.Bits == 32 {
					x = float64(math.Float32frombits(uint32(v.R)))
				}
				part = strconv.FormatFloat(x, 'g', -1, int(s.Bits))
			default:
				part = fmt.Sprintf("%p", v.P)
			}
		}
		if uint64(text.Len())+uint64(len(part))+newlineBytes > c.budget.limits.OutputBytes {
			return ErrOutput
		}
		if err := c.reserve(uint64(len(part))*2 + 32); err != nil {
			return err
		}
		_, _ = text.WriteString(part)
	}
	if newline {
		_ = text.WriteByte('\n')
	}
	if uint64(text.Len()) > c.budget.limits.OutputBytes-c.budget.output {
		return ErrOutput
	}
	c.budget.output += uint64(text.Len())
	sink := c.output
	if sink == nil {
		sink = defaultOutput
	}
	return sink(text.String())
}
