package evm

import (
	"bytes"
	"encoding"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

// JSONObjectType is shared by native packages that exchange JSON objects.
var JSONObjectType = values.NewType("map[string]interface{}", values.Pointer)

func (p *Program) checkJSONTypes() error {
	ids := p.jsonTypes
	if ids == (JSONTypes{}) {
		return nil
	}
	shape := func(id uint32) *values.Shape {
		if id == 0 || uint64(id) >= uint64(len(p.types)) {
			return nil
		}
		return p.types[id].shape
	}
	for _, part := range []struct {
		id  uint32
		typ *values.Type
	}{{ids.Boolean, values.Boolean}, {ids.Integer, values.Int64}, {ids.Unsigned, values.Uint64}, {ids.Float, values.Float64}, {ids.String, values.StringType}} {
		if s := shape(part.id); s == nil || s.Type != part.typ {
			return fmt.Errorf("invalid JSON type for %s", part.typ.Name())
		}
	}
	array, object := shape(ids.Array), shape(ids.Object)
	if array == nil || array.Kind != values.SliceKind || array.Elem.Type != values.AnyType || object == nil || object.Kind != values.MapKind || object.Key.Type != values.StringType || object.Elem.Type != values.AnyType {
		return fmt.Errorf("invalid JSON collection types")
	}
	return nil
}

func (cv recordConverter) decodeJSON(raw []byte, depth int) (values.Value, error) {
	if len(raw) == 0 {
		return values.Value{}, nil
	}
	if err := cv.charge(uint64(len(raw)) * 8); err != nil {
		return values.Value{}, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return values.Value{}, err
	}
	return cv.jsonValue(v, depth)
}

func (cv recordConverter) jsonValue(x any, depth int) (values.Value, error) {
	var v values.Value
	if depth > 128 {
		return v, fmt.Errorf("JSON nesting exceeds 128")
	}
	if err := cv.b.contextErr(); err != nil {
		return v, err
	}
	ids := cv.p.jsonTypes
	var id uint32
	switch x := x.(type) {
	case nil:
		return v, nil
	case bool:
		id, v = ids.Boolean, values.Bool(x)
	case string:
		if !utf8.ValidString(x) {
			if err := cv.charge(uint64(len(x)) * 3); err != nil {
				return v, err
			}
			x = strings.Map(func(r rune) rune { return r }, x)
		}
		id, v = ids.String, values.String(x)
	case json.Number:
		if n, err := x.Int64(); err == nil {
			id, v = ids.Integer, values.Int(n)
		} else if n, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			id, v = ids.Unsigned, values.Uint(n)
		} else {
			n, err := x.Float64()
			if err != nil {
				return v, err
			}
			id, v = ids.Float, values.Float(n)
		}
	case []any:
		if x == nil {
			return v, nil
		}
		p, err := allocCells(uint64(len(x)), &cv.b.allocated, cv.b.limits)
		if err != nil {
			return v, err
		}
		a := values.CellSlice(p, uint64(len(x)))
		for i, e := range x {
			a[i], err = cv.jsonValue(e, depth+1)
			if err != nil {
				return v, err
			}
		}
		id, v = ids.Array, values.SliceValue(p, uint32(len(x)), uint32(len(x)))
	case map[string]any:
		if x == nil {
			return v, nil
		}
		id = ids.Object
		if id == 0 || int(id) >= len(cv.p.types) || cv.p.types[id].shape.Kind != values.MapKind {
			return v, fmt.Errorf("unlinked JSON object type")
		}
		key := cv.p.types[id].shape.Key
		if err := cv.charge(64 + uint64(len(x))*128); err != nil {
			return v, err
		}
		m := values.NewMap(key, cv.p.typeShapes)
		for name, e := range x {
			value, err := cv.jsonValue(e, depth+1)
			if err != nil {
				return v, err
			}
			k := values.String(name)
			h, err := key.HashContext(cv.b.ctx, k, cv.p.typeShapes)
			if err != nil {
				return v, err
			}
			if err = m.PutContext(cv.b.ctx, k, value, h); err != nil {
				return v, err
			}
		}
		v.P = unsafe.Pointer(m)
	default:
		return cv.jsonScalar(x, depth)
	}
	if id == 0 || int(id) >= len(cv.p.types) {
		return values.Value{}, fmt.Errorf("unlinked JSON value type")
	}
	return cv.p.packInterface(uint64(id), v, &cv.b.allocated, cv.b.limits)
}

// JSON projections retain native scalar types so their JSON/text marshalers
// still run. Only those custom leaves need serialization, never whole records.
func (cv recordConverter) jsonScalar(x any, depth int) (values.Value, error) {
	switch x.(type) {
	case json.Marshaler, encoding.TextMarshaler:
		raw, err := json.Marshal(x)
		if err != nil {
			return values.Value{}, err
		}
		return cv.decodeJSON(raw, depth)
	}
	r := reflect.ValueOf(x)
	ids := cv.p.jsonTypes
	var id uint32
	var v values.Value
	switch r.Kind() {
	case reflect.Bool:
		id, v = ids.Boolean, values.Bool(r.Bool())
	case reflect.String:
		return cv.jsonValue(r.String(), depth)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		id, v = ids.Integer, values.Int(r.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n := r.Uint()
		if n <= math.MaxInt64 {
			id, v = ids.Integer, values.Int(int64(n))
		} else {
			id, v = ids.Unsigned, values.Uint(n)
		}
	case reflect.Float32, reflect.Float64:
		n, bits := r.Float(), r.Type().Bits()
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return values.Value{}, fmt.Errorf("JSON requires finite numbers")
		}
		// Match encoding/json's decimal/exponent choice, then the same number
		// classification used for actual JSON input. In particular 1.0 is int64.
		format := byte('f')
		abs := math.Abs(n)
		if bits == 32 {
			abs = float64(float32(abs))
		}
		if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
			format = 'e'
		}
		return cv.jsonValue(json.Number(strconv.FormatFloat(n, format, -1, bits)), depth)
	case reflect.Slice:
		if r.Type().Elem().Kind() != reflect.Uint8 {
			break
		}
		if r.IsNil() {
			return values.Value{}, nil
		}
		if err := cv.charge(uint64(base64.StdEncoding.EncodedLen(r.Len()))); err != nil {
			return values.Value{}, err
		}
		id, v = ids.String, values.String(base64.StdEncoding.EncodeToString(r.Bytes()))
	}
	if id == 0 || int(id) >= len(cv.p.types) {
		return values.Value{}, fmt.Errorf("unsupported or unlinked JSON scalar %T", x)
	}
	return cv.p.packInterface(uint64(id), v, &cv.b.allocated, cv.b.limits)
}

func (c *Call) recordJSON(typ *RecordType) error {
	var row kvo.Record
	if !typ.Mutable {
		var err error
		row, err = readRecordCell(c.Argument(0), typ.Model)
		if err != nil {
			return err
		}
	} else {
		scope := c.budget.recordScope()
		if scope == nil || scope.closed {
			return fmt.Errorf("KVO access outside record scope")
		}
		r := scope.lookup(c.Argument(0))
		if r == nil || r.model != typ.Model {
			return fmt.Errorf("invalid or expired %s record", typ.Name)
		}
		edit, path, err := r.editPath(nil, c.budget)
		if err != nil {
			return err
		}
		if err = c.FlushRecord(edit); err != nil {
			return err
		}
		row = kvo.Encoded(edit.Snapshot())
		for _, step := range path {
			row = row.Child(step)
		}
	}
	data, err := row.TryToJSON(typ.Model, kvo.JSONOptions{Context: c.Context, Charge: c.ChargeAllocation})
	if err != nil {
		return err
	}
	value, err := (recordConverter{c.program, c.budget}).jsonValue(data, 0)
	if err != nil {
		return err
	}
	c.SetResult(0, values.Value{P: value.P})
	return nil
}
