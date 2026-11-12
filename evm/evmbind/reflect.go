package evmbind

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
)

func nativeType(n *node, reg map[string]reflect.Type, cache map[*node]reflect.Type) reflect.Type {
	if t := cache[n]; t != nil {
		return t
	}
	var t reflect.Type
	if n.named {
		t = reg[n.name]
	} else if n.kind == nodePointer {
		t = reflect.PointerTo(nativeType(n.elem, reg, cache))
	} else {
		t = reflectBasics[n.kind]
	}
	if t == nil {
		fail("missing native Go type %s", n.name)
	}
	cache[n] = t
	switch n.kind {
	case nodeStruct:
		if t.Kind() != reflect.Struct || t.NumField() != len(n.fields) {
			fail("native struct mismatch: %s", n.name)
		}
		for i, f := range n.fields {
			rf := t.Field(i)
			if rf.Name != f.name || rf.Type != nativeType(f.t, reg, cache) || string(rf.Tag) != n.shape.Fields[i].Tag {
				fail("native field mismatch: %s.%s", n.name, f.name)
			}
		}
	case nodeInterface:
		if t.Kind() != reflect.Interface || t.NumMethod() != len(n.methods) {
			fail("native interface mismatch: %s", n.name)
		}
	case nodePointer, nodeStrings, nodeJSONObject, nodeCallback:
	default:
		if t.Kind() != reflectBasics[n.kind].Kind() {
			fail("native scalar mismatch: %s", n.name)
		}
	}
	return t
}

var reflectBasics = map[nodeKind]reflect.Type{
	nodeCallback:   reflect.TypeFor[func()](),
	nodeStrings:    reflect.TypeFor[[]string](),
	nodeJSONObject: reflect.TypeFor[map[string]any](),
	nodeFloat32:    reflect.TypeFor[float32](),
	nodeFloat64:    reflect.TypeFor[float64](),
	nodeString:     reflect.TypeFor[string](),
	nodeBool:       reflect.TypeFor[bool](),
	nodeInt:        reflect.TypeFor[int](),
	nodeInt8:       reflect.TypeFor[int8](),
	nodeInt16:      reflect.TypeFor[int16](),
	nodeInt32:      reflect.TypeFor[int32](),
	nodeInt64:      reflect.TypeFor[int64](),
	nodeUint:       reflect.TypeFor[uint](),
	nodeUint8:      reflect.TypeFor[uint8](),
	nodeUint16:     reflect.TypeFor[uint16](),
	nodeUint32:     reflect.TypeFor[uint32](),
	nodeUint64:     reflect.TypeFor[uint64](),
}

func reflectWrapper(op *operation, host reflect.Type, ts map[*node]reflect.Type) Wrapper {
	t := host
	if op.recv != nil {
		t = ts[op.recv]
	}
	if t == nil {
		fail("missing host for %s", op.key)
	}
	m, ok := t.MethodByName(op.name)
	if !ok {
		fail("missing native method %s", op.key)
	}
	skip := 1
	if op.recv == nil {
		skip = 2
	}
	if t.Kind() == reflect.Interface {
		skip--
	}
	if m.Type.IsVariadic() || m.Type.NumIn() != skip+len(op.args) || m.Type.NumOut() != len(op.outs) {
		fail("native signature mismatch: %s", op.key)
	}
	if op.recv == nil && m.Type.In(skip-1) != reflect.TypeFor[*evm.Call]() {
		fail("%s must accept *evm.Call", op.key)
	}
	for i, n := range op.args {
		if m.Type.In(skip+i) != ts[n] {
			fail("native argument mismatch: %s", op.key)
		}
	}
	for i, n := range op.outs {
		if m.Type.Out(i) != ts[n] {
			fail("native result mismatch: %s", op.key)
		}
	}
	return func(f *Frame) {
		var args []reflect.Value
		fn := m.Func
		start := 0
		if op.recv == nil {
			args = append(args, reflect.ValueOf(f.Host), reflect.ValueOf(f.Call))
		} else {
			start = 1
			if op.recv.kind == nodeInterface {
				fn = reflect.ValueOf(f.Receiver).MethodByName(op.name)
			} else {
				args = append(args, decode(f, op.recv, f.Argument(0), ts))
			}
		}
		for i, n := range op.args {
			args = append(args, decode(f, n, f.Argument(start+i), ts))
		}
		for i, v := range fn.Call(args) {
			f.SetResult(i, encode(f, op.outs[i], v))
		}
	}
}
func decode(f *Frame, n *node, v values.Value, ts map[*node]reflect.Type) reflect.Value {
	t := ts[n]
	x := reflect.New(t).Elem()
	switch n.kind {
	case nodeStrings:
		return reflect.ValueOf(f.Strings(v))
	case nodeJSONObject:
		return reflect.ValueOf(f.JSONObject(v))
	case nodeCallback:
		return reflect.ValueOf(f.Callback(v))
	case nodeInterface:
		if a := f.DecodeInterface(v); a != nil {
			x.Set(reflect.ValueOf(a))
		}
	case nodePointer:
		if v.P != nil {
			if v.R == values.NativeAddress {
				return reflect.NewAt(t.Elem(), v.P)
			}
			if old := f.Object(n.name, v.P); old != nil {
				return reflect.ValueOf(old)
			}
			p := reflect.New(t.Elem())
			f.Remember(n.name, v.P, p.Interface(), func() { store(f, n.elem, v.P, p.Elem()) })
			p.Elem().Set(decode(f, n.elem, n.elem.shape.Stored(v.P), ts))
			return p
		}
	case nodeStruct:
		for i, field := range n.fields {
			x.Field(i).Set(decode(f, field.t, field.t.shape.Stored(values.CellAt(v.P, field.offset)), ts))
		}
	case nodeFloat32:
		x.SetFloat(float64(v.AsFloat32()))
	case nodeFloat64:
		x.SetFloat(v.AsFloat())
	case nodeString:
		x.SetString(v.AsString())
	case nodeBool:
		x.SetBool(v.R != 0)
	default:
		if n.shape.Kind == values.IntKind {
			x.SetInt(int64(v.R))
		} else {
			x.SetUint(v.R)
		}
	}
	return x
}
func store(f *Frame, n *node, p unsafe.Pointer, x reflect.Value) {
	if n.kind != nodeStruct {
		n.shape.Store(p, encode(f, n, x))
		return
	}
	for i, field := range n.fields {
		field.t.shape.Store(values.CellAt(p, field.offset), encode(f, field.t, x.Field(i)))
	}
}
func encode(f *Frame, n *node, x reflect.Value) values.Value {
	switch n.kind {
	case nodeInterface:
		return f.EncodeInterface(x.Interface())
	case nodePointer:
		if x.IsNil() {
			return values.Value{}
		}
		if p := f.Origin(x.Interface()); p != nil {
			return values.Value{P: p}
		}
		return values.Value{R: values.NativeAddress, P: x.UnsafePointer()}
	case nodeStruct:
		p := f.New(n.shape.Cells)
		store(f, n, p, x)
		return values.Value{P: p}
	case nodeFloat32:
		return values.Float32(float32(x.Float()))
	case nodeFloat64:
		return values.Float(x.Float())
	case nodeString:
		return f.String(x.String())
	case nodeBool:
		return values.Bool(x.Bool())
	default:
		if n.shape.Kind == values.IntKind {
			return values.Int(x.Int())
		}
		if n.shape.Kind == values.UintKind {
			return values.Uint(x.Uint())
		}
		panic(fmt.Errorf("unsupported %s", n.name))
	}
}
