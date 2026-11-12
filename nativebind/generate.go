package nativebind

import (
	"fmt"
	"io"
	"reflect"
)

// FieldSpec describes one exported native Go field to the shared generator.
// Type is its Go source spelling in the generated file's import environment.
type FieldSpec struct {
	Name, Type string
	Kind       reflect.Kind
}

// GenerateFields emits native operations using direct, typed Go expressions.
// Callers supply imports for nativebind, values, reflect, unsafe and field types.
// Both evmbind and record generators use this emitter.
func GenerateFields(w io.Writer, name, typ string, fields []FieldSpec) {
	line := func(f string, args ...any) { fmt.Fprintf(w, f+"\n", args...) }
	line("var %s = nativebind.GeneratedFields{Type:reflect.TypeFor[%s](), New:func()unsafe.Pointer{return unsafe.Pointer(new(%s))}, Fields:[]nativebind.GeneratedField{", name, typ, typ)
	for _, f := range fields {
		field := "(*(" + typ + "))(p)." + f.Name
		get, set := "", ""
		switch f.Kind {
		case reflect.Bool:
			get = "values.Bool(bool(" + field + "))"
			set = field + "=" + f.Type + "(v.R!=0)"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			get = "values.Int(int64(" + field + "))"
			set = field + "=" + f.Type + "(v.AsInt())"
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			get = "values.Uint(uint64(" + field + "))"
			set = field + "=" + f.Type + "(v.R)"
		case reflect.Float32:
			get = "values.Float32(float32(" + field + "))"
			set = field + "=" + f.Type + "(v.AsFloat32())"
		case reflect.Float64:
			get = "values.Float(float64(" + field + "))"
			set = field + "=" + f.Type + "(v.AsFloat())"
		case reflect.String:
			get = "values.String(string(" + field + "))"
			set = field + "=" + f.Type + "(v.AsString())"
		case reflect.Pointer:
			get = "values.Value{P:unsafe.Pointer(" + field + ")}"
			set = field + "=(" + f.Type + ")(v.P)"
		default:
			get = "values.Value{P:unsafe.Pointer(&" + field + ")}"
			set = "if v.P==nil {var zero " + f.Type + ";" + field + "=zero} else {" + field + "=*(*(" + f.Type + "))(v.P)}"
		}
		line("{Name:%q,Get:func(p unsafe.Pointer)values.Value{return %s},Set:func(p unsafe.Pointer,v values.Value){%s}},", f.Name, get, set)
	}
	line("}}")
}
