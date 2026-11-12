package evmbind

import (
	"reflect"
	"strings"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
)

// directOpcode classifies the complete call once. Pointer receivers use method
// instructions; cell receivers use the ordinary first argument. A zero bank in
// the arguments/results means an unsupported conversion, not a void value.
func directOpcode(receiver values.Repr, args, results []values.Repr) evm.Opcode {
	if len(results) > 1 {
		return 0
	}
	for _, bank := range args {
		if bank == 0 {
			return 0
		}
	}
	var result values.Repr
	if len(results) == 1 {
		result = results[0]
		if result == 0 {
			return 0
		}
	}
	if receiver == values.Pair {
		args = append([]values.Repr{receiver}, args...)
	} else if receiver != 0 && receiver != values.Pointer {
		return 0
	}
	return evm.HostCallOpcode(receiver == values.Pointer, args, result)
}

func directBank(s *values.Shape) values.Repr {
	switch s.Kind {
	case values.PointerKind:
		return values.Pointer
	case values.StringKind:
		return values.Pair
	case values.IntKind, values.UintKind, values.BoolKind, values.FloatKind:
		return values.Raw
	default:
		return 0
	}
}

func scalarRead(kind reflect.Kind, typ, value string) string {
	accessor := "AsUint"
	switch kind {
	case reflect.Bool:
		accessor = "AsBool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		accessor = "AsInt"
	case reflect.Float32:
		accessor = "AsFloat32"
	case reflect.Float64:
		accessor = "AsFloat"
	case reflect.String:
		accessor = "AsString"
	}
	return typ + "(" + value + "." + accessor + "())"
}

// String ownership is handled by the caller (Frame.String, or the record
// method's charged clone). This primitive only packs the scalar into a cell.
func scalarWrite(kind reflect.Kind, value string) string {
	constructor, cast := "Uint", "uint64"
	switch kind {
	case reflect.Bool:
		constructor, cast = "Bool", "bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		constructor, cast = "Int", "int64"
	case reflect.Float32:
		constructor, cast = "Float32", "float32"
	case reflect.Float64:
		constructor, cast = "Float", "float64"
	case reflect.String:
		constructor, cast = "String", "string"
	}
	return "values." + constructor + "(" + cast + "(" + value + "))"
}

func writeInvocation(line func(string, ...any), target string, args, results []string) {
	assign := ""
	if len(results) > 0 {
		assign = strings.Join(results, ",") + ":="
	}
	line("%s%s(%s)", assign, target, strings.Join(args, ","))
}
