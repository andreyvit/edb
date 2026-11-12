package evm

import (
	"embed"
	"fmt"
	"log"
	"strings"

	"github.com/andreyvit/edb/values"
)

// The virtual libraries contain only our small VM implementations. No Go
// standard-library source, init functions, or package state is loaded.
// _stdlib keeps these VM sources out of Go package discovery; the explicit
// embed pattern still includes them.
//
//go:generate go run ./internal/genstdlib
//go:embed _stdlib/*.go
var standardSources embed.FS

//go:embed evmruntime/log/log.go evmruntime/ambient.go
var runtimeSources embed.FS

// StandardLibrarySource supplies a virtual package to the compiler. Libraries
// have no globals; only reachable functions become part of a linked Program.
func StandardLibraryPaths() []string {
	return []string{"errors", "evmruntime", "evmruntime/log", "fmt", "log", "math", "strconv", "strings"}
}

func StandardLibrarySource(path string) (string, bool) {
	if path == "evmruntime" {
		primitive, _ := standardSources.ReadFile("_stdlib/evmruntime.go")
		source, _ := runtimeSources.ReadFile("evmruntime/ambient.go")
		return string(primitive) + "\n" + strings.TrimPrefix(string(source), "package evmruntime"), true
	}
	if path == "evmruntime/log" {
		b, err := runtimeSources.ReadFile("evmruntime/log/log.go")
		return string(b), err == nil
	}
	switch path {
	case "fmt", "log", "math", "strings", "strconv", "errors", "evmruntime":
		b, err := standardSources.ReadFile("_stdlib/" + path + ".go")
		return string(b), err == nil
	}
	return "", false
}

// BindStandardFunction binds a compiler-checked virtual library primitive.
// Signatures come from the virtual package, never from reflected Go functions.
func BindStandardFunction(path, name string, signature Signature) (*ExternalFunction, bool) {
	if path != "evm/debug" && !standardPrimitive(path, name) {
		return nil, false
	}
	var call Native
	switch path {
	case "evmruntime":
		switch name {
		case "IncreaseCPUBudget":
			call = func(c *Call) error { return c.IncreaseCPUBudget(c.Argument(0).R) }
		case "IncreaseMemBudget":
			call = func(c *Call) error { return c.IncreaseMemBudget(c.Argument(0).R, c.Argument(1).R) }
		}
	case "evm/debug":
		if name == "print" || name == "println" {
			call = standardPrint
		}
	case "math":
		call = standardMath
	case "strings":
		call = standardStrings
	case "strconv":
		call = standardStrconv
	case "fmt":
		switch name {
		case "scalar", "describe", "children", "checkSize", "output", "comparable", "integer", "byteSlice", "decodeRune":
			call = standardFormat
		}
	case "errors":
		if name == "comparable" {
			call = standardComparable
		}
	}
	if call == nil {
		return nil, false
	}
	return &ExternalFunction{Name: name, Signature: signature, Call: call, panics: path == "strings" && name == "Repeat" || path == "strconv" && (name == "FormatFloat" || name == "FormatInt" || name == "FormatUint")}, true
}

func (c *Call) argument(i int) values.Value {
	g := c.native
	slot, repr := int(g.params.Slots[i]), g.Signature.Params[i].Repr()
	var v values.Value
	if repr != values.Pointer {
		v.R = c.R[slot]
	}
	if repr != values.Raw {
		v.P = c.P[slot]
	}
	return v
}
func (c *Call) result(i int, v values.Value) {
	g := c.native
	slot, repr := int(g.results.Slots[i]), g.Signature.Results[i].Repr()
	if repr != values.Pointer {
		c.R[slot] = v.R
	}
	if repr != values.Raw {
		c.P[slot] = v.P
	}
}
func (c *Call) reserve(n uint64) error {
	return charge(n, &c.budget.allocated, c.budget.limits)
}
func (c *Call) sliceResult(i int, data []values.Value) error {
	p, err := allocCells(uint64(len(data)), &c.budget.allocated, c.budget.limits)
	if err != nil {
		return err
	}
	copy(values.CellSlice(p, uint64(len(data))), data)
	c.result(i, values.SliceValue(p, uint32(len(data)), uint32(len(data))))
	return nil
}
func defaultOutput(text string) error { log.Print(text); return nil }

func standardPrimitive(path, name string) bool { _, _, ok := standardSignature(path, name); return ok }

func validateStandardSignature(imp *Import, shapes map[*values.Shape]*values.Shape) error {
	if imp.Package == "evm/debug" {
		if len(imp.Signature.Results) != 0 {
			return fmt.Errorf("debug output cannot return values")
		}
		for _, t := range imp.Signature.Params {
			if standardShape(t, shapes) == nil {
				return fmt.Errorf("debug argument has no storage shape")
			}
		}
		return nil
	}
	params, results, ok := standardSignature(imp.Package, imp.Name)
	if !ok || len(params) != len(imp.Signature.Params) || len(results) != len(imp.Signature.Results) {
		return fmt.Errorf("invalid virtual function signature %s.%s", imp.Package, imp.Name)
	}
	for i, ts := range [][]*values.Type{imp.Signature.Params, imp.Signature.Results} {
		names := params
		if i == 1 {
			names = results
		}
		for j, t := range ts {
			if !matchesStandardType(names[j], t, shapes) {
				return fmt.Errorf("invalid virtual function type %s.%s", imp.Package, imp.Name)
			}
		}
	}
	return nil
}
func standardShape(t *values.Type, shapes map[*values.Shape]*values.Shape) *values.Shape {
	for _, s := range shapes {
		if s.Type == t {
			return s
		}
	}
	return nil
}
func matchesStandardType(name string, t *values.Type, shapes map[*values.Shape]*values.Shape) bool {
	var want *values.Type
	switch name {
	case "string":
		want = values.StringType
	case "bool":
		want = values.Boolean
	case "int":
		want = values.IntType
	case "int64":
		want = values.Int64
	case "uint64":
		want = values.Uint64
	case "uint32":
		want = values.Uint32
	case "rune":
		want = values.Int32
	case "byte":
		want = values.Uint8
	case "float32":
		want = values.Float32Type
	case "float64":
		want = values.Float64
	case "any":
		want = values.AnyType
	case "error":
		want = values.ErrorType
	default:
		if !strings.HasPrefix(name, "[]") {
			return false
		}
		s := standardShape(t, shapes)
		return s != nil && s.Kind == values.SliceKind && matchesStandardType(name[2:], s.Elem.Type, shapes)
	}
	return t == want
}
