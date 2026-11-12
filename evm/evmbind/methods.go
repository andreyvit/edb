package evmbind

import (
	"fmt"
	"go/token"
	"reflect"
	"runtime"
	"slices"
	"strings"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

// MethodFlags declare trusted host effects. Omit them for conservative ownership.
type MethodFlags uint8

const (
	NoRetain MethodFlags = 1 << iota
	NoInvalidate
	// BorrowOnly promises that the helper neither retains borrowed arguments nor
	// invalidates their storage (including through callbacks or transaction calls).
	BorrowOnly = NoRetain | NoInvalidate
)

func (f MethodFlags) String() string {
	switch f {
	case 0:
		return "conservative"
	case NoRetain:
		return "no-retain"
	case NoInvalidate:
		return "no-invalidate"
	case BorrowOnly:
		return "borrow-only"
	default:
		return fmt.Sprintf("MethodFlags(%d)", uint8(f))
	}
}

// Method exposes an ordinary Go function as a method of a logical model.
// Registration does not depend on the record's storage representation.
type Method struct {
	model    *kvo.Model
	name     string
	function any
	flags    MethodFlags
}

// Model is the logical receiver. Environments exposing only part of a schema
// can use it to select applicable registrations before calling RegisterMethods.
func (m Method) Model() *kvo.Model { return m.model }

// RecordMethod declares an immutable record method. fn takes kvo.Record
// first, followed by scalar arguments, and returns zero or more scalars.
// Use GenerateMethods for reflection-free calls; no Go name prefix is stripped.
func RecordMethod(model *kvo.Model, name string, fn any, flags ...MethodFlags) Method {
	m := Method{model: model, name: name, function: fn}
	for _, f := range flags {
		m.flags |= f
	}
	return m
}

// StaticMethod is a generated adapter for one Go function. Several model
// methods may share it. Signature protects against stale generated output.
type StaticMethod struct {
	Signature string
	Call      evm.Native
	Direct    evm.DirectBinding
}
type StaticMethods map[string]StaticMethod

type MethodOptions struct {
	Mode   Mode
	Static StaticMethods
}

// RegisterMethods adds methods to the package owning their model receivers.
// The package is changed only after all registrations have been validated.
// Automatic falls back to reflection; StaticOnly rejects missing/stale adapters.
func RegisterMethods(pkg *evm.Package, methods []Method, opt MethodOptions) error {
	if pkg == nil {
		return fmt.Errorf("evmbind: missing method package")
	}
	if opt.Mode > StaticOnly {
		return fmt.Errorf("evmbind: invalid method binding mode")
	}
	added := make([]evm.ExternalMethod, 0, len(methods))
	for _, m := range methods {
		f, err := describeMethod(m)
		if err != nil {
			return err
		}
		receiver := pkg.Record(m.model, false)
		if receiver == nil {
			return fmt.Errorf("evmbind: %s does not declare model %s", pkg.Path, m.model.Name())
		}
		for _, old := range append(slices.Clone(pkg.Methods), added...) {
			if old.Receiver == receiver.Pointer && old.Name == m.name {
				return fmt.Errorf("evmbind: duplicate method %s.%s", receiver.Name, m.name)
			}
		}
		if m.model.PropBySelector(m.name) != nil {
			return fmt.Errorf("evmbind: method %s.%s conflicts with a property", receiver.Name, m.name)
		}
		adapter, found := opt.Static[f.symbol]
		valid := found && adapter.Signature == f.signature && adapter.Call != nil && adapter.Direct.Op == f.opcode() && (adapter.Direct.Op == 0 || adapter.Direct.Call != nil)
		var bound evm.ExternalMethod
		if valid {
			bound.Call, bound.Direct = adapter.Call, adapter.Direct
		} else if opt.Mode == StaticOnly {
			return fmt.Errorf("evmbind: missing or stale generated method %s", f.symbol)
		} else {
			bound.Call = f.reflected()
		}
		bound.Name, bound.Receiver = m.name, receiver.Pointer
		bound.Signature = f.sig
		bound.Ownership = &evm.OwnershipEffects{NoRetain: m.flags&NoRetain != 0, NoInvalidate: m.flags&NoInvalidate != 0}
		added = append(added, bound)
	}
	candidate := *pkg
	candidate.Methods = append(slices.Clone(pkg.Methods), added...)
	if err := evm.ValidateDirectBindings([]evm.Package{candidate}); err != nil {
		return err
	}
	pkg.Methods = candidate.Methods
	return nil
}

type methodFunction struct {
	fn                reflect.Value
	typ               reflect.Type
	symbol, signature string
	sig               evm.Signature // explicit arguments only; the receiver is a model-specific record
}

func describeMethod(m Method) (methodFunction, error) {
	var f methodFunction
	if m.model == nil || !token.IsIdentifier(m.name) || m.name == "_" {
		return f, fmt.Errorf("evmbind: invalid record method %q", m.name)
	}
	if m.flags & ^BorrowOnly != 0 {
		return f, fmt.Errorf("evmbind: invalid method effects %s", m.flags)
	}
	f.fn = reflect.ValueOf(m.function)
	if !f.fn.IsValid() || f.fn.Kind() != reflect.Func || f.fn.IsNil() {
		return f, fmt.Errorf("evmbind: %s requires a Go function", m.name)
	}
	f.typ = f.fn.Type()
	if f.typ.IsVariadic() || f.typ.NumIn() == 0 || f.typ.In(0) != reflect.TypeFor[kvo.Record]() {
		return f, fmt.Errorf("evmbind: %s must take kvo.Record first and cannot be variadic", m.name)
	}
	f.symbol = runtime.FuncForPC(f.fn.Pointer()).Name()
	var signature strings.Builder
	signature.WriteString("record-method-v1(")
	for i := 1; i < f.typ.NumIn(); i++ {
		t := f.typ.In(i)
		v := methodScalar(t)
		if v == nil {
			return f, fmt.Errorf("evmbind: %s argument %d: unsupported %s", m.name, i, t)
		}
		f.sig.Params = append(f.sig.Params, v)
		fmt.Fprintf(&signature, "%s;", methodTypeName(t))
	}
	signature.WriteString(")(")
	for i := 0; i < f.typ.NumOut(); i++ {
		t := f.typ.Out(i)
		v := methodScalar(t)
		if v == nil {
			return f, fmt.Errorf("evmbind: %s result %d: unsupported %s", m.name, i, t)
		}
		f.sig.Results = append(f.sig.Results, v)
		fmt.Fprintf(&signature, "%s;", methodTypeName(t))
	}
	signature.WriteByte(')')
	f.signature = signature.String()
	return f, nil
}
func methodTypeName(t reflect.Type) string {
	if t.PkgPath() != "" {
		return t.PkgPath() + "." + t.Name()
	}
	return t.String()
}
func methodScalar(t reflect.Type) *values.Type {
	switch t.Kind() {
	case reflect.Bool:
		return values.Boolean
	case reflect.Int:
		return values.IntType
	case reflect.Int8:
		return values.Int8
	case reflect.Int16:
		return values.Int16
	case reflect.Int32:
		return values.Int32
	case reflect.Int64:
		return values.Int64
	case reflect.Uint:
		return values.UintType
	case reflect.Uint8:
		return values.Uint8
	case reflect.Uint16:
		return values.Uint16
	case reflect.Uint32:
		return values.Uint32
	case reflect.Uint64:
		return values.Uint64
	case reflect.Float32:
		return values.Float32Type
	case reflect.Float64:
		return values.Float64
	case reflect.String:
		return values.StringType
	}
	return nil
}
func (f methodFunction) opcode() evm.Opcode {
	args, results := make([]values.Repr, len(f.sig.Params)), make([]values.Repr, len(f.sig.Results))
	for i, t := range f.sig.Params {
		args[i] = t.Repr()
	}
	for i, t := range f.sig.Results {
		results[i] = t.Repr()
	}
	return directOpcode(values.Pair, args, results)
}
func (f methodFunction) reflected() evm.Native {
	return func(c *evm.Call) (err error) {
		defer Catch(f.symbol, &err)
		args := make([]reflect.Value, f.typ.NumIn())
		args[0] = reflect.ValueOf(kvo.FromValue(c.Argument(0)))
		for i := 1; i < len(args); i++ {
			v := c.Argument(i)
			x := reflect.New(f.typ.In(i)).Elem()
			switch x.Kind() {
			case reflect.Bool:
				x.SetBool(v.AsBool())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				x.SetInt(v.AsInt())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				x.SetUint(v.AsUint())
			case reflect.Float32:
				x.SetFloat(float64(v.AsFloat32()))
			case reflect.Float64:
				x.SetFloat(v.AsFloat())
			case reflect.String:
				x.SetString(v.AsString())
			}
			args[i] = x
		}
		for i, x := range f.fn.Call(args) {
			var v values.Value
			switch x.Kind() {
			case reflect.Bool:
				v = values.Bool(x.Bool())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				v = values.Int(x.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				v = values.Uint(x.Uint())
			case reflect.Float32:
				v = values.Float32(float32(x.Float()))
			case reflect.Float64:
				v = values.Float(x.Float())
			case reflect.String:
				if err := c.ChargeAllocation(uint64(x.Len())); err != nil {
					return err
				}
				v = values.String(strings.Clone(x.String()))
			}
			c.SetResult(i, v)
		}
		return nil
	}
}
