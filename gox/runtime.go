package gox

import (
	"fmt"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	ht "github.com/andreyvit/edb/gox/internal/htmltemplate"
	"github.com/andreyvit/edb/values"
)

var builderShape = &values.Shape{Type: values.NewType("gox.Builder", values.Pointer), Kind: values.StructKind, Cells: 0}
var builderPointer = &values.Shape{Type: values.NewType("*gox.Builder", values.Pointer), Kind: values.PointerKind, Cells: 1, Elem: builderShape}
var trustedTypes = map[string]*values.Type{}

func init() {
	for _, name := range []string{"HTML", "CSS", "JS", "JSStr", "URL", "Srcset"} {
		trustedTypes[name] = values.NewType("gox."+name, values.Pair)
	}
}
func runtimePackage() evm.Package {
	p := evm.Package{Path: "gox", Types: []evm.TypeDecl{{Name: "Builder", Shape: builderShape}, {Shape: builderPointer}}}
	for _, name := range []string{"HTML", "CSS", "JS", "JSStr", "URL", "Srcset"} {
		p.Types = append(p.Types, evm.TypeDecl{Name: name, Shape: &values.Shape{Type: trustedTypes[name], Kind: values.StringKind, Cells: 1}})
	}
	add := func(name string, params []*values.Type, fn func(*Builder, *evm.Call) error) {
		p.Functions = append(p.Functions, evm.ExternalFunction{Name: name, Signature: evm.Signature{Params: append([]*values.Type{builderPointer.Type}, params...)}, Call: func(c *evm.Call) error {
			b, ok := c.Context.Value(builderKey{}).(*Builder)
			if !ok || c.Argument(0).P != unsafe.Pointer(b) {
				return fmt.Errorf("invalid rendering context")
			}
			return fn(b, c)
		}})
	}
	add("Step", []*values.Type{values.IntType}, func(b *Builder, c *evm.Call) error { return b.step(int(c.Argument(1).AsInt())) })
	add("Value", []*values.Type{values.IntType, values.AnyType}, func(b *Builder, c *evm.Call) error {
		v, err := nativeValue(c, 2)
		if err != nil {
			return err
		}
		return b.value(int(c.Argument(1).AsInt()), v)
	})
	add("String", []*values.Type{values.IntType, values.StringType}, func(b *Builder, c *evm.Call) error {
		return b.stringValue(int(c.Argument(1).AsInt()), c.Argument(2).AsString())
	})
	add("Int", []*values.Type{values.IntType, values.IntType}, func(b *Builder, c *evm.Call) error { return b.value(int(c.Argument(1).AsInt()), c.Argument(2).AsInt()) })
	add("Bool", []*values.Type{values.IntType, values.Boolean}, func(b *Builder, c *evm.Call) error { return b.value(int(c.Argument(1).AsInt()), c.Argument(2).R != 0) })
	add("Class", []*values.Type{values.StringType}, func(b *Builder, c *evm.Call) error { return b.Class(c.Argument(1).AsString()) })
	add("Attr", []*values.Type{values.StringType, values.AnyType}, func(b *Builder, c *evm.Call) error {
		v, err := nativeValue(c, 2)
		if err != nil {
			return err
		}
		return b.Attr(c.Argument(1).AsString(), v)
	})
	add("Text", []*values.Type{values.StringType}, func(b *Builder, c *evm.Call) error { return b.Text(c.Argument(1).AsString()) })
	for _, name := range []string{"Class", "Attr", "Text"} {
		for _, f := range p.Functions {
			if f.Name == name {
				params := append([]*values.Type(nil), f.Signature.Params[1:]...)
				method := evm.ExternalMethod{Name: name, Receiver: builderPointer.Type, Signature: evm.Signature{Params: params}}
				method.Call = func(c *evm.Call) error {
					b, ok := c.Context.Value(builderKey{}).(*Builder)
					if !ok || c.Argument(0).P != unsafe.Pointer(b) {
						return fmt.Errorf("invalid rendering context")
					}
					switch name {
					case "Class":
						return b.Class(c.Argument(1).AsString())
					case "Text":
						return b.Text(c.Argument(1).AsString())
					default:
						v, err := nativeValue(c, 2)
						if err != nil {
							return err
						}
						return b.Attr(c.Argument(1).AsString(), v)
					}
				}
				p.Methods = append(p.Methods, method)
			}
		}
	}
	p.Methods = append(p.Methods, evm.ExternalMethod{Name: "HTML", Receiver: builderPointer.Type, Signature: evm.Signature{Params: []*values.Type{trustedTypes["HTML"]}}, Call: func(c *evm.Call) error {
		b, ok := c.Context.Value(builderKey{}).(*Builder)
		if !ok || c.Argument(0).P != unsafe.Pointer(b) {
			return fmt.Errorf("invalid rendering context")
		}
		return b.HTML(HTML(c.Argument(1).AsString()))
	}})
	p.Methods = append(p.Methods,
		evm.ExternalMethod{Name: "Open", Receiver: builderPointer.Type, Signature: evm.Signature{Params: []*values.Type{values.StringType}}, Call: func(c *evm.Call) error {
			b, ok := c.Context.Value(builderKey{}).(*Builder)
			if !ok || c.Argument(0).P != unsafe.Pointer(b) {
				return fmt.Errorf("invalid rendering context")
			}
			return b.Open(c.Argument(1).AsString())
		}},
		evm.ExternalMethod{Name: "Close", Receiver: builderPointer.Type, Call: func(c *evm.Call) error {
			b, ok := c.Context.Value(builderKey{}).(*Builder)
			if !ok || c.Argument(0).P != unsafe.Pointer(b) {
				return fmt.Errorf("invalid rendering context")
			}
			return b.Close()
		}},
	)
	// Small side-effecting calls read arguments directly from guest registers.
	// Keep the normal callbacks for indirect and deferred invocations.
	direct := func(fn func(*Builder, values.Value) error) evm.DirectFunc {
		return func(c *evm.Call, x, y values.Value) (values.Value, error) {
			b, ok := c.Context.Value(builderKey{}).(*Builder)
			if !ok || x.P != unsafe.Pointer(b) {
				return values.Value{}, fmt.Errorf("invalid rendering context")
			}
			return values.Value{}, fn(b, y)
		}
	}
	callbacks := map[string]evm.DirectFunc{
		"Step":  direct(func(b *Builder, v values.Value) error { return b.step(int(v.AsInt())) }),
		"Class": direct(func(b *Builder, v values.Value) error { return b.Class(v.AsString()) }),
		"Text":  direct(func(b *Builder, v values.Value) error { return b.Text(v.AsString()) }),
		"HTML":  direct(func(b *Builder, v values.Value) error { return b.HTML(HTML(v.AsString())) }),
		"Open":  direct(func(b *Builder, v values.Value) error { return b.Open(v.AsString()) }),
		"Close": direct(func(b *Builder, _ values.Value) error { return b.Close() }),
	}
	for i := range p.Functions {
		f := &p.Functions[i]
		if fn := callbacks[f.Name]; fn != nil {
			f.Direct = evm.DirectBinding{Op: evm.HostCallOpcode(false, []values.Repr{values.Pointer, f.Signature.Params[1].Repr()}, 0), Call: fn}
		}
	}
	for i := range p.Methods {
		m := &p.Methods[i]
		if fn := callbacks[m.Name]; fn != nil {
			var args []values.Repr
			for _, t := range m.Signature.Params {
				args = append(args, t.Repr())
			}
			m.Direct = evm.DirectBinding{Op: evm.HostCallOpcode(true, args, 0), Call: fn}
		}
	}
	return p
}
func nativeValue(c *evm.Call, index int) (any, error) {
	v := c.Argument(index)
	shape, raw, err := c.Program().UnpackInterface(v)
	if err != nil || shape == nil {
		return nil, err
	}
	typ := shape.Type
	switch typ {
	case values.StringType:
		return raw.AsString(), nil
	case values.Boolean:
		return raw.R != 0, nil
	case values.IntType, values.Int64:
		return raw.AsInt(), nil
	case values.UintType, values.Uint64:
		return raw.R, nil
	case values.Float64:
		return raw.AsFloat(), nil
	}
	for name, t := range trustedTypes {
		if typ == t {
			s := raw.AsString()
			switch name {
			case "HTML":
				return ht.HTML(s), nil
			case "CSS":
				return ht.CSS(s), nil
			case "JS":
				return ht.JS(s), nil
			case "JSStr":
				return ht.JSStr(s), nil
			case "URL":
				return ht.URL(s), nil
			case "Srcset":
				return ht.Srcset(s), nil
			}
		}
	}
	return c.JSONValue(v, 16<<20)
}
