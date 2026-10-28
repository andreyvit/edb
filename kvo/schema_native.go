package kvo

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// ErrNativeMapping identifies an invalid native declaration or an unregistered
// projection. Such errors are programmer errors and panic during setup.
var ErrNativeMapping = errors.New("kvo: invalid native mapping")

// NativeField binds one explicit struct field to a property of its object model.
// Index addresses the direct Go field; anonymous fields are never flattened.
type NativeField struct {
	Index                    int
	Prop                     PropImpl
	Compact                  bool
	PresenceIsSignificant    bool
	KeepDictionaryZeroValues bool
}

// NativeStruct is immutable, validated tag metadata. Encoding plans and their
// Go type compatibility checks currently live in the research implementation.
type NativeStruct struct {
	typ    reflect.Type
	fields []NativeField
}

func (n *NativeStruct) Type() reflect.Type { return n.typ }

// Fields returns a copy, so a binding compiler cannot modify shared schema data.
func (n *NativeStruct) Fields() []NativeField { return append([]NativeField(nil), n.fields...) }

func nativeMappingPanic(typ reflect.Type, field, reason string) {
	panic(fmt.Errorf("%w: %v.%s: %s", ErrNativeMapping, typ, field, reason))
}

func checkNativeTypes(types []reflect.Type) []reflect.Type {
	if len(types) == 0 {
		return nil
	}
	seen := make(map[reflect.Type]bool, len(types))
	for _, t := range types {
		if t == nil || t.Kind() != reflect.Struct {
			nativeMappingPanic(t, "", "register a struct type, using reflect.TypeFor[T]()")
		}
		if seen[t] {
			nativeMappingPanic(t, "", "duplicate registration")
		}
		seen[t] = true
	}
	return append([]reflect.Type(nil), types...)
}

// NativeTypes returns the explicitly allowed struct projections for this type.
func (typ *EntityType) NativeTypes() []reflect.Type {
	return append([]reflect.Type(nil), typ.nativeTypes...)
}

func (model *Model) initNativeStructs() {
	if len(model.entityType.nativeTypes) == 0 {
		return
	}
	model.nativeStructs = make(map[reflect.Type]*NativeStruct, len(model.entityType.nativeTypes))
	for _, t := range model.entityType.nativeTypes {
		n := &NativeStruct{typ: t}
		seen := make(map[PropCode]bool)
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			tag, ok := f.Tag.Lookup("edb")
			if !ok {
				nativeMappingPanic(t, f.Name, "missing edb tag (use edb:\"-\" to exclude)")
			}
			if tag == "-" {
				continue
			}
			if !f.IsExported() {
				nativeMappingPanic(t, f.Name, "unexported field must be excluded")
			}
			name, options, _ := strings.Cut(tag, ",")
			if name == "" || name == "-" {
				nativeMappingPanic(t, f.Name, "explicit property name required")
			}
			prop := model.PropByName(name)
			if prop == nil {
				nativeMappingPanic(t, f.Name, "property is not declared on model: "+name)
			}
			if seen[prop.Code()] {
				nativeMappingPanic(t, f.Name, "duplicate property: "+name)
			}
			seen[prop.Code()] = true
			flags := model.PropFlags(prop.Code())
			nf := NativeField{Index: i, Prop: prop, Compact: flags&Compact != 0, PresenceIsSignificant: flags&PresenceIsSignificant != 0, KeepDictionaryZeroValues: flags&KeepDictionaryZeroValues != 0}
			if options != "" {
				for _, option := range strings.Split(options, ",") {
					if option != "compact" {
						nativeMappingPanic(t, f.Name, "unknown edb option: "+option)
					}
					nf.Compact = true
				}
			} else if strings.Contains(tag, ",") {
				nativeMappingPanic(t, f.Name, "empty edb option")
			}
			if nf.Compact && prop.ValueKind() != ValueKindMap && prop.ValueKind() != ValueKindList {
				nativeMappingPanic(t, f.Name, "compact requires a container property")
			}
			n.fields = append(n.fields, nf)
		}
		model.nativeStructs[t] = n
	}
}

// NativeStruct resolves only an explicitly registered projection. Call after
// NewModel; tag validation occurs when that model is declared.
func (model *Model) NativeStruct(t reflect.Type) *NativeStruct {
	n := model.nativeStructs[t]
	if n == nil {
		nativeMappingPanic(t, "", "not registered for model "+model.Name())
	}
	return n
}
