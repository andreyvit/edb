package kvo

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/andreyvit/edb/nativebind"
)

// ErrNativeMapping identifies an invalid native declaration or an unregistered
// projection. Such errors are programmer errors and panic during setup.
var ErrNativeMapping = errors.New("kvo: invalid native mapping")

// NativeField binds one explicit struct field to a property of its object model.
// Index addresses a direct field, or is -1 when Path traverses inlined structs.
type NativeField struct {
	Index                    int
	Path                     []int
	Prop                     PropImpl
	Compact                  bool
	PresenceIsSignificant    bool
	KeepDictionaryZeroValues bool
}

// NativeStruct is immutable, validated tag metadata. Persistent tables compile this metadata
// into a strict native conversion plan by Bind.
type NativeStruct struct {
	typ    reflect.Type
	fields []NativeField
}

func (n *NativeStruct) Type() reflect.Type { return n.typ }

// Fields returns a copy, so a binding compiler cannot modify shared schema data.
func (n *NativeStruct) Fields() []NativeField {
	fields := append([]NativeField(nil), n.fields...)
	for i := range fields {
		fields[i].Path = append([]int(nil), fields[i].Path...)
	}
	return fields
}

// FieldPath returns the complete Go field path, including explicit inline layers.
func (f NativeField) FieldPath() []int {
	if len(f.Path) != 0 {
		return append([]int(nil), f.Path...)
	}
	return []int{f.Index}
}

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

// RegisterNativeType attaches a Go struct after the logical model is declared.
// An omitted or nil field map uses kvo tags. An explicit map associates each
// model property with a direct exported Go field; other fields stay unexposed.
// Registration compiles and owns the mapping, so later map edits have no effect.
// Call during single-threaded setup, before constructing bindings or accessing
// records. Duplicate registrations panic.
func (model *Model) RegisterNativeType(t reflect.Type, fields ...map[PropCode]string) {
	types := checkNativeTypes(append(model.entityType.NativeTypes(), t))
	if len(fields) > 1 {
		nativeMappingPanic(t, "", "provide at most one field map")
	}
	var n *NativeStruct
	if len(fields) == 1 && fields[0] != nil {
		n = &NativeStruct{typ: t, fields: resolveNativeFields(t, model, fields[0])}
	} else {
		// Validate separately so a failed registration leaves the model untouched.
		entity := *model.entityType
		entity.nativeTypes = []reflect.Type{t}
		probe := *model
		probe.entityType = &entity
		probe.initNativeStructs()
		n = probe.nativeStructs[t]
	}
	if model.nativeStructs == nil {
		model.nativeStructs = make(map[reflect.Type]*NativeStruct)
	}
	model.nativeStructs[t] = n
	model.entityType.nativeTypes = types
}

func resolveNativeFields(t reflect.Type, model *Model, names map[PropCode]string) []NativeField {
	if len(names) != len(model.AllProps()) {
		nativePanic(t, "access map must cover the exposed model")
	}
	var fields []NativeField
	seen := map[int]bool{}
	for _, prop := range model.AllProps() {
		code := prop.Code()
		f, ok := t.FieldByName(names[code])
		if !ok || !f.IsExported() || len(f.Index) != 1 || seen[f.Index[0]] {
			nativePanic(t, fmt.Sprintf("invalid or duplicate native field %q for property %d", names[code], code))
		}
		seen[f.Index[0]] = true
		flags := model.PropFlags(code)
		fields = append(fields, NativeField{Index: f.Index[0], Prop: prop, Compact: flags&Compact != 0, PresenceIsSignificant: flags&PresenceIsSignificant != 0, KeepDictionaryZeroValues: flags&KeepDictionaryZeroValues != 0})
	}
	return fields
}

func (model *Model) initNativeStructs() {
	if len(model.entityType.nativeTypes) == 0 {
		return
	}
	model.nativeStructs = make(map[reflect.Type]*NativeStruct, len(model.entityType.nativeTypes))
	for _, t := range model.entityType.nativeTypes {
		n := &NativeStruct{typ: t}
		seen := make(map[PropCode]bool)
		var visit func(reflect.Type, []int)
		visit = func(owner reflect.Type, prefix []int) {
			for i := 0; i < owner.NumField(); i++ {
				f := owner.Field(i)
				path := append(append([]int(nil), prefix...), i)
				tag, ok := f.Tag.Lookup("kvo")
				if !ok {
					tag, ok = f.Tag.Lookup("edb")
				}
				if !ok {
					nativeMappingPanic(t, f.Name, "missing kvo tag (use kvo:\"-\" to exclude)")
				}
				if tag == "-" {
					continue
				}
				if !f.IsExported() {
					nativeMappingPanic(t, f.Name, "unexported field must be excluded")
				}
				if tag == ",inline" {
					if f.Type.Kind() != reflect.Struct {
						nativeMappingPanic(t, f.Name, "inline requires a value struct")
					}
					visit(f.Type, path)
					continue
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
				index := i
				if len(path) > 1 {
					index = -1
				}
				nf := NativeField{Index: index, Path: path, Prop: prop, Compact: flags&Compact != 0, PresenceIsSignificant: flags&PresenceIsSignificant != 0, KeepDictionaryZeroValues: flags&KeepDictionaryZeroValues != 0}
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
		}
		visit(t, nil)
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

// RegisterNativeFields installs generated getters/setters for registered structs
// and their inline embeddings. Call after RegisterNativeType and before any
// binding or projection is built, including through a containing model.
// Missing types keep reflected access; supplied bindings must match exactly.
func (model *Model) RegisterNativeFields(generated ...*nativebind.GeneratedFields) {
	if atomic.LoadUint32(&model.nativeCompiled) != 0 {
		nativeMappingPanic(nil, "", "native fields registered after model use")
	}
	allowed := make(map[reflect.Type]bool)
	for t, n := range model.nativeStructs {
		allowed[t] = true
		for _, f := range n.fields {
			owner := t
			path := f.FieldPath()
			for _, i := range path[:len(path)-1] {
				owner = owner.Field(i).Type
				allowed[owner] = true
			}
		}
	}
	pending := make(map[reflect.Type]*nativebind.Struct)
	for _, g := range generated {
		if g == nil || !allowed[g.Type] {
			nativeMappingPanic(nil, "", "generated fields need a registered struct or inline embedding")
		}
		if model.nativeFields[g.Type] != nil || pending[g.Type] != nil {
			nativeMappingPanic(g.Type, "", "duplicate generated fields")
		}
		pending[g.Type] = nativebind.BindFields(g.Type, g, nativebind.StaticOnly)
	}
	if model.nativeFields == nil {
		model.nativeFields = make(map[reflect.Type]*nativebind.Struct)
	}
	for t, b := range pending {
		model.nativeFields[t] = b
	}
}

func (model *Model) bindNativeFields(t reflect.Type) *nativebind.Struct {
	if b := model.nativeFields[t]; b != nil {
		return b
	}
	return nativebind.BindFields(t, nil, nativebind.Automatic)
}
