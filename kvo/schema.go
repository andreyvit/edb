package kvo

import (
	"errors"
	"fmt"
	"go/token"
	"iter"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/andreyvit/edb/nativebind"
)

type Schema struct {
	TPropCode AnyType

	modelsByName        map[string]*Model
	modelsByTypeCodeSet map[typeCodeSet]*Model

	propsByName map[string]PropImpl
	propsByCode []PropImpl
	sparseProps map[PropCode]PropImpl
}

func NewSchema() *Schema {
	sch := &Schema{}
	sch.TPropCode = NewIntType("prop", func(fc *FmtContext, key uint64) string {
		prop := sch.PropByCode(key)
		if prop == nil {
			return fmt.Sprintf("unk_0x%x", key)
		} else {
			return prop.Name()
		}
	})
	return sch
}

// Models returns the schema's entity models in name order.
func (schema *Schema) Models() []*Model {
	names := slices.Sorted(maps.Keys(schema.modelsByName))
	models := make([]*Model, len(names))
	for i, n := range names {
		models[i] = schema.modelsByName[n]
	}
	return models
}

func (schema *Schema) PropByCode(code PropCode) PropImpl {
	if code >= densePropLimit {
		return schema.sparseProps[code]
	}
	return denseMapGet(schema.propsByCode, code)
}
func (schema *Schema) MustPropByCode(code PropCode) PropImpl {
	prop := schema.PropByCode(code)
	if prop == nil {
		panic(fmt.Sprintf("prop %d not found", code))
	}
	return prop
}

// Schemas that project external models can have sparse stable field codes.
const densePropLimit = 65536

func (schema *Schema) addProp(prop PropImpl) {
	code, name := prop.Code(), prop.Name()
	for _, part := range strings.Split(name, ":") {
		if part == "" {
			panic("empty property name or namespace")
		}
	}
	if code == 0 || code >= RuntimePropBase {
		panic("KVO property code is zero or reserved for runtime fields")
	}

	if schema.propsByName == nil {
		schema.propsByName = make(map[string]PropImpl)
	}
	if schema.propsByName[name] != nil {
		panic(fmt.Sprintf("prop %s already exists", name))
	}
	if prev := schema.PropByCode(code); prev != nil {
		panic(fmt.Sprintf("prop code %d is already assigned to %s, cannot use it for %s", code, prev.Name(), name))
	}
	schema.propsByName[name] = prop
	if code < densePropLimit {
		denseMapSet(&schema.propsByCode, code, prop)
	} else {
		if schema.sparseProps == nil {
			schema.sparseProps = make(map[PropCode]PropImpl)
		}
		schema.sparseProps[code] = prop
	}
}

// RuntimePropBase reserves the upper half of the property namespace for runtime fields.
const RuntimePropBase uint64 = 1 << 63

type PropCode = uint64

// PropertyCode is shared by typed property references and schema declarations.
type PropertyCode interface{ Code() PropCode }

// CodeOf resolves a property reference at a schema/configuration boundary.
// Codec and dynamic-key loops should keep their numeric codes directly.
func CodeOf(prop any) PropCode {
	switch p := prop.(type) {
	case PropertyCode:
		return p.Code()
	case uint64:
		return p
	case int:
		if p >= 0 {
			return uint64(p)
		}
	}
	panic(fmt.Sprintf("kvo: expected a property reference or code, got %T (%v)", prop, prop))
}

type PropImpl interface {
	Name() string
	FullName() string
	Code() PropCode
	AnyType() AnyType
	TypeModel() *Model
	ValueKind() ValueKind
}

type Prop struct {
	defaultFlags PropFlag
	name         string
	code         PropCode
	typ          AnyType
	model        *Model
	schema       *Schema
}

func NewProp(schema *Schema, code PropCode, name string, typ AnyType, build func(b *PropBuilder)) PropCode {
	p := &Prop{
		code:   code,
		name:   name,
		typ:    typ,
		schema: schema,
	}
	if build != nil {
		b := PropBuilder{}
		build(&b)
		p.defaultFlags = b.Flags
	}
	schema.addProp(p)
	return code
}

func (p *Prop) Name() string         { return p.name }
func (p *Prop) FullName() string     { return p.name }
func (p *Prop) Code() PropCode       { return p.code }
func (p *Prop) AnyType() AnyType     { return p.typ }
func (p *Prop) ValueKind() ValueKind { return p.typ.ValueKind() }
func (p *Prop) TypeModel() *Model    { return p.typ.Model() }

// PropBuilder supplies declaration defaults shared by every model using a property.
type PropBuilder struct{ Flags PropFlag }

func (p *Prop) DefaultFlags() PropFlag { return p.defaultFlags }

type ModelDefinition struct {
	Name  string
	Props []*PropInstance
}

type Model struct {
	primaryKey     PropImpl
	schema         *Schema
	code           uint64
	name           string
	props          []*PropInstance
	entityType     *EntityType
	nativeStructs  map[reflect.Type]*NativeStruct
	nativeFields   map[reflect.Type]*nativebind.Struct
	nativeCompiled uint32

	densePropCount int
	flexPropCount  int
	fixedWords     int
	propsByCode    map[PropCode]*PropInstance
	propsByName    map[string]*PropInstance
	selectors      []string
}

type PropInstance struct {
	Flags    PropFlag
	Prop     PropImpl
	Dense    bool
	Required bool
	LocalID  LocalID

	denseIndex int
}

func (pi *PropInstance) PropInstance() *PropInstance {
	return pi
}

func NewModel(schema *Schema, entityType *EntityType, build func(b *ModelBuilder)) *Model {
	name := entityType.name
	if name == "" {
		panic("model name missing")
	}
	if schema.modelsByName[name] != nil || entityType.model != nil {
		panic(fmt.Errorf("model %s already defined", name))
	}
	model := &Model{
		schema:     schema,
		name:       name,
		entityType: entityType,
	}

	model.propsByCode = make(map[PropCode]*PropInstance)
	model.propsByName = make(map[string]*PropInstance)

	b := ModelBuilder{model: model}
	build(&b)
	slices.Sort(model.selectors)

	model.fixedWords = 1 + model.densePropCount
	model.initNativeStructs()

	if schema.modelsByName == nil {
		schema.modelsByName = make(map[string]*Model)
	}
	if schema.modelsByTypeCodeSet == nil {
		schema.modelsByTypeCodeSet = make(map[typeCodeSet]*Model)
	}
	if schema.modelsByName[model.name] != nil {
		panic(fmt.Errorf("model %s already defined", model.name))
	}
	codeSet := entityType.typeCodeSet()
	if prior := schema.modelsByTypeCodeSet[codeSet]; prior != nil {
		panic("unreachable")
	}
	schema.modelsByName[model.name] = model
	schema.modelsByTypeCodeSet[codeSet] = model
	entityType.model = model

	return model
}

func (model *Model) Name() string {
	return model.name
}

func (model *Model) Type() AnyType {
	return model.entityType
}

func (model *Model) mustPropInstanceByCode(key uint64) *PropInstance {
	pi := model.propsByCode[key]
	if pi == nil {
		if prop := model.schema.PropByCode(key); prop != nil {
			panic(fmt.Errorf("%s does not have prop %s", model.Name(), prop.Name()))
		} else {
			panic(fmt.Errorf("%s does not have prop %d", model.Name(), key))
		}
	}
	return pi
}

func (model *Model) AllProps() []PropImpl {
	res := make([]PropImpl, len(model.props))
	for i, pi := range model.props {
		res[i] = pi.Prop
	}
	return res
}

func (model *Model) MustPropByCode(key uint64) PropImpl {
	return model.mustPropInstanceByCode(key).Prop
}

func (model *Model) PropByCode(key uint64) PropImpl {
	if pi := model.propsByCode[key]; pi != nil {
		return pi.Prop
	}
	return nil
}

// PropByName resolves every accepted property name: the model-local name, the
// original unqualified name, the full colon-qualified name, and its underscore
// spelling. Renaming a property locally preserves the other names.
func (model *Model) PropByName(name string) PropImpl {
	if pi := model.propsByName[name]; pi != nil {
		return pi.Prop
	}
	return nil
}

// Selectors enumerates the identifier-safe property names in lexical order.
// Each alias identifies the same model-specific property. Colon-qualified names
// remain available through PropByName and native mapping tags, not dot access.
func (model *Model) Selectors() iter.Seq2[string, PropImpl] {
	return func(yield func(string, PropImpl) bool) {
		for _, name := range model.selectors {
			if !yield(name, model.propsByName[name].Prop) {
				return
			}
		}
	}
}

// PropBySelector resolves exactly the names enumerated by Selectors.
func (model *Model) PropBySelector(name string) PropImpl {
	if !isPropSelector(name) {
		return nil
	}
	return model.PropByName(name)
}

func isPropSelector(name string) bool {
	return name != "_" && token.IsIdentifier(name)
}

type ModelBuilder struct {
	model *Model
}

type PropOption interface {
	markerIsPropOption()
}

type PropFlag int

func (PropFlag) markerIsPropOption() {}

// ErrPropOption identifies an invalid property option, a schema declaration error.
var ErrPropOption = errors.New("kvo: invalid property option")

const (
	PropDense PropFlag = 1 << iota
	PropRequired
	// Compact selects Compact storage for this child and its descendants.
	Compact
	// PresenceIsSignificant keeps an explicitly stored zero distinct from absence.
	PresenceIsSignificant
	// KeepDictionaryZeroValues retains zero-valued entries in this dictionary.
	// It applies to string- and numeric-keyed Map properties, not to the
	// presence of the property itself or to nested dictionaries in its values.
	KeepDictionaryZeroValues
	// PrimaryKey declares an ordinary stored property as this model's database
	// key. It preserves explicit zero values; models may omit a key entirely.
	PrimaryKey
)

// PropName gives a namespaced property its local field name on this model.
type PropName string

type namedProperty struct {
	PropImpl
	name string
}

func (p namedProperty) Name() string { return p.name }

type LocalID uint64

// Inline includes another model's properties directly, preserving names and
// storage policies. It adds no container property or wire nesting.
func (b *ModelBuilder) Inline(model *Model) {
	if model == nil || model.schema != b.model.schema {
		panic("inline model must belong to the same schema")
	}
	for _, p := range model.props {
		b.Prop(p.Prop, p.Flags, PropName(p.Prop.Name()), p.LocalID)
	}
}

// Prop includes a typed property reference or a schema declaration. Numeric
// codes are not properties; resolve dynamic codes through Schema first.
func (b *ModelBuilder) Prop[P PropertyCode](property P, opts ...any) {
	b.addProp(property.Code(), opts...)
}

func (b *ModelBuilder) addProp(propCode PropCode, opts ...any) {
	prop := b.model.schema.PropByCode(propCode)
	if prop == nil {
		panic(fmt.Sprintf("property code %d is not declared", propCode))
	}
	name := prop.Name()
	if i := strings.LastIndexByte(name, ':'); i >= 0 {
		name = name[i+1:]
	}
	localName := name
	pi := &PropInstance{
		Prop:       prop,
		Dense:      false,
		Required:   false,
		denseIndex: 0,
	}
	if p, ok := prop.(interface{ DefaultFlags() PropFlag }); ok && p.DefaultFlags() != 0 {
		opts = append([]any{p.DefaultFlags()}, opts...)
	}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case PropFlag:
			if opt & ^(PropDense|PropRequired|Compact|PresenceIsSignificant|KeepDictionaryZeroValues|PrimaryKey) != 0 {
				panic(fmt.Errorf("%w: unexpected prop flags %d", ErrPropOption, opt))
			}
			pi.Flags |= opt
			pi.Dense = pi.Flags&PropDense != 0
			pi.Required = pi.Flags&PropRequired != 0
		case PropName:
			name = string(opt)
			if name == "" {
				panic("empty model property name")
			}
			pi.Prop = namedProperty{prop, name}
		case LocalID:
			pi.LocalID = opt
		default:
			panic(fmt.Errorf("unexpected prop option %T", opt))
		}
	}
	if name == "" {
		panic("empty model property name")
	}
	if name != prop.Name() {
		pi.Prop = namedProperty{prop, name}
	}
	if pi.Flags&PrimaryKey != 0 {
		if b.model.primaryKey != nil {
			panic(fmt.Errorf("%w: multiple primary keys on %s", ErrPropOption, b.model.Name()))
		}
		pi.Flags |= PropRequired
		pi.Required = true
		b.model.primaryKey = pi.Prop
	}
	if pi.Flags&KeepDictionaryZeroValues != 0 {
		if _, ok := prop.AnyType().(*MapType); !ok {
			panic(fmt.Errorf("%w: %s: KeepDictionaryZeroValues requires a Map property", ErrPropOption, name))
		}
	}
	b.model.props = append(b.model.props, pi)

	if prior := b.model.propsByCode[propCode]; prior != nil {
		panic(fmt.Sprintf("model %s already has property code %d (%s)", b.model.name, propCode, prior.Prop.FullName()))
	}
	// Both qualified names and local selectors identify the same declaration.
	for _, alias := range []string{name, localName, prop.FullName(), strings.ReplaceAll(prop.FullName(), ":", "_")} {
		if prior := b.model.propsByName[alias]; prior != nil {
			if prior != pi {
				panic(fmt.Sprintf("model %s has conflicting property name %s (%s and %s)", b.model.name, alias, prior.Prop.FullName(), prop.FullName()))
			}
			continue
		}
		b.model.propsByName[alias] = pi
		if isPropSelector(alias) {
			b.model.selectors = append(b.model.selectors, alias)
		}
	}
	b.model.propsByCode[propCode] = pi

	if pi.Dense {
		b.model.densePropCount++
		pi.denseIndex = b.model.densePropCount
	} else {
		b.model.flexPropCount++
	}
}

// PrimaryKey returns the explicitly stored key property, or nil for a model
// whose records do not carry database identity.
func (model *Model) PrimaryKey() PropImpl { return model.primaryKey }

// PropFlags returns the model-specific options for a declared property.
func (model *Model) PropFlags(key PropCode) PropFlag {
	return model.mustPropInstanceByCode(key).Flags
}
