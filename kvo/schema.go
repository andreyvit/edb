package kvo

import (
	"fmt"
)

type Schema struct {
	TPropCode AnyType

	modelsByName        map[string]*Model
	modelsByTypeCodeSet map[typeCodeSet]*Model

	propsByName map[string]PropImpl
	propsByCode []PropImpl
	maxPropCode PropCode
}

func NewSchema() *Schema {
	sch := &Schema{}
	sch.TPropCode = NewScalarSubtype[PropCode]("prop", TUint64)
	return sch
}

func (schema *Schema) PropByCode(code PropCode) PropImpl {
	return denseMapGet(schema.propsByCode, code)
}
func (schema *Schema) MustPropByCode(code PropCode) PropImpl {
	prop := schema.PropByCode(code)
	if prop == nil {
		panic(fmt.Sprintf("prop %d not found", code))
	}
	return prop
}

func (schema *Schema) addProp(prop PropImpl) {
	code, name := prop.Code(), prop.Name()
	if name == "" {
		panic("invalid")
	}
	if code == 0 {
		panic("invalid")
	}

	if schema.propsByName == nil {
		schema.propsByName = make(map[string]PropImpl)
	}
	if schema.propsByName[name] != nil {
		panic(fmt.Sprintf("prop %s already exists", name))
	}
	if prev := denseMapGet(schema.propsByCode, code); prev != nil {
		panic(fmt.Sprintf("prop code %d is already assigned to %s, cannot use it for %s", code, prev.Name(), name))
	}
	schema.propsByName[name] = prop
	denseMapSet(&schema.propsByCode, prop.Code(), prop)
	schema.maxPropCode = max(schema.maxPropCode, code)
}

type PropCode = uint64

type PropImpl interface {
	Name() string
	Code() PropCode
	AnyType() AnyType
	TypeModel() *Model
	ValueKind() ValueKind
}

type Prop struct {
	name   string
	code   PropCode
	typ    AnyType
	model  *Model
	schema *Schema
}

func NewProp(schema *Schema, code PropCode, name string, typ AnyType, build func(b *PropBuilder)) PropCode {
	p := &Prop{
		code:   code,
		name:   name,
		typ:    typ,
		schema: schema,
	}
	schema.addProp(p)
	return code
}

func (p *Prop) Name() string         { return p.name }
func (p *Prop) Code() PropCode       { return p.code }
func (p *Prop) AnyType() AnyType     { return p.typ }
func (p *Prop) ValueKind() ValueKind { return p.typ.ValueKind() }
func (p *Prop) TypeModel() *Model    { return p.typ.Model() }

// func (p *ScalarProp) ValueToScalar(value T) uint64 {
// 	return p.typ.ScalarConverter().ValueToScalar(value)
// }
// func (p *ScalarProp) ScalarToValue(scalar uint64) T {
// 	return p.typ.ScalarConverter().ScalarToValue(scalar)
// }

// func (p *ScalarProp) Get(obj RawObject) T {
// 	raw := obj.GetScalar(p.Code())
// 	return p.ScalarToValue(raw)
// }

// func (p *ScalarProp) Set(builder *ObjectBuilder, value T) {
// 	builder.SetScalar(p.Code(), p.ValueToScalar(value))
// }

type PropBuilder struct{}

type ModelDefinition struct {
	Name  string
	Props []*PropInstance
}

type Model struct {
	schema     *Schema
	code       uint64
	name       string
	props      []*PropInstance
	entityType *EntityType

	densePropCount int
	flexPropCount  int
	fixedWords     int
	propsByCode    map[PropCode]*PropInstance
	propsByName    map[string]*PropInstance
}

type PropInstance struct {
	Prop     PropImpl
	Dense    bool
	Required bool

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
	model := &Model{
		schema:     schema,
		name:       name,
		entityType: entityType,
	}
	// if model.Code == 0 {
	// 	panic("model code missing")
	// }

	model.propsByCode = make(map[PropCode]*PropInstance)
	model.propsByName = make(map[string]*PropInstance)

	b := ModelBuilder{model: model}
	build(&b)

	model.fixedWords = 1 + model.densePropCount

	entityType.model = model

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

func (model *Model) MustPropByCode(key uint64) PropImpl {
	return model.mustPropInstanceByCode(key).Prop
}

func (model *Model) PropByCode(key uint64) PropImpl {
	if pi := model.propsByCode[key]; pi != nil {
		return pi.Prop
	}
	return nil
}

type ModelBuilder struct {
	model *Model
}

type PropOption interface {
	markerIsPropOption()
}

type PropFlag int

func (PropFlag) markerIsPropOption() {}

const (
	PropDense PropFlag = 1 << iota
	PropRequired
)

func (b *ModelBuilder) Prop(propCode PropCode, opts ...any) {
	prop := b.model.schema.PropByCode(propCode)
	name := prop.Name()
	pi := &PropInstance{
		Prop:       prop,
		Dense:      false,
		Required:   false,
		denseIndex: 0,
	}
	for _, opt := range opts {
		switch opt := opt.(type) {
		case PropFlag:
			switch opt {
			case PropDense:
				pi.Dense = true
			case PropRequired:
				pi.Required = true
			}
		default:
			panic(fmt.Errorf("unexpected prop option %T", opt))
		}
	}
	b.model.props = append(b.model.props, pi)

	// if prior := b.model.propsByCode[prop.Code]; prior != nil {
	// 	panic(fmt.Errorf("model %s already has prop %s with code %d, attempting to add prop %s with same code", model.Name, prior.Name, prop.Code, prop.Name))
	// }
	if prior := b.model.propsByName[name]; prior != nil {
		panic(fmt.Errorf("model %s already has prop %s", b.model.name, name))
	}
	b.model.propsByCode[propCode] = pi
	b.model.propsByName[name] = pi

	if pi.Dense {
		b.model.densePropCount++
		pi.denseIndex = b.model.densePropCount
	} else {
		b.model.flexPropCount++
	}
}

// func NewScalarProp[T any](name string)
