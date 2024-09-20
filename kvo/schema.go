package kvo

import "fmt"

type Schema struct {
	modelsByName map[string]*Model
	modelsByCode map[uint64]*Model

	propsByName map[string]PropImpl
	propsByCode []PropImpl
	maxPropCode PropCode
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

type PropCode = uint64

type PropImpl interface {
	Name() string
	Code() PropCode
	AnyType() AnyType
	TypeModel() *Model
}

type ScalarProp[T any] struct {
	name string
	code PropCode
	typ  *Type[T]
}

type PropOptions struct {
}

func NewProp[T any](schema *Schema, code PropCode, name string, typ *Type[T], build func(b *PropBuilder)) PropCode {
	if name == "" {
		panic("invalid")
	}
	p := &ScalarProp[T]{
		code: code,
		name: name,
		typ:  typ,
	}
	if schema.propsByName[name] != nil {
		panic(fmt.Sprintf("prop %s already exists", name))
	}
	if prev := denseMapGet(schema.propsByCode, code); prev != nil {
		panic(fmt.Sprintf("prop code %d is already assigned to %s, cannot use it for %s", code, prev.Name(), name))
	}
	if schema.propsByName == nil {
		schema.propsByName = make(map[string]PropImpl)
	}
	schema.propsByName[name] = p
	denseMapSet(&schema.propsByCode, p.code, PropImpl(p))
	schema.maxPropCode = max(schema.maxPropCode, code)
	return code
}

func (p *ScalarProp[T]) Name() string {
	return p.name
}

func (p *ScalarProp[T]) Code() PropCode {
	return p.code
}

func (p *ScalarProp[T]) AnyType() AnyType {
	return p.typ
}

func (p *ScalarProp[T]) ValueToScalar(value T) uint64 {
	return p.typ.ScalarConverter().ValueToScalar(value)
}
func (p *ScalarProp[T]) ScalarToValue(scalar uint64) T {
	return p.typ.ScalarConverter().ScalarToValue(scalar)
}

func (p *ScalarProp[T]) TypeModel() *Model {
	return nil
}

// func (p *ScalarProp[T]) Get(obj RawObject) T {
// 	raw := obj.GetScalar(p.Code())
// 	return p.ScalarToValue(raw)
// }

// func (p *ScalarProp[T]) Set(builder *ObjectBuilder, value T) {
// 	builder.SetScalar(p.Code(), p.ValueToScalar(value))
// }

type PropBuilder struct{}

type ModelDefinition struct {
	Name  string
	Props []*PropInstance
}

type Model struct {
	schema *Schema
	code   uint64
	name   string
	props  []*PropInstance

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

func NewModel(schema *Schema, compositeType *Type[any], build func(b *ModelBuilder)) *Model {
	name := compositeType.name
	if name == "" {
		panic("model name missing")
	}
	model := &Model{
		schema: schema,
		name:   name,
	}
	// if model.Code == 0 {
	// 	panic("model code missing")
	// }

	model.propsByCode = make(map[PropCode]*PropInstance)
	model.propsByName = make(map[string]*PropInstance)

	b := ModelBuilder{model: model}
	build(&b)

	model.fixedWords = 1 + model.densePropCount

	if schema.modelsByName == nil {
		schema.modelsByName = make(map[string]*Model)
	}
	if schema.modelsByCode == nil {
		schema.modelsByCode = make(map[uint64]*Model)
	}
	if schema.modelsByName[model.name] != nil {
		panic(fmt.Errorf("model %s already defined", model.name))
	}
	// if prior := schema.modelsByCode[model.Code]; prior != nil {
	// 	panic(fmt.Errorf("model code %d used by both %s and %s", model.Code, model.Name, prior.Name))
	// }
	schema.modelsByName[model.name] = model
	// schema.modelsByCode[model.Code] = model

	return model
}

func (model *Model) Name() string {
	return model.name
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