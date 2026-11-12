// Package record binds registered Go structs to persistent KVO property codes.
package kvo

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

// Optional is used only on properties marked PresenceIsSignificant. It adds no
// wire type: Present controls whether the ordinary value is stored at all.
type Optional[T any] struct {
	Value   T
	Present bool
}

func isOptional(t reflect.Type) bool {
	return t.PkgPath() == reflect.TypeFor[Optional[int]]().PkgPath() && strings.HasPrefix(t.Name(), "Optional[")
}

type nativeKind uint8

const (
	nativeSigned nativeKind = iota
	nativeUnsigned
	nativeBool
	nativeFloat
	nativeTime
	nativeString
	nativeBytes
	nativeObject
	nativeDictionary
	nativeNumbers
	nativeList
	nativePointer
	nativeOption
	nativeJSON
	nativeTimestamp
)

type nativeField struct {
	access      *nativebind.Field
	key         uint64
	index       int
	path        []int
	selector    string
	layout      row2.Layout
	significant bool
	plan        *nativePlan
	msgName     string
	msgInline   bool
}

type nativePlan struct {
	representations *representationSet
	index           []uint16
	base            uint64
	native          *nativebind.Struct
	inlined         []*nativebind.Struct
	valueType       *values.Type
	model           *Model
	layout          *modelLayout
	typ             reflect.Type
	kind            nativeKind
	wireEncoding    row2.Encoding
	fields          []nativeField // sorted once, not on each row
	elem            *nativePlan
	mapKey          *nativePlan

	keepDictionaryZeros bool
	msgFallback         bool
	msgDecoder          func(msgpack.View) values.Value
	msgAliases          []string
}

type nativePlanKey struct {
	goType reflect.Type
	schema AnyType
}

func nativePanic(t reflect.Type, reason string) {
	panic(fmt.Errorf("%w: %v: %s", ErrNativeMapping, t, reason))
}

func nativeCompile(t reflect.Type, schema AnyType, cache map[nativePlanKey]*nativePlan, fields FieldMap) *nativePlan {
	key := nativePlanKey{t, schema}
	if p := cache[key]; p != nil {
		return p
	}
	decoder := messagePackScalarDecoder(t)
	p := &nativePlan{typ: t, wireEncoding: Encoding(schema), msgFallback: decoder == nil && messagePackFallback(t), msgDecoder: decoder}
	if t.Kind() == reflect.Struct && !p.msgFallback {
		p.msgAliases = messagePackInlineAliases(t)
	}
	p.representations = &representationSet{plan: p}
	if t.Kind() == reflect.Struct {
		p.native = nativebind.BindFields(t, nil, nativebind.Automatic)
		p.valueType = values.NewType(t.String(), values.Pointer)
	}
	cache[key] = p // recursive object declarations may refer back to this plan
	if isOptional(t) {
		p.kind, p.elem = nativeOption, nativeCompile(t.Field(0).Type, schema, cache, fields)
		if p.elem.kind == nativeOption {
			nativePanic(t, "nested Optional is not supported")
		}
		return p
	}
	if t.Kind() == reflect.Pointer {
		p.kind, p.elem = nativePointer, nativeCompile(t.Elem(), schema, cache, fields)
		// Pointer methods belong to the element's decoder too. Reuse its
		// compiled decision, including direct timestamps and scalar adapters.
		p.msgFallback = p.elem.msgFallback
		if p.elem.kind == nativeOption || p.elem.kind == nativePointer {
			nativePanic(t, "use a single pointer or Optional")
		}
		return p
	}
	switch schema := schema.(type) {
	case *EntityType:
		if t.Kind() != reflect.Struct {
			nativePanic(t, "object needs a registered struct")
		}
		p.kind = nativeObject
		p.model = schema.Model()
		atomic.StoreUint32(&p.model.nativeCompiled, 1)
		p.native = p.model.bindNativeFields(t)
		p.layout = layoutFor(p.model)
		bound := fields.resolve(t, schema.Model())
		if len(bound) != len(schema.Model().AllProps()) {
			nativePanic(t, "persistent binding must cover every model property")
		}
		for _, f := range bound {
			path := f.FieldPath()
			sf := t.FieldByIndex(path)
			owner := t
			var names []string
			for _, i := range path[:len(path)-1] {
				embedding := owner.Field(i)
				names = append(names, embedding.Name)
				if !messagePackInline(embedding) {
					p.msgFallback = true
				}
				owner = embedding.Type
				if p.fieldBinding(owner) == nil {
					p.inlined = append(p.inlined, p.model.bindNativeFields(owner))
				}
			}
			names = append(names, sf.Name)
			child := nativeCompile(sf.Type, f.Prop.AnyType(), cache, fields)
			pointerPresence := fields != nil && child.kind == nativePointer && f.PresenceIsSignificant
			if !pointerPresence && f.PresenceIsSignificant != (child.kind == nativeOption) {
				nativePanic(t, "PresenceIsSignificant property requires Optional[T], and Optional requires that flag")
			}
			if f.KeepDictionaryZeroValues {
				child = nativeWithDictionaryZeros(child)
			}
			primary := p.model.PropFlags(f.Prop.Code())&PrimaryKey != 0
			layout := row2.Fast2
			if f.Compact {
				layout = row2.Compact2
			}
			p.fields = append(p.fields, nativeField{access: p.fieldAccess(path), key: f.Prop.Code(), index: f.Index, path: path, selector: strings.Join(names, "."), layout: layout, significant: f.PresenceIsSignificant || primary, plan: child, msgName: messagePackName(sf), msgInline: messagePackInline(sf)})
		}
		slices.SortFunc(p.fields, func(a, b nativeField) int {
			if a.key < b.key {
				return -1
			}
			if a.key > b.key {
				return 1
			}
			return 0
		})
		p.indexFields()
	case *MapType:
		if t.Kind() != reflect.Map {
			nativePanic(t, "dictionary needs a Go map")
		}
		p.kind = nativeNumbers
		switch keyType := schema.MapKeyType().(type) {
		case *ScalarDataType:
			if keyType.NativeType().Kind() != reflect.String || t.Key().Kind() != reflect.String {
				nativePanic(t, "dictionary keys must be strings")
			}
			p.kind = nativeDictionary
		case *WordType:
			if !nativeUnsignedKind(t.Key().Kind()) && !nativeSignedKind(t.Key().Kind()) || keyType.NativeType().Kind() != t.Key().Kind() {
				nativePanic(t, "numeric dictionary keys must match the declared integer kind")
			}
		default:
			nativePanic(t, "unsupported dictionary key type")
		}
		p.mapKey = nativeCompile(t.Key(), schema.MapKeyType(), cache, fields)
		p.msgFallback = p.msgFallback || p.mapKey.msgFallback
		p.elem = nativeCompile(t.Elem(), schema.ItemType(), cache, fields)

		if p.elem.kind == nativeOption {
			nativePanic(t, "Optional belongs on a presence-significant property")
		}
	case *ListType:
		if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
			nativePanic(t, "List needs a slice or array")
		}
		p.kind, p.elem = nativeList, nativeCompile(t.Elem(), schema.ItemType(), cache, fields)
		if p.elem.kind == nativeOption {
			nativePanic(t, "List positions always exist; Optional belongs on a property")
		}
	case *JSONType:
		p.kind = nativeJSON
	case *ScalarDataType:
		if schema == TTimestamp && t.ConvertibleTo(reflect.TypeFor[time.Time]()) {
			p.kind = nativeTimestamp
			if t == reflect.TypeFor[time.Time]() {
				p.msgFallback = false
			}
		} else if schema.NativeType().Kind() == reflect.String && t.Kind() == reflect.String {
			p.kind = nativeString
		} else if schema.NativeType().Kind() == reflect.Slice && t.Kind() == reflect.Slice && t.Elem() == reflect.TypeFor[byte]() {
			p.kind = nativeBytes
		} else {
			nativePanic(t, "incompatible text/binary type")
		}
	case *WordType:
		expected := schema.NativeType()
		if expected.Kind() != t.Kind() {
			nativePanic(t, "scalar kind or width differs from schema")
		}
		switch {
		case expected == reflect.TypeFor[time.Time]() && t == expected:
			p.kind = nativeTime
			p.msgFallback = false
		case nativeSignedKind(expected.Kind()) && nativeSignedKind(t.Kind()):
			p.kind = nativeSigned
		case nativeUnsignedKind(expected.Kind()) && nativeUnsignedKind(t.Kind()):
			p.kind = nativeUnsigned
		case expected.Kind() == reflect.Bool && t.Kind() == reflect.Bool:
			p.kind = nativeBool
		case (expected.Kind() == reflect.Float32 || expected.Kind() == reflect.Float64) && (t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64):
			p.kind = nativeFloat
		default:
			nativePanic(t, "incompatible scalar type for "+schema.Name())
		}
	default:
		nativePanic(t, "unsupported schema type")
	}
	return p
}

// Copy only the property binding, never the shared cached plan. Two properties
// may use the same Go/schema Map type with different entry-presence policies.
func nativeWithDictionaryZeros(p *nativePlan) *nativePlan {
	q := *p
	switch q.kind {
	case nativePointer, nativeOption:
		q.elem = nativeWithDictionaryZeros(q.elem)
	case nativeDictionary, nativeNumbers:
		q.keepDictionaryZeros = true
		if q.elem.kind == nativeDictionary || q.elem.kind == nativeNumbers {
			q.elem = nativeWithDictionaryZeros(q.elem)
		}
	default:
		nativePanic(p.typ, "KeepDictionaryZeroValues requires a dictionary")
	}
	return &q
}
func nativeSignedKind(k reflect.Kind) bool   { return k >= reflect.Int && k <= reflect.Int64 }
func nativeUnsignedKind(k reflect.Kind) bool { return k >= reflect.Uint && k <= reflect.Uintptr }
