package kvolab

// Native bindings intentionally remain outside the production codec. The real
// kvo schema owns property identities and explicit projection registrations.
import (
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/andreyvit/edb/kvo"
)

// Optional is used only on properties marked PresenceIsSignificant. It adds no
// wire type: Present controls whether the ordinary value is stored at all.
type Optional[T any] struct {
	Value   T
	Present bool
}

func (Optional[T]) nativeOptional() {}

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
)

type nativeField struct {
	key         uint64
	index       int
	compact     bool
	significant bool
	plan        *nativePlan
}

type nativePlan struct {
	typ                 reflect.Type
	kind                nativeKind
	fields              []nativeField // sorted once, not on each row
	elem                *nativePlan
	reuseMapValues      bool
	keepDictionaryZeros bool
}

type nativePlanKey struct {
	goType reflect.Type
	schema kvo.AnyType
}

func nativePanic(t reflect.Type, reason string) {
	panic(fmt.Errorf("%w: %v: %s", kvo.ErrNativeMapping, t, reason))
}

func nativeCompile(t reflect.Type, schema kvo.AnyType, cache map[nativePlanKey]*nativePlan) *nativePlan {
	key := nativePlanKey{t, schema}
	if p := cache[key]; p != nil {
		return p
	}
	p := &nativePlan{typ: t}
	cache[key] = p // recursive object declarations may refer back to this plan
	if t.Kind() == reflect.Struct && t.Implements(reflect.TypeFor[interface{ nativeOptional() }]()) {
		p.kind, p.elem = nativeOption, nativeCompile(t.Field(0).Type, schema, cache)
		if p.elem.kind == nativeOption {
			nativePanic(t, "nested Optional is not supported")
		}
		return p
	}
	if t.Kind() == reflect.Pointer {
		p.kind, p.elem = nativePointer, nativeCompile(t.Elem(), schema, cache)
		if p.elem.kind == nativeOption || p.elem.kind == nativePointer {
			nativePanic(t, "use a single pointer or Optional")
		}
		return p
	}
	switch schema := schema.(type) {
	case *kvo.EntityType:
		if t.Kind() != reflect.Struct {
			nativePanic(t, "object needs a registered struct")
		}
		p.kind = nativeObject
		for _, f := range schema.Model().NativeStruct(t).Fields() {
			child := nativeCompile(t.Field(f.Index).Type, f.Prop.AnyType(), cache)
			if f.PresenceIsSignificant != (child.kind == nativeOption) {
				nativePanic(t, "PresenceIsSignificant property requires Optional[T], and Optional requires that flag")
			}
			if f.KeepDictionaryZeroValues {
				child = nativeWithDictionaryZeros(child)
			}
			p.fields = append(p.fields, nativeField{f.Prop.Code(), f.Index, f.Compact, f.PresenceIsSignificant, child})
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
	case *kvo.MapType:
		if t.Kind() != reflect.Map {
			nativePanic(t, "dictionary needs a Go map")
		}
		p.kind = nativeNumbers
		switch keyType := schema.MapKeyType().(type) {
		case *kvo.ScalarDataType:
			if keyType.NativeType().Kind() != reflect.String || t.Key().Kind() != reflect.String {
				nativePanic(t, "dictionary keys must be strings")
			}
			p.kind = nativeDictionary
		case *kvo.WordType:
			if !nativeUnsignedKind(keyType.NativeType().Kind()) || !nativeUnsignedKind(t.Key().Kind()) {
				nativePanic(t, "numeric dictionary keys must be unsigned integers")
			}
		default:
			nativePanic(t, "unsupported dictionary key type")
		}
		p.elem = nativeCompile(t.Elem(), schema.ItemType(), cache)
		p.reuseMapValues = nativeHasMutableStorage(p.elem)
		if p.elem.kind == nativeOption {
			nativePanic(t, "Optional belongs on a presence-significant property")
		}
	case *kvo.ListType:
		if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
			nativePanic(t, "List needs a slice or array")
		}
		p.kind, p.elem = nativeList, nativeCompile(t.Elem(), schema.ItemType(), cache)
		if p.elem.kind == nativeOption {
			nativePanic(t, "List positions always exist; Optional belongs on a property")
		}
	case *kvo.ScalarDataType:
		if schema.NativeType().Kind() == reflect.String && t.Kind() == reflect.String {
			p.kind = nativeString
		} else if schema.NativeType().Kind() == reflect.Slice && t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			p.kind = nativeBytes
		} else {
			nativePanic(t, "incompatible text/binary type")
		}
	case *kvo.WordType:
		expected := schema.NativeType()
		switch {
		case expected == reflect.TypeFor[time.Time]() && t == expected:
			p.kind = nativeTime
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
	default:
		nativePanic(p.typ, "KeepDictionaryZeroValues requires a dictionary")
	}
	return &q
}
func nativeSignedKind(k reflect.Kind) bool   { return k >= reflect.Int && k <= reflect.Int64 }
func nativeUnsignedKind(k reflect.Kind) bool { return k >= reflect.Uint && k <= reflect.Uintptr }

// nativeCodec owns reusable writer/map scratch. One instance is used by one
// goroutine; the compiled schema plan is immutable and can be shared.
type nativeCodec[T any] struct {
	plan               *nativePlan
	writer             currentWriter
	maps               nativeMapWorkspace
	rebuildMapValues   bool
	sortedMapSnapshots bool
}

func bindNative[T any](typ *kvo.EntityType) *nativeCodec[T] {
	p := nativeCompile(reflect.TypeFor[T](), typ, make(map[nativePlanKey]*nativePlan))
	if p.kind != nativeObject {
		nativePanic(p.typ, "row root must be a struct Object")
	}
	return &nativeCodec[T]{plan: p}
}

func nativeHasMutableStorage(p *nativePlan) bool {
	switch p.kind {
	case nativePointer, nativeBytes, nativeDictionary, nativeNumbers, nativeList:
		return true
	case nativeOption:
		return nativeHasMutableStorage(p.elem)
	case nativeObject:
		for _, f := range p.fields {
			if nativeHasMutableStorage(f.plan) {
				return true
			}
		}
	}
	return false
}
