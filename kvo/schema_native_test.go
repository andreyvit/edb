package kvo

import (
	"errors"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
)

func nativeTestModel(types ...reflect.Type) *Model {
	s := NewSchema()
	e := NewEntityType(s, "sample", types...)
	NewProp(s, 1, "count", TInt64, nil)
	NewProp(s, 2, "labels", MapOf(TMachineString, TMachineString), nil)
	NewProp(s, 3, "not_in_model", TUint64, nil)
	return NewModel(s, e, func(b *ModelBuilder) {
		b.Prop(s.MustPropByCode(1), PropDense|PresenceIsSignificant)
		b.Prop(s.MustPropByCode(2), Compact|KeepDictionaryZeroValues)
	})
}

func TestNativeRegistration(t *testing.T) {
	type whole struct {
		Count  int64             `edb:"count"`
		Labels map[string]string `edb:"labels,compact"`
		cache  any               `edb:"-"`
	}
	type projection struct {
		Count int64 `edb:"count"`
	}
	m := nativeTestModel(reflect.TypeFor[whole](), reflect.TypeFor[projection]())
	n := m.NativeStruct(reflect.TypeFor[whole]())
	fs := n.Fields()
	if n.Type() != reflect.TypeFor[whole]() || len(fs) != 2 || fs[0].Prop.Code() != 1 || !fs[0].PresenceIsSignificant || !fs[1].Compact || !fs[1].KeepDictionaryZeroValues || fs[0].KeepDictionaryZeroValues {
		t.Fatalf("unexpected binding: %+v", fs)
	}
	fs[0].Index = 99
	if n.Fields()[0].Index != 0 {
		t.Fatal("field metadata was mutable")
	}
	types := m.Type().(*EntityType).NativeTypes()
	types[0] = nil
	if m.Type().(*EntityType).NativeTypes()[0] == nil {
		t.Fatal("registration was mutable")
	}
	if len(m.NativeStruct(reflect.TypeFor[projection]()).Fields()) != 1 {
		t.Fatal("projection")
	}
	if m.PropFlags(1) != PropDense|PresenceIsSignificant {
		t.Fatal("combined flags")
	}
}

func TestDictionaryZeroPolicyDeclaration(t *testing.T) {
	for _, numeric := range []bool{false, true} {
		s := NewSchema()
		e := NewEntityType(s, "dictionary_policy")
		key := AnyScalarType(TMachineString)
		if numeric {
			key = TUint64
		}
		mapType := MapOf(key, TUint64)
		for i, name := range []string{"ordinary", "kept", "optional", "both"} {
			NewProp(s, uint64(i+1), name, mapType, nil)
		}
		flags := []PropFlag{0, KeepDictionaryZeroValues, PresenceIsSignificant, KeepDictionaryZeroValues | PresenceIsSignificant}
		m := NewModel(s, e, func(b *ModelBuilder) {
			for i, f := range flags {
				b.Prop(s.MustPropByCode(uint64(i+1)), f)
			}
		})
		for i, want := range flags {
			if m.PropFlags(uint64(i+1)) != want {
				t.Fatal("property policy leaked through shared Map type")
			}
		}
	}
	for _, typ := range []AnyType{TUint64, TMachineString, TBytes, ListOf(TUint64), NewEntityType(NewSchema(), "object")} {
		t.Run(typ.Name(), func(t *testing.T) {
			defer func() {
				if err, ok := recover().(error); !ok || !errors.Is(err, ErrPropOption) {
					t.Fatalf("panic = %v", err)
				}
			}()
			s := NewSchema()
			e := NewEntityType(s, "invalid")
			NewProp(s, 1, "value", typ, nil)
			NewModel(s, e, func(b *ModelBuilder) { b.Prop(s.MustPropByCode(1), KeepDictionaryZeroValues) })
		})
	}
}

func TestNativeDeclarationErrors(t *testing.T) {
	cases := []any{
		struct {
			A *struct{} `kvo:",inline"`
		}{},
		struct {
			A int64 `kvo:",inline"`
		}{},
		struct {
			A struct {
				Count int64 `kvo:"count"`
			} `kvo:",inline"`
			B int64 `kvo:"count"`
		}{},
		struct{ Count int64 }{},
		struct {
			Count int64 `edb:""`
		}{},
		struct {
			Count int64 `edb:",compact"`
		}{},
		struct {
			Count int64 `edb:"missing"`
		}{},
		struct {
			Count int64 `edb:"not_in_model"`
		}{},
		struct {
			Count int64 `edb:"count,omitempty"`
		}{},
		struct {
			Count int64 `edb:"count,"`
		}{},
		struct {
			Count int64 `edb:"count,compact"`
		}{},
		struct {
			Count int64 `edb:"-,compact"`
		}{},
		struct {
			count int64 `edb:"count"`
		}{},
		struct {
			A int64 `edb:"count"`
			B int64 `edb:"count"`
		}{},
	}
	for _, c := range cases {
		t.Run(reflect.TypeOf(c).String(), func(t *testing.T) {
			defer func() {
				e, ok := recover().(error)
				if !ok || !errors.Is(e, ErrNativeMapping) {
					t.Fatalf("panic = %v", e)
				}
			}()
			nativeTestModel(reflect.TypeOf(c))
		})
	}
	for _, types := range [][]reflect.Type{{nil}, {reflect.TypeFor[int]()}, {reflect.TypeFor[*struct{}]()}, {reflect.TypeFor[struct{}](), reflect.TypeFor[struct{}]()}} {
		func() {
			defer func() {
				if e, ok := recover().(error); !ok || !errors.Is(e, ErrNativeMapping) {
					t.Fatalf("panic = %v", e)
				}
			}()
			nativeTestModel(types...)
		}()
	}
	func() {
		defer func() {
			if e, ok := recover().(error); !ok || !errors.Is(e, ErrNativeMapping) {
				t.Fatalf("panic = %v", e)
			}
		}()
		nativeTestModel().NativeStruct(reflect.TypeFor[struct{}]())
	}()
}

func TestNativeSchemaKinds(t *testing.T) {
	if TInt64.NativeType().Kind() != reflect.Int64 || TUint64.NativeType().Kind() != reflect.Uint64 || TBool.NativeType().Kind() != reflect.Bool {
		t.Fatal("lost scalar semantics")
	}
	if TMachineString.NativeType().Kind() != reflect.String || TBytes.NativeType() != reflect.TypeFor[[]byte]() {
		t.Fatal("span semantics")
	}
	typ := ListOf(TInt64)
	if typ.ValueKind() != ValueKindList || typ.ItemType() != TInt64 || typ.MapKeyType() != nil || typ.Name() != "[]int64" {
		t.Fatal("list declaration")
	}
}

func TestNativeRegistrationAfterModel(t *testing.T) {
	type row struct {
		Count int64 `kvo:"count"`
	}
	type invalid struct {
		Count int64 `kvo:"missing"`
	}
	s := NewSchema()
	count := NewSignedProp[int64](s, 1, "count", TInt64)
	m := NewModel(s, NewEntityType(s, "late"), func(b *ModelBuilder) { b.Prop(count) })
	m.RegisterNativeType(reflect.TypeFor[row]())
	r := Project(reflect.TypeFor[row](), m, nil).Record(reflect.ValueOf(&row{Count: 42}))
	if got := r.GetValue(1).AsInt(); got != 42 {
		t.Fatalf("count = %d", got)
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[row](), reflect.TypeFor[invalid](), reflect.TypeFor[int]()} {
		func() {
			defer func() {
				err, _ := recover().(error)
				if !errors.Is(err, ErrNativeMapping) {
					t.Fatalf("registration error = %v", err)
				}
			}()
			m.RegisterNativeType(typ)
		}()
	}
	if got := m.Type().(*EntityType).NativeTypes(); len(got) != 1 || got[0] != reflect.TypeFor[row]() {
		t.Fatal(got)
	}
	if got := r.GetValue(1).AsInt(); got != 42 {
		t.Fatal(got)
	}
}

func TestNativeRegistrationFieldMap(t *testing.T) {
	type tagged struct {
		Count  int64  `kvo:"count" msgpack:"n"`
		Hidden string `kvo:"-" msgpack:"hidden"`
	}
	type untagged struct {
		Number int64  `msgpack:"n"`
		Hidden string `msgpack:"hidden"`
	}
	s := NewSchema()
	count := NewSignedProp[int64](s, 1, "sample:count", TInt64)
	m := NewModel(s, NewEntityType(s, "mapped"), func(b *ModelBuilder) { b.Prop(count) })
	m.RegisterNativeType(reflect.TypeFor[tagged](), nil)
	fields := map[PropCode]string{1: "Number"}
	m.RegisterNativeType(reflect.TypeFor[untagged](), fields)
	fields[1] = "Hidden"
	for _, row := range []any{&tagged{42, "keep"}, &untagged{42, "keep"}} {
		typ := reflect.TypeOf(row).Elem()
		t.Run(typ.Name(), func(t *testing.T) {
			p := Project(typ, m, nil)
			if p != Project(typ, m, nil) || !p.DirectMessagePack() {
				t.Fatal("registered projection is not shared or direct")
			}
			for _, r := range []Record{p.Record(reflect.ValueOf(row)), p.MessagePack(msgpack.Encode(nil, reflect.ValueOf(row)))} {
				if got := r.Get(count); got != 42 {
					t.Fatal(got)
				}
				if n := testing.AllocsPerRun(100, func() { _ = r.Get(count) }); n != 0 {
					t.Fatalf("read allocates: %g", n)
				}
				e := r.Edit()
				e.Set(count, 73)
				if got := e.Record().Get(count); got != 73 {
					t.Fatal(got)
				}
				if raw := e.Record().MessagePack(); raw != nil {
					dst := reflect.New(typ)
					if err := msgpack.TryDecode(raw, dst); err != nil {
						t.Fatal(err)
					}
					if dst.Elem().FieldByName("Hidden").String() != "keep" {
						t.Fatal("unmapped field changed")
					}
				}
			}
		})
	}
}

func TestNativeRegistrationInvalidFieldMap(t *testing.T) {
	type row struct {
		A, B   int64
		hidden int64
	}
	for _, fields := range []map[PropCode]string{
		{}, {1: "A"}, {1: "A", 3: "B"}, {1: "A", 2: "A"},
		{1: "A", 2: "Missing"}, {1: "A", 2: "hidden"},
	} {
		m := nativeTestModel()
		func() {
			defer func() {
				if err, _ := recover().(error); !errors.Is(err, ErrNativeMapping) {
					t.Fatalf("error = %v", err)
				}
			}()
			m.RegisterNativeType(reflect.TypeFor[row](), fields)
		}()
		if len(m.Type().(*EntityType).NativeTypes()) != 0 || len(m.nativeStructs) != 0 {
			t.Fatal("failed registration changed model")
		}
	}
}
