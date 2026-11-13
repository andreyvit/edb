package edb

import (
	"reflect"
	"testing"
)

// The key and the value must use the same T, including in explicitly instantiated
// APIs. The usual call sites infer T from the declared key.
var (
	_ func(Txish, *SKey[int], *int) bool = SGet[int]
	_ func(Txish, *SKey[int], *int)      = SPut[int]
)

func TestSingletonValueTypes(t *testing.T) {
	type count int
	type settings struct {
		Name string
		IDs  []uint64
	}
	t.Run("named scalar", func(t *testing.T) {
		testSingletonRoundTrip(t, count(42))
	})
	t.Run("struct", func(t *testing.T) {
		testSingletonRoundTrip(t, settings{"customer import", []uint64{1, 3}})
	})
	t.Run("pointer", func(t *testing.T) {
		testSingletonRoundTrip(t, &settings{"customer import", []uint64{1, 3}})
	})
	t.Run("map", func(t *testing.T) {
		testSingletonRoundTrip(t, map[string]int{"USD": 1, "EUR": 2})
	})
	t.Run("bytes", func(t *testing.T) {
		testSingletonRoundTrip(t, []byte{0, 1, 255})
	})
	if reflect.TypeFor[*SKey[int]]().ConvertibleTo(reflect.TypeFor[*SKey[string]]()) {
		t.Fatal("singleton key can be converted to a different value type")
	}
}

func testSingletonRoundTrip[T any](t *testing.T, want T) {
	t.Helper()
	schema := new(Schema)
	sk := AddSingletonKey[T](AddKVMap(schema, "settings"), "value")
	if sk.ValueType() != reflect.TypeFor[T]() || sk.String() != "settings.value" || string(sk.Raw()) != "value" {
		t.Fatalf("incorrect singleton declaration: %v, type %v, key %q", sk, sk.ValueType(), sk.Raw())
	}
	db := setup(t, schema)
	db.Read(func(tx *Tx) {
		var got T
		if SGet(tx, sk, &got) {
			t.Fatal("undeclared value found")
		}
	})
	db.Write(TxMetadata{}, func(tx *Tx) { SPut(tx, sk, &want) })
	db.Read(func(tx *Tx) {
		var got T
		if !SGet(tx, sk, &got) || !reflect.DeepEqual(got, want) {
			t.Fatalf("singleton round trip = %#v, want %#v", got, want)
		}
	})
}

func TestSingletonDuplicateRegistration(t *testing.T) {
	mp := AddKVMap(new(Schema), "settings")
	first := AddSingletonKey[int](mp, "value")
	assertRegistrationPanic(t, "singleton settings.value is already defined with value type int", func() {
		AddSingletonKey[string](mp, "value")
	})
	assertRegistrationPanic(t, "singleton settings.value is already defined with value type int", func() {
		AddSingletonKey[int](mp, "value")
	})
	if first.ValueType() != reflect.TypeFor[int]() || len(mp.singletonTypes) != 1 {
		t.Fatal("rejected registration changed the existing declaration")
	}
	AddSingletonKey[string](mp, "other")
	AddSingletonKey[string](AddKVMap(new(Schema), "other_settings"), "value")
}
