package edb

import (
	"reflect"
	"testing"
)

func TestRawRange_ConstructorsAndModifiers(t *testing.T) {
	l := []byte{1}
	u := []byte{2}

	_ = RawOO()
	_ = RawIO(l)
	_ = RawEO(l)
	_ = RawOI(u)
	_ = RawOE(u)
	_ = RawII(l, u)
	_ = RawIE(l, u)
	_ = RawEI(l, u)
	_ = RawEE(l, u)

	r := RawPrefix([]byte{9})
	if r.Prefix == nil || len(r.Prefix) != 1 || r.Prefix[0] != 9 {
		t.Fatalf("RawPrefix returned unexpected range: %+v", r)
	}
	r2 := r.Prefixed([]byte{8}).Reversed()
	if !r2.Reverse || len(r2.Prefix) != 1 || r2.Prefix[0] != 8 {
		t.Fatalf("Prefixed/Reversed returned unexpected range: %+v", r2)
	}
}

func TestSchemaIncludeAndReflectType(t *testing.T) {
	var a, b Schema
	a.Include(&b)

	if reflectType(reflect.TypeOf(&User{})) == nil {
		t.Fatalf("reflectType returned nil")
	}
}

