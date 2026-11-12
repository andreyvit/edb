package evmbind

import (
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/andreyvit/edb/values"
)

func TestScalarAliasKinds(t *testing.T) {
	s, err := parse("aliases", fstest.MapFS{"api.go": {Data: []byte(`package aliases
type Row struct { Byte byte; Rune rune }
func Echo(b byte, r rune) (byte, rune) { panic("host") }
`)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		goType    reflect.Type
		valueType *values.Type
	}{
		{"byte", reflect.TypeFor[byte](), values.Uint8},
		{"rune", reflect.TypeFor[rune](), values.Int32},
	} {
		n := s.nodes[tc.name]
		if n == nil {
			t.Fatalf("missing node for %s", tc.name)
		}
		if got := nativeKind(n); got != tc.goType.Kind() {
			t.Errorf("%s generated conversion uses %s, want %s", tc.name, got, tc.goType.Kind())
		}
		if got := nativeType(n, nil, make(map[*node]reflect.Type)); got != tc.goType || n.shape.Type != tc.valueType {
			t.Errorf("%s reflection and VM scalar types disagree", tc.name)
		}
	}
}
