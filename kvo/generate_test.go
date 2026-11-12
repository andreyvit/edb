package kvo_test

import (
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/andreyvit/edb/kvo"
)

type canonicalBindingRow struct {
	Flags    kvo.PropFlag               `kvo:"flags"`
	Optional kvo.Optional[kvo.PropFlag] `kvo:"optional"`
	Time     time.Time                  `kvo:"time"`
}

func TestGeneratedBindingCanonicalImports(t *testing.T) {
	s := kvo.NewSchema()
	flagType := kvo.NewScalarSubtype[kvo.PropFlag]("flags", kvo.TInt)
	flags := kvo.NewSignedProp[kvo.PropFlag](s, 1, "flags", flagType)
	optional := kvo.NewSignedProp[kvo.PropFlag](s, 2, "optional", flagType)
	at := kvo.NewLegacyTimeProp(s, 3, "time")
	m := kvo.NewModel(s, kvo.NewEntityType(s, "canonical", reflect.TypeFor[canonicalBindingRow]()), func(b *kvo.ModelBuilder) {
		b.Prop(flags)
		b.Prop(optional, kvo.PresenceIsSignificant)
		b.Prop(at)
	})
	source := kvo.Bind(reflect.TypeFor[canonicalBindingRow](), m).Generate("kvo_test", "Static")
	f, err := parser.ParseFile(token.NewFileSet(), "bindings.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	imports := make(map[string]int)
	for _, spec := range f.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		imports[path]++
		if imports[path] != 1 {
			t.Fatalf("duplicate import %q: schema types, optional values, and timestamps must share it", path)
		}
	}
	if imports["github.com/andreyvit/edb/kvo"] != 1 || imports["github.com/andreyvit/edb/kvo/model"] != 0 || imports["github.com/andreyvit/edb/kvo/record"] != 0 {
		t.Fatalf("noncanonical imports: %v", imports)
	}
}
