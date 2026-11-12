package kvo

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// All three benchmark entry points call the fast path setter. Cached deliberately
// pays a separate lookup, preserving the comparison with precomputed options.
// BenchmarkEditorPropertyAccess measures the public direct setter separately.
func SetExplicitOptionsForTest(e *Editor, key uint64, options row2.KeyAccessOptions, value values.Value) {
	e.FastSetPathValue([]row2.Step{{Key: key, Options: options}}, value)
}

func SetCachedOptionsForTest(e *Editor, key uint64, value values.Value) {
	e.FastSetPathValue([]row2.Step{{Key: key, Options: e.view.propertyWriteOptions(key)}}, value)
}

func SetSchemaOptionsForTest(e *Editor, key uint64, value values.Value) {
	e.FastSetPathValue([]row2.Step{{Key: key, Options: WriteOptions(e.Model().PropFlags(key))}}, value)
}

func TestEditorWritePolicy(t *testing.T) {
	s := NewSchema()
	key := NewProp(s, 1, "key", TUint64, nil)
	zero := NewProp(s, 2, "zero", TUint64, nil)
	child := NewEntityType(s, "child")
	NewModel(s, child, func(b *ModelBuilder) { b.Prop(s.MustPropByCode(zero)) })
	nested := NewProp(s, 3, "nested", child, nil)
	model := NewModel(s, NewEntityType(s, "root"), func(b *ModelBuilder) {
		b.Prop(s.MustPropByCode(key), PrimaryKey)
		b.Prop(s.MustPropByCode(zero), PresenceIsSignificant)
		b.Prop(s.MustPropByCode(nested), Compact|PresenceIsSignificant)
	})
	typedZero := BindUnsignedProp[uint64](s, zero)
	for _, layout := range []row2.Layout{row2.Fast2, row2.Compact2} {
		for _, decoded := range []bool{false, true} {
			r := Encoded(row2.Empty(layout)).WithModel(model)
			if decoded {
				r = r.Fastest()
			}
			e := r.Edit()
			e.SetValue(key, values.Uint(0))
			e.Set(typedZero, uint64(0))
			if !e.Snapshot().Has(key) || !e.Snapshot().Has(zero) {
				t.Fatal("model defaults lost present zeros")
			}
			e.FastSetPathValue([]row2.Step{{Key: zero}}, values.Uint(0))
			if e.Snapshot().Has(zero) {
				t.Fatal("explicit path merged model defaults")
			}
			e.SetValue(zero, values.Uint(3))
			if e.GetValue(zero).AsUint() != 3 || e.GetPathValue([]row2.Step{{Key: zero}}).AsUint() != 3 {
				t.Fatal("direct and path reads disagree")
			}
			c := e.newChild(nested)
			if c.Snapshot().Layout() != row2.Compact2 || !e.Snapshot().Has(nested) {
				t.Fatal("cached defaults lost compact present child")
			}
			e.Delete(s.MustPropByCode(zero))
			if e.Snapshot().Has(zero) {
				t.Fatal("deletion retained a present zero")
			}
			if err := e.TrySetValue(999, values.Uint(1)); err == nil {
				t.Fatal("unknown property accepted")
			}
			if err := e.TryFastSetPathValue(nil, values.Uint(1)); !errors.Is(err, row2.ErrKind) {
				t.Fatalf("empty path error = %v", err)
			}
		}
	}
	// The cache occupies existing padding, not extra per-field storage.
	if size := unsafe.Sizeof(modelField{}); size != 40 {
		t.Fatalf("model field size = %d", size)
	}
}
