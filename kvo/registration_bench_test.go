package kvo_test

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
)

var bindingSink *kvo.Binding

func BenchmarkBindingReuse(b *testing.B) {
	_, model := statrow.Model()
	// Compilation itself does not allocate representation IDs. Opening a record
	// does, so exclude it from this benchmark of repeated metadata construction.
	bindingSink = kvo.Bind(reflect.TypeFor[statrow.State](), model)
	b.ReportAllocs()
	for b.Loop() {
		bindingSink = kvo.Bind(reflect.TypeFor[statrow.State](), model)
	}
}
