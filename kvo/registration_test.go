package kvo

import (
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

func TestDescriptorPublication(t *testing.T) {
	var table descriptorTable[uint64]
	shape := &values.Shape{Type: values.NewType("publication", values.Pair), Kind: values.TypedPairKind, Cells: 1}
	var first [1]values.RepresentationID
	table.register(shape, []uint64{42}, first[:])
	original := &(*table.entries.Load())[first[0]]
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for i := range 32 {
				// Another registry can consume IDs between our publications.
				values.RegisterRepresentation(shape)
				var ids [2]values.RepresentationID
				table.register(shape, []uint64{uint64(i + 1), uint64(i + 2)}, ids[:])
				entries := *table.entries.Load()
				if entries[ids[0]] != uint64(i+1) || entries[ids[1]] != uint64(i+2) || ids[0].Shape() != shape || *original != 42 {
					t.Error("published descriptor changed or was lost during growth")
				}
			}
		})
	}
	workers.Wait()
	t.Logf("record descriptor %d B; collection descriptor %d B; registry header %d B", unsafe.Sizeof(recordRepresentation{}), unsafe.Sizeof(collectionRepresentation{}), unsafe.Sizeof(table))
}

func TestBindingMetadataReuse(t *testing.T) {
	type row struct {
		Name string `kvo:"name"`
	}
	schema := NewSchema()
	name := NewStringProp[string](schema, 1, "name", TMachineString)
	model := NewModel(schema, NewEntityType(schema, "row", reflect.TypeFor[row]()), func(b *ModelBuilder) { b.Prop(name) })
	b := Bind(reflect.TypeFor[row](), model)
	p := Project(reflect.TypeFor[row](), model, nil)
	data := reflect.ValueOf(&row{Name: "kept"})
	id := b.Record(data).Value().Representation()
	projectionID := p.Record(data).Value().Representation()
	before := len(*recordRepresentations.entries.Load())
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			for range 16 {
				x := Bind(reflect.TypeFor[row](), model)
				y := Project(reflect.TypeFor[row](), model, nil)
				if x != b || y != p || x.Record(data).Value().Representation() != id || y.Record(data).Value().Representation() != projectionID || x.Record(data).Get(name) != "kept" {
					t.Error("repeated binding created different metadata")
				}
			}
		})
	}
	workers.Wait()
	if after := len(*recordRepresentations.entries.Load()); after != before {
		t.Fatalf("repeated binding grew descriptors from %d to %d", before, after)
	}
	if n := testing.AllocsPerRun(100, func() { Bind(reflect.TypeFor[row](), model); Project(reflect.TypeFor[row](), model, nil) }); n != 0 {
		t.Fatalf("reusing binding metadata allocated %g objects", n)
	}
}
