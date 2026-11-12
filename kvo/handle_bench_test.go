package kvo_test

import (
	"context"
	"reflect"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

const handleRows = 256

type handleFixture struct {
	name string
	open func(int) kvo.Record
	rows [handleRows]kvo.Record
}

// Fixtures vary by row and share registration metadata. The database key is
// omitted from the payload and from Record, as in the production no-PK rollout.
// Fixture encoding and model compilation are outside the timed operations.
func recordHandleFixtures(b testing.TB) (*kvo.Schema, *kvo.Model, []handleFixture) {
	b.Helper()
	schema, model := statrow.Model()
	ref := kvo.Bind(reflect.TypeFor[statrow.State](), model)
	gen := ref.WithGenerated(&statrow.Static, nativebind.StaticOnly)
	var fast, compact [handleRows]row2.View
	var mp [handleRows][]byte
	var native [handleRows]reflect.Value
	var memory [handleRows]kvo.Record
	for i := range handleRows {
		src := statrow.Sample(uint64(i + 1))
		native[i] = reflect.ValueOf(src)
		fast[i] = row2.Trusted(gen.Encode(nil, native[i].Elem(), row2.Fast2))
		compact[i] = row2.Trusted(gen.Encode(nil, native[i].Elem(), row2.Compact2))
		var err error
		mp[i], err = msgpack.TryEncode(nil, native[i])
		if err != nil {
			b.Fatal(err)
		}
		memory[i] = kvo.Encoded(fast[i]).WithModel(model).Fastest()
	}
	fixtures := []handleFixture{
		{name: "Fast2", open: func(i int) kvo.Record {
			return gen.Encoded(fast[i])
		}},
		{name: "Compact2", open: func(i int) kvo.Record {
			return gen.Encoded(compact[i])
		}},
		{name: "MessagePack", open: func(i int) kvo.Record {
			return gen.MessagePack(mp[i])
		}},
		{name: "Generated", open: func(i int) kvo.Record { return gen.Record(native[i]) }},
		{name: "Reflected", open: func(i int) kvo.Record { return ref.Record(native[i]) }},
		{name: "Memory", open: func(i int) kvo.Record { return memory[i] }},
	}
	for j := range fixtures {
		f := &fixtures[j]
		for i := range f.rows {
			f.rows[i] = f.open(i)
			if got := f.rows[i].GetValue(1).AsUint(); got != uint64(i+1) {
				b.Fatalf("%s row %d: ID = %d", f.name, i, got)
			}
		}
		if f.name == "Generated" || f.name == "Reflected" {
			if got := f.rows[13].Fastest().Native().(*statrow.State).Key; got != 14 {
				b.Fatalf("%s: primary key = %d", f.name, got)
			}
		}
	}
	return schema, model, fixtures
}

// These boundaries intentionally cannot inline: they model an opaque indexer
// or host callback. PointerBoundary is NOT a pointer-receiver implementation;
// the current Record.Get still takes a value receiver inside the helper.
//
//go:noinline
func recordValueBoundary(r kvo.Record, key uint64) uint64 { return r.GetValue(key).AsUint() }

//go:noinline
func recordPointerBoundary(r *kvo.Record, key uint64) uint64 { return r.GetValue(key).AsUint() }

// BenchmarkRecordHandle is a production-API baseline, independent of any proposed
// replacement. Each operation processes one row. The working set avoids relying
// on one permanently hot record. Retain keeps independent results alive; it must
// not be optimized into a cursor slot that overwrites earlier records.
func BenchmarkRecordHandle(b *testing.B) {
	_, _, fixtures := recordHandleFixtures(b)
	for _, f := range fixtures {
		for _, workload := range []string{"Read", "ValueBoundary", "PointerBoundary", "FieldsAndChild", "OpenRead", "Retain", "Fastest"} {
			b.Run(f.name+"/"+workload, func(b *testing.B) {
				retained := make([]kvo.Record, handleRows)
				var sum uint64
				b.ReportAllocs()
				b.ResetTimer()
				switch workload {
				case "Read":
					for i := 0; i < b.N; i++ {
						sum += f.rows[i&(handleRows-1)].GetValue(1).AsUint()
					}
				case "ValueBoundary":
					for i := 0; i < b.N; i++ {
						sum += recordValueBoundary(f.rows[i&(handleRows-1)], 1)
					}
				case "PointerBoundary":
					for i := 0; i < b.N; i++ {
						sum += recordPointerBoundary(&f.rows[i&(handleRows-1)], 1)
					}
				case "FieldsAndChild":
					for i := 0; i < b.N; i++ {
						r := f.rows[i&(handleRows-1)]
						for _, key := range [...]uint64{1, 3, 11, 12, 19, 20, 21} {
							sum += r.GetValue(key).AsUint()
						}
						child := r.Child(row2.Step{Key: 9})
						sum += child.GetValue(5).AsUint() + child.GetValue(7).AsUint()
					}
				case "OpenRead":
					for i := 0; i < b.N; i++ {
						r := f.open(i & (handleRows - 1))
						sum += r.GetValue(1).AsUint()
					}
				case "Retain":
					for i := 0; i < b.N; i++ {
						j := i & (handleRows - 1)
						retained[j] = f.open(j)
						sum += retained[j].GetValue(1).AsUint()
					}
				case "Fastest":
					for i := 0; i < b.N; i++ {
						j := i & (handleRows - 1)
						retained[j] = f.open(j).Fastest()
						sum += retained[j].GetValue(1).AsUint()
					}
				}
				b.StopTimer()
				if workload == "Retain" || workload == "Fastest" {
					for i := 0; i < min(b.N, handleRows); i++ {
						if got := retained[i].GetValue(1).AsUint(); got != uint64(i+1) {
							b.Fatalf("retained row %d changed: %d", i, got)
						}
					}
				}
				accessSum = sum
				b.ReportMetric(float64(unsafe.Sizeof(kvo.Record{})), "handle-B")
				b.ReportMetric(float64(unsafe.Sizeof(kvo.Editor{})), "editor-B")
			})
		}
	}
}

// Measure the real host-result path and guest property dispatch, both with a
// reusable call-site token and with rows retained in a guest slice. Each sample
// opens/closes a scope so escaping results cannot accumulate between iterations.
func BenchmarkRecordHandleEVM(b *testing.B) {
	schema, model, fixtures := recordHandleFixtures(b)
	const script = `import "handles"
func Scan() uint64 {
 var sum uint64
 for i := uint64(0); i < 256; i++ { r := handles.Lookup(i); sum += r.id + r.group }
 return sum
}
func Retain() uint64 {
 rows := make([]*handles.Customer, 256)
 for i := uint64(0); i < 256; i++ { rows[i] = handles.Lookup(i) }
 var sum uint64
 for i := 0; i < 256; i++ { sum += rows[i].id + rows[i].group }
 return sum
}`
	for _, f := range fixtures {
		pkg, err := evm.ModelPackage("handles", schema)
		if err != nil {
			b.Fatal(err)
		}
		rowType := pkg.Record(model, false)
		pkg.Functions = append(pkg.Functions, evm.ExternalFunction{
			Ownership: &evm.OwnershipEffects{NoRetain: true, NoInvalidate: true},
			Name:      "Lookup", Signature: evm.Signature{Params: []*values.Type{values.Uint64}, Results: []*values.Type{rowType.Pointer}},
			Call: func(c *evm.Call) error { return c.SetRecordResult(model, f.open(int(c.Argument(0).R))) },
		})
		module, err := evmcompiler.Compile("handles.go", script, pkg)
		if err != nil {
			b.Fatal(err)
		}
		program, err := evm.Link(module, pkg)
		if err != nil {
			b.Fatal(err)
		}
		var want uint64
		for i := range handleRows {
			want += uint64(i+1) + 1 + uint64(i+1)%23
		}
		for _, entry := range []string{"Scan", "Retain"} {
			b.Run(f.name+"/"+entry, func(b *testing.B) {
				vm := evm.New(program)
				// The VM's allocation accounting spans runs; benchmark iteration
				// counts must not eventually trip a production execution budget.
				vm.Limits.Allocations = ^uint64(0)
				vm.Limits.AllocationBytes = ^uint64(0)
				var out [1]values.Value
				run := func() {
					err := vm.Run(context.Background(), entry, nil, out[:])
					if err != nil || out[0].R != want {
						b.Fatalf("%s: got %d, %v; want %d", entry, out[0].R, err, want)
					}
				}
				run()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run()
				}
				b.ReportMetric(handleRows, "rows/op")
			})
		}
	}
}
