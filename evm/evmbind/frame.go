package evmbind

import (
	"bytes"
	"encoding/json"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
)

type objectKey struct {
	p    unsafe.Pointer
	name string
}
type Frame struct {
	*evm.Call
	Host, Receiver any
	s              *Schema
	objects        map[objectKey]any
	origins        map[any]unsafe.Pointer
	writes         []func()
}

func (f *Frame) New(cells uint64) unsafe.Pointer {
	p, err := f.AllocCells(cells)
	if err != nil {
		panic(err)
	}
	return p
}
func (f *Frame) Remember(name string, p unsafe.Pointer, x any, write func()) {
	if f.objects == nil {
		f.objects = map[objectKey]any{}

	}
	f.objects[objectKey{p, name}] = x
	f.SetOrigin(x, p)
	f.writes = append(f.writes, write)
}

// SetOrigin preserves identity when several result slots refer to a newly returned pointer.
func (f *Frame) SetOrigin(x any, p unsafe.Pointer) {
	if f.origins == nil {
		f.origins = make(map[any]unsafe.Pointer)
	}
	f.origins[x] = p
}
func (f *Frame) String(x string) values.Value {
	if err := f.ChargeAllocation(uint64(len(x))); err != nil {
		panic(err)
	}
	return values.String(x)
}

// Strings and JSONObject copy read-only host arguments. Host mutations of the
// copy do not change the script's collection. JSON numbers retain their text.
func (f *Frame) Strings(v values.Value) []string {
	if v.P == nil {
		return nil
	}
	n := uint64(v.SliceLen())
	if err := f.ChargeAllocation(16 * n); err != nil {
		panic(err)
	}
	out := make([]string, n)
	for i, cell := range values.CellSlice(v.P, n) {
		out[i] = cell.AsString()
	}
	return out
}
func (f *Frame) JSONObject(v values.Value) map[string]any {
	// Limit materialization even if a small script map references a large tree.
	data, err := f.Call.EncodeJSONObject(v, 1<<20)
	if err != nil {
		panic(err)
	}
	if err := f.ChargeAllocation(uint64(len(data)) * 8); err != nil {
		panic(err)
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var out map[string]any
	if err = d.Decode(&out); err != nil {
		panic(err)
	}
	return out
}

// Callback borrows a guest func() until the external invocation returns.
func (f *Frame) Callback(v values.Value) func() {
	if v.R == 0 {
		return nil
	}
	return func() {
		if f.Call == nil {
			panic("guest callback used after its external call returned")
		}
		if err := f.Call.RunCallback(v); err != nil {
			panic(err)
		}
	}
}
func (f *Frame) Object(name string, p unsafe.Pointer) any { return f.objects[objectKey{p, name}] }
func (f *Frame) Origin(x any) unsafe.Pointer              { return f.origins[x] }
func (f *Frame) DecodeInterface(v values.Value) any {
	x, err := f.s.NativeInterface(f.Program(), v)
	if err != nil {
		panic(err)
	}
	return x
}
func (f *Frame) EncodeInterface(x any) values.Value {
	v, e := f.s.Interface(f.Program(), x)
	if e != nil {
		panic(e)
	}
	return v
}

// CopyBack applies pending VM-cell pointer mutations after a successful call.
// Native addresses do not enqueue copies.
func (f *Frame) CopyBack() {
	for _, write := range f.writes {
		write()
	}
}
