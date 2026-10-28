package kvolab

import (
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
)

func checkValue(t *testing.T, want Value, got Cell) {
	t.Helper()
	kind := got.Kind
	if kind == IndexedRecord || kind == OffsetRecord {
		kind = Record
	}
	if kind == IndexedDict || kind == OffsetDict {
		kind = Dict
	}
	if kind != want.Kind {
		t.Fatalf("kind got %d want %d", kind, want.Kind)
	}
	switch kind {
	case Uint, Int, Float:
		if got.U != want.U {
			t.Fatalf("value got %d want %d", got.U, want.U)
		}
	case Text:
		if string(got.Bytes()) != want.S {
			t.Fatal("text mismatch")
		}
	case Blob:
		if !reflect.DeepEqual(got.Bytes(), want.B) {
			t.Fatal("blob mismatch")
		}
	case Record:
		for _, f := range want.Fields {
			c, ok := got.Object().Get(f.Key)
			if !ok {
				t.Fatalf("missing %d", f.Key)
			}
			checkValue(t, f.Value, c)
		}
		count := 0
		got.Object().Walk(func(k uint64, c Cell) bool { count++; return true })
		if count != len(want.Fields) {
			t.Fatal("count")
		}
	case Dict:
		for _, f := range want.Entries {
			c, ok := got.Object().GetString(f.Key)
			if !ok {
				t.Fatalf("missing %q", f.Key)
			}
			checkValue(t, f.Value, c)
		}
	case List:
		_, b := payload(got.Raw)
		n, b := takeU(b)
		if int(n) != len(want.Items) {
			t.Fatal("list count")
		}
		for _, item := range want.Items {
			c, rest := readValue(b)
			checkValue(t, item, c)
			b = rest
		}
		if len(b) != 0 {
			t.Fatal("trailing list bytes")
		}
	}
}
func TestRoundTrip(t *testing.T) {
	for name, v := range fixtures() {
		for _, p := range []Policy{Compact, Indexed, Offsets} {
			t.Run(name+string(rune('0'+p)), func(t *testing.T) {
				data := Encode(v, p)
				c, rest := readValue(data)
				if len(rest) != 0 {
					t.Fatal("trailing")
				}
				checkValue(t, v, c)
			})
		}
	}
}
func TestSemantics(t *testing.T) {
	v := Obj(Field{0, U(0)}, Field{1, Value{Kind: Null}}, Field{2, Value{Kind: False}}, Field{3, Value{Kind: True}}, Field{4, S("")}, Field{5, Array()}, Field{6, Map()}, Field{7, I(math.MinInt64)}, Field{8, I(-1)}, Field{9, U(math.MaxUint64)}, Field{10, Value{Kind: Float, U: math.Float64bits(math.Inf(1))}}, Field{math.MaxUint64, Map(Entry{"", S("empty key")}, Entry{"a\x00b", S("NUL")}, Entry{"é", S("unicode")})})
	for _, p := range []Policy{Compact, Indexed, Offsets} {
		data := Encode(v, p)
		c, _ := readValue(data)
		checkValue(t, v, c)
		if _, ok := View(data).Get(11); ok {
			t.Fatal("missing is present")
		}
		if _, ok := View(data).Get(math.MaxUint64 - 1); ok {
			t.Fatal("missing high key")
		}
	}
}
func TestMixedChildren(t *testing.T) {
	v := fixtures()["state"]
	for _, root := range []Policy{Compact, Indexed, Offsets} {
		for _, children := range []Policy{Compact, Indexed, Offsets} {
			c, _ := readValue(EncodeMixed(v, root, children))
			checkValue(t, v, c)
		}
	}
}
func TestRandomNumeric(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 9))
	for trial := 0; trial < 200; trial++ {
		v := Obj()
		var key uint64
		for i := 0; i < trial%100; i++ {
			key += r.Uint64N(20) + 1
			v.Fields = append(v.Fields, Field{key, U(r.Uint64())})
		}
		for _, p := range []Policy{Compact, Indexed, Offsets} {
			data := Encode(v, p)
			c, _ := readValue(data)
			checkValue(t, v, c)
		}
	}
}
func TestOwnedMutation(t *testing.T) {
	v := numeric(32, 0)
	base := Encode(v, Indexed)
	owned := append([]byte(nil), base...)
	if !View(owned).SetExisting(17, math.MaxUint64) {
		t.Fatal("missing")
	}
	c, _ := View(owned).Get(17)
	if c.U != math.MaxUint64 {
		t.Fatal("write")
	}
	original, _ := View(base).Get(17)
	if original.U == c.U {
		t.Fatal("base mutated")
	}
	if View(owned).SetExisting(99, 1) {
		t.Fatal("insert unsupported")
	}
}
func TestLayout(t *testing.T) {
	t.Logf("view=%d cell=%d immutableMap=%d mutableMap=%d immutableRecord=%d", unsafe.Sizeof(View{}), unsafe.Sizeof(Cell{}), unsafe.Sizeof(kvo.ImmutableMap{}), unsafe.Sizeof(kvo.MutableMap{}), unsafe.Sizeof(kvo.ImmutableRecord{}))
}
func TestZeroAllocationReads(t *testing.T) {
	for _, p := range []Policy{Compact, Indexed, Offsets} {
		v := View(Encode(fixtures()["state"], p))
		allocs := testing.AllocsPerRun(1000, func() {
			c, _ := v.Get(57)
			d, _ := c.Object().Get(5)
			e, _ := d.Object().GetString("key-0003")
			if len(e.Bytes()) == 0 {
				panic("empty")
			}
		})
		if allocs != 0 {
			t.Fatalf("policy %d allocations %g", p, allocs)
		}
	}
}
