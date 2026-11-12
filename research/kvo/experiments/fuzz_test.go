package kvolab

import (
	"math"
	"testing"
)

func FuzzTreeRoundTrip(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{255, 0, 128, 17, 93})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > 2048 {
			return
		}
		pos := 0
		next := func() byte { v := data[pos%len(data)]; pos++; return v }
		var build func(int) Value
		build = func(depth int) Value {
			k := next() % 10
			if depth > 3 {
				k %= 7
			}
			switch k {
			case 0:
				return U(uint64(next())<<56 | uint64(next()))
			case 1:
				return I(int64(int8(next())))
			case 2:
				return S(string([]byte{next(), next(), next()}))
			case 3:
				return Value{Kind: Null}
			case 4:
				return Value{Kind: False}
			case 5:
				return Value{Kind: Float, U: math.Float64bits(float64(next()) / 3)}
			case 6:
				return Value{Kind: Blob, B: []byte{next(), next()}}
			case 7:
				v := Obj()
				n := int(next() % 6)
				var key uint64
				for i := 0; i < n; i++ {
					key += uint64(next()) + 1
					v.Fields = append(v.Fields, Field{key, build(depth + 1)})
				}
				return v
			case 8:
				v := Map()
				n := int(next() % 6)
				for i := 0; i < n; i++ {
					v.Entries = append(v.Entries, Entry{string([]byte{byte('a' + i), next()}), build(depth + 1)})
				}
				return v
			default:
				v := Array()
				n := int(next() % 6)
				for i := 0; i < n; i++ {
					v.Items = append(v.Items, build(depth+1))
				}
				return v
			}
		}
		v := build(0)
		shape := scShapeOf(v)
		rawSC := AppendSC(nil, shape, v)
		if len(SkipSC(rawSC, shape)) != 0 {
			t.Fatal("schema compact trailing")
		}
		decodedSC := scDecode(SCView{rawSC, shape})
		cellSC, _ := readValue(Encode(decodedSC, Compact))
		checkValue(t, v, cellSC)
		for _, policy := range []Policy{Compact, Indexed, Offsets} {
			raw := Encode(v, policy)
			c, rest := readValue(raw)
			if len(rest) != 0 {
				t.Fatal("trailing")
			}
			checkValue(t, v, c)
		}
	})
}
func FuzzChangeReader(f *testing.F) {
	f.Add(AppendChanges2(nil, changeFixture(3)))
	f.Add(AppendChanges(nil, changeFixture(2), true))
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		var scratch [8]PathRef
		r, err := OpenChanges(data, scratch[:])
		if err != nil {
			return
		}
		for i := 0; i <= len(data); i++ {
			_, ok, err := r.Next()
			if err != nil || !ok {
				return
			}
		}
		t.Fatal("unbounded change count")
	})
}
func TestOffsetWidths(t *testing.T) {
	for _, n := range []int{0, 1, 255, 256, 4096} {
		v := dictionary(n, false)
		c, _ := readValue(Encode(v, Offsets))
		checkValue(t, v, c)
	}
	for _, n := range []int{0, 1, 255, 256, 8192} {
		v := numeric(n, 0)
		c, _ := readValue(Encode(v, Offsets))
		checkValue(t, v, c)
	}
}
func TestDetachedSubtree(t *testing.T) {
	for _, root := range []Policy{Compact, Indexed, Offsets} {
		v := fixtures()["state"]
		parent := View(EncodeMixed(v, root, Compact))
		child, ok := parent.Get(57)
		if !ok {
			t.Fatal("missing")
		}
		detached := append([]byte(nil), child.Raw...)
		c, _ := readValue(detached)
		checkValue(t, fixtures()["profile"], c)
	}
}
func TestChangeReaderNoAllocations(t *testing.T) {
	data := AppendChanges2(nil, changeFixture(16))
	if n := testing.AllocsPerRun(1000, func() {
		var scratch [8]PathRef
		r, err := OpenChanges(data, scratch[:])
		if err != nil {
			panic(err)
		}
		for {
			_, ok, err := r.Next()
			if err != nil {
				panic(err)
			}
			if !ok {
				break
			}
		}
	}); n != 0 {
		t.Fatalf("reader allocated: %g", n)
	}
}
