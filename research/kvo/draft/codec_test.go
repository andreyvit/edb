package kvodraft

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"math"
	"math/rand/v2"
	"testing"
)

func mustNumeric(t *testing.T, fields []Field, layout Layout) []byte {
	t.Helper()
	return AppendNumeric(nil, fields, layout)
}
func checkValue(t *testing.T, a, b Value) {
	t.Helper()
	if a.IsZero() && b.IsZero() {
		return
	}
	if a.wire != b.wire || a.word != b.word || !bytes.Equal(a.data, b.data) {
		t.Fatalf("value mismatch: %#v %#v", a, b)
	}
}
func expectPanic(t *testing.T, want error, fn func()) {
	t.Helper()
	defer func() {
		if got := recover(); got != want {
			t.Fatalf("panic=%v, want %v", got, want)
		}
	}()
	fn()
}
func openTest(t *testing.T, raw []byte) View {
	t.Helper()
	v, err := Open(raw)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func TestRoundTrip(t *testing.T) {
	fields := []Field{{0, ZeroValue()}, {1, Uint(0)}, {2, Bool(false)}, {3, Int(math.MinInt64)}, {4, Uint(math.MaxUint64)}, {5, Float(math.Inf(1))}, {6, ByteString(nil)}, {7, ByteString([]byte("text"))}, {math.MaxUint64, ByteString([]byte{0xff, 0x80})}}
	pairs := []Pair{{"", Uint(0)}, {"\x00", Uint(7)}, {"a", ByteString([]byte("x"))}, {"é", Int(-9)}}
	values := []Value{Uint(0), ZeroValue(), Int(-7), ByteString(nil), Float(math.NaN())}
	for _, layout := range []Layout{Compact, Indexed} {
		v := openTest(t, AppendNumeric(nil, fields, layout))
		it := v.Iter()
		count := 0
		for _, field := range fields {
			checkValue(t, field.Value, v.Get(field.Key))
			if field.Value.IsZero() {
				continue
			}
			if !it.Next() || it.Entry().Key != field.Key {
				t.Fatal("iteration")
			}
			checkValue(t, field.Value, it.Entry().Value)
			count++
		}
		if it.Next() || v.Len() != count {
			t.Fatal("entry count")
		}
		if !v.Get(8).IsZero() {
			t.Fatal("absent")
		}
		v = openTest(t, AppendDictionary(nil, pairs, layout))
		for _, p := range pairs {
			checkValue(t, p.Value, v.GetString(p.Key))
		}
		if v.Len() != 3 || !v.GetString("missing").IsZero() {
			t.Fatal("dictionary zeros")
		}
		v = openTest(t, AppendList(nil, values, layout))
		if v.Len() != len(values) {
			t.Fatal("list lost positions")
		}
		for i, want := range values {
			checkValue(t, want, v.At(i))
		}
	}
}
func TestZeroDefaults(t *testing.T) {
	zeros := []Value{Value{}, Uint(0), Int(0), Word(0), Float(0), Float(math.Copysign(0, -1)), Bool(false), ByteString(nil), ByteString([]byte{})}
	for _, zero := range zeros {
		if zero.Uint() != 0 || zero.Int() != 0 || zero.Float() != 0 || zero.Bool() || len(zero.Bytes()) != 0 || zero.Object().Len() != 0 {
			t.Fatal("zero accessor")
		}
		if !zero.Object().Get(1).IsZero() || !zero.Object().GetString("key").IsZero() {
			t.Fatal("empty child")
		}
		if len(AppendValue(nil, zero)) != 1 {
			t.Fatal("standalone zero needs only its class")
		}
	}
	for _, layout := range []Layout{Compact, Indexed} {
		fields := make([]Field, len(zeros))
		for i, zero := range zeros {
			fields[i] = Field{uint64(i), zero}
		}
		empty := openTest(t, AppendNumeric(nil, fields, layout))
		if empty.Len() != 0 || !ObjectValue(empty).IsZero() {
			t.Fatal("stored zeros")
		}
		it := empty.Iter()
		if it.Next() {
			t.Fatal("empty iteration")
		}
		list := openTest(t, AppendList(nil, zeros, layout))
		if list.Len() != len(zeros) {
			t.Fatal("zero list positions")
		}
		if ObjectValue(list).IsZero() {
			t.Fatal("nonempty zero list is not empty")
		}
	}
	// Raw bytes cannot be guessed to be an empty object without model context.
	encodedEmpty := AppendDictionary(nil, nil, Compact)
	if ByteString(encodedEmpty).IsZero() {
		t.Fatal("raw bytes became a container")
	}
	if !ObjectValue(Trusted(encodedEmpty)).IsZero() {
		t.Fatal("empty container")
	}
}
func TestUniformAndBoundaries(t *testing.T) {
	for _, layout := range []Layout{Compact, Indexed} {
		for _, n := range []int{0, 1, 2, 7, 8, 128, 256} {
			for _, value := range []Value{ZeroValue(), Uint(7), Word(9), ByteString([]byte("abc"))} {
				fields, values := make([]Field, n), make([]Value, n)
				for i := range fields {
					fields[i] = Field{uint64(i) * 10000, value}
					values[i] = value
				}
				v := openTest(t, AppendNumeric(nil, fields, layout))
				expected := n
				if value.IsZero() {
					expected = 0
				}
				if v.Len() != expected {
					t.Fatal("map count")
				}
				v = openTest(t, AppendList(nil, values, layout))
				if v.Len() != n {
					t.Fatal("list count")
				}
				if n > 0 {
					checkValue(t, value, v.At(n-1))
				}
			}
		}
	}
	for _, n := range []uint64{0, 127, 128, 255, 256, 65535, 65536, 1 << 21, 1 << 32, 1 << 56, math.MaxUint64} {
		got, err := ReadValue(AppendValue(nil, Uint(n)))
		if err != nil || got.Uint() != n {
			t.Fatal("unsigned", err)
		}
	}
	for _, n := range []int64{math.MinInt64, -129, -1, 0, 127, math.MaxInt64} {
		got, err := ReadValue(AppendValue(nil, Int(n)))
		if err != nil || got.Int() != n {
			t.Fatal("signed", err)
		}
	}
	for _, layout := range []Layout{Compact, Indexed} {
		v := openTest(t, AppendNumeric(nil, []Field{{0, Uint(3)}, {math.MaxUint64, ByteString([]byte("x"))}}, layout))
		if !bytes.Equal(v.Get(math.MaxUint64).Bytes(), []byte("x")) {
			t.Fatal("wide key")
		}
	}
}
func TestSchemaFreeRewrite(t *testing.T) {
	opaque := []byte{0xff, 0x80, 1, 0}
	if _, err := Open(opaque); err == nil {
		t.Fatal("test needs invalid nested bytes")
	}
	model := NewRecordModel(1, 3, 4) // Field 2 is unknown to this model.
	for _, from := range []Layout{Compact, Indexed} {
		for _, to := range []Layout{Compact, Indexed} {
			base := openTest(t, AppendNumeric(nil, []Field{{1, Uint(1)}, {2, ByteString(opaque)}, {3, Uint(3)}}, from))
			var scratch [8]Edit
			var arena [64]byte
			ed := NewEditor(base, scratch[:], arena[:])
			model.Set(&ed, 3, Uint(math.MaxUint64))
			model.Set(&ed, 1, Uint(0))
			model.Set(&ed, 4, ByteString([]byte("new")))
			view := openTest(t, ed.Append(nil, to))
			if !bytes.Equal(view.Get(2).Bytes(), opaque) {
				t.Fatal("unknown changed")
			}
			if !view.Get(1).IsZero() || view.Len() != 3 {
				t.Fatal("zero not removed")
			}
			if model.Get(view, 3).Uint() != math.MaxUint64 {
				t.Fatal("following field")
			}
		}
	}
}
func TestEditorOwnershipAndNoop(t *testing.T) {
	raw := AppendNumeric(nil, []Field{{1, Uint(1)}, {2, ByteString([]byte("old"))}}, Indexed)
	var scratch [1]Edit
	var arena [2]byte
	ed := NewEditor(Trusted(raw), scratch[:], arena[:])
	if !bytes.Equal(raw, ed.Append(nil, Indexed)) {
		t.Fatal("noop")
	}
	input := []byte("new, larger than scratch")
	ed.Set(2, ByteString(input))
	input[0] = '!'
	if string(ed.Get(2).Bytes()) != "new, larger than scratch" {
		t.Fatal("ownership")
	}
	ed.Set(2, ByteString(nil))
	if !ed.Get(2).IsZero() {
		t.Fatal("zero overlay")
	}
	ed.Set(1, Uint(0))
	if openTest(t, ed.Append(nil, Compact)).Len() != 0 {
		t.Fatal("all cleared")
	}
	empty := NewEditor(View{}, nil, nil)
	empty.Set(17, Uint(9))
	if openTest(t, empty.Append(nil, Compact)).Get(17).Uint() != 9 {
		t.Fatal("empty editor")
	}
}
func TestErrorPolicy(t *testing.T) {
	view := Trusted(AppendNumeric(nil, []Field{{1, Uint(2)}}, Compact))
	model := NewRecordModel(1)
	ed := NewEditor(view, nil, nil)
	expectPanic(t, ErrUnknownKey, func() { model.Set(&ed, 2, Uint(9)) })
	expectPanic(t, ErrUnknownKey, func() { model.Get(view, 2) })
	expectPanic(t, ErrUnknownKey, func() { model.Delete(&ed, 2) })
	// Arbitrary numeric dictionary keys remain legal in the unbound API.
	ed.Set(2, Uint(9))
	if ed.Get(2).Uint() != 9 {
		t.Fatal("dictionary insert")
	}
	expectPanic(t, ErrOrder, func() { AppendNumeric(nil, []Field{{2, Uint(0)}, {1, Uint(2)}}, Compact) })
	expectPanic(t, ErrOrder, func() { AppendDictionary(nil, []Pair{{"a", Uint(0)}, {"a", Uint(2)}}, Compact) })
	expectPanic(t, ErrValue, func() { ed.Set(1, Value{wire: 99}) })
	expectPanic(t, ErrValue, func() { AppendList(nil, []Value{{wire: 99}}, Compact) })
	expectPanic(t, ErrKind, func() { view.GetString("x") })
	expectPanic(t, ErrType, func() { ByteString([]byte("x")).Uint() })
	expectPanic(t, ErrType, func() { Uint(7).Bytes() })
	expectPanic(t, ErrType, func() { Uint(7).Float() })
	expectPanic(t, ErrValue, func() { Uint(7).Bool() })
	expectPanic(t, ErrFormat, func() { ByteString([]byte{0xff}).Object() })
	list := Trusted(AppendList(nil, []Value{ZeroValue()}, Compact))
	expectPanic(t, ErrIndex, func() { list.At(-1) })
	expectPanic(t, ErrIndex, func() { list.At(1) })
	expectPanic(t, ErrKind, func() { NewEditor(list, nil, nil) })
}
func TestTryPatchOwned(t *testing.T) {
	raw := AppendNumeric(nil, []Field{{1, Word(7)}, {2, Word(8)}}, Indexed)
	owned := bytes.Clone(raw)
	if !TryPatchOwned(owned, 2, Word(math.MaxUint64)) {
		t.Fatal("patch")
	}
	if openTest(t, owned).Get(2).Uint() != math.MaxUint64 {
		t.Fatal("word")
	}
	before := bytes.Clone(owned)
	if TryPatchOwned(owned, 2, ByteString([]byte("x"))) || TryPatchOwned(owned, 2, Uint(1)) || TryPatchOwned(owned, 3, Word(9)) {
		t.Fatal("unexpected patch")
	}
	if !bytes.Equal(owned, before) {
		t.Fatal("failed patch changed bytes")
	}
	before = bytes.Clone(owned)
	if TryPatchOwned(owned, 2, Word(0)) || !bytes.Equal(owned, before) {
		t.Fatal("zero patch must use deletion/rebuild")
	}
	ed := NewEditor(openTest(t, owned), nil, nil)
	ed.Set(2, Word(0))
	v := openTest(t, ed.Append(nil, Indexed))
	if v.Len() != 1 || v.Get(2).Uint() != v.Get(99).Uint() {
		t.Fatal("zero semantics")
	}
	mixedFields := []Field{{0, Uint(1)}, {1, Uint(2)}, {2, Int(-9)}, {3, Float(1.5)}, {4, ByteString([]byte("x"))}, {5, Word(3)}}
	owned = AppendNumeric(nil, mixedFields, Indexed)
	if !TryPatchOwned(owned, 2, Word(1000)) {
		t.Fatal("mixed patch")
	}
	v = openTest(t, owned)
	if v.Get(2).Uint() != 1000 || v.Get(1).Uint() != 2 || v.Get(3).Float() != 1.5 {
		t.Fatal("class bits")
	}
}
func TestGolden(t *testing.T) {
	fields := []Field{{1, Uint(7)}, {2, ByteString([]byte("hi"))}}
	for _, c := range []struct {
		l   Layout
		hex string
	}{
		{Compact, "b8010a070e026869"},
		{Indexed, "3902000000010232070000000000000018000000020000006869"},
	} {
		got := AppendNumeric(nil, fields, c.l)
		if hex.EncodeToString(got) != c.hex {
			t.Fatalf("got %x", got)
		}
		openTest(t, got)
	}
	if got := AppendNumeric(nil, []Field{{1, Uint(7)}}, Compact); hex.EncodeToString(got) != "500107" {
		t.Fatalf("single field %x", got)
	}
}
func TestMalformed(t *testing.T) {
	for _, layout := range []Layout{Compact, Indexed} {
		raw := AppendNumeric(nil, []Field{{1, Uint(3)}, {2, ByteString([]byte("abc"))}}, layout)
		for i := 1; i < len(raw); i++ {
			if _, err := Open(raw[:i]); err == nil {
				t.Fatalf("truncation %d", i)
			}
		}
		if _, err := Open(append(bytes.Clone(raw), 0)); err == nil {
			t.Fatal("trailing")
		}
	}
	overCount := binary.AppendUvarint(nil, uint64(MaxEntries+1)<<6)
	for _, raw := range [][]byte{{6}, {0x80}, {0xb8, 0}, {0xb8, 1, 10, 1, 2, 2}, {0xb8, 1, 7, 1, 2, 0}, overCount} {
		if _, err := Open(raw); err != ErrFormat {
			t.Fatalf("accepted %x: %v", raw, err)
		}
	}
	for _, raw := range [][]byte{nil, {7}, {1}, {6, 0xff}, {5, 0}} {
		if _, err := ReadValue(raw); err != ErrFormat {
			t.Fatalf("standalone %x: %v", raw, err)
		}
	}
}
func FuzzOpen(f *testing.F) {
	f.Add([]byte{})
	for _, layout := range []Layout{Compact, Indexed} {
		f.Add(AppendNumeric(nil, []Field{{1, Uint(7)}, {3, ByteString([]byte("payload"))}}, layout))
		f.Add(AppendDictionary(nil, []Pair{{"a", Uint(7)}, {"z", ByteString([]byte("payload"))}}, layout))
		f.Add(AppendList(nil, []Value{ZeroValue(), Uint(7), ByteString([]byte("payload"))}, layout))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8192 {
			return
		}
		v, err := Open(data)
		if err != nil {
			return
		}
		it, count := v.Iter(), 0
		for it.Next() {
			e := it.Entry()
			if count < 64 {
				var got Value
				switch v.Kind() {
				case Numeric:
					got = v.Get(e.Key)
				case Dictionary:
					got = v.GetString(string(e.KeyBytes))
				case Sequence:
					got = v.At(count)
				}
				checkValue(t, e.Value, got)
			}
			count++
		}
		if count != v.Len() {
			t.Fatal("count")
		}
	})
}
func FuzzNumericRoundTrip(f *testing.F) {
	f.Add(uint64(2))
	f.Fuzz(func(t *testing.T, seed uint64) {
		rng := rand.New(rand.NewPCG(seed, 11))
		fields := make([]Field, int(rng.Uint64N(64)))
		var key uint64
		for i := range fields {
			key += rng.Uint64N(100000) + 1
			v := Uint(rng.Uint64())
			if i%3 == 0 {
				v = ByteString([]byte{byte(rng.Uint64()), byte(i)})
			}
			if i%7 == 0 {
				v = ZeroValue()
			}
			fields[i] = Field{key, v}
		}
		for _, layout := range []Layout{Compact, Indexed} {
			v := openTest(t, AppendNumeric(nil, fields, layout))
			for _, want := range fields {
				checkValue(t, want.Value, v.Get(want.Key))
			}
		}
	})
}
func TestEditorAgainstMap(t *testing.T) {
	for seed := uint64(0); seed < 32; seed++ {
		for _, from := range []Layout{Compact, Indexed} {
			for _, to := range []Layout{Compact, Indexed} {
				rng := rand.New(rand.NewPCG(seed, 37))
				fields, want := make([]Field, 16), make(map[uint64]Value)
				for i := range fields {
					fields[i] = Field{uint64(i * 2), Uint(uint64(i + 1))}
					want[fields[i].Key] = fields[i].Value
				}
				var scratch [2]Edit
				var arena [4]byte
				editor := NewEditor(Trusted(AppendNumeric(nil, fields, from)), scratch[:], arena[:])
				for i := 0; i < 96; i++ {
					key := rng.Uint64N(48)
					if rng.Uint64N(4) == 0 {
						editor.Set(key, ZeroValue())
						delete(want, key)
					} else {
						value := Uint(rng.Uint64())
						if i%3 == 0 {
							value = ByteString([]byte{byte(i), byte(seed), 0})
						}
						editor.Set(key, value)
						want[key] = value
					}
					checkValue(t, want[key], editor.Get(key))
				}
				out := openTest(t, editor.Append(nil, to))
				if out.Len() != len(want) {
					t.Fatal("count")
				}
				for key, value := range want {
					checkValue(t, value, out.Get(key))
				}
			}
		}
	}
}
