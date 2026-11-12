package kvolab

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"testing"
)

// Descriptor inference is fixture setup, never part of measured access. It
// merges every collection element, never treats observed bools as constants,
// and never turns runtime numeric dictionary keys into declared fields.
func scShapeOf(v Value) *SCShape {
	s := &SCShape{Kind: v.Kind}
	switch v.Kind {
	case True, False:
		s.Kind = True
	case Record:
		if len(v.Fields) > 0 && v.Fields[0].Key >= 10000 {
			s.Kind = SCNumericMap
			for _, f := range v.Fields {
				s.Elem = scMerge(s.Elem, scShapeOf(f.Value))
			}
		} else {
			for _, f := range v.Fields {
				s.Fields = append(s.Fields, SCField{f.Key, scShapeOf(f.Value)})
			}
		}
	case Dict:
		for _, e := range v.Entries {
			s.Elem = scMerge(s.Elem, scShapeOf(e.Value))
		}
	case List:
		for _, e := range v.Items {
			s.Elem = scMerge(s.Elem, scShapeOf(e))
		}
	}
	if s.Elem == nil {
		s.Elem = &SCShape{Kind: SCAny}
	}
	return s
}
func scMerge(a, b *SCShape) *SCShape {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a.Kind == SCAny || b.Kind == SCAny {
		return &SCShape{Kind: SCAny}
	}
	if a.Kind == Null && b.Kind != Null {
		return &SCShape{Kind: SCNullable, Elem: b}
	}
	if b.Kind == Null && a.Kind != Null {
		return &SCShape{Kind: SCNullable, Elem: a}
	}
	if a.Kind == SCNullable {
		if b.Kind == Null {
			return a
		}
		if b.Kind == SCNullable {
			b = b.Elem
		}
		return &SCShape{Kind: SCNullable, Elem: scMerge(a.Elem, b)}
	}
	if b.Kind == SCNullable {
		return scMerge(b, a)
	}
	if a.Kind != b.Kind {
		return &SCShape{Kind: SCAny}
	}
	s := &SCShape{Kind: a.Kind}
	switch a.Kind {
	case Record:
		m := map[uint64]*SCShape{}
		for _, f := range a.Fields {
			m[f.Key] = f.Shape
		}
		for _, f := range b.Fields {
			m[f.Key] = scMerge(m[f.Key], f.Shape)
		}
		for k, v := range m {
			s.Fields = append(s.Fields, SCField{k, v})
		}
		sort.Slice(s.Fields, func(i, j int) bool { return s.Fields[i].Key < s.Fields[j].Key })
	case List, Dict, SCNumericMap:
		s.Elem = scMerge(a.Elem, b.Elem)
	}
	return s
}
func scDecode(v SCView) Value {
	s, b := v.Shape, v.Data
	out := Value{Kind: s.Kind}
	switch s.Kind {
	case SCAny:
		c, _ := readValue(b)
		return compactCellValue(c)
	case SCNullable:
		if b[0] == 0 {
			return Value{Kind: Null}
		}
		return scDecode(SCView{b[1:], s.Elem})
	case Null:
	case True:
		if b[0] == 0 {
			out.Kind = False
		}
	case Uint:
		out.U = v.Uint()
	case Int:
		out.U = uint64(v.Int())
	case Float:
		out.U = binary.LittleEndian.Uint64(b)
	case Text:
		out.S = string(v.Bytes())
	case Blob:
		out.B = bytes.Clone(v.Bytes())
	case Record:
		it := v.Fields()
		for {
			slot, x, ok := it.Next()
			if !ok {
				break
			}
			out.Fields = append(out.Fields, Field{s.Fields[slot].Key, scDecode(x)})
		}
	case SCNumericMap, Dict, List:
		n, rest := takeU(b)
		var key uint64
		if s.Kind == SCNumericMap {
			out.Kind = Record
		}
		for i := uint64(0); i < n; i++ {
			var text string
			if s.Kind == SCNumericMap {
				delta, r := takeU(rest)
				rest = r
				key += delta
			}
			if s.Kind == Dict {
				l, r := takeU(rest)
				text = string(r[:l])
				rest = r[l:]
			}
			tail := SkipSC(rest, s.Elem)
			x := scDecode(SCView{rest[:len(rest)-len(tail)], s.Elem})
			rest = tail
			switch s.Kind {
			case SCNumericMap:
				out.Fields = append(out.Fields, Field{key, x})
			case Dict:
				out.Entries = append(out.Entries, Entry{text, x})
			case List:
				out.Items = append(out.Items, x)
			}
		}
	}
	return out
}
func compactCellValue(c Cell) Value {
	v := Value{Kind: c.Kind, U: c.U}
	switch c.Kind {
	case Text:
		v.S = string(c.Bytes())
	case Blob:
		v.B = bytes.Clone(c.Bytes())
	case Record:
		c.Raw.Walk(func(k uint64, x Cell) bool { v.Fields = append(v.Fields, Field{k, compactCellValue(x)}); return true })
	case List, Dict:
		_, b := payload(c.Raw)
		n, b := takeU(b)
		for i := uint64(0); i < n; i++ {
			var k string
			if c.Kind == Dict {
				l, r := takeU(b)
				k = string(r[:l])
				b = r[l:]
			}
			x, r := readValue(b)
			b = r
			if c.Kind == Dict {
				v.Entries = append(v.Entries, Entry{k, compactCellValue(x)})
			} else {
				v.Items = append(v.Items, compactCellValue(x))
			}
		}
	}
	return v
}

// Small independent MessagePack writer: only fixture primitives/containers,
// no reflection, struct tags, pooled encoders, or MessagePack-library calls.
func appendMPCount(dst []byte, n int, mapKind bool) []byte {
	base, tag16, tag32 := byte(0x90), byte(0xdc), byte(0xdd)
	if mapKind {
		base, tag16, tag32 = 0x80, 0xde, 0xdf
	}
	if n < 16 {
		return append(dst, base|byte(n))
	}
	if n <= 65535 {
		return binary.BigEndian.AppendUint16(append(dst, tag16), uint16(n))
	}
	return binary.BigEndian.AppendUint32(append(dst, tag32), uint32(n))
}
func appendMPUint(dst []byte, n uint64) []byte {
	switch {
	case n < 128:
		return append(dst, byte(n))
	case n <= 255:
		return append(dst, 0xcc, byte(n))
	case n <= 65535:
		return binary.BigEndian.AppendUint16(append(dst, 0xcd), uint16(n))
	case n <= math.MaxUint32:
		return binary.BigEndian.AppendUint32(append(dst, 0xce), uint32(n))
	default:
		return binary.BigEndian.AppendUint64(append(dst, 0xcf), n)
	}
}
func appendMPText(dst []byte, s string) []byte {
	n := len(s)
	switch {
	case n < 32:
		dst = append(dst, 0xa0|byte(n))
	case n <= 255:
		dst = append(dst, 0xd9, byte(n))
	case n <= 65535:
		dst = binary.BigEndian.AppendUint16(append(dst, 0xda), uint16(n))
	default:
		dst = binary.BigEndian.AppendUint32(append(dst, 0xdb), uint32(n))
	}
	return append(dst, s...)
}
func appendOwnMP(dst []byte, v Value, s *SCShape, shaped bool) []byte {
	switch v.Kind {
	case Null:
		return append(dst, 0xc0)
	case False:
		return append(dst, 0xc2)
	case True:
		return append(dst, 0xc3)
	case Uint:
		return appendMPUint(dst, v.U)
	case Int:
		n := int64(v.U)
		switch {
		case n >= 0:
			return appendMPUint(dst, uint64(n))
		case n >= -32:
			return append(dst, byte(n))
		case n >= -128:
			return append(dst, 0xd0, byte(n))
		case n >= -32768:
			return binary.BigEndian.AppendUint16(append(dst, 0xd1), uint16(n))
		case n >= math.MinInt32:
			return binary.BigEndian.AppendUint32(append(dst, 0xd2), uint32(n))
		default:
			return binary.BigEndian.AppendUint64(append(dst, 0xd3), uint64(n))
		}
	case Float:
		return binary.BigEndian.AppendUint64(append(dst, 0xcb), v.U)
	case Text:
		return appendMPText(dst, v.S)
	case Blob:
		n := len(v.B)
		switch {
		case n <= 255:
			dst = append(dst, 0xc4, byte(n))
		case n <= 65535:
			dst = binary.BigEndian.AppendUint16(append(dst, 0xc5), uint16(n))
		default:
			dst = binary.BigEndian.AppendUint32(append(dst, 0xc6), uint32(n))
		}
		return append(dst, v.B...)
	case Record:
		array := shaped && s != nil && s.Kind == Record && len(v.Fields) == len(s.Fields)
		dst = appendMPCount(dst, len(v.Fields), !array)
		j := 0
		for _, f := range v.Fields {
			var child *SCShape
			if s != nil && s.Kind == Record {
				for j < len(s.Fields) && s.Fields[j].Key < f.Key {
					j++
				}
				if j < len(s.Fields) && s.Fields[j].Key == f.Key {
					child = s.Fields[j].Shape
				}
			} else if s != nil && s.Kind == SCNumericMap {
				child = s.Elem
			}
			if !array {
				dst = appendMPUint(dst, f.Key)
			}
			dst = appendOwnMP(dst, f.Value, child, shaped)
		}
		return dst
	case Dict:
		dst = appendMPCount(dst, len(v.Entries), true)
		var child *SCShape
		if s != nil {
			child = s.Elem
		}
		for _, e := range v.Entries {
			dst = appendMPText(dst, e.Key)
			dst = appendOwnMP(dst, e.Value, child, shaped)
		}
		return dst
	case List:
		dst = appendMPCount(dst, len(v.Items), false)
		var child *SCShape
		if s != nil {
			child = s.Elem
		}
		for _, x := range v.Items {
			dst = appendOwnMP(dst, x, child, shaped)
		}
		return dst
	default:
		panic("kind")
	}
}

func TestSchemaCompactRoundTrip(t *testing.T) {
	cases := fixtures()
	cfg, _ := configurationFixture()
	cases["configuration"] = cfg
	cases["presence"] = Obj(Field{1, U(0)}, Field{2, Value{Kind: Null}}, Field{3, Value{Kind: False}}, Field{4, S("")}, Field{5, Array(Value{Kind: True}, Value{Kind: False})}, Field{6, Array(Value{Kind: Null}, U(0), U(math.MaxUint64))}, Field{7, I(math.MinInt64)})
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			s := scShapeOf(v)
			data := AppendSC(nil, s, v)
			if len(SkipSC(data, s)) != 0 {
				t.Fatal("trailing")
			}
			got := scDecode(SCView{data, s})
			if !bytes.Equal(Encode(got, Compact), Encode(v, Compact)) {
				t.Fatal("round trip")
			}
			if !bytes.Equal(appendOwnMP(nil, v, nil, false), mpEncode(v, false)) {
				t.Fatal("independent MP writer differs")
			}
		})
	}
}
func TestSchemaCompactSizes(t *testing.T) {
	t.Log("fixture,SC,SC-plus-2-envelope-bytes,MP-map,MP-shaped,MP-short")
	cases := fixtures()
	cfg, _ := configurationFixture()
	cases["configuration"] = cfg
	for _, name := range []string{"profile", "state", "settings", "transaction", "sparse-event", "dictionary-128", "dictionary-long-128", "blob-32k", "configuration"} {
		v := cases[name]
		s := scShapeOf(v)
		n := len(AppendSC(nil, s, v))
		t.Logf("%s,%d,%d,%d,%d,%d", name, n, n+2, len(appendOwnMP(nil, v, nil, false)), len(appendOwnMP(nil, v, s, true)), len(mpEncode(v, true)))
	}
	for _, name := range []string{"profile", "transaction", "sparse-event", "blob-32k"} {
		v := cases[name]
		v.Fields = v.Fields[1:]
		s := scShapeOf(v)
		t.Logf("omit-root-key/%s SC=%d MP-map=%d MP-shaped=%d MP-short=%d", name, len(AppendSC(nil, s, v)), len(mpEncode(v, false)), len(appendOwnMP(nil, v, s, true)), len(mpEncode(v, true)))
	}
}
func TestSchemaPresenceAndEvolution(t *testing.T) {
	full := numeric(1024, 0)
	s := scShapeOf(full)
	rng := rand.New(rand.NewPCG(19, 42))
	for _, n := range []int{0, 1, 6, 128, 512, 1023, 1024} {
		v := Obj()
		permutation := rng.Perm(1024)
		for _, i := range permutation[:n] {
			v.Fields = append(v.Fields, full.Fields[i])
		}
		Sort(&v)
		data := AppendSC(nil, s, v)
		got := scDecode(SCView{data, s})
		if !bytes.Equal(Encode(v, Compact), Encode(got, Compact)) {
			t.Fatal("presence")
		}
		t.Logf("present=%d/1024 selector=%d SC=%d MP-map=%d MP-shaped=%d", n, data[0], len(data), len(mpEncode(v, false)), len(appendOwnMP(nil, v, s, true)))
	}
	// An application knows field 1 but the stored writer schema also knows field 2.
	v := Obj(Field{1, U(3)}, Field{2, Map(Entry{"future", S("opaque to the application")})})
	writer := scShapeOf(v)
	data := AppendSC(nil, writer, v)
	one, _ := (SCView{data, writer}).Get(1)
	unknown, _ := (SCView{data, writer}).Get(2)
	// Replace an existing raw span; the unknown bytes need no owning decode.
	start := len(data) - len(one.Data) - len(unknown.Data)
	replacement := AppendSC(nil, one.Shape, U(900))
	updated := append([]byte{}, data[:start]...)
	updated = append(updated, replacement...)
	updated = append(updated, data[start+len(one.Data):]...)
	nextUnknown, _ := (SCView{updated, writer}).Get(2)
	if !bytes.Equal(unknown.Data, nextUnknown.Data) {
		t.Fatal("unknown field changed")
	}
	changed, _ := (SCView{updated, writer}).Get(1)
	if changed.Uint() != 900 {
		t.Fatal("patch")
	}
}
func TestSchemaNumericBoundaries(t *testing.T) {
	for _, n := range []uint64{0, 63, 64, 127, 128, 255, 256, 16383, 16384, 1 << 32, 1 << 56, math.MaxUint64} {
		s := &SCShape{Kind: Uint}
		data := AppendSC(nil, s, U(n))
		if (SCView{data, s}).Uint() != n {
			t.Fatal("uint")
		}
		t.Logf("uint=%d SC=%d MP=%d", n, len(data), len(appendMPUint(nil, n)))
	}
	for _, n := range []int64{math.MinInt64, -129, -128, -33, -32, -1, 0, 63, 64, 127, 128, math.MaxInt64} {
		s := &SCShape{Kind: Int}
		data := AppendSC(nil, s, I(n))
		if (SCView{data, s}).Int() != n {
			t.Fatal("int")
		}
	}
}
func BenchmarkSchemaCompact(b *testing.B) {
	for _, name := range []string{"profile", "settings", "transaction", "dictionary-128"} {
		v := fixtures()[name]
		s := scShapeOf(v)
		for _, mode := range []string{"schema", "own-msgpack-map", "own-msgpack-shaped"} {
			b.Run(name+"/encode-reused/"+mode, func(b *testing.B) {
				dst := make([]byte, 0, 32768)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					switch mode {
					case "schema":
						dst = AppendSC(dst[:0], s, v)
					case "own-msgpack-map":
						dst = appendOwnMP(dst[:0], v, nil, false)
					case "own-msgpack-shaped":
						dst = appendOwnMP(dst[:0], v, s, true)
					}
				}
				sinkBytes = dst
			})
		}
	}
	for _, n := range []int{32, 128} {
		v := numeric(n, 0)
		s := scShapeOf(v)
		sc := SCView{AppendSC(nil, s, v), s}
		mp := MPView(mpEncode(v, false))
		for _, mode := range []string{"schema", "msgpack-view"} {
			b.Run(fmt.Sprintf("numeric%d/get/%s", n, mode), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					k := query(i, n)
					if mode == "schema" {
						x, _ := sc.Get(k)
						sinkU = x.Uint()
					} else {
						x, _ := mp.Get(k)
						sinkU = x.Uint()
					}
				}
			})
		}
	}
}

// The following two functions represent generated readers bound to this exact
// all-present writer descriptor. A different generation needs another binding.
// Both create owning strings/maps/slices, and both process the payload once.
func decodeSCConfiguration(data []byte) Configuration {
	if data[0] != 0 {
		panic("requires all-present descriptor")
	}
	data = data[1:]
	var out Configuration
	n, r := takeU(data)
	out.Name = string(r[:n])
	data = r[n:]
	out.Enabled = data[0] != 0
	data = data[1:]
	n, data = takeU(data)
	out.Threshold = int64(n>>1) ^ -int64(n&1)
	n, data = takeU(data)
	out.Labels = make(map[string]string, int(n))
	for i := uint64(0); i < n; i++ {
		k, r := takeU(data)
		key := string(r[:k])
		l, r := takeU(r[k:])
		out.Labels[key] = string(r[:l])
		data = r[l:]
	}
	n, data = takeU(data)
	out.Rules = make([]Rule, int(n))
	for i := range out.Rules {
		if data[0] != 0 {
			panic("rule presence")
		}
		data = data[1:]
		n, r := takeU(data)
		out.Rules[i].Name = string(r[:n])
		data = r[n:]
		n, data = takeU(data)
		out.Rules[i].Weight = int64(n>>1) ^ -int64(n&1)
		out.Rules[i].Enabled = data[0] != 0
		data = data[1:]
	}
	if len(data) != 0 {
		panic("trailing")
	}
	return out
}
func decodeMPShapedConfiguration(data []byte) Configuration {
	_, n, data := mpHead(data)
	if n != 5 {
		panic("configuration arity")
	}
	var out Configuration
	out.Name = string(MPView(data).Bytes())
	data = mpSkip(data)
	out.Enabled = data[0] == 0xc3
	data = data[1:]
	out.Threshold = MPView(data).Int()
	data = mpSkip(data)
	_, n, data = mpHead(data)
	out.Labels = make(map[string]string, n)
	for i := 0; i < n; i++ {
		key := string(MPView(data).Bytes())
		data = mpSkip(data)
		out.Labels[key] = string(MPView(data).Bytes())
		data = mpSkip(data)
	}
	_, n, data = mpHead(data)
	out.Rules = make([]Rule, n)
	for i := range out.Rules {
		_, n, data = mpHead(data)
		if n != 3 {
			panic("rule arity")
		}
		out.Rules[i].Name = string(MPView(data).Bytes())
		data = mpSkip(data)
		out.Rules[i].Weight = MPView(data).Int()
		data = mpSkip(data)
		out.Rules[i].Enabled = data[0] == 0xc3
		data = data[1:]
	}
	if len(data) != 0 {
		panic("trailing")
	}
	return out
}
func TestSchemaConfiguration(t *testing.T) {
	v, want := configurationFixture()
	s := scShapeOf(v)
	for _, got := range []Configuration{decodeSCConfiguration(AppendSC(nil, s, v)), decodeMPShapedConfiguration(appendOwnMP(nil, v, s, true))} {
		if sumConfiguration(got) != sumConfiguration(want) || len(got.Rules) != len(want.Rules) || got.Enabled != want.Enabled {
			t.Fatal("configuration")
		}
		for i := range want.Rules {
			if got.Rules[i] != want.Rules[i] {
				t.Fatal("rule")
			}
		}
	}
	data := AppendSC(nil, s, v)
	if n := testing.AllocsPerRun(1000, func() {
		x, _ := (SCView{data, s}).Get(4)
		y, ok := x.GetString("option-031")
		if !ok || len(y.Bytes()) == 0 {
			panic("lookup")
		}
	}); n != 0 {
		t.Fatalf("%g allocations", n)
	}
}
func BenchmarkSchemaConfiguration(b *testing.B) {
	v, _ := configurationFixture()
	s := scShapeOf(v)
	sc := AppendSC(nil, s, v)
	mp := appendOwnMP(nil, v, s, true)
	for _, mode := range []string{"schema-generated", "msgpack-shaped-generated"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			var sum uint64
			for i := 0; i < b.N; i++ {
				if mode == "schema-generated" {
					sum += sumConfiguration(decodeSCConfiguration(sc))
				} else {
					sum += sumConfiguration(decodeMPShapedConfiguration(mp))
				}
			}
			sinkU = sum
		})
	}
}

func TestSchemaRequiredContract(t *testing.T) {
	// These are explicit alternative contracts for the synthetic fixtures, not
	// assertions that every field in the source application is required.
	cfg, _ := configurationFixture()
	configShape := scShapeOf(cfg)
	configShape.RequiredRecord = true
	configShape.Fields[4].Shape.Elem.RequiredRecord = true
	transaction := fixtures()["transaction"]
	transactionShape := scShapeOf(transaction)
	transactionShape.Fields[3].Shape.Elem.RequiredRecord = true
	for _, c := range []struct {
		name string
		v    Value
		s    *SCShape
	}{{"configuration", cfg, configShape}, {"transaction-required-items", transaction, transactionShape}} {
		data := AppendSC(nil, c.s, c.v)
		got := scDecode(SCView{data, c.s})
		if !bytes.Equal(Encode(got, Compact), Encode(c.v, Compact)) {
			t.Fatal("required round trip")
		}
		t.Logf("%s required-SC=%d ordinary-SC=%d MP-shaped=%d", c.name, len(data), len(AppendSC(nil, scShapeOf(c.v), c.v)), len(appendOwnMP(nil, c.v, c.s, true)))
	}
	// Explicit zero/false values still exist under a required schema.
	present := Obj(Field{1, U(0)}, Field{2, Value{Kind: False}})
	s := scShapeOf(present)
	s.RequiredRecord = true
	got := scDecode(SCView{AppendSC(nil, s, present), s})
	if len(got.Fields) != 2 || got.Fields[1].Value.Kind != False {
		t.Fatal("presence")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("missing required accepted")
			}
		}()
		AppendSC(nil, s, Obj())
	}()
}

type SCExampleRow struct {
	ID     uint64
	Count  uint64
	Detail struct {
		Code     string
		Quantity int64
	}
}

// An example of generated row binding. No serialization annotations; only the
// row ID is excluded. The first field of the nested object remains encoded.
func appendSCExampleRow(dst []byte, row SCExampleRow) []byte {
	dst = binary.AppendUvarint(dst, row.Count)
	dst = binary.AppendUvarint(dst, uint64(len(row.Detail.Code)))
	dst = append(dst, row.Detail.Code...)
	return binary.AppendUvarint(dst, uint64(row.Detail.Quantity<<1)^uint64(row.Detail.Quantity>>63))
}
func ExampleSCExampleRow() {
	row := SCExampleRow{ID: 100, Count: 7}
	row.Detail.Code = "abc"
	row.Detail.Quantity = -2
	payload := appendSCExampleRow(nil, row)
	row.ID = 200
	fmt.Println(bytes.Equal(payload, appendSCExampleRow(nil, row)))
	fmt.Printf("%x\n", payload)
	// Output:
	// true
	// 070361626303
}
