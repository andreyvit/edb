package kvolab

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/vmihailenco/msgpack/v5"
)

func numeric(n int, base uint64) Value {
	v := Obj()
	for i := 0; i < n; i++ {
		x := uint64((i*37)%1000 + 1)
		if i%11 == 0 {
			x = 1<<45 + uint64(i)
		}
		v.Fields = append(v.Fields, Field{base + uint64(i+1), U(x)})
	}
	return v
}
func dictionary(n int, long bool) Value {
	v := Map()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		if long {
			key = "namespace/configuration/long-common-prefix/" + key
		}
		v.Entries = append(v.Entries, Entry{key, S(fmt.Sprintf("value-%04d", i))})
	}
	return v
}
func fixtures() map[string]Value {
	profile := Obj(Field{1, U(1 << 58)}, Field{2, S("Avery Example")}, Field{3, S("avery@example.invalid")}, Field{4, S("+000000000000")}, Field{5, dictionary(8, false)}, Field{6, Array(S("alpha"), S("beta"), S("gamma"))}, Field{7, Obj(Field{1, S("Example Road")}, Field{2, S("Example City")}, Field{3, U(12345)})})
	state := numeric(48, 0)
	for i := 0; i < 8; i++ {
		child := numeric(16, 1<<50+uint64(i)*100)
		state.Fields = append(state.Fields, Field{uint64(49 + i), child})
	}
	state.Fields = append(state.Fields, Field{57, profile}, Field{58, dictionary(6, false)})
	settings := numeric(160, 0)
	for i := range settings.Fields {
		switch i % 5 {
		case 0:
			settings.Fields[i].Value = Value{Kind: True}
		case 1:
			settings.Fields[i].Value = S(strings.Repeat("configuration ", 3))
		case 2:
			settings.Fields[i].Value = dictionary(4, false)
		case 3:
			settings.Fields[i].Value = Array(Obj(Field{1, S("rule")}, Field{2, I(-7)}), Obj(Field{1, S("other")}, Field{2, U(9)}))
		}
	}
	items := Array()
	for i := 0; i < 32; i++ {
		items.Items = append(items.Items, Obj(Field{1, U(1<<54 + uint64(i))}, Field{2, S(fmt.Sprintf("item-%d", i))}, Field{3, U(uint64(i + 1))}, Field{4, I(7900)}))
	}
	transaction := Obj(Field{1, U(1 << 58)}, Field{2, U(1700000000)}, Field{3, I(-45)}, Field{4, items}, Field{5, dictionary(8, false)})
	sparse := Obj(Field{1, U(1 << 58)}, Field{2, U(1 << 57)}, Field{7, U(12)}, Field{42, U(1700000000)}, Field{103, I(-23)}, Field{400, S("generic event")})
	return map[string]Value{"profile": profile, "state": state, "settings": settings, "transaction": transaction, "sparse-event": sparse, "dictionary-128": dictionary(128, false), "dictionary-long-128": dictionary(128, true), "blob-32k": Obj(Field{1, U(1 << 57)}, Field{2, Value{Kind: Blob, B: bytes.Repeat([]byte{1, 3, 5, 7}, 8192)}})}
}
func oldKVO(v Value) kvo.ImmutableRecordData {
	var fill func(kvo.MutableMap, Value)
	fill = func(m kvo.MutableMap, v Value) {
		for _, f := range v.Fields {
			switch f.Value.Kind {
			case Uint:
				m.Set(f.Key, f.Value.U)
			case Record:
				fill(m.UpdateMap(f.Key), f.Value)
			default:
				panic("old KVO supports numeric maps only")
			}
		}
	}
	m := kvo.NewRecord(nil)
	fill(m, v)
	return m.Record().Pack()
}

// shortName is an intentionally strong MessagePack baseline: one-byte names for
// the first 94 schema fields, then two-byte names. Runtime ID maps retain IDs.
func shortName(k uint64) string {
	if k <= 94 {
		return string(rune(32 + k))
	}
	k--
	return string([]byte{byte(33 + k/94), byte(33 + k%94)})
}
func mpEncode(v Value, shortKeys bool) []byte {
	var buf bytes.Buffer
	e := msgpack.NewEncoder(&buf)
	e.UseCompactInts(true)
	var enc func(Value)
	check := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	enc = func(v Value) {
		switch v.Kind {
		case Null:
			check(e.EncodeNil())
		case False:
			check(e.EncodeBool(false))
		case True:
			check(e.EncodeBool(true))
		case Uint:
			check(e.EncodeUint(v.U))
		case Int:
			check(e.EncodeInt(int64(v.U)))
		case Float:
			check(e.EncodeFloat64(math.Float64frombits(v.U)))
		case Text:
			check(e.EncodeString(v.S))
		case Blob:
			check(e.EncodeBytes(v.B))
		case Record:
			check(e.EncodeMapLen(len(v.Fields)))
			for _, f := range v.Fields {
				if shortKeys && f.Key < 10000 {
					check(e.EncodeString(shortName(f.Key)))
				} else {
					check(e.EncodeUint(f.Key))
				}
				enc(f.Value)
			}
		case Dict:
			check(e.EncodeMapLen(len(v.Entries)))
			for _, entry := range v.Entries {
				check(e.EncodeString(entry.Key))
				enc(entry.Value)
			}
		case List:
			check(e.EncodeArrayLen(len(v.Items)))
			for _, item := range v.Items {
				enc(item)
			}
		default:
			panic("kind")
		}
	}
	enc(v)
	return buf.Bytes()
}
func TestSizes(t *testing.T) {
	t.Log("fixture,compact,indexed,mixed,offsets,msgpack-short,msgpack-integer")
	for _, name := range []string{"profile", "state", "settings", "transaction", "sparse-event", "dictionary-128", "dictionary-long-128", "blob-32k"} {
		v := fixtures()[name]
		t.Logf("%s,%d,%d,%d,%d,%d,%d", name, len(Encode(v, Compact)), len(Encode(v, Indexed)), len(EncodeMixed(v, Indexed, Compact)), len(Encode(v, Offsets)), len(mpEncode(v, true)), len(mpEncode(v, false)))
	}
	for _, n := range []int{8, 32, 128, 512} {
		v := numeric(n, 0)
		t.Logf("numeric-%d compact=%d indexed=%d offsets=%d kvo=%d mp-short=%d mp-integer=%d", n, len(Encode(v, Compact)), len(Encode(v, Indexed)), len(Encode(v, Offsets)), len(oldKVO(v).Bytes()), len(mpEncode(v, true)), len(mpEncode(v, false)))
	}
}
