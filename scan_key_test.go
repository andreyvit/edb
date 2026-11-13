package edb

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestCursorUintKey(t *testing.T) {
	db := setup(t, basicSchema)
	db.Write(TxMetadata{}, func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "same", Email: "a"}, &User{ID: math.MaxUint64, Name: "same", Email: "b"})
	})
	db.Read(func(tx *Tx) {
		for _, c := range []Cursor[User]{
			TableScan[User](tx, FullScan()),
			IndexScan[User](tx, usersByEmail, FullScan()),
			IndexScan[User](tx, usersByName, ExactScan("same")),
		} {
			assertPanics(t, func() { c.UintKey() })
			for _, want := range []uint64{1, math.MaxUint64} {
				if !c.Next() || c.UintKey() != want || uint64(c.Key().(ID)) != want {
					t.Fatalf("cursor key: wanted %d", want)
				}
				if n := testing.AllocsPerRun(100, func() {
					if c.UintKey() != want {
						t.Fatal("key changed")
					}
				}); n != 0 {
					t.Fatalf("UintKey allocated %g times", n)
				}
			}
			if c.Next() {
				t.Fatal("unexpected extra row")
			}
		}
	})
}

type customUintKey uint64

func (v customUintKey) MarshalBinary() ([]byte, error) {
	return binary.LittleEndian.AppendUint64(nil, uint64(v)^0x123456789abcdef0), nil
}

func (v *customUintKey) UnmarshalBinary(b []byte) error {
	if len(b) != 8 {
		return fmt.Errorf("invalid key length %d", len(b))
	}
	*v = customUintKey(binary.LittleEndian.Uint64(b) ^ 0x123456789abcdef0)
	return nil
}

func TestCursorUintKeyEncoding(t *testing.T) {
	for _, key := range []any{uint(7), uint8(255), uint16(1234), uint32(123456), uint64(math.MaxUint64), uintptr(9), ID(42), customUintKey(42)} {
		t.Run(reflect.TypeOf(key).String(), func(t *testing.T) {
			typ := reflect.TypeOf(key)
			tbl := &TableDef{name: "keys", keyType: typ, keyEnc: flatEncodingOf(typ)}
			c := &RawTableCursor{table: tbl, k: tbl.EncodeKey(key)}
			if got, want := c.UintKey(), reflect.ValueOf(key).Uint(); got != want {
				t.Fatalf("UintKey = %d, wanted %d", got, want)
			}
		})
	}
	for _, key := range []any{"abcdefgh", int64(1), AB{A: 1, B: 2}} {
		typ := reflect.TypeOf(key)
		tbl := &TableDef{name: "keys", keyType: typ, keyEnc: flatEncodingOf(typ)}
		c := &RawTableCursor{table: tbl, k: tbl.EncodeKey(key)}
		assertPanics(t, func() { c.UintKey() })
	}
	for _, raw := range [][]byte{nil, {1}, make([]byte, 9), append(make([]byte, 7), 1), append(make([]byte, 9), 1)} {
		c := &RawTableCursor{table: usersTable, k: raw}
		assertPanics(t, func() { c.UintKey() })
	}
	typ := reflect.TypeFor[uint8]()
	tbl := &TableDef{name: "keys", keyType: typ, keyEnc: flatEncodingOf(typ)}
	c := &RawTableCursor{table: tbl, k: usersTable.EncodeKey(ID(256))}
	assertPanics(t, func() { c.UintKey() })
}

func BenchmarkCursorKey(b *testing.B) {
	db := setup(b, basicSchema)
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 0x123456789, Name: "alice", Email: "a"}) })
	tx := db.BeginRead()
	defer tx.Close()
	for _, source := range []struct {
		name string
		cur  RawCursor
	}{
		{"table", tx.TableScan(usersTable, FullScan())},
		{"index", tx.IndexScan(usersByName, ExactScan("alice"))},
	} {
		if !source.cur.Next() {
			b.Fatal("missing row")
		}
		b.Run(source.name+"/Key", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if source.cur.Key().(ID) != 0x123456789 {
					b.Fatal("incorrect key")
				}
			}
		})
		b.Run(source.name+"/UintKey", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if source.cur.UintKey() != 0x123456789 {
					b.Fatal("incorrect key")
				}
			}
		})
	}
}
