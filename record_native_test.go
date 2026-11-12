package edb

import (
	"reflect"
	"testing"

	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

type tableNativeSchema struct {
	fields map[reflect.Type]*nativebind.Struct
	types  map[reflect.Type]*values.Type
}

func (s tableNativeSchema) NativeFields(t reflect.Type) *nativebind.Struct { return s.fields[t] }
func (s tableNativeSchema) NativeType(t reflect.Type) *values.Type         { return s.types[t] }

func TestTableNativeSchema(t *testing.T) {
	shared := tableNativeSchema{fields: map[reflect.Type]*nativebind.Struct{}, types: map[reflect.Type]*values.Type{}}
	for typ, g := range statrow.Static.Fields {
		shared.fields[typ] = nativebind.BindFields(typ, g, nativebind.StaticOnly)
		shared.types[typ] = values.NewType(typ.String(), values.Pointer)
	}
	for _, format := range []StorageFormat{MessagePack, Fast2, Compact2} {
		t.Run(format.String(), func(t *testing.T) {
			_, model := statrow.Model()
			schema := new(Schema)
			table := DefineTable[uint64](schema, "state", model, func(b *TableBuilder[uint64]) {
				b.SetNativeSchema(shared)
				if format != MessagePack {
					b.SetRecordStorage(RecordStorage{Format: format, Generated: &statrow.Static, Mode: nativebind.StaticOnly})
				}
			}).TableDef()
			db := setup(t, schema)
			db.Write(func(tx *Tx) { Put(tx, statrow.Sample(13)) })
			var hot kvo.Record
			db.Read(func(tx *Tx) {
				r, _, err := tx.TryGetRecord(table, uint64(13))
				if err != nil {
					t.Fatal(err)
				}
				hot = r.Fastest()
			})
			if hot.NativeType() != shared.NativeType(reflect.TypeFor[statrow.State]()) {
				t.Fatal("table lost shared type")
			}
			if err := hot.Edit().TryFastSetPathValue([]row2.Step{{Key: 9}, {Key: 5}}, values.Uint(900)); err != nil {
				t.Fatal(err)
			}
			native := hot.Native().(*statrow.State)
			if native.Key != 0 || native.Profile.Balance != 900 {
				t.Fatal("native mapping invented omitted key or lost mutation")
			}
			db.Write(func(tx *Tx) { tx.PutByKey(table, uint64(13), native) })
			if native.Key != 0 {
				t.Fatal("explicit-key write changed caller's struct")
			}
			db.Read(func(tx *Tx) {
				if Get[statrow.State](tx, uint64(13)).Profile.Balance != 900 {
					t.Fatal("native save did not persist")
				}
			})
		})
	}
}
