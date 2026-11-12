package edb

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/values"
)

var recordHiddenDecodes int

type recordHidden string

func (v *recordHidden) UnmarshalMsgpack(b []byte) error {
	recordHiddenDecodes++
	*v = recordHidden(msgpack.Trusted(b).Bytes())
	return nil
}

type messagePackMember struct {
	ID     uint64       `msgpack:"-"`
	Name   string       `msgpack:"n"`
	Hidden recordHidden `msgpack:"h"`
}

func TestMessagePackRecordKeepsUnexposedBytes(t *testing.T) {
	models := kvo.NewSchema()
	entity := kvo.NewEntityType(models, "member")
	kvo.NewProp(models, 1, "name", kvo.TMachineString, nil)
	model := kvo.NewModel(models, entity, func(b *kvo.ModelBuilder) { b.Prop(models.MustPropByCode(1)) })
	schema := new(Schema)
	var idx *IndexDef
	model.RegisterNativeType(reflect.TypeFor[messagePackMember](), map[kvo.PropCode]string{1: "Name"})
	table := DefineTable[uint64](schema, "member", model, func(b *TableBuilder[uint64]) {
		idx = b.AddIndex[string]("name", nil, IndexOptDeprecatedUnique).IndexDef()
		b.SetIndexer(func(_ uint64, r kvo.Record, ib *IndexBuilder) {
			ib.Add(idx, string(r.GetValue(1).AsBytes()))
		})
	}).TableDef()
	db := setup(t, schema)
	db.Write(func(tx *Tx) { Put(tx, &messagePackMember{7, "before", "untouched"}) })
	db.Write(func(tx *Tx) {
		bucket := table.dataBucketIn(tx)
		key := table.EncodeKey(uint64(7))
		var v value
		if err := v.decode(bucket.Get(key), false); err != nil {
			t.Fatal(err)
		}
		// This value is absent from the Go struct and must survive edits verbatim.
		extra := []byte{0xd6, 42, 1, 2, 3, 4}
		v.Data = msgpack.Trusted(v.Data).Set("future", extra).Raw()
		out := make([]byte, maxValueHeaderSize)
		out = append(out, v.Data...)
		indexOff := len(out)
		out = append(out, v.Index...)
		ensure(bucket.Put(key, putValueHeader(out, v.Flags, v.SchemaVer, v.ModCount, indexOff)))
		recordHiddenDecodes = 0
		r, _, err := tx.TryGetRecord(table, uint64(7))
		if err != nil {
			t.Fatal(err)
		}
		if string(r.GetValue(1).AsBytes()) != "before" {
			t.Fatal("borrowed access")
		}
		edit, err := tx.TryOpenRecordEdit(table, uint64(7))
		if err != nil {
			t.Fatal(err)
		}
		if err := edit.Editor().TrySetValue(1, values.Bytes([]byte("after"))); err != nil {
			t.Fatal(err)
		}
		meta, err := tx.TrySaveRecord(edit)
		if err != nil || meta.ModCount != 2 {
			t.Fatal(meta, err)
		}
		if recordHiddenDecodes != 0 {
			t.Fatal("property edit or index decoded full row")
		}
		if err := v.decode(bucket.Get(key), false); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(msgpack.Trusted(v.Data).Lookup("future").Raw(), extra) {
			t.Fatal("unknown bytes discarded")
		}
		if _, err := tx.TrySaveRecord(edit); err != nil {
			t.Fatal(err)
		}
		cur := ExactIndexScan[messagePackMember](tx, idx, "after")
		if !cur.Next() {
			t.Fatal("index not updated")
		}
		current, _, err := cur.TryRecord()
		if err != nil {
			t.Fatal(err)
		}
		fast := current.Fastest()
		if string(fast.GetValue(1).AsBytes()) != "after" || recordHiddenDecodes != 1 {
			t.Fatal("decode-once view")
		}
	})
}
