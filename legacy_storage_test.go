package edb

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/fast1"
)

type legacyMember struct {
	ID    uint64 `msgpack:"-"`
	Email string `msgpack:"e"`
}

func TestReleasedStorageFormats(t *testing.T) {
	models := kvo.NewSchema()
	entity := kvo.NewEntityType(models, "snapshot")
	child := kvo.NewEntityType(models, "child")
	kvo.NewProp(models, 1, "child", child, nil)
	kvo.NewProp(models, 2, "count", kvo.TUint64, nil)
	kvo.NewModel(models, child, func(b *kvo.ModelBuilder) { b.Prop(models.MustPropByCode(2)) })
	model := kvo.NewModel(models, entity, func(b *kvo.ModelBuilder) { b.Prop(models.MustPropByCode(1)) })

	schema := new(Schema)
	var byEmail *IndexDef
	members := DefineTable(schema, "members", testIndexAccess[legacyMember](indexEmail), func(b *TableBuilder[uint64]) {
		b.SetSchemaVersion(2)

		byEmail = b.AddIndex[string]("email", nil, IndexOptDeprecatedUnique).IndexDef()
		b.SetIndexer(func(key uint64, row kvo.Record, b *IndexBuilder) { b.Add(byEmail, row.Get(indexEmail)) })
	}).TableDef()
	snapshots := DefineKVTable(schema, "snapshots", model, nil, nil)
	db := setup(t, schema)
	key := []byte{7}
	member := &legacyMember{ID: 7, Email: "a@example.test"}
	// Captured with EDB v0.4.11. The memento includes the index length
	// in the header but omits the index bytes.
	memento := x("01 02 01 12 12 81 a1 65 ae 61406578616d706c652e74657374")
	row := append(bytes.Clone(memento), x("01 01 0f 61406578616d706c652e74657374 01")...)
	snapshot := x("0200000000000000 1000000003000000 1000000005000000 0100000000000000 " +
		"0100000000000000 0200000000000000 0102000000000000")
	// Legacy snapshots are native-endian arrays of 64-bit words.
	for off := 0; off < len(snapshot); off += 8 {
		binary.NativeEndian.PutUint64(snapshot[off:], binary.LittleEndian.Uint64(snapshot[off:]))
	}
	db.Write(func(tx *Tx) {
		Put(tx, member)
		c := tx.TableScan(members, FullScan())
		if !c.Next() || !bytes.Equal(c.RawRow(), row) || !bytes.Equal(c.ValueMemento(), memento) {
			t.Fatal("legacy row or memento encoding changed")
		}
		record := fast1.NewRecord(entity)
		record.UpdateMap(1).Set(2, 513)
		tx.KVPut(snapshots, key, record.Record())
		if got := tx.KVGetRaw(snapshots, key); !bytes.Equal(got, snapshot) {
			t.Fatalf("legacy snapshot encoding changed: %x", got)
		}
		tx.KVPutRaw(snapshots, key, snapshot)
	})
	db.Read(func(tx *Tx) {
		if got := Get[legacyMember](tx, member.ID); got == nil || *got != *member {
			t.Fatal("reading legacy row", got)
		}
		if got := Lookup[legacyMember](tx, byEmail, member.Email); got == nil || *got != *member {
			t.Fatal("reading legacy index", got)
		}
		got, meta, err := tx.DecodeMementoVal(members, members.EncodeKey(member.ID), memento)
		if err != nil || meta != (ValueMeta{SchemaVer: 2, ModCount: 1}) || *got.Interface().(*legacyMember) != *member {
			t.Fatal("reading archived memento", meta, err)
		}
		if n := tx.KVGet(snapshots, key).GetMap(1).Get(2); n != 513 {
			t.Fatal("reading archived snapshot", n)
		}
	})
}
