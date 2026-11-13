// Command storage-compat rehearses old/new/old persistence using a disposable DB.
// run.py builds this same program against each EDB revision.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/fast1"
	"github.com/vmihailenco/msgpack/v5"
)

type status int

// A custom scalar deliberately has a non-integer wire representation.
func (s status) MarshalMsgpack() ([]byte, error) {
	return msgpack.Marshal(fmt.Sprint("status:", int(s)))
}
func (s *status) UnmarshalMsgpack(b []byte) error {
	var str string
	if err := msgpack.Unmarshal(b, &str); err != nil {
		return err
	}
	_, err := fmt.Sscanf(str, "status:%d", s)
	return err
}

type child struct {
	Name   string
	Counts map[string]int64
}
type row struct {
	ID      uint64 `msgpack:"-"`
	Email   string
	Group   string
	Tags    []string
	Time    time.Time
	Status  status
	Payload []byte
	Child   *child
	Max     uint64
}
type key struct {
	Tenant uint64
	Name   string
}
type compositeRow struct {
	Key   key `msgpack:"-"`
	Count int64
}
type stringRow struct {
	Key   string `msgpack:"-"`
	Value string
}

var schema = new(edb.Schema)
var email *edb.Index[string]
var group *edb.Index[string]
var rows = edb.DefineTable(schema, "rows", rowModel, func(b *edb.TableBuilder[uint64]) {
	b.SetSchemaVersion(3)
	email = b.AddIndex[string]("email", nil, edb.IndexOptDeprecatedUnique)
	group = b.AddIndex[string]("group", nil)
	configureRowIndexes(b)
})

// BEGIN ROW INDEXER
var rowModels = kvo.NewSchema()
var emailProp = kvo.NewStringProp[string](rowModels, 1, "Email", kvo.TMachineString)
var groupProp = kvo.NewStringProp[string](rowModels, 2, "Group", kvo.TMachineString)
var rowModel = func() *kvo.Model {
	model := kvo.NewModel(rowModels, kvo.NewEntityType(rowModels, "row"), func(b *kvo.ModelBuilder) { b.Prop(emailProp); b.Prop(groupProp) })
	model.RegisterNativeType(reflect.TypeFor[row](), map[kvo.PropCode]string{1: "Email", 2: "Group"})
	return model
}()

func configureRowIndexes(b *edb.TableBuilder[uint64]) {
	b.SetIndexer(func(_ uint64, r kvo.Record, ib *edb.IndexBuilder) {
		ib.Add(email, emailProp.Get(r))
		ib.Add(group, groupProp.Get(r))
	})
}

// END ROW INDEXER

var composites = edb.DefineTable(schema, "composites", emptyModel[compositeRow](), func(b *edb.TableBuilder[key]) {})
var strings = edb.DefineTable(schema, "strings", emptyModel[stringRow](), func(b *edb.TableBuilder[string]) {})
var snapshotIndex *edb.KVIndex
var snapshots = func() *edb.KVTable {
	s := kvo.NewSchema()
	child := kvo.NewEntityType(s, "child")
	kvo.NewProp(s, 1, "child", child, nil)
	kvo.NewProp(s, 2, "count", kvo.TUint64, nil)
	kvo.NewModel(s, child, func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(2)) })
	m := kvo.NewModel(s, kvo.NewEntityType(s, "snapshot"), func(b *kvo.ModelBuilder) { b.Prop(s.MustPropByCode(1)) })
	return edb.DefineKVTable(schema, "snapshots", m, nil, func(b *edb.KVTableBuilder) {
		snapshotIndex = b.DefineIndex("value", nil, func(ik []byte) []byte { return ik[1:] }, func(b *edb.KVIndexContentBuilder, pk []byte, r fast1.ImmutableRecord) {
			b.Add(append([]byte{byte(r.Root().GetMap(1).Get(2))}, pk...))
		})
	})
}()
var raw = edb.DefineRawTable(schema, "raw", nil)

func want(id uint64, stage int) *row {
	r := &row{id, fmt.Sprintf("%d@example.test", id), fmt.Sprint(id % 7), []string{"vip", "x\x00y"}, time.Unix(-int64(id), 123456789).UTC(), status(id % 3), bytes.Repeat([]byte{byte(id)}, int(id%1024)+1), &child{"nested", map[string]int64{"negative": -int64(id)}}, ^uint64(0) - id}
	if stage > 0 && id%2 == 0 {
		r.Email = "changed-" + r.Email
		r.Group = "changed"
		r.Child.Counts["negative"] *= 2
	}
	return r
}
func check(ok bool, msg string) {
	if !ok {
		panic(msg)
	}
}
func verify(db *edb.DB, path string, stage int) {
	db.Read(func(tx *edb.Tx) {
		n := 0
		for c := edb.FullTableScan[row](tx); c.Next(); {
			r := c.Row()
			n++
			check(!(stage > 0 && r.ID%11 == 0), "deleted row returned")
			r.Time = r.Time.UTC()
			check(reflect.DeepEqual(r, want(r.ID, stage)), fmt.Sprintf("row mismatch: got %#v, want %#v", r, want(r.ID, stage)))
			looked := edb.Lookup[row](tx, email, r.Email)
			check(looked != nil, "missing unique lookup")
			looked.Time = looked.Time.UTC()
			check(reflect.DeepEqual(looked, r), "unique lookup mismatch")
			check(c.Meta().SchemaVer == 3, "schema metadata changed")
			expected := uint64(1)
			if stage > 0 && r.ID%2 == 0 {
				expected++
			}
			check(c.Meta().ModCount == expected, "modification metadata changed")
		}
		expected := 1000
		if stage > 0 {
			expected -= 1000 / 11
		}
		check(n == expected, "wrong row count")
		count := 0
		for c := edb.IndexScan[row](tx, group, edb.FullScan()); c.Next(); {
			count++
			r := c.Row()
			r.Time = r.Time.UTC()
			check(reflect.DeepEqual(r, want(r.ID, stage)), "nonunique lookup mismatch")
		}
		check(count == n, "extra or missing index entries")
		for i := uint64(1); i <= 1000; i++ {
			ck := key{i % 5, fmt.Sprint("key\x00", i)}
			check(reflect.DeepEqual(edb.Get[compositeRow](tx, ck), &compositeRow{ck, -int64(i)}), "composite key changed")
			sk := fmt.Sprint("key/", i)
			check(reflect.DeepEqual(edb.Get[stringRow](tx, sk), &stringRow{sk, "value\x00"}), "string key changed")
		}
		pk := []byte{7, 0, 1}
		snap := tx.KVGet(snapshots, pk)
		check(snap.GetMap(1).Get(2) == uint64(19+stage), "nested Fast1 changed")
		c := tx.KVIndexScan(snapshotIndex, edb.RawRange{})
		check(c.Next() && !c.Object().IsMissing(), "Fast1 index missing")
		check(!c.Next(), "extra Fast1 index")
		check(bytes.Equal(tx.KVGetRaw(raw, pk), []byte{0, 1, 2, 255}), "opaque KV bytes changed")
		m, err := os.ReadFile(path + ".memento")
		if err != nil {
			panic(err)
		}
		r, meta, err := tx.DecodeMementoVal(rows, rows.EncodeKey(uint64(2)), m)
		if err != nil {
			panic(err)
		}
		archived := r.Interface().(*row)
		archived.Time = archived.Time.UTC()
		check(meta.ModCount == 1 && reflect.DeepEqual(archived, want(2, 0)), "old memento changed")
	})
}
func main() {
	if len(os.Args) != 3 {
		panic("usage: storage-compat DB seed|modify|verify")
	}
	path, mode := os.Args[1], os.Args[2]
	db, err := edb.Open(path, schema, edb.Options{ReuseTxBuffers: true})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if mode == "seed" {
		db.Write(edb.TxMetadata{}, func(tx *edb.Tx) {
			for i := uint64(1); i <= 1000; i++ {
				edb.Put(tx, want(i, 0), &compositeRow{key{i % 5, fmt.Sprint("key\x00", i)}, -int64(i)}, &stringRow{fmt.Sprint("key/", i), "value\x00"})
			}
			snap := fast1.NewRecord(nil)
			snap.UpdateMap(1).Set(2, 19)
			tx.KVPut(snapshots, []byte{7, 0, 1}, snap.Record())
			tx.KVPutRaw(raw, []byte{7, 0, 1}, []byte{0, 1, 2, 255})
			c := tx.TableScan(rows, edb.ExactScan(uint64(2)))
			check(c.Next(), "memento cursor")
			if err := os.WriteFile(path+".memento", c.ValueMemento(), 0600); err != nil {
				panic(err)
			}
		})
		verify(db, path, 0)
	} else if mode == "modify" {
		verify(db, path, 0)
		db.Write(edb.TxMetadata{}, func(tx *edb.Tx) {
			for i := uint64(1); i <= 1000; i++ {
				check(!edb.Put(tx, want(i, 0)), "no-op changed row")
			}
			for i := uint64(1); i <= 1000; i++ {
				if i%11 == 0 {
					edb.DeleteByKey[row](tx, i)
				} else if i%2 == 0 {
					check(edb.Put(tx, want(i, 1)), "update lost")
				}
			}
			snap := fast1.UpdateRecord(fast1.LoadRecord(tx.KVGetRaw(snapshots, []byte{7, 0, 1}), nil))
			snap.UpdateMap(1).Set(2, 20)
			tx.KVPut(snapshots, []byte{7, 0, 1}, snap.Record())
			tx.Reindex(rows, nil)
		})
		rollback := errors.New("rollback")
		err := db.Tx(edb.TxMetadata{}, true, func(tx *edb.Tx) error {
			edb.Put(tx, want(1001, 0))
			edb.DeleteByKey[row](tx, uint64(1))
			return rollback
		})
		check(errors.Is(err, rollback), "rollback error lost")
		verify(db, path, 1)
	} else if mode == "verify" {
		verify(db, path, 1)
	} else {
		panic("bad mode " + strconv.Quote(mode))
	}
	fmt.Println(mode + ": 3,000-row compatibility, indexes, custom scalar, timestamps, memento, Fast1 and rollback checks passed")
}

func emptyModel[T any]() *kvo.Model {
	s := kvo.NewSchema()
	m := kvo.NewModel(s, kvo.NewEntityType(s, "empty"), func(*kvo.ModelBuilder) {})
	m.RegisterNativeType(reflect.TypeFor[T](), map[kvo.PropCode]string{})
	return m
}
