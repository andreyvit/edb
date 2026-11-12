package fdb

import (
	"fmt"
	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestDeepMigrationAudit(t *testing.T) {
	cases, failures, rejected := 0, 0, 0
	for name, old := range auditOldMigrators {
		table := auditTables[name]
		if table == nil {
			t.Fatal(name)
		}
		if reflect.ValueOf(table).Elem().FieldByName("migrator").IsNil() {
			continue
		}
		migrate := registeredSchemaMigrator(t, table)
		typ := table.RowType()
		p := kvo.Project(typ, table.Model(), nil)
		db, err := edb.Open(edb.InMemory, Schema, edb.Options{IsTesting: true})
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewPCG(57, 19))
		for scenario := 0; scenario < 100; scenario++ {
			row := reflect.New(typ)
			if scenario > 0 {
				auditFillRandom(row.Elem(), 0, rng)
			}
			auditFill(row.Elem().Field(0), 0, 1)
			var raw []byte
			if auditSafe(func() { raw = msgpack.Encode(nil, row); msgpack.Decode(raw, row.Elem()) }) != nil {
				rejected++
				continue
			}
			for version := uint64(0); version < table.SchemaVersion(); version++ {
				for _, format := range []string{"native", "MessagePack"} {
					want := reflect.New(typ)
					msgpack.Decode(raw, want.Elem())
					auditFill(want.Elem().Field(0), 0, 1)
					var expected []byte
					var oldErr any
					db.Read(func(tx *edb.Tx) {
						oldErr = auditSafe(func() { old(tx, want.Interface(), version); expected = msgpack.Encode(nil, want) })
					})
					if oldErr != nil {
						rejected++
						continue
					}
					cases++
					var problem any
					db.Read(func(tx *edb.Tx) {
						problem = auditSafe(func() {
							input := reflect.New(typ)
							msgpack.Decode(raw, input.Elem())
							auditFill(input.Elem().Field(0), 0, 1)
							r := p.Record(input)
							if format == "MessagePack" {
								r = p.MessagePack(raw)
							}
							e := r.Edit()
							migrate(tx, table.EncodeKeyVal(input.Elem().Field(0)), e, version)
							actual := e.MessagePack()
							if actual == nil {
								actual = msgpack.Encode(nil, reflect.ValueOf(e.Record().Native()))
							}
							got := reflect.New(typ)
							msgpack.Decode(actual, got.Elem())
							actual = msgpack.Encode(nil, got)
							got = reflect.New(typ)
							msgpack.Decode(actual, got.Elem())
							expectedValue := reflect.New(typ)
							msgpack.Decode(expected, expectedValue.Elem())
							if !reflect.DeepEqual(expectedValue.Interface(), got.Interface()) {
								panic(auditDifference(expectedValue.Elem(), got.Elem(), ""))
							}
						})
					})
					if problem != nil {
						failures++
						fmt.Printf("FAIL-MIGRATION %s scenario=%d version=%d format=%s: %v\n", name, scenario, version, format, problem)
					}
				}
			}
		}
		db.Close()
	}
	fmt.Printf("migration audit: cases=%d rejected=%d failures=%d\n", cases, rejected, failures)
	if failures > 0 {
		t.Fail()
	}
}
func auditFirstDiff(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

func auditDifference(a, b reflect.Value, path string) string {
	if reflect.DeepEqual(a.Interface(), b.Interface()) {
		return ""
	}
	switch a.Kind() {
	case reflect.Pointer:
		if !a.IsNil() && !b.IsNil() {
			return auditDifference(a.Elem(), b.Elem(), path)
		}
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			if a.Field(i).CanInterface() {
				if d := auditDifference(a.Field(i), b.Field(i), path+"."+a.Type().Field(i).Name); d != "" {
					return d
				}
			}
		}
	case reflect.Slice, reflect.Array:
		if a.Len() == b.Len() {
			for i := 0; i < a.Len(); i++ {
				if d := auditDifference(a.Index(i), b.Index(i), fmt.Sprintf("%s[%d]", path, i)); d != "" {
					return d
				}
			}
		}
	case reflect.Map:
		if a.Len() == b.Len() {
			for _, k := range a.MapKeys() {
				if bv := b.MapIndex(k); bv.IsValid() {
					if d := auditDifference(a.MapIndex(k), bv, fmt.Sprintf("%s[%v]", path, k)); d != "" {
						return d
					}
				}
			}
		}
	}
	return fmt.Sprintf("%s: expected=%#v actual=%#v", path, a.Interface(), b.Interface())
}
