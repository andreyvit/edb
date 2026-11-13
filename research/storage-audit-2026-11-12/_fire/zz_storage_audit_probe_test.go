package fdb

import (
	"bytes"
	"fmt"
	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"math/rand/v2"
	"reflect"
	"sort"
	"testing"
	"time"
)

var tt = reflect.TypeFor[time.Time]()

func auditFill(v reflect.Value, depth, variant int) {
	if !v.CanSet() || depth > 5 {
		return
	}
	if v.Type() == tt {
		v.Set(reflect.ValueOf(time.Date(2024, 3, 7, 12, 13, 14, 123456789, time.UTC)))
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(true)
	case reflect.String:
		v.SetString("probe-é-\x00-x")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := int64(1)
		if variant == 2 && v.Type().PkgPath() == "" {
			n = -17
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1.25)
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		auditFill(v.Elem(), depth+1, variant)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			auditFill(v.Field(i), depth+1, variant)
		}
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			auditFill(v.Index(i), depth+1, variant)
		}
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 2, 2))
		for i := 0; i < 2; i++ {
			auditFill(v.Index(i), depth+1, variant)
		}
	case reflect.Map:
		k := reflect.New(v.Type().Key()).Elem()
		auditFill(k, depth+1, variant)
		x := reflect.New(v.Type().Elem()).Elem()
		auditFill(x, depth+1, variant)
		v.Set(reflect.MakeMap(v.Type()))
		v.SetMapIndex(k, x)
	}
}
func auditSafe(f func()) (err any) { defer func() { err = recover() }(); f(); return }

func auditFillRandom(v reflect.Value, depth int, rng *rand.Rand) {
	if !v.CanSet() || depth > 5 || rng.IntN(5) == 0 {
		return
	}
	if v.Type().ConvertibleTo(tt) && v.Kind() == reflect.Struct {
		v.Set(reflect.ValueOf(time.Unix(int64(rng.IntN(2000000000)), int64(rng.IntN(1000000000))).UTC()).Convert(v.Type()))
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(rng.IntN(2) == 1)
	case reflect.String:
		v.SetString(fmt.Sprintf("field-%d-é-\x00", rng.IntN(20)))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v.Type().PkgPath() == "" {
			v.SetInt(int64(rng.IntN(20)) - 5)
		} else {
			v.SetInt(int64(rng.IntN(3)))
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(uint64(rng.IntN(8)))
	case reflect.Float32, reflect.Float64:
		v.SetFloat(float64(rng.IntN(20)) / 4)
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		auditFillRandom(v.Elem(), depth+1, rng)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			auditFillRandom(v.Field(i), depth+1, rng)
		}
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			auditFillRandom(v.Index(i), depth+1, rng)
		}
	case reflect.Slice:
		n := rng.IntN(4)
		v.Set(reflect.MakeSlice(v.Type(), n, n))
		for i := 0; i < n; i++ {
			auditFillRandom(v.Index(i), depth+1, rng)
		}
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		for i := 0; i < rng.IntN(4); i++ {
			k := reflect.New(v.Type().Key()).Elem()
			auditFillRandom(k, depth+1, rng)
			x := reflect.New(v.Type().Elem()).Elem()
			auditFillRandom(x, depth+1, rng)
			v.SetMapIndex(k, x)
		}
	}
}

func TestDeepStorageAudit(t *testing.T) {
	names := make([]string, 0, len(auditTables))
	for name := range auditTables {
		names = append(names, name)
	}
	sort.Strings(names)
	cases, invalid, failures, indices := 0, 0, 0, 0
	for _, name := range names {
		table := auditTables[name]
		indices += len(table.Indices())
		if len(table.Indices()) == 0 {
			continue
		}
		db, err := edb.Open(edb.InMemory, Schema, edb.Options{IsTesting: true})
		if err != nil {
			t.Fatal(err)
		}
		typ := table.RowType()
		rng := rand.New(rand.NewPCG(17, 42))
		for field := -1; field < typ.NumField()+80; field++ {
			for variant := 1; variant <= 3; variant++ {
				row := reflect.New(typ)
				auditFill(row.Elem().Field(0), 0, 1)
				if field >= 1 && field < typ.NumField() {
					auditFill(row.Elem().Field(field), 0, variant)
				} else if field >= typ.NumField() {
					for i := 1; i < typ.NumField(); i++ {
						if rng.IntN(3) == 0 {
							if variant == 3 {
								auditFillRandom(row.Elem().Field(i), 0, rng)
							} else {
								auditFill(row.Elem().Field(i), 0, variant)
							}
						}
					}
				}
				ref := &auditOldBuilder{keys: map[string]map[string]bool{}}
				if problem := auditSafe(func() { auditOldIndexers[name](row.Interface(), ref) }); problem != nil {
					invalid++
					fmt.Printf("OLD-REJECT %s field=%d variant=%d: %v\n", name, field, variant, problem)
					continue
				}
				canonical := reflect.New(typ)
				if problem := auditSafe(func() {
					msgpack.Decode(msgpack.Encode(nil, row), canonical.Elem())
					canonical.Elem().Field(0).Set(row.Elem().Field(0))
				}); problem != nil {
					invalid++
					continue
				}
				encodedRef := &auditOldBuilder{keys: map[string]map[string]bool{}}
				if problem := auditSafe(func() { auditOldIndexers[name](canonical.Interface(), encodedRef) }); problem != nil {
					invalid++
					continue
				}
				cases++
				problem := auditSafe(func() {
					db.Write(edb.TxMetadata{}, func(tx *edb.Tx) {
						tx.Put(table, row.Interface())
						check := func(stage string) {
							for _, idx := range table.Indices() {
								got := map[string]bool{}
								for c := tx.IndexScan(idx, edb.FullScan()); c.Next(); {
									if !bytes.Equal(c.RawKey(), table.EncodeKey(row.Elem().Field(0).Interface())) {
										panic("primary key changed")
									}
									got[string(idx.EncodeKey(c.IndexKey()))] = true
								}
								want := ref.keys[idx.FullName()]
								if len(got) != len(want) {
									panic(fmt.Sprintf("%s %s count got %d want %d", stage, idx.FullName(), len(got), len(want)))
								}
								for key := range want {
									if !got[key] {
										panic(fmt.Sprintf("%s %s key differs", stage, idx.FullName()))
									}
								}
							}
						}
						check("native-put")
						ref = encodedRef
						tx.Reindex(table, nil)
						check("encoded-reindex")
					})
				})
				if problem != nil {
					failures++
					fmt.Printf("FAIL-INDEX %s field=%d variant=%d: %v\n", name, field, variant, problem)
				}
			}
		}
		db.Close()
	}
	fmt.Printf("index audit: tables=%d indices=%d cases=%d rejected-old=%d failures=%d\n", len(names), indices, cases, invalid, failures)
	if failures != 0 {
		t.Fail()
	}
}
