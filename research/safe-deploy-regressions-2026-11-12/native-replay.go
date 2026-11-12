//go:build ignore

// Build unchanged from both deployed Fire and candidate Fire source trees.
// Run only on disposable databases assembled from bounded read-only samples.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/fdb"
	"go.etcd.io/bbolt"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func un64(s string) []byte { b, e := base64.StdEncoding.DecodeString(s); must(e); return b }
func emit(v any)           { must(json.NewEncoder(os.Stdout).Encode(v)) }

var timeType = reflect.TypeFor[time.Time]()

func canonical(v reflect.Value) any {
	if !v.IsValid() {
		return nil
	}
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		return canonical(v.Elem())
	}
	if v.Type().ConvertibleTo(timeType) {
		tm := v.Convert(timeType).Interface().(time.Time)
		return []any{"time", tm.UTC().Format(time.RFC3339Nano)}
	}
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool()
	case reflect.String:
		return v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return []any{"int", strconv.FormatInt(v.Int(), 10)}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return []any{"uint", strconv.FormatUint(v.Uint(), 10)}
	case reflect.Float32, reflect.Float64:
		return []any{"float", strconv.FormatUint(math.Float64bits(v.Float()), 16)}
	case reflect.Struct:
		out := map[string]any{}
		for i := range v.NumField() {
			f := v.Type().Field(i)
			if f.IsExported() {
				out[f.Name] = canonical(v.Field(i))
			}
		}
		return out
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return nil
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			b := make([]byte, v.Len())
			reflect.Copy(reflect.ValueOf(b), v)
			return []any{"bytes", base64.StdEncoding.EncodeToString(b)}
		}
		out := make([]any, v.Len())
		for i := range v.Len() {
			out[i] = canonical(v.Index(i))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		out := make([][2]any, 0, v.Len())
		it := v.MapRange()
		for it.Next() {
			k, e := json.Marshal(canonical(it.Key()))
			must(e)
			out = append(out, [2]any{string(k), canonical(it.Value())})
		}
		sort.Slice(out, func(i, j int) bool { return out[i][0].(string) < out[j][0].(string) })
		return out
	default:
		return []any{"unsupported", v.Kind().String()}
	}
}

func nativeHash(v reflect.Value) string { b, e := json.Marshal(canonical(v)); must(e); return hash(b) }
func catch(f func()) (err string) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Sprintf("%T", e)
			fmt.Fprintf(os.Stderr, "probe panic: %v\n", e)
		}
	}()
	f()
	return
}

func importRows(input, path string) {
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		panic("destination must not exist")
	}
	db, e := bbolt.Open(path, 0600, &bbolt.Options{NoSync: true})
	must(e)
	defer db.Close()
	f, e := os.Open(input)
	must(e)
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 65536), 1024<<20)
	tx, e := db.Begin(true)
	must(e)
	n := 0
	for sc.Scan() {
		var row struct {
			Path             []string
			Key, Value, Kind string
		}
		must(json.Unmarshal(sc.Bytes(), &row))
		var b *bbolt.Bucket
		for _, part := range row.Path {
			if b == nil {
				b, e = tx.CreateBucketIfNotExists(un64(part))
			} else {
				b, e = b.CreateBucketIfNotExists(un64(part))
			}
			must(e)
		}
		must(b.Put(un64(row.Key), un64(row.Value)))
		n++
		if n%500 == 0 {
			must(tx.Commit())
			tx, e = db.Begin(true)
			must(e)
		}
	}
	must(sc.Err())
	must(tx.Commit())
	emit(map[string]any{"imported": n})
}

func indexSummary(db *edb.DB, phase string) {
	must(db.Bolt().View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if b.Bucket([]byte("data")) == nil {
				return nil
			}
			return b.ForEach(func(k, v []byte) error {
				if v != nil || !bytes.HasPrefix(k, []byte("i_")) {
					return nil
				}
				h := sha256.New()
				n := 0
				must(b.Bucket(k).ForEach(func(ik, iv []byte) error {
					must(binary.Write(h, binary.LittleEndian, uint64(len(ik))))
					h.Write(ik)
					must(binary.Write(h, binary.LittleEndian, uint64(len(iv))))
					h.Write(iv)
					n++
					return nil
				}))
				emit(map[string]any{"phase": phase, "table": string(name), "index": string(k), "count": n, "hash": hex.EncodeToString(h.Sum(nil))})
				return nil
			})
		})
	}))
}

func readRows(db *edb.DB, phase string) {
	for _, tbl := range fdb.Schema.Tables() {
		db.Read(func(tx *edb.Tx) {
			for c := tx.TableScan(tbl, edb.FullScan()); c.Next(); {
				out := map[string]any{"phase": phase, "table": tbl.Name(), "key": hash(c.RawKey()), "raw": hash(c.RawRow())}
				failure := catch(func() {
					v, meta := c.RowVal()
					out["native"] = nativeHash(v)
					if tenant := v.Elem().FieldByName("TenantID"); tenant.IsValid() {
						out["tenant"] = nativeHash(tenant)
					}
					out["schema"] = meta.SchemaVer
					out["mod"] = meta.ModCount
					memento := c.ValueMemento()
					mv, mm, e := tx.DecodeMementoVal(tbl, c.RawKey(), memento)
					must(e)
					if nativeHash(mv) != out["native"] || mm != meta {
						panic("memento differs from native read")
					}
				})
				if failure != "" {
					out["error"] = failure
				}
				emit(out)
			}
		})
	}
}

func main() {
	if len(os.Args) < 3 {
		panic("usage: native-replay import SAMPLE DB | audit DB | read DB")
	}
	if os.Args[1] == "import" {
		importRows(os.Args[2], os.Args[3])
		return
	}
	path := os.Args[2]
	if !strings.Contains(path, "/fire-edb-regressions-") {
		panic("disposable audit path required")
	}
	started := time.Now()
	db, e := edb.Open(path, fdb.Schema, edb.Options{IsTesting: true, ReuseTxBuffers: true})
	must(e)
	defer db.Close()
	emit(map[string]any{"phase": "open", "elapsed_ms": time.Since(started).Milliseconds()})
	if os.Args[1] == "mutate" {
		seed, err := strconv.ParseUint(os.Args[3], 10, 64)
		must(err)
		limit, err := strconv.Atoi(os.Args[4])
		must(err)
		mutateRows(db, seed, limit)
		return
	}
	if os.Args[1] == "prepare" {
		for _, tbl := range fdb.Schema.Tables() {
			failure := catch(func() { db.Write(func(tx *edb.Tx) { tx.Reindex(tbl, nil) }) })
			emit(map[string]any{"phase": "prepare", "table": tbl.Name(), "error": failure})
		}
		indexSummary(db, "prepared-index")
		return
	}
	if os.Args[1] == "inspect" {
		db.Read(func(tx *edb.Tx) {
			for _, name := range strings.Split(os.Args[3], ",") {
				tbl := fdb.Schema.TableNamed(name)
				for c := tx.TableScan(tbl, edb.FullScan()); c.Next(); {
					v, _ := c.RowVal()
					emit(map[string]any{"table": name, "key": hash(c.RawKey()), "value": canonical(v)})
				}
			}
		})
		return
	}
	readRows(db, "initial")
	if os.Args[1] == "read" {
		return
	}
	for _, tbl := range fdb.Schema.Tables() {
		err := catch(func() { db.Write(func(tx *edb.Tx) { tx.Reindex(tbl, nil) }) })
		emit(map[string]any{"phase": "reindex", "table": tbl.Name(), "error": err})
	}
	readRows(db, "reindexed")
	indexSummary(db, "reindexed-index")
	for _, tbl := range fdb.Schema.Tables() {
		err := catch(func() {
			db.Write(func(tx *edb.Tx) {
				var keys [][]byte
				for c := tx.TableScan(tbl, edb.FullScan()); c.Next(); {
					keys = append(keys, bytes.Clone(c.RawKey()))
				}
				for _, key := range keys {
					row, _ := tx.GetByKeyRaw(tbl, key)
					v := reflect.ValueOf(row)
					tenant := v.Elem().FieldByName("TenantID")
					if tenant.IsValid() && tenant.CanSet() && tenant.Kind() == reflect.Uint64 {
						tenant.SetUint(tenant.Uint() ^ 0x100000)
					}
					tx.PutVal(tbl, v)
				}
			})
		})
		emit(map[string]any{"phase": "native-write", "table": tbl.Name(), "error": err})
	}
	readRows(db, "written")
	indexSummary(db, "written-index")
	for _, tbl := range fdb.Schema.Tables() {
		err := catch(func() {
			db.Write(func(tx *edb.Tx) {
				var keys [][]byte
				for c := tx.TableScan(tbl, edb.FullScan()); c.Next(); {
					keys = append(keys, bytes.Clone(c.RawKey()))
				}
				for i, key := range keys {
					if i%3 == 0 {
						tx.DeleteByKeyRaw(tbl, key)
					}
				}
			})
		})
		emit(map[string]any{"phase": "delete", "table": tbl.Name(), "error": err})
	}
	readRows(db, "deleted")
	indexSummary(db, "deleted-index")
	emit(map[string]any{"phase": "finished", "elapsed_ms": time.Since(started).Milliseconds()})
}
