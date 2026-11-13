//go:build ignore

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"time"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/fdb"
)

type mutation struct {
	path  string
	apply func(uint64)
}

// Added plugin fields are outside the existing-behavior control. Omit only nil
// additions; populated values must remain visible to the comparison.
func existingShape(v any) any {
	switch v := v.(type) {
	case map[string]any:
		for k, x := range v {
			if (k == "Plugins" || k == "PluginDraft") && x == nil {
				delete(v, k)
			} else {
				v[k] = existingShape(x)
			}
		}
	case []any:
		for i, x := range v {
			v[i] = existingShape(x)
		}
	}
	return v
}

func existingHash(v reflect.Value) string {
	b, err := json.Marshal(existingShape(canonical(v)))
	must(err)
	return hash(b)
}

func mutationChoices(v reflect.Value, path string, depth int, out *[]mutation) {
	if !v.IsValid() || depth > 8 {
		return
	}
	if v.Kind() == reflect.Interface {
		if !v.IsNil() {
			mutationChoices(v.Elem(), path, depth+1, out)
		}
		return
	}
	if !v.CanSet() {
		return
	}
	add := func(fn func(uint64)) { *out = append(*out, mutation{path, fn}) }
	if v.Type().ConvertibleTo(timeType) {
		add(func(s uint64) {
			tm := v.Convert(timeType).Interface().(time.Time)
			if s%3 == 0 {
				tm = time.Time{}
			} else {
				tm = tm.Add(time.Duration(s%101-50) * time.Second)
			}
			v.Set(reflect.ValueOf(tm).Convert(v.Type()))
		})
		return
	}
	switch v.Kind() {
	case reflect.Pointer:
		if !v.IsNil() {
			add(func(uint64) { v.SetZero() })
			mutationChoices(v.Elem(), path+"*", depth+1, out)
		}
	case reflect.Bool:
		add(func(uint64) { v.SetBool(!v.Bool()) })
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		add(func(s uint64) {
			vals := []int64{0, 1, -1, v.Int() + 1, v.Int() - 1, 1<<31 - 1, -1 << 31}
			v.SetInt(vals[s%uint64(len(vals))])
		})
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		add(func(s uint64) {
			vals := []uint64{0, 1, v.Uint() + 1, v.Uint() ^ 0x10000, 1<<32 - 1}
			v.SetUint(vals[s%uint64(len(vals))])
		})
	case reflect.Float32, reflect.Float64:
		add(func(s uint64) { vals := []float64{0, 1, -1, v.Float() + 0.25}; v.SetFloat(vals[s%4]) })
	case reflect.String:
		add(func(s uint64) {
			if s%3 == 0 {
				v.SetString("")
			} else {
				v.SetString(v.String() + "/audit-π\x00")
			}
		})
	case reflect.Struct:
		for i := range v.NumField() {
			f := v.Type().Field(i)
			if f.IsExported() && f.Name != "Plugins" && f.Name != "PluginDraft" {
				mutationChoices(v.Field(i), path+"."+f.Name, depth+1, out)
			}
		}
	case reflect.Slice:
		add(func(s uint64) {
			switch s % 3 {
			case 0:
				v.SetZero()
			case 1:
				v.Set(reflect.MakeSlice(v.Type(), 0, 0))
			case 2:
				if v.Len() > 0 {
					v.Set(v.Slice(0, v.Len()-1))
				}
			}
		})
		for i := 0; i < min(v.Len(), 4); i++ {
			mutationChoices(v.Index(i), fmt.Sprintf("%s[%d]", path, i), depth+1, out)
		}
	case reflect.Map:
		add(func(s uint64) {
			if s%2 == 0 {
				v.SetZero()
			} else {
				v.Set(reflect.MakeMap(v.Type()))
			}
		})
		keys := v.MapKeys()
		sort.Slice(keys, func(i, j int) bool {
			a, _ := json.Marshal(canonical(keys[i]))
			b, _ := json.Marshal(canonical(keys[j]))
			return bytes.Compare(a, b) < 0
		})
		for i, key := range keys[:min(len(keys), 4)] {
			copy := reflect.New(v.Type().Elem()).Elem()
			copy.Set(v.MapIndex(key))
			var nested []mutation
			mutationChoices(copy, fmt.Sprintf("%s{#%d}", path, i), depth+1, &nested)
			for _, m := range nested {
				*out = append(*out, mutation{m.path, func(s uint64) { m.apply(s); v.SetMapIndex(key, copy) }})
			}
		}
	}
}

func mutateRows(db *edb.DB, seed uint64, limit int) {
	var skipped map[string]bool
	if path := os.Getenv("AUDIT_SKIP_MUTATIONS"); path != "" {
		data, err := os.ReadFile(path)
		must(err)
		must(json.Unmarshal(data, &skipped))
	}
	for _, tbl := range fdb.Schema.Tables() {
		var keys [][]byte
		db.Read(func(tx *edb.Tx) {
			for c := tx.TableScan(tbl, edb.FullScan()); c.Next(); {
				keys = append(keys, bytes.Clone(c.RawKey()))
			}
		})
		// Rotate the evenly spaced sample by seed; retain bounded work per table.
		if limit > 0 && len(keys) > limit {
			selected := make([][]byte, limit)
			for i := range selected {
				selected[i] = keys[(i*len(keys)/limit+int(seed%uint64(len(keys))))%len(keys)]
			}
			keys = selected
		}
		for _, key := range keys {
			if skipped[tbl.Name()+"/"+hash(key)] {
				continue
			}
			digest := sha256.Sum256(append(bytes.Clone(key), []byte(fmt.Sprint(seed))...))
			s := binary.LittleEndian.Uint64(digest[:])
			mode := s % 7
			out := map[string]any{"phase": "mutation", "table": tbl.Name(), "key": hash(key), "mode": mode}
			failure := catch(func() {
				var rawBefore []byte
				var beforeMeta edb.ValueMeta
				var expectHash string
				func() {
					tx := db.BeginUpdate(edb.TxMetadata{})
					defer tx.Close()
					row, meta := tx.GetByKeyRaw(tbl, key)
					beforeMeta = meta
					v := reflect.ValueOf(row)
					out["before"] = existingHash(v)
					rawBefore = bytes.Clone(tx.BoltTx().Bucket([]byte(tbl.Name())).Bucket([]byte("data")).Get(key))
					pk := reflect.New(tbl.KeyType()).Elem()
					pk.Set(tbl.RowKeyVal(v))
					if mode != 0 {
						var choices []mutation
						mutationChoices(v.Elem(), "", 0, &choices)
						if len(choices) > 0 {
							m := choices[(s/7)%uint64(len(choices))]
							out["field"] = m.path
							m.apply(s / 7 / uint64(len(choices)))
						}
						tbl.SetRowKeyVal(v, pk)
					}
					oldMeta, newMeta := tx.PutVal(tbl, v)
					out["put_mod_delta"] = int64(newMeta.ModCount) - int64(oldMeta.ModCount)
					stored, _ := tx.GetByKeyRaw(tbl, key)
					expectHash = existingHash(reflect.ValueOf(stored))
					out["in_tx"] = expectHash
					if mode != 1 {
						must(tx.Commit())
					}
				}()
				db.Read(func(tx *edb.Tx) {
					row, meta := tx.GetByKeyRaw(tbl, key)
					out["after"] = existingHash(reflect.ValueOf(row))
					out["mod_delta"] = int64(meta.ModCount) - int64(beforeMeta.ModCount)
					out["schema"] = meta.SchemaVer
					if mode == 1 {
						raw := tx.BoltTx().Bucket([]byte(tbl.Name())).Bucket([]byte("data")).Get(key)
						if !bytes.Equal(raw, rawBefore) || meta != beforeMeta {
							panic("rollback changed the row")
						}
					} else if out["after"] != expectHash {
						panic("committed read changed native values")
					}
				})
			})
			out["error"] = failure
			emit(out)
		}
	}
	indexSummary(db, "mutated-index")
}
