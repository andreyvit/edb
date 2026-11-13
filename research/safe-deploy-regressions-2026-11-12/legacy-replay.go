//go:build ignore

// Only the legacy import path changes for the deployed build: edb/kvo there,
// edb/kvo/codec/fast1 in the candidate. All probe operations are otherwise equal.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/andreyvit/edb"
	legacy "github.com/andreyvit/edb/kvo/codec/fast1"
	"github.com/prairiegroupinc/fire/analytics/customerledgers"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/fdb"
	"github.com/prairiegroupinc/fire/ledgers"
	"go.etcd.io/bbolt"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func digest(v any) string  { b, e := json.Marshal(v); must(e); return hash(b) }
func emit(v any)           { must(json.NewEncoder(os.Stdout).Encode(v)) }
func safe(f func()) (failure string) {
	defer func() {
		if e := recover(); e != nil {
			failure = fmt.Sprintf("%T", e)
			fmt.Fprintf(os.Stderr, "legacy panic: %v\n", e)
		}
	}()
	f()
	return
}

// Historical snapshots can contain removed schema properties. Keep these words
// opaque, just as the storage layer does; do not guess whether they are objects.
func wordKind(m legacy.ImmutableMap, key uint64) (kind int) {
	defer func() {
		if e := recover(); e != nil {
			if !strings.Contains(fmt.Sprint(e), "does not have prop") {
				panic(e)
			}
		}
	}()
	if t := m.KeyModel(key); t != nil {
		return int(t.ValueKind())
	}
	return 0
}

func canonical(m legacy.ImmutableMap, depth int) any {
	if m.IsMissing() {
		return nil
	}
	if depth > 24 {
		panic("map recursion limit")
	}
	keys := append([]uint64(nil), m.Keys()...)
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]any, 0, len(keys))
	for _, k := range keys {
		kind := wordKind(m, k)
		var value any = m.Get(k)
		if kind == 2 {
			value = canonical(m.GetMap(k), depth+1)
		}
		out = append(out, []any{k, kind, value})
	}
	return out
}

func wordPaths(m legacy.ImmutableMap, path []uint64, depth int, out *[][]uint64) {
	if m.IsMissing() || depth > 16 {
		return
	}
	for _, k := range m.Keys() {
		p := append(append([]uint64(nil), path...), k)
		switch wordKind(m, k) {
		case 1:
			*out = append(*out, p)
		case 2:
			wordPaths(m.GetMap(k), p, depth+1, out)
		}
	}
}

func classifications(m legacy.ImmutableMap) any {
	var out []any
	for flags := 0; flags < 64; flags++ {
		tenant := &bm.RuntimeTenant{Settings: &bm.TenantSettings{}}
		tenant.Settings.RequireExplicitLoyaltyOptin = flags&1 != 0
		tenant.Settings.RequireSubscriberStatus = flags&2 != 0
		tenant.Settings.TreatShopInactiveAsActive = flags&4 != 0
		tenant.Settings.IncludeGuestsInParticipantMetrics = flags&8 != 0
		ctx := &ledgers.AlgContext{Tenant: tenant, IsRequiringActiveAccounts: flags&16 != 0, UseRecordedMembership: flags&32 != 0}
		out = append(out, []any{customerledgers.MembershipStatus(m, ctx), customerledgers.IsParticipant(m, ctx), customerledgers.IsHoldingPositiveSpendablePointBalance(m, ctx), customerledgers.EffectiveAccountState(m, flags&4 != 0)})
	}
	return out
}

func main() {
	if len(os.Args) != 2 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("private disposable database required")
	}
	db, e := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true, ReuseTxBuffers: true})
	must(e)
	defer db.Close()
	for _, tbl := range fdb.Schema.KVTables() {
		if tbl.RootModel() == nil {
			continue
		}
		var keys [][]byte
		db.Read(func(tx *edb.Tx) {
			for c := tx.KVTableScan(tbl, edb.RawRange{}); c.Next(); {
				keys = append(keys, bytes.Clone(c.RawKey()))
			}
		})
		for _, key := range keys {
			out := map[string]any{"table": tbl.Name(), "key": hash(key)}
			out["error"] = safe(func() {
				var original, packed []byte
				db.Read(func(tx *edb.Tx) {
					m := tx.KVGet(tbl, key)
					original = bytes.Clone(tx.KVGetRaw(tbl, key))
					out["initial"] = digest(canonical(m, 0))
					if tbl.Name() == "cld" {
						out["classifications"] = digest(classifications(m))
					}
					var paths [][]uint64
					wordPaths(m, nil, 0, &paths)
					out["word_paths"] = len(paths)
					edit := legacy.UpdateRecord(m.RecordWithThisRoot())
					for i, path := range paths {
						if i%7 != 0 {
							continue
						}
						child := edit
						for _, k := range path[:len(path)-1] {
							child = child.UpdateMap(k)
						}
						k := path[len(path)-1]
						value := child.Get(k)
						child.Set(k, 0)
						child.Set(k, value+1)
						child.Set(k, value^0x10000)
					}
					packed = bytes.Clone(edit.Record().Pack().Bytes())
					out["edited"] = digest(canonical(legacy.LoadRecord(packed, tbl.RootType()).Root(), 0))
					out["packed"] = hash(packed)
					if !bytes.Equal(original, tx.KVGetRaw(tbl, key)) || digest(canonical(m, 0)) != out["initial"] {
						panic("edit changed retained snapshot")
					}
				})
				// Delete+put forces every snapshot index to be populated, independent
				// of which original index leaves were sampled.
				db.Write(edb.TxMetadata{}, func(tx *edb.Tx) { tx.KVPutRaw(tbl, key, nil); tx.KVPutRaw(tbl, key, packed) })
				db.Read(func(tx *edb.Tx) {
					if digest(canonical(tx.KVGet(tbl, key), 0)) != out["edited"] {
						panic("save changed packed snapshot")
					}
				})
				func() { tx := db.BeginUpdate(edb.TxMetadata{}); defer tx.Close(); tx.KVPutRaw(tbl, key, original) }()
				db.Read(func(tx *edb.Tx) {
					if !bytes.Equal(tx.KVGetRaw(tbl, key), packed) {
						panic("rollback changed packed snapshot")
					}
				})
			})
			emit(out)
		}
	}
	must(db.Bolt().View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if !bytes.Contains(name, []byte("_i_")) {
				return nil
			}
			h := sha256.New()
			n := 0
			must(b.ForEach(func(k, v []byte) error {
				must(binary.Write(h, binary.LittleEndian, uint64(len(k))))
				h.Write(k)
				h.Write(v)
				n++
				return nil
			}))
			emit(map[string]any{"index": string(name), "count": n, "hash": hex.EncodeToString(h.Sum(nil))})
			return nil
		})
	}))
}
