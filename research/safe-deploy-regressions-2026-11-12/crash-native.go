//go:build ignore

// Build unchanged in old/new Fire. Parent kills writer at controlled row/commit
// boundaries; check verifies durable acknowledgments, atomicity and all indexes.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/fdb"
	"go.etcd.io/bbolt"
)

const generationKey = "__edb_crash_generation_20261112"

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func emit(v any) { must(json.NewEncoder(os.Stdout).Encode(v)) }
func generation(c *bm.Customer) int64 {
	v := reflect.ValueOf(c.Extras[generationKey])
	if !v.IsValid() {
		return 0
	}
	if v.CanInt() {
		return v.Int()
	}
	if v.CanUint() {
		return int64(v.Uint())
	}
	panic("generation type changed")
}
func indexDigest(db *edb.DB) string {
	h := sha256.New()
	must(db.Bolt().View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("customers"))
		return b.ForEach(func(k, v []byte) error {
			if v != nil || !bytes.HasPrefix(k, []byte("i_")) {
				return nil
			}
			h.Write(k)
			return b.Bucket(k).ForEach(func(k, v []byte) error {
				must(binary.Write(h, binary.LittleEndian, uint64(len(k))))
				h.Write(k)
				must(binary.Write(h, binary.LittleEndian, uint64(len(v))))
				h.Write(v)
				return nil
			})
		})
	}))
	return fmt.Sprintf("%x", h.Sum(nil))
}
func main() {
	if len(os.Args) < 3 || !strings.Contains(os.Args[2], "/fire-edb-regressions-") {
		panic("mode and private disposable database required")
	}
	db, e := edb.Open(os.Args[2], fdb.Schema, edb.Options{NoPersistentFreeList: true, ReuseTxBuffers: os.Getenv("AUDIT_REUSE_TX_BUFFERS") == "true"})
	must(e)
	defer db.Close()
	if db.Bolt().NoSync {
		panic("fsync disabled")
	}
	var ids []bm.CustomerID
	db.Read(func(tx *edb.Tx) {
		c := tx.TableScan(tx.Schema().TableByRow((*bm.Customer)(nil)), edb.FullScan())
		for c.Next() && len(ids) < 32 {
			row, _ := c.Row()
			ids = append(ids, row.(*bm.Customer).ID)
		}
	})
	if len(ids) != 32 {
		panic("need 32 sampled customers")
	}
	switch os.Args[1] {
	case "write":
		for n := int64(1); ; n++ {
			db.Write(edb.TxMetadata{}, func(tx *edb.Tx) {
				for i, id := range ids {
					c := edb.Get[bm.Customer](tx, id)
					if c.Extras == nil {
						c.Extras = map[string]any{}
					}
					c.Extras[generationKey] = n
					c.Email = fmt.Sprintf("crash-audit-%d-%d@example.invalid", n, i)
					edb.Put(tx, c)
					emit(map[string]any{"phase": "row", "generation": n, "row": i})
				}
			})
			emit(map[string]any{"phase": "ack", "generation": n})
		}
	case "check":
		ack, e := strconv.ParseInt(os.Args[3], 10, 64)
		must(e)
		current := int64(-1)
		db.Read(func(tx *edb.Tx) {
			for i, id := range ids {
				c := edb.Get[bm.Customer](tx, id)
				n := generation(c)
				if current < 0 {
					current = n
				}
				if n != current {
					panic("partial transaction after crash")
				}
				if n > 0 && c.Email != fmt.Sprintf("crash-audit-%d-%d@example.invalid", n, i) {
					panic("row fields from different generations")
				}
			}
			for e := range tx.BoltTx().Check() {
				must(e)
			}
		})
		if current < ack || current > ack+1 {
			panic("durable acknowledgment mismatch")
		}
		before := indexDigest(db)
		db.Write(edb.TxMetadata{}, func(tx *edb.Tx) { tx.Reindex(tx.Schema().TableByRow((*bm.Customer)(nil)), nil) })
		after := indexDigest(db)
		if before != after {
			panic("indexes differ from recovered rows")
		}
		emit(map[string]any{"ack": ack, "generation": current, "rows": len(ids), "indexes_match": true, "errors": 0})
	default:
		panic("unknown mode")
	}
}
