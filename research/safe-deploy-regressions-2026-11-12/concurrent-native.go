//go:build ignore

// Build unchanged in each pinned Fire source tree. Production fsync is enabled.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/fdb"
)

const counterKey = "__edb_audit_counter_20261112"

var aborted = errors.New("planned audit rollback")

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func digest(v any) string {
	b, e := json.Marshal(v)
	must(e)
	return fmt.Sprintf("%x", sha256.Sum256(b))
}
func count(c *bm.Customer) int64 {
	v := reflect.ValueOf(c.Extras[counterKey])
	if !v.IsValid() {
		return 0
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(v.Uint())
	}
	panic("counter changed native type")
}
func stable(c *bm.Customer, wasNil bool) string {
	x := *c
	x.Extras = maps.Clone(x.Extras)
	delete(x.Extras, counterKey)
	if wasNil && len(x.Extras) == 0 {
		x.Extras = nil
	}
	return digest(&x)
}

func main() {
	if len(os.Args) != 3 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("private disposable database and iteration count required")
	}
	iterations, e := strconv.Atoi(os.Args[2])
	must(e)
	reuse := os.Getenv("AUDIT_REUSE_TX_BUFFERS") == "true"
	db, e := edb.Open(os.Args[1], fdb.Schema, edb.Options{ReuseTxBuffers: reuse, NoPersistentFreeList: true})
	must(e)
	if db.Bolt().NoSync {
		panic("fsync disabled")
	}
	var ids []bm.CustomerID
	var originals []*bm.Customer
	db.Read(func(tx *edb.Tx) {
		c := tx.TableScan(tx.Schema().TableByRow((*bm.Customer)(nil)), edb.FullScan())
		for c.Next() && len(ids) < 32 {
			row, _ := c.Row()
			customer := row.(*bm.Customer)
			ids = append(ids, customer.ID)
			originals = append(originals, customer)
		}
	})
	if len(ids) != 32 {
		panic("need 32 sampled customers")
	}
	base := make([]string, 32)
	wasNil := make([]bool, 32)
	for i, c := range originals {
		if _, ok := c.Extras[counterKey]; ok {
			panic("counter already exists")
		}
		wasNil[i] = c.Extras == nil
		base[i] = stable(c, wasNil[i])
	}
	var attempts, commits, rollbacks, reads, retries atomic.Int64
	var writers, readers sync.WaitGroup
	done := make(chan struct{})
	start := time.Now()
	for r := 0; r < 8; r++ {
		readers.Add(1)
		go func(r int) {
			defer readers.Done()
			last := make([]int64, 32)
			var held *bm.Customer
			var heldHash string
			loops := 0
			for {
				select {
				case <-done:
					return
				default:
				}
				if held != nil && digest(held) != heldHash {
					panic("retained native row changed after transaction")
				}
				i := (loops*7 + r) % 32
				db.Read(func(tx *edb.Tx) {
					c := edb.Get[bm.Customer](tx, ids[i])
					n := count(c)
					if n < last[i] {
						panic("committed counter went backwards")
					}
					last[i] = n
					if stable(c, wasNil[i]) != base[i] {
						panic("unrelated customer fields changed")
					}
					held = c
					heldHash = digest(c)
				})
				reads.Add(1)
				loops++
				if loops%2000 == 0 {
					runtime.GC()
				}
			}
		}(r)
	}
	for i, id := range ids {
		writers.Add(1)
		go func(i int, id bm.CustomerID) {
			defer writers.Done()
			for step := 1; step <= iterations; step++ {
				localAttempts := 0
				err := db.Tx(true, func(tx *edb.Tx) error {
					attempts.Add(1)
					localAttempts++
					c := edb.Get[bm.Customer](tx, id)
					if stable(c, wasNil[i]) != base[i] {
						panic("writer observed unrelated change")
					}
					if c.Extras == nil {
						c.Extras = map[string]any{}
					}
					c.Extras[counterKey] = count(c) + 1
					edb.Put(tx, c)
					if (step+i)%17 == 0 {
						return aborted
					}
					return nil
				})
				if localAttempts > 1 {
					retries.Add(int64(localAttempts - 1))
				}
				if (step+i)%17 == 0 {
					if !errors.Is(err, aborted) {
						panic("rollback result mismatch")
					}
					rollbacks.Add(1)
				} else {
					must(err)
					commits.Add(1)
				}
			}
		}(i, id)
	}
	writers.Wait()
	close(done)
	readers.Wait()
	var final []string
	db.Read(func(tx *edb.Tx) {
		for i, id := range ids {
			want := int64(iterations - (iterations+i)/17 + i/17)
			c := edb.Get[bm.Customer](tx, id)
			if count(c) != want || stable(c, wasNil[i]) != base[i] {
				panic("final committed state mismatch")
			}
			if stable(originals[i], wasNil[i]) != base[i] || count(originals[i]) != 0 {
				panic("retained original changed")
			}
			final = append(final, digest(c))
		}
	})
	duration := time.Since(start)
	db.Close()
	db, e = edb.Open(os.Args[1], fdb.Schema, edb.Options{ReuseTxBuffers: reuse, NoPersistentFreeList: true})
	must(e)
	db.Read(func(tx *edb.Tx) {
		for i, id := range ids {
			if digest(edb.Get[bm.Customer](tx, id)) != final[i] {
				panic("reopen changed committed state")
			}
		}
		for err := range tx.BoltTx().Check() {
			must(err)
		}
	})
	db.Close()
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{"writers": 32, "readers": 8, "iterations": iterations, "attempts": attempts.Load(), "commits": commits.Load(), "rollbacks": rollbacks.Load(), "callback_retries": retries.Load(), "reads": reads.Load(), "seconds": duration.Seconds(), "final": digest(final), "fsync": true, "reuse_tx_buffers": reuse, "errors": 0}))
}
