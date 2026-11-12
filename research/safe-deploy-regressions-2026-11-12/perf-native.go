//go:build ignore

// Storage CPU/allocation control, not a production latency benchmark. Fsync is
// optional; durable concurrency is a separate probe.
package main

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/fdb"
	"go.etcd.io/bbolt"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}

var sink any

func cpuSeconds() float64 {
	var r syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &r))
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}

func main() {
	if len(os.Args) != 3 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("private disposable database and seconds per case required")
	}
	seconds, e := strconv.Atoi(os.Args[2])
	must(e)
	rowLimit := 128
	if s := os.Getenv("AUDIT_ROWS"); s != "" {
		rowLimit, e = strconv.Atoi(s)
		must(e)
		if rowLimit < 1 || rowLimit > 4096 {
			panic("row limit must be 1..4096")
		}
	}
	fsync := os.Getenv("AUDIT_FSYNC") == "true"
	runtime.GOMAXPROCS(2)
	db, e := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true, NoPersistentFreeList: true})
	must(e)
	defer db.Close()
	db.Bolt().NoSync = !fsync
	db.Bolt().StrictMode = false
	names := strings.Split("customers,Orders,CustomerStats,changes,changes_points,ownerships,jobs,products,TenantSettings,tenants", ",")
	if only := os.Getenv("AUDIT_TABLES"); only != "" {
		names = strings.Split(only, ",")
	}
	for _, name := range names {
		fieldName := "TenantID"
		mutation := os.Getenv("AUDIT_MUTATION")
		if mutation == "ordinary" {
			switch name {
			case "customers":
				fieldName = "Name"
			case "changes_points":
				fieldName = "PrivateNote"
			case "jobs":
				fieldName = "LastAttemptTime"
			}
		}
		rollback := os.Getenv("AUDIT_ROLLBACK") == "true"
		var keys [][]byte
		var totalBytes int64
		must(db.Bolt().View(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(name)).Bucket([]byte("data"))
			var all [][]byte
			var sizes []int
			must(b.ForEach(func(k, v []byte) error {
				if len(v) <= 1<<20 {
					all = append(all, bytes.Clone(k))
					sizes = append(sizes, len(v))
				}
				return nil
			}))
			n := min(rowLimit, len(all))
			for i := 0; i < n; i++ {
				j := i * (len(all) - 1) / max(1, n-1)
				keys = append(keys, all[j])
				totalBytes += int64(sizes[j])
			}
			return nil
		}))
		if len(keys) == 0 {
			continue
		}
		for _, tbl := range fdb.Schema.Tables() {
			if tbl.Name() != name {
				continue
			}
			for _, write := range []bool{false, true} {
				if mode := os.Getenv("AUDIT_MODE"); mode == "read" && write || mode == "write" && !write {
					continue
				}
				canWrite := false
				db.Read(func(tx *edb.Tx) {
					row, _ := tx.GetByKeyRaw(tbl, keys[0])
					f := reflect.ValueOf(row).Elem().FieldByName(fieldName)
					canWrite = f.IsValid() && f.CanSet() && (f.CanUint() || f.Kind() == reflect.String || f.Type() == reflect.TypeFor[time.Time]())
					canWrite = canWrite || mutation == "none"
				})
				if write && !canWrite {
					continue
				}
				batch := func() {
					fn := func(tx *edb.Tx) {
						for _, key := range keys {
							row, _ := tx.GetByKeyRaw(tbl, key)
							if write {
								f := reflect.ValueOf(row).Elem().FieldByName(fieldName)
								switch {
								case mutation == "none":
									// Preserve every field but still invoke ordinary Put.
								case f.CanUint():
									f.SetUint(f.Uint() ^ 0x100000)
								case f.Kind() == reflect.String:
									s := f.String()
									if strings.HasSuffix(s, " audit") {
										s = strings.TrimSuffix(s, " audit")
									} else {
										s += " audit"
									}
									f.SetString(s)
								default:
									f.Set(reflect.ValueOf(f.Interface().(time.Time).Add(time.Nanosecond)))
								}
								tx.Put(tbl, row)
							}
							sink = row
						}
					}
					if write {
						if rollback {
							tx := db.BeginUpdate()
							fn(tx)
							tx.Close()
						} else {
							db.Write(fn)
						}
					} else {
						db.Read(fn)
					}
				}
				batch()
				runtime.GC()
				var profile *os.File
				if prefix := os.Getenv("AUDIT_CPU_PROFILE"); prefix != "" {
					profile, e = os.Create(prefix + "-" + name + "-" + strconv.FormatBool(write) + ".pprof")
					must(e)
					must(pprof.StartCPUProfile(profile))
				}
				var a, b runtime.MemStats
				boltBefore := db.Bolt().Stats()
				runtime.ReadMemStats(&a)
				cpuStart := cpuSeconds()
				started := time.Now()
				operations := 0
				for time.Since(started) < time.Duration(seconds)*time.Second {
					batch()
					operations += len(keys)
				}
				elapsed := time.Since(started)
				cpuElapsed := cpuSeconds() - cpuStart
				boltAfter := db.Bolt().Stats()
				boltDelta := boltAfter.Sub(&boltBefore)
				runtime.ReadMemStats(&b)
				if profile != nil {
					pprof.StopCPUProfile()
					must(profile.Close())
				}
				mode := "read"
				if write {
					mode = "read-update-write"
				}
				must(json.NewEncoder(os.Stdout).Encode(map[string]any{"table": name, "mode": mode, "field": fieldName, "mutation": mutation, "rollback": rollback, "rows": len(keys), "mean_raw_bytes": totalBytes / int64(len(keys)), "operations": operations, "seconds": elapsed.Seconds(), "ns_per_op": float64(elapsed.Nanoseconds()) / float64(operations), "cpu_ns_per_op": cpuElapsed * 1e9 / float64(operations), "bytes_per_op": float64(b.TotalAlloc-a.TotalAlloc) / float64(operations), "allocs_per_op": float64(b.Mallocs-a.Mallocs) / float64(operations), "gc_cycles": b.NumGC - a.NumGC, "bolt_write_ns_per_op": float64(boltDelta.TxStats.GetWriteTime().Nanoseconds()) / float64(operations), "bolt_page_bytes_per_op": float64(boltDelta.TxStats.GetPageAlloc()) / float64(operations), "bolt_writes_per_op": float64(boltDelta.TxStats.GetWrite()) / float64(operations), "fsync": fsync, "reuse_tx_buffers": false, "gomaxprocs": 2}))
			}
		}
	}
}
