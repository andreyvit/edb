//go:build ignore

// Actual Fire export/import bodies with a storage-only context. No server,
// background worker, integration client or network service is initialized.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/bm/mcom"
	"github.com/prairiegroupinc/fire/fdb"
	"github.com/prairiegroupinc/fire/fire"
	"github.com/prairiegroupinc/fire/fire/core/coredbxfer"
)

type contextContract interface{ fire.Context }
type auditContext struct {
	contextContract
	db                         *edb.DB
	tx                         *edb.Tx
	reading                    bool
	cancelled                  bool
	progress, cancelAt, writes int
}

func (c *auditContext) DBTx() *edb.Tx {
	if c.tx == nil {
		c.tx = c.db.BeginRead()
		c.reading = true
	}
	return c.tx
}
func (c *auditContext) DoneReading() {
	if c.reading {
		c.tx.Close()
		c.tx = nil
		c.reading = false
	}
}
func (c *auditContext) MustWrite(f func()) {
	c.DoneReading()
	c.db.Write(edb.TxMetadata{}, func(tx *edb.Tx) { c.tx = tx; defer func() { c.tx = nil }(); f() })
	c.writes++
}
func (c *auditContext) Err() error {
	if c.cancelled {
		return context.Canceled
	}
	return nil
}
func (c *auditContext) ReportJobProgress(string) bool {
	c.progress++
	return c.cancelAt > 0 && c.progress >= c.cancelAt
}
func (c *auditContext) L() fire.Logger {
	return fire.NewLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
}
func must(e error) {
	if e != nil {
		panic(e)
	}
}
func emit(v any)             { must(json.NewEncoder(os.Stdout).Encode(v)) }
func digest(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }
func runExport(db *edb.DB, tenant *bm.RuntimeTenant, filter *coredbxfer.CustomerFilter, batch, cancel int) ([]byte, int, int, error) {
	c := &auditContext{db: db}
	var buf bytes.Buffer
	batches := 0
	opts := &coredbxfer.ExportOptions{CustomerFilter: filter, ReadBatchSize: batch,
		AfterReadBatch: func() {
			batches++
			if db.Bolt().Stats().OpenTxN != 0 {
				panic("export retained read transaction")
			}
		},
		IsCancelled: func() bool { return cancel > 0 && batches >= cancel },
	}
	n, e := coredbxfer.ExportData(c, tenant, &buf, opts)
	if c.tx != nil || db.Bolt().Stats().OpenTxN != 0 {
		panic("export leaked transaction")
	}
	return buf.Bytes(), n, batches, e
}
func export(db *edb.DB, dir string) {
	must(os.MkdirAll(dir, 0700))
	customers := map[bm.TenantID][]mcom.CustomerID{}
	var tenants []bm.TenantID
	db.Read(func(tx *edb.Tx) {
		for t := range edb.FullTableScan[bm.Tenant](tx).Rows() {
			tenants = append(tenants, t.ID)
		}
		for c := range edb.FullTableScan[bm.Customer](tx).Rows() {
			customers[c.TenantID] = append(customers[c.TenantID], c.ID)
		}
	})
	slices.Sort(tenants)
	for i, id := range tenants {
		tenant := &bm.RuntimeTenant{Tenant: &bm.Tenant{ID: id}}
		own := customers[id]
		if len(own) > 2 {
			own = own[:2]
		}
		mixed := slices.Clone(own)
		for _, other := range tenants {
			if other != id && len(customers[other]) > 0 {
				mixed = append(mixed, customers[other][0])
				break
			}
		}
		mixed = append(mixed, own...) // Duplicate valid IDs must not duplicate output.
		filters := []*coredbxfer.CustomerFilter{nil, coredbxfer.NewCustomerFilter(own), coredbxfer.NewCustomerFilter(mixed), coredbxfer.NewCustomerFilter(nil)}
		var ownBytes []byte
		for fi, filter := range filters {
			var expected []byte
			for _, batch := range []int{1, 7, 128, 10000} {
				b, rows, batches, e := runExport(db, tenant, filter, batch, 0)
				must(e)
				if expected == nil {
					expected = bytes.Clone(b)
				} else if !bytes.Equal(expected, b) {
					panic("batch size changed export")
				}
				if batch == 1 && fi == 0 {
					must(os.WriteFile(filepath.Join(dir, fmt.Sprintf("tenant-%03d.wal", i)), b, 0600))
				}
				emit(map[string]any{"tenant_ordinal": i, "filter": fi, "batch": batch, "rows": rows, "batches": batches, "bytes": len(b), "hash": digest(b)})
			}
			if fi == 1 {
				ownBytes = bytes.Clone(expected)
			}
			if fi == 2 && !bytes.Equal(ownBytes, expected) {
				panic("foreign or duplicate customer changed filtered export")
			}
			for _, cancel := range []int{1, 3} {
				_, _, batches, e := runExport(db, tenant, filter, 1, cancel)
				if batches >= cancel && !errors.Is(e, context.Canceled) {
					panic("missing cancellation")
				}
				if batches < cancel {
					must(e)
				}
			}
		}
	}
}
func importData(db *edb.DB, dir string, cancelAt int) {
	files, e := filepath.Glob(filepath.Join(dir, "tenant-*.wal"))
	must(e)
	for i, name := range files {
		b, e := os.ReadFile(name)
		must(e)
		c := &auditContext{db: db, cancelAt: cancelAt}
		tenant := &bm.RuntimeTenant{Tenant: &bm.Tenant{ID: bm.TenantID(0x7ffd000000000000 + i)}}
		stats, e := coredbxfer.ImportData(c, tenant, bytes.NewReader(b))
		if cancelAt > 0 && c.progress >= cancelAt {
			if !errors.Is(e, context.Canceled) {
				panic("missing import cancellation")
			}
		} else {
			must(e)
		}
		if c.tx != nil || db.Bolt().Stats().OpenTxN != 0 {
			panic("import leaked transaction")
		}
		emit(map[string]any{"tenant_ordinal": i, "rows": stats.Rows, "writes": c.writes, "cancelled": errors.Is(e, context.Canceled)})
	}
}
func main() {
	if len(os.Args) != 4 || !strings.Contains(os.Args[2], "/fire-edb-regressions-") || !strings.Contains(os.Args[3], "/fire-edb-regressions-") {
		panic("mode, private database and private WAL directory required")
	}
	db, e := edb.Open(os.Args[2], fdb.Schema, edb.Options{IsTesting: true})
	must(e)
	defer db.Close()
	switch os.Args[1] {
	case "export":
		export(db, os.Args[3])
	case "import":
		importData(db, os.Args[3], 0)
	case "cancel-import":
		importData(db, os.Args[3], 17)
	default:
		panic("invalid mode")
	}
}
