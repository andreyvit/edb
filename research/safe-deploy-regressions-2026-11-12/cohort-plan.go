//go:build ignore

// Emits private read-only lookup requests. Redirect stdout outside Git.
package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/fdb"
)

func main() {
	if len(os.Args) != 2 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("disposable sample path required")
	}
	db, err := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	type candidate struct {
		tenant, customer uint64
		priority         int
		rank             [32]byte
	}
	var candidates []candidate
	counts := map[int]int{}
	db.Read(func(tx *edb.Tx) {
		for c := tx.TableScan(fdb.OrdersTable, edb.FullScan()); c.Next(); {
			v, meta := c.RowVal()
			r := v.Interface().(*bm.Order)
			if r.TenantID == 0 || r.CustomerID == 0 {
				continue
			}
			priority := 3
			if meta.SchemaVer < 4 {
				priority = 2
				if len(r.PointsUsed) > 0 || r.PtsRedeemedDirectly > 0 {
					priority = 0
				}
				if r.ObsoleteIsPointsUsedRefunded {
					priority = 1
				}
			}
			counts[priority]++
			candidates = append(candidates, candidate{uint64(r.TenantID), uint64(r.CustomerID), priority, sha256.Sum256(c.RawKey())})
		}
	})
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].priority != candidates[j].priority {
			return candidates[i].priority < candidates[j].priority
		}
		return string(candidates[i].rank[:]) < string(candidates[j].rank[:])
	})
	selected := map[uint64]bool{}
	tenants := map[uint64]int{}
	selectedPriorities := map[int]int{}
	var requests []map[string]any
	enc := func(v uint64, tuple bool) string {
		b := binary.BigEndian.AppendUint64(nil, v)
		if tuple {
			b = append(b, 1)
		}
		return base64.StdEncoding.EncodeToString(b)
	}
	for _, c := range candidates {
		if selected[c.customer] || tenants[c.tenant] >= 2 {
			continue
		}
		selected[c.customer] = true
		tenants[c.tenant]++
		selectedPriorities[c.priority]++
		for _, tbl := range []string{"customers", "CustomerStats"} {
			requests = append(requests, map[string]any{"table": tbl, "key": enc(c.customer, true)})
		}
		if tenants[c.tenant] == 1 {
			for _, tbl := range []string{"tenants", "TenantSettings"} {
				requests = append(requests, map[string]any{"table": tbl, "key": enc(c.tenant, true)})
			}
		}
		for _, tbl := range []string{"Orders", "changes_points", "changes", "changes_assets", "changes_tiers", "ownerships", "coupons", "Subscriptions"} {
			requests = append(requests, map[string]any{"table": tbl, "index": "by_customer", "prefix": enc(c.customer, false), "limit": 10000})
		}
		for _, index := range []string{"by_referrer", "by_referree"} {
			requests = append(requests, map[string]any{"table": "referrals", "index": index, "prefix": enc(c.customer, false), "limit": 10000})
		}
		requests = append(requests, map[string]any{"table": "ReferralContact", "index": "by_referrer", "prefix": enc(c.customer, false), "limit": 10000})
		if len(selected) >= 96 {
			break
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(requests); err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stderr, "customers=%d tenants=%d candidates=%v selected=%v requests=%d\n", len(selected), len(tenants), counts, selectedPriorities, len(requests))
}
