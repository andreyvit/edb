//go:build ignore

// Select real referrals and fetch both customers, contact and tenant settings.
// Stdout is a private plan containing identifiers; redirect outside Git.
package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"strings"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/bm/mreferrals"
	"github.com/prairiegroupinc/fire/fdb"
)

func main() {
	if len(os.Args) != 2 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("private disposable database required")
	}
	db, err := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	var requests []map[string]any
	counts := map[uint64]int{}
	add := func(table string, id uint64) {
		if id == 0 {
			return
		}
		key := append(binary.BigEndian.AppendUint64(nil, id), 1)
		requests = append(requests, map[string]any{"table": table, "key": base64.StdEncoding.EncodeToString(key)})
	}
	n := 0
	db.Read(func(tx *edb.Tx) {
		for row := range edb.FullTableScan[mreferrals.Referral](tx).Rows() {
			if counts[uint64(row.TenantID)] >= 3 || row.ContactID == 0 {
				continue
			}
			counts[uint64(row.TenantID)]++
			add("referrals", uint64(row.ID))
			add("ReferralContact", uint64(row.ContactID))
			for _, id := range []uint64{uint64(row.ReferrerID), uint64(row.ReferreeID)} {
				add("customers", id)
				add("CustomerStats", id)
			}
			add("TenantSettings", uint64(row.TenantID))
			add("tenants", uint64(row.TenantID))
			n++
			if n >= 64 {
				break
			}
		}
	})
	if err := json.NewEncoder(os.Stdout).Encode(requests); err != nil {
		panic(err)
	}
	if err := json.NewEncoder(os.Stderr).Encode(map[string]int{"referrals": n, "tenants": len(counts), "requests": len(requests)}); err != nil {
		panic(err)
	}
}
