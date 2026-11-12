//go:build ignore

// Build unchanged in deployed/candidate Fire. Exercise new legacy snapshots
// produced from native business rows, in addition to reading old snapshots.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/bm/mdocuploads"
	"github.com/prairiegroupinc/fire/bm/mreferrals"
	"github.com/prairiegroupinc/fire/fdb"
	"github.com/prairiegroupinc/fire/fire"
)

type contextContract interface{ fire.Context }
type probeContext struct {
	contextContract
	tx *edb.Tx
}

func (c probeContext) DBTx() *edb.Tx { return c.tx }
func must(e error) {
	if e != nil {
		panic(e)
	}
}
func hash(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }
func main() {
	if len(os.Args) != 2 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("private disposable database required")
	}
	db, e := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true})
	must(e)
	defer db.Close()
	db.Read(func(tx *edb.Tx) {
		rc := probeContext{tx: tx}
		tenants := map[bm.TenantID]*bm.RuntimeTenant{}
		for _, tbl := range fdb.Schema.Tables() {
			switch tbl.Name() {
			case "customers", "Orders", "referrals", "ReferralContact", "receipts":
			default:
				continue
			}
			c := tx.TableScan(tbl, edb.FullScan())
			for c.Next() {
				row, _ := c.Row()
				var snapshots []string
				switch r := row.(type) {
				case *bm.Customer:
					tenant := tenants[r.TenantID]
					if tenant == nil {
						t := edb.Get[bm.Tenant](tx, r.TenantID)
						if t == nil {
							t = &bm.Tenant{ID: r.TenantID}
						}
						s := edb.Get[bm.TenantSettings](tx, r.TenantID)
						if s == nil {
							s = &bm.TenantSettings{}
						}
						tenant = &bm.RuntimeTenant{Tenant: t, Settings: s}
						tenants[r.TenantID] = tenant
					}
					cs := fdb.CustomerBalance(tx, r.TenantID, r.ID)
					for flags := 0; flags < 8; flags++ {
						v := *tenant
						v.CustomerAccountStateHandling.TreatShopInactiveAsActive = flags&1 != 0
						v.CustomerAccountStateHandling.EffectiveShopTagsToMakeCustomerActive = nil
						if flags&2 != 0 {
							v.CustomerAccountStateHandling.EffectiveShopTagsToMakeCustomerActive = r.ShopTags
						}
						stats := cs
						if flags&4 != 0 {
							stats = nil
						}
						snapshots = append(snapshots, hash(fdb.BuildCustomerSnapshot(r, &v, stats).Pack().Bytes()))
					}
				case *bm.Order:
					snapshots = append(snapshots, hash(fdb.BuildOrderSnapshot(rc, r).Pack().Bytes()))
				case *mreferrals.Referral:
					snapshots = append(snapshots, hash(fdb.BuildReferralSnapshot(rc, r).Pack().Bytes()))
				case *mreferrals.ReferralContact:
					snapshots = append(snapshots, hash(fdb.BuildReferralLinkSnapshot(r).Pack().Bytes()))
				case *mdocuploads.Document:
					for _, n := range []uint16{0, 1, 255, 65535} {
						snapshots = append(snapshots, hash(fdb.BuildDocSnapshot(r, n, bm.ID(n), n, n).Pack().Bytes()))
					}
				default:
					panic(fmt.Sprintf("unexpected table type %T", row))
				}
				must(json.NewEncoder(os.Stdout).Encode(map[string]any{"table": tbl.Name(), "key": hash(c.RawKey()), "snapshots": snapshots}))
			}
		}
	})
}
