//go:build ignore

// Real legacy ledgers through the deployed/candidate analytics consumers.
// For the old build change only the legacy import path to edb/kvo.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"

	"github.com/andreyvit/edb"
	legacy "github.com/andreyvit/edb/kvo/codec/fast1"
	"github.com/prairiegroupinc/fire/analytics"
	"github.com/prairiegroupinc/fire/analytics/mraggregates"
	"github.com/prairiegroupinc/fire/analytics/statregistry"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/bm/manalytics"
	"github.com/prairiegroupinc/fire/fdb"
	"github.com/prairiegroupinc/fire/ledgers"
	"github.com/prairiegroupinc/fire/nm"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func hash(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }
func code(p any) uint64 {
	if v, ok := p.(interface{ Code() uint64 }); ok {
		return v.Code()
	}
	return p.(uint64)
}

func canonical(m legacy.ImmutableMap) any {
	if m.IsMissing() {
		return nil
	}
	keys := append([]uint64(nil), m.Keys()...)
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	result := make([]any, 0, len(keys))
	for _, key := range keys {
		kind := int(m.KeyModel(key).ValueKind())
		var value any = m.Get(key)
		if kind == 2 {
			value = canonical(m.GetMap(key))
		}
		result = append(result, []any{key, kind, value})
	}
	return result
}

func encoded(m legacy.ImmutableMap) []byte { b, e := json.Marshal(canonical(m)); must(e); return b }
func safe(f func()) (failure string) {
	defer func() {
		if e := recover(); e != nil {
			failure = fmt.Sprintf("%T", e)
			fmt.Fprintf(os.Stderr, "analytics panic: %v\n%s", e, debug.Stack())
		}
	}()
	f()
	return
}
func main() {
	if len(os.Args) != 3 {
		panic("private legacy and native databases required")
	}
	for _, p := range os.Args[1:] {
		if !strings.Contains(p, "/fire-edb-regressions-") {
			panic("private disposable path required")
		}
	}
	db, e := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true})
	must(e)
	defer db.Close()
	native, e := edb.Open(os.Args[2], fdb.Schema, edb.Options{IsTesting: true})
	must(e)
	defer native.Close()
	tenants := map[bm.TenantID]*bm.RuntimeTenant{}
	native.Read(func(tx *edb.Tx) {
		for s := range edb.FullTableScan[bm.TenantSettings](tx).Rows() {
			t := edb.Get[bm.Tenant](tx, s.TenantID)
			if t == nil {
				t = &bm.Tenant{ID: s.TenantID}
			}
			rt := &bm.RuntimeTenant{Tenant: t, Settings: s, RuntimeVenueSet: &bm.RuntimeVenueSet{}}
			venue := &bm.RuntimeVenue{Tenant: rt}
			venue.Tiers.IsTiersEnabled = true
			venue.Tiers.EnabledTiers = s.Tiers
			venue.Referrals.IsReferralsEnabled = true
			rt.EnabledVenues = []*bm.RuntimeVenue{venue}
			rt.AllVenues = rt.EnabledVenues
			rt.DefaultVenue = venue
			tenants[s.TenantID] = rt
		}
	})
	limit, _ := strconv.Atoi(os.Getenv("AUDIT_ROW_LIMIT"))
	for _, tbl := range fdb.Schema.KVTables() {
		if tbl.Name() != "tld" && tbl.Name() != "cld" {
			continue
		}
		db.Read(func(tx *edb.Tx) {
			var previous legacy.ImmutableMap
			var previousPrefix string
			count := 0
			for c := tx.KVTableScan(tbl, edb.RawRange{}); c.Next(); {
				if limit > 0 && count >= limit {
					break
				}
				count++
				key := c.RawKey()
				tid := bm.TenantID(binary.BigEndian.Uint64(key[:8]))
				prefixLen := 8
				if tbl.Name() == "cld" {
					prefixLen = 16
				}
				if string(key[:prefixLen]) != previousPrefix {
					previous = legacy.ImmutableMap{}
				}
				end := c.Object()
				tenant := tenants[tid]
				missingSettings := tenant == nil
				if tenant == nil {
					tenant = &bm.RuntimeTenant{Tenant: &bm.Tenant{ID: tid}, Settings: &bm.TenantSettings{}, RuntimeVenueSet: &bm.RuntimeVenueSet{}}
					tenant.DefaultVenue = &bm.RuntimeVenue{Tenant: tenant}
				}
				out := map[string]any{"table": tbl.Name(), "key": hash(key), "missing_settings": missingSettings}
				cases := 0
				out["error"] = safe(func() {
					h := sha256.New()
					for flags := 0; flags < 8; flags++ {
						rt := *tenant
						s := *tenant.Settings
						rt.Settings = &s
						s.RequireExplicitLoyaltyOptin = flags&1 != 0
						s.RequireSubscriberStatus = flags&2 != 0
						s.IncludeGuestsInParticipantMetrics = flags&4 != 0
						ctx := &ledgers.AlgContext{Tenant: &rt, IsRequiringActiveAccounts: flags&2 != 0, UseRecordedMembership: flags&4 != 0}
						for _, before := range []legacy.ImmutableMap{{}, previous, end} {
							if tbl.Name() == "cld" {
								for _, transition := range []ledgers.Transition{ledgers.Staying, ledgers.Entering, ledgers.Leaving} {
									agg := legacy.NewRecord(nm.TTenantLedger)
									for _, alg := range statregistry.Pack.TenantLedgerAlgs {
										alg.EnumChanges(ctx, agg, before, end, transition, func(change ledgers.Change) { change.ApplyTo(agg) })
									}
									b := encoded(agg.Record().PackedRoot())
									h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(b))))
									h.Write(b)
									cases++
								}
							} else {
								filters := []*manalytics.Filter{nil, {NoAccess: true}, {CampaignID: 1}}
								for campaign := range end.GetMap(code(nm.KDocCampaigns)).Items() {
									filters = append(filters, &manalytics.Filter{CampaignID: bm.ID(campaign)})
									if len(filters) >= 6 {
										break
									}
								}
								for _, filter := range filters {
									var agg *mraggregates.Aggregate
									if flags&4 != 0 {
										agg = &mraggregates.Aggregate{}
									}
									result := analytics.ComputeRangeStats(statregistry.Pack, before, end, nil, agg, ctx, filter, flags&1 != 0)
									b := encoded(result)
									h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(b))))
									h.Write(b)
									cases++
								}
							}
						}
					}
					out["hash"] = fmt.Sprintf("%x", h.Sum(nil))
				})
				out["cases"] = cases
				must(json.NewEncoder(os.Stdout).Encode(out))
				previous = end
				previousPrefix = string(key[:prefixLen])
			}
		})
	}
}
