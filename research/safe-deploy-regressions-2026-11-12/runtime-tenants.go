//go:build ignore

// Load sampled tenant configuration through Fire's real runtime builder.
// Use sandbox-exec with network denied in addition to the rejecting transport.
// The bootstrap database is empty; workers and HTTP serving are never started.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/andreyvit/edb"
	"github.com/jonboulle/clockwork"
	blurr "github.com/prairiegroupinc/fire"
	"github.com/prairiegroupinc/fire/blurrhosts"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/bm/mrules"
	"github.com/prairiegroupinc/fire/bm/mrulesexpr"
	"github.com/prairiegroupinc/fire/bm/muploads"
	"github.com/prairiegroupinc/fire/envs"
	"github.com/prairiegroupinc/fire/farm/farmconf"
	"github.com/prairiegroupinc/fire/fdb"
	"github.com/prairiegroupinc/fire/fire"
	"github.com/prairiegroupinc/fire/fire/core/coreconf"
	"github.com/prairiegroupinc/fire/fire/core/coreexpr"
	"github.com/prairiegroupinc/fire/fire/fireappsettings"
	"github.com/prairiegroupinc/fire/fire/fireintegration"
	"github.com/prairiegroupinc/fire/fireback"
	"github.com/prairiegroupinc/fire/firejobs"
	_ "github.com/prairiegroupinc/fire/preinit"
)

type denyTransport struct{ calls atomic.Int64 }

func (t *denyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return nil, errors.New("audit denies all network requests")
}
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
func main() {
	if len(os.Args) != 4 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") || !strings.Contains(os.Args[2], "/fire-edb-regressions-") {
		panic("private sample DB, fresh bootstrap directory, tenant limit required")
	}
	limit, e := strconv.Atoi(os.Args[3])
	must(e)
	dir := os.Args[2]
	must(os.Mkdir(dir, 0700))
	transport := &denyTransport{}
	keys, keyname := blurr.FakeSecretsKeyring()
	conf := &blurr.Config{Secrets: blurr.FakeSecrets(), SecretsKeyring: keys, SecretsDefaultKeyName: keyname, ConfigOptions: fireappsettings.ConfigOptions{
		DBFile: filepath.Join(dir, "bootstrap.db"), AppName: "Offline audit", NodeNameOverride: "local", TenantsDataDir: filepath.Join(dir, "tenants"),
		FileUploadDirTrusted: filepath.Join(dir, "trusted"), FileUploadDirUntrusted: filepath.Join(dir, "untrusted"), FileUploadDirTransient: filepath.Join(dir, "transient"),
		Hosts: blurrhosts.Strings{FireAPIBaseURLString: "https://fire.test.bubblehouse.com/", FireWebBaseURLString: "https://app.test.bubblehouse.com/"},
	}}
	resourceRoot := os.Getenv("AUDIT_RESOURCE_DIR")
	if resourceRoot != "" {
		if !strings.Contains(resourceRoot, "/fire-edb-regressions-") {
			panic("private resource directory required")
		}
		conf.FileUploadDirTrusted = filepath.Join(resourceRoot, "trusted")
		conf.FileUploadDirUntrusted = filepath.Join(resourceRoot, "untrusted")
	}
	app := fireback.New(fireintegration.Pack, conf, farmconf.Fake("local"), fireback.Options{
		Env: envs.Test, Clock: clockwork.NewFakeClockAt(time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)), TestTransport: transport, EnabledJobs: map[*firejobs.Kind]bool{},
		TestingBehaviors: fire.TestingBehaviors{SkipJournals: true, LazyLoadTemplates: true, InMemoryUploadedFiles: resourceRoot == "", SmallJobQueueInitialCapacity: true},
	})
	defer app.Close()
	db, e := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true})
	must(e)
	defer db.Close()
	ordinal := 0
	db.Read(func(tx *edb.Tx) {
		customers := map[bm.TenantID][]*bm.Customer{}
		orders := map[bm.CustomerID][]*bm.Order{}
		if os.Getenv("AUDIT_RULES") == "true" {
			for c := range edb.FullTableScan[bm.Customer](tx).Rows() {
				customers[c.TenantID] = append(customers[c.TenantID], c)
			}
			for o := range edb.FullTableScan[bm.Order](tx).Rows() {
				if len(orders[o.CustomerID]) < 2 {
					orders[o.CustomerID] = append(orders[o.CustomerID], o)
				}
			}
			statsPresent := map[bm.CustomerID]bool{}
			for s := range edb.FullTableScan[bm.CustomerStatsOverall](tx).Rows() {
				statsPresent[s.CustomerID] = true
			}
			score := func(c *bm.Customer) int {
				result := 0
				if len(orders[c.ID]) > 0 {
					result += 2
				}
				if statsPresent[c.ID] {
					result++
				}
				return result
			}
			for tenant, rows := range customers {
				sort.SliceStable(rows, func(i, j int) bool { return score(rows[i]) > score(rows[j]) })
				customers[tenant] = rows[:min(len(rows), 16)]
			}
		}
		for t := range edb.FullTableScan[bm.Tenant](tx).Rows() {
			if edb.Get[bm.TenantSettings](tx, t.ID) == nil {
				continue
			}
			if limit > 0 && ordinal >= limit {
				break
			}
			started := time.Now()
			rt := app.LoadRuntimeTenantFrom(tx, fdb.LoadTenantSource(tx, t))
			if os.Getenv("AUDIT_RULES") == "true" {
				queryRuntimeRules(app, tx, rt, ordinal, customers[t.ID], orders)
			}
			venues := []any{}
			roles := map[int]any{}
			if rt.RuntimeVenueSet != nil {
				for _, v := range rt.AllVenues {
					markets := []any{}
					for _, m := range v.OrderedMarkets {
						markets = append(markets, []any{m.Identity, m.Currency, m.Locale, m.EffectiveDomain, m.EffectivePath, m.LoyaltyProgram, m.PointsExpiration})
					}
					venues = append(venues, []any{v.ID, v.Ordinal, v.EcommerceIntegration, v.VenueCurrency, v.IsOrderProcessingDisabled, v.ShopifyGiftsEnabled, v.UseBubbleLogins, markets})
				}
			}
			for role, r := range rt.Roles {
				roles[int(role)] = []any{r.Disabled, r.Name}
			}
			tabs := []any{}
			resolvedTabs := []any{}
			resources := rt.Settings.PluginResources
			if f := reflect.ValueOf(rt).Elem().FieldByName("PluginResources"); f.IsValid() {
				resources = f.Interface().(map[string]*muploads.FileRef)
			}
			if rt.Plugin != nil {
				for _, tab := range rt.Plugin.StaticTabs {
					tabs = append(tabs, []any{tab.Section, tab.ItemID, tab.SemPath, tab.TabID, tab.TabTitle, tab.PageTitle, tab.HTMLFile})
					if resourceRoot != "" {
						file, err := fdb.LoadPluginResource(tx, rt.ID, resources[tab.HTMLFile])
						must(err)
						body, err := app.ReadUploadedFile2(file.Ref())
						must(err)
						hash := fmt.Sprintf("%x", sha256.Sum256(body))
						if int64(len(body)) != file.Size || hash != file.Hash {
							panic("plugin resource body mismatch")
						}
						resolvedTabs = append(resolvedTabs, []any{tab.Section, tab.ItemID, tab.SemPath, tab.TabID, tab.TabTitle, tab.PageTitle, file.FileName, hash})
					}
				}
			}
			crashed := false
			for _, e := range rt.Errors {
				if strings.Contains(e, "load crashed:") {
					crashed = true
				}
			}
			must(json.NewEncoder(os.Stdout).Encode(map[string]any{"tenant_ordinal": ordinal, "seconds": time.Since(started).Seconds(), "crashed": crashed, "errors": rt.Errors, "runtime_hash": digest([]any{venues, roles, rt.BaseCurrency, rt.OfflineAPI, rt.OfflineWeb, rt.OfflineAdmin, rt.OfflineOrderDownloads, rt.OfflineAllWebhooks, rt.IsReadOnly, rt.IsProcessingEnabled}), "http_attempts": transport.calls.Load(), "plugin_tabs": len(tabs), "plugin_tabs_hash": digest(tabs), "plugin_tab_values_private": tabs, "resolved_plugin_tabs_hash": digest(resolvedTabs)}))
			ordinal++
		}
	})
}

func queryRuntimeRules(app *fireback.App, tx *edb.Tx, rt *bm.RuntimeTenant, ordinal int, customers []*bm.Customer, orders map[bm.CustomerID][]*bm.Order) {
	defer func() {
		if failure := recover(); failure != nil {
			must(json.NewEncoder(os.Stdout).Encode(map[string]any{"phase": "rules", "tenant_ordinal": ordinal, "panic": fmt.Sprintf("%T: %v", failure, failure)}))
		}
	}()
	keys := map[uint64]bool{193: true, 195: true, 196: true, 197: true}
	for _, rule := range rt.EnabledRules {
		for key := range rule.Aspects {
			keys[key] = true
		}
	}
	codes := make([]uint64, 0, len(keys))
	for key := range keys {
		codes = append(codes, key)
	}
	sort.Slice(codes, func(i, j int) bool { return codes[i] < codes[j] })
	actualCustomers := len(customers)
	customers = append([]*bm.Customer{nil}, customers...)
	var results []any
	statsRows, orderRows, queryCount := 0, 0, 0
	for _, customer := range customers {
		var stats *bm.CustomerStatsOverall
		customerOrders := []*bm.Order{nil}
		if customer != nil {
			stats = edb.Get[bm.CustomerStatsOverall](tx, customer.ID)
			if stats != nil {
				statsRows++
			}
			for _, order := range orders[customer.ID] {
				if order.TenantID == rt.ID {
					customerOrders = append(customerOrders, order)
					orderRows++
				}
			}
		}
		for _, order := range customerOrders {
			for _, venue := range rt.AllVenues {
				for _, market := range venue.OrderedMarkets {
					for _, year := range []int{2025, 2026, 2027} {
						var input mrulesexpr.Input
						coreexpr.Fill(&input, rt.EnabledRuleFlags, rt, customer, coreexpr.KnownStats(stats), order, market, time.Date(year, 11, 12, 0, 0, 0, 0, time.UTC))
						queries := make([]mrules.ValueQuery, len(codes))
						for i, code := range codes {
							queries[i].Key = code
						}
						coreconf.Query(app, rt, &input, queries)
						queryCount += len(queries)
						results = append(results, []any{input, queries})
					}
				}
			}
		}
	}
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{"phase": "rules", "tenant_ordinal": ordinal, "customers": actualCustomers, "stats": statsRows, "orders": orderRows, "inputs": len(results), "queries": queryCount, "rules": len(rt.EnabledRules), "hash": digest(results)}))
}
