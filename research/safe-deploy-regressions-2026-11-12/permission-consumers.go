//go:build ignore

// Compare existing permission consumers after decoding and saving real users.
// For the deployed build, change only the mpermissions import to alias bm.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"os"
	"slices"
	"strings"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/bm/mmarkets"
	"github.com/prairiegroupinc/fire/bm/mpermissions"
	"github.com/prairiegroupinc/fire/envs"
	"github.com/prairiegroupinc/fire/fdb"
	"github.com/prairiegroupinc/fire/fire/core/coreusers"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func add(h hash.Hash, v any) { b, e := json.Marshal(v); must(e); h.Write(b); h.Write([]byte{0}) }
func digest(v any) string    { h := sha256.New(); add(h, v); return fmt.Sprintf("%x", h.Sum(nil)) }
func main() {
	if len(os.Args) == 2 && os.Args[1] == "permissions" {
		must(json.NewEncoder(os.Stdout).Encode(mpermissions.AllPermissions))
		return
	}
	if len(os.Args) != 3 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("private database and old permission list required")
	}
	var permissions []mpermissions.Permission
	b, e := os.ReadFile(os.Args[2])
	must(e)
	must(json.Unmarshal(b, &permissions))
	db, e := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true})
	must(e)
	defer db.Close()
	var tenants []*bm.RuntimeTenant
	var users []*bm.User
	db.Read(func(tx *edb.Tx) {
		for t := range edb.FullTableScan[bm.Tenant](tx).Rows() {
			s := edb.Get[bm.TenantSettings](tx, t.ID)
			if s != nil {
				tenants = append(tenants, &bm.RuntimeTenant{Tenant: t, Settings: s})
			}
		}
		users = edb.All(edb.FullTableScan[bm.User](tx))
	})
	slices.SortFunc(tenants, func(a, b *bm.RuntimeTenant) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	for i, user := range users {
		before := digest(user)
		// Both rollback and commit must preserve unprojected auth fields.
		func() {
			tx := db.BeginUpdate()
			defer tx.Close()
			copy := edb.Get[bm.User](tx, user.ID)
			copy.RequirePasswordChange = !copy.RequirePasswordChange
			edb.Put(tx, copy)
		}()
		db.Read(func(tx *edb.Tx) {
			if digest(edb.Get[bm.User](tx, user.ID)) != before {
				panic("rollback changed user")
			}
		})
		db.Write(func(tx *edb.Tx) {
			copy := edb.Get[bm.User](tx, user.ID)
			copy.RequirePasswordChange = !copy.RequirePasswordChange
			edb.Put(tx, copy)
		})
		db.Read(func(tx *edb.Tx) {
			copy := edb.Get[bm.User](tx, user.ID)
			copy.RequirePasswordChange = !copy.RequirePasswordChange
			if digest(copy) != before {
				panic("save changed unrelated auth fields")
			}
			user = copy
		})
		h := sha256.New()
		checks := 0
		for _, env := range []envs.Env{envs.Production, envs.Dev} {
			role := user.ActorEnvRole(env)
			add(h, []any{role, role.DefaultShopRole(), role.DefaultPartnerRole()})
			for _, p := range permissions {
				add(h, role.Allows(p))
				checks++
			}
		}
		for _, tenant := range tenants {
			role := user.ActorShopRole(tenant.ID, tenant.Slug)
			defaults := role.DefaultPermissions()
			add(h, role)
			for _, p := range permissions {
				add(h, slices.Contains(defaults, p))
				checks++
			}
			for flags := 0; flags < 4; flags++ {
				t := *tenant
				s := *tenant.Settings
				t.Settings = &s
				s.AllowToAssignUsersToMarkets = flags&1 != 0
				s.AllowToAssignUsersToCampaigns = flags&2 != 0
				markets := append([]mmarkets.Key{"", mmarkets.DefaultMarketKey, "__audit_unknown_market__"}, user.ShopMarkets[t.Slug]...)
				campaigns := append([]string{"", "__audit_unknown_campaign__"}, user.ShopCampaigns[t.Slug]...)
				add(h, coreusers.UserAllowedMarkets(&t, user))
				add(h, coreusers.UserAllowedCampaigns(&t, user))
				for _, m := range markets {
					add(h, coreusers.UserCanAccessMarket(&t, user, m))
					checks++
				}
				for _, c := range campaigns {
					add(h, coreusers.UserCanAccessCampaign(&t, user, c))
					checks++
				}
			}
		}
		must(json.NewEncoder(os.Stdout).Encode(map[string]any{"user_ordinal": i, "tenants": len(tenants), "permissions": len(permissions), "checks": checks, "hash": fmt.Sprintf("%x", h.Sum(nil)), "auth_fields_preserved": true}))
	}
}
