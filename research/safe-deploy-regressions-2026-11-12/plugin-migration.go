//go:build ignore

// Offline execution of the actual startup migration bodies on sampled settings.
// The context supplies only storage, IDs, time and a discarded logger. Any other
// application service call fails. No Fire server or external client is started.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/fdb"
	"github.com/prairiegroupinc/fire/fdb/dbmigrations"
	"github.com/prairiegroupinc/fire/fire"
	"github.com/prairiegroupinc/fire/fire/core/coreplugin"
	"github.com/prairiegroupinc/fire/nm"
	"github.com/prairiegroupinc/fire/plugins"
	"github.com/prairiegroupinc/fire/plugins/bh"
	"github.com/prairiegroupinc/fire/plugins/tenantenv"
	"go.etcd.io/bbolt"
)

type contextContract interface{ fire.Context }
type appContract interface{ fire.App }
type auditApp struct {
	appContract
	next bm.ID
}

func (a *auditApp) NewID() bm.ID { a.next++; return a.next }

type auditContext struct {
	contextContract
	app *auditApp
	tx  *edb.Tx
}

func (c auditContext) DBTx() *edb.Tx     { return c.tx }
func (c auditContext) FireApp() fire.App { return c.app }
func (c auditContext) NewID() bm.ID      { return c.app.NewID() }
func (c auditContext) Now() time.Time    { return time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC) }
func (c auditContext) L() fire.Logger {
	return fire.NewLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
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
func unchangedSettings(s *bm.TenantSettings) string {
	s.XNormalize()
	s.PluginSource = ""
	s.PluginResources = nil
	s.PluginDraft = nil
	s.Plugins = nil
	return digest(s)
}
func state(db *edb.DB) string {
	h := sha256.New()
	must(db.Bolt().View(func(tx *bbolt.Tx) error {
		var visit func(*bbolt.Bucket)
		visit = func(b *bbolt.Bucket) {
			must(b.ForEach(func(k, v []byte) error {
				h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(k))))
				h.Write(k)
				if v == nil {
					h.Write([]byte{1})
					visit(b.Bucket(k))
				} else {
					h.Write([]byte{0})
					h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(v))))
					h.Write(v)
				}
				return nil
			}))
		}
		for _, name := range []string{"TenantSettings", "settings-snapshots", "plugins", "plugin-revisions"} {
			h.Write([]byte(name))
			visit(tx.Bucket([]byte(name)))
		}
		return nil
	}))
	return fmt.Sprintf("%x", h.Sum(nil))
}
func main() {
	if len(os.Args) != 3 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("private database and plan|run required")
	}
	db, e := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true})
	must(e)
	defer db.Close()
	before := map[bm.TenantID]string{}
	resources := map[bm.TenantID]string{}
	var queries []map[string]any
	db.Read(func(tx *edb.Tx) {
		for s := range edb.FullTableScan[bm.TenantSettings](tx).Rows() {
			if s.PluginSource != "" || len(s.PluginResources) > 0 {
				resources[s.TenantID] = digest(s.PluginResources)
				for _, ref := range s.PluginResources {
					key := append(binary.BigEndian.AppendUint64(nil, uint64(ref.FileID)), 1)
					queries = append(queries, map[string]any{"table": "files", "key": base64.StdEncoding.EncodeToString(key)})
				}
			}
			before[s.TenantID] = unchangedSettings(s)
		}
	})
	if os.Args[2] == "plan" {
		must(json.NewEncoder(os.Stdout).Encode(queries))
		return
	}
	if os.Args[2] != "run" {
		panic("invalid mode")
	}
	app := &auditApp{next: 0x7ffe000000000000}
	migrate := func(tx *edb.Tx) {
		c := auditContext{app: app, tx: tx}
		dbmigrations.MigratePluginSources(c)
		dbmigrations.MigratePluginVersions(c)
	}
	initial := state(db)
	func() { tx := db.BeginUpdate(); defer tx.Close(); migrate(tx) }()
	if state(db) != initial {
		panic("migration rollback changed state")
	}
	started := time.Now()
	db.Write(migrate)
	seconds := time.Since(started).Seconds()
	committed := state(db)
	db.Write(migrate)
	if state(db) != committed {
		panic("migration rerun changed state")
	}
	counts := map[string]int{}
	db.Read(func(tx *edb.Tx) {
		for s := range edb.FullTableScan[bm.TenantSettings](tx).Rows() {
			counts["settings"]++
			if s.PluginSource != "" || len(s.PluginResources) > 0 || s.PluginDraft != nil {
				panic("legacy plugin fields remain")
			}
			for _, installation := range s.Plugins {
				counts["installations"]++
				p := fdb.LoadPlugin(tx, s.TenantID, installation.PluginID)
				if p == nil || p.PublishedID != installation.ActiveID || p.PublishedVersion != 1 {
					panic("invalid installed plugin")
				}
				r := fdb.LoadPluginRevision(tx, p, installation.ActiveID)
				if r == nil || r.Version != 1 {
					panic("invalid active revision")
				}
				if digest(r.Configuration.Resources) != resources[s.TenantID] {
					panic("migration changed resource references")
				}
				for _, ref := range r.Configuration.Resources {
					_, e := fdb.LoadPluginResource(tx, s.TenantID, ref)
					must(e)
					counts["resource_metadata"]++
				}
				plugin := coreplugin.New(context.Background(), tenantenv.New(nm.Schema).ParseSources(r.Configuration.SourceFiles()), &plugins.TenantContext{Slug: "audit"}, func(tab *bh.StaticTab) error {
					_, e := fdb.LoadPluginResource(tx, s.TenantID, r.Configuration.Resources[tab.HTMLFile])
					return e
				})
				counts["initialization_errors"] += len(plugin.Plugin.Errors)
				counts["static_tabs"] += len(plugin.StaticTabs)
				counts["blocks"] += len(plugin.Blocks)
			}
			if unchangedSettings(s) != before[s.TenantID] {
				panic("migration changed unrelated settings")
			}
		}
	})
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{"counts": counts, "migration_seconds": seconds, "rollback_unchanged": true, "rerun_unchanged": true, "other_settings_unchanged": true, "resource_bodies_checked": false, "full_navigation_validation": false}))
}
