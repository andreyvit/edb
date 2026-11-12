//go:build ignore

// Candidate-only, read-only startup preflight over a private production sample.
// Never starts Fire, initializes a plugin or fetches a resource.
package main

import (
	"encoding/json"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/fdb"
	"github.com/prairiegroupinc/fire/fdb/dbmigrations"
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
	counts := map[string]int{}
	db.Read(func(tx *edb.Tx) {
		for settings := range edb.FullTableScan[bm.TenantSettings](tx).Rows() {
			counts["settings"]++
			if settings.PluginSource != "" {
				counts["legacy_live_source"]++
			}
			if len(settings.PluginResources) != 0 {
				counts["legacy_resources"]++
			}
			if settings.PluginDraft != nil {
				counts["legacy_draft"]++
			}
			if len(settings.Plugins) != 0 {
				counts["plugin_installations"]++
			}
			if len(settings.Plugins) == 0 && (settings.PluginSource != "" || len(settings.PluginResources) != 0 || settings.PluginDraft != nil) {
				counts["version_migration_candidates"]++
			}
		}
	})
	for iteration := 0; iteration < 5; iteration++ {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		started := time.Now()
		var plan *dbmigrations.PluginSourcePlan
		db.Read(func(tx *edb.Tx) { plan, err = dbmigrations.PlanPluginSources(tx) })
		if err != nil {
			panic(err)
		}
		elapsed := time.Since(started)
		runtime.ReadMemStats(&after)
		result := map[string]any{"iteration": iteration, "counts": counts, "source_changes": len(plan.Changes), "invalid_sources": len(plan.Invalid), "seconds": elapsed.Seconds(), "allocated_bytes": after.TotalAlloc - before.TotalAlloc}
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			panic(err)
		}
	}
}
