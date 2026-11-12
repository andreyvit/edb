# Production regression probes

Audit programs, not application code. Build `native-replay.go` unchanged from
two independent Fire source trees: the deployed revision and the candidate.
Each tree must use its own dependency versions. Do not use the candidate's
key encoder or callbacks as the production control.

`sample-bolt.py` reads bounded samples from an immutable ZFS snapshot through
`os.pread`. It validates both Bolt metadata checksums, page bounds and key order.
It never opens a writer, runs Fire, creates remote files, or locks the database.
The sampler has a byte budget and skips values above 8 MiB. Its stderr manifest
records coverage and skipped values. Distributed leaves do not form a random
sample or a complete tenant cohort.

The sample contains private production data. Keep all inputs, databases, stdout
and stderr in a private directory outside Git. `inspect` emits complete native
values, including secrets. Never print those files in a transcript. Replay
stdout normally contains hashes; panic stderr can contain private keys.

```sh
mkdir -m 700 /tmp/fire-edb-regressions-EXAMPLE
# From each pinned Fire source tree, using its own module graph:
GOWORK=off go build -o /tmp/fire-edb-regressions-EXAMPLE/native-old \
  /absolute/path/to/native-replay.go /absolute/path/to/native-mutations.go
# Build native-new from the candidate tree in the same way.

native-old import sample.jsonl source.db
cp source.db old.db
cp source.db new.db
native-old audit old.db > old.jsonl 2> old.stderr
native-new audit new.db > new.jsonl 2> new.stderr
python3 compare.py old.jsonl new.jsonl differences.json
```

Use absolute paths beneath `/tmp/fire-edb-regressions-EXAMPLE` for database
arguments; the replay refuses other paths. `import` refuses an existing file.
For sampling, pipe the Python source to an SSH Python process and redirect both
streams locally. Do not save a sampler or sample on production.

The replay compares native field values, nil versus empty collections, exact
numbers, memento decoding, row metadata and every rebuilt index bucket. It then
changes tenant IDs on disposable native rows and deletes every third row.
Unknown tables remain raw. Invalid historical rows can roll back one table's
write/reindex phase; count failures rather than treating completion as success.

Sampled table state says existing indexes are built, but their original index
entries are absent. The explicit reindex phase builds them from sampled rows.
Initial migrations that query indexes therefore do not see complete related
data. These probes alone cannot prove joined migrations or business behavior.
Later probes must use connected cohorts or separately verified index fixtures.

Raw bytes and modification counts can differ after forced reindexing without a
native value change. Classify each difference; do not suppress it by default.
Added fields also change canonical hashes even when nil in both logical models.

`native-old prepare prepared-old.db` rebuilds indexes without the later audit
mutations. Make that file from `source.db`. `run-mutations.py DIRECTORY` repeatedly
clones this common baseline with macOS `cp -c`, then runs identical seeded native
field mutations in the two independent binaries. Each row gets its own write
transaction; one mode makes no change, one rolls back, and five commit. Values
include zeroes, boundary integers, nil/empty containers, nested fields, timestamps
and strings with Unicode/NUL. Primary keys are restored before writing.

The runner compares committed native values, in-transaction reads, rollback
bytes/metadata, revision deltas and complete index buckets. It retains all
differences, including invalid enum/pointer cases and nondeterministic MessagePack
map ordering. Its revision-only category is a diagnostic, not an exemption from
review. Existing nil plugin fields are the only canonical field exclusions.
The default run performs continuous work for 9,600 seconds; it has no sleeps.
Run `python3 classify-mutations.py PRIVATE` after or during the soak. It reads only
completed seeds, retains unknown outcomes separately and exits nonzero if any
remain. Its known categories require evidence from the compared versions;
classification is not a general exemption for differences.

`AUDIT_SKIP_MUTATIONS=PRIVATE/skipped.json` optionally supplies a JSON object
whose `table/key-hash` keys have value `true`. Apply the same file to both
versions. This supports a second control: omit exactly the rows with differing
write success or deliberately invalid enum round trips, replay the same seed,
and check whether all existing index hashes now agree. Keep the original
unfiltered evidence and report how many rows were omitted.
`run-matched-mutations.py PRIVATE` selects eight full-size seeds with index
differences across the completed soak. Build the two-file native replay as
`PRIVATE/native-filtered-old` and `PRIVATE/native-filtered-new` first. It checks
the original row count, applies the same exclusions, and rejects every remaining
value/error/index difference. Revision counts and the five new empty indexes
remain separately counted. The earlier small smoke seed is excluded.

Build `cohort-plan.go` separately from the deployed Fire tree. It selects 96
customers, at most two per tenant, favoring old order schema versions with
redeemed points. Its stdout is a private lookup plan. `fetch-bolt.py` executes
that plan against an immutable snapshot through SSH stdin. It retrieves exact
customer/settings rows and complete bounded customer index ranges. For every
order found, it also fetches the original points-by-order index and all referenced
point rows. This preserves the actual join used by the order refund migration.
It reports absent indexes and refuses truncated ranges, oversized values or a
read-budget overrun. It does not claim a complete tenant export.

`legacy-replay.go` exercises Fast1 snapshots and their unchanged analytics
consumers. For the deployed build only, replace its import path
`github.com/andreyvit/edb/kvo/codec/fast1` with `github.com/andreyvit/edb/kvo`;
all operations remain identical. It compares every modeled word/map, edits nested
counters, packs/reloads, checks retained data, forces snapshot-index insertion,
and rolls back a restoration. Customer ledgers also exercise membership,
participation, account-state and positive-point-balance decisions under 64 flag
combinations. Increase snapshot coverage with `--kv-leaves 4096`.

`scan-oracle.go` reads raw index buckets and independently decodes their tuple
footers. It selects expected entries by component equality, prefixes, logical
byte bounds and primary-key bounds, then compares the actual public cursor in
both directions. Build the source unchanged in both Fire trees and run each on a
copy of `prepared-old.db`. This checks existing production-distribution indexes
without using either implementation's seek strategy as the oracle.

## Additional probe catalogue

Use the default shared Go build cache. Build each standalone `.go` file with
`GOWORK=off GOFLAGS=-p=4 go build -o PRIVATE/NAME /ABS/SOURCE.go` from the intended
frozen Fire tree. Files have an `ignore` build tag to keep them out of ordinary
suites; explicit file builds still work. The only multi-file executable is
`native-replay.go` plus `native-mutations.go`. Make a fresh private copy of the
specified baseline for each process; never run two writers against one file.

`PRIVATE` below means an absolute directory named `fire-edb-regressions-*`, mode
0700. Redirect stdout and stderr there even when a probe normally emits hashes.
Error messages, profiles, lookup plans, WAL exports, runtime diagnostics and
native inspection output can contain private tenant data or credentials.
No private fixture or result belongs in Git.

| Source | Build / command / purpose |
| --- | --- |
| `enum-census.go` | Same source in both trees; no arguments or data. Walk registered persisted fields and record selected enum encoders. Recursive types terminate at their first repeated occurrence. |
| `enum-wire.go` | Both trees: `encode`, then `decode PRIVATE/other-version.jsonl`. Confirms actual deployed versus candidate encoders. |
| `enum-forward.go` | Candidate, no arguments. All valid origin/platform values across binary-text, string and integer widths; native/property/edit paths. |
| `enum-map-forward.go` | Candidate, no arguments. Mixed social-platform map keys, partial edits, deletes and retained values. |
| `nil-collection-boundary.go` | Both trees: `PRIVATE/fresh.db`; synthetic invalid nil product variants/coupons. Checks whether the write rolls back or persists, then invokes unchanged native consumers. No private data is used. |
| `referral-plan.go` | Deployed tree: `PRIVATE/prepared-old.db`; stdout is an exact private join plan for real referral records. Execute through `fetch-bolt.py` against the immutable snapshot. |
| `largest-bolt.py` | Read-only metadata walk over selected snapshot buckets. Use its argument help; capture key plans privately. `fetch-bolt.py` retrieves selected large values under explicit bounds. |
| `raw-buckets.go` | `PRIVATE/DB comma,separated,buckets`; read-only complete byte hashes. Distinguishes preservation of obsolete opaque values from current native codec coverage. |
| `export-pagination.go` | Candidate: `PRIVATE/prepared-copy.db`; actual tenant cursors with fresh transactions at six page sizes, compared to independent membership and unsplit scans. Historical unreadable integration rows are removed only in this disposable fixture and counted. |
| `concurrent-native.go` | Both trees: `PRIVATE/prepared-copy.db ITERATIONS`; 32 writers, eight readers, fsync enabled. `AUDIT_REUSE_TX_BUFFERS=true/false` selects the setting. Repeat candidate builds with `go build -race` only for the concurrency scenario. |
| `crash-native.go` | Both trees: `write DB` / `check DB ACK`. Prepare `PRIVATE/crash-base.db` from the deployed-prepared fixture with at least 32 customer rows; the probe selects the first 32. Use `run-crashes.py PRIVATE --cycles N --first-seed S`; each cycle kills both versions and verifies atomicity, acknowledgments and index reconstruction. |
| `change-notifications.go` | Candidate: `PRIVATE/prepared-copy.db`; actual Fire notification flags, independent native/owned payloads, rollback/delete and reentrant user snapshots. |
| `perf-native.go` | Both trees: `PRIVATE/prepared-copy.db SECONDS_PER_CASE`; reads and ordinary native writes with process CPU/allocation/GC/Bolt metrics. See controls below. |
| `build-snapshots.go` | Both trees: `PRIVATE/prepared-copy.db`; actual generated customer/order/referral/receipt data, with policy variants. |
| `analytics-consumers.go` | Both trees: `PRIVATE/legacy-copy.db PRIVATE/native-copy.db`; actual customer ledger transitions and tenant range-stat consumers. For deployed source only, change the legacy import path from `edb/kvo/codec/fast1` to `edb/kvo`. `AUDIT_ROW_LIMIT` allows smoke runs. |
| `startup-settings.go` | Candidate: `PRIVATE/prepared-copy.db`; actual `PlanPluginSources` over every captured settings row, with timing/allocation counts. |
| `plugin-migration.go` | Candidate: `PRIVATE/prepared-copy.db plan` emits private file lookups; fetch them from the same immutable snapshot. `run` exercises actual migration rollback, commit, rerun, unrelated settings, resource metadata and plugin initialization. Resource bodies are not present. |
| `export-import.go` | Both trees: `export PRIVATE/prepared-copy.db PRIVATE/wal-dir`, then `import PRIVATE/fresh.db PRIVATE/wal-dir` or `cancel-import`. Actual Fire bodies with only storage/context/logger services. Full/filtered/foreign/duplicate customers, batch release, cancellation and tenant-ID remapping. |
| `permission-consumers.go` | Build old source with `mpermissions` imported as an alias of `fire/bm`; candidate uses `fire/bm/mpermissions`. Old executable `permissions` emits the common numeric permission list. Then both: `PRIVATE/prepared-copy.db PRIVATE/old-permissions.json`. Checks existing decisions plus auth-field preservation across rollback/save. |
| `runtime-tenants.go` | Both trees: `PRIVATE/prepared-copy.db PRIVATE/fresh-bootstrap-dir LIMIT`; zero means all tenants. Run under the network-denying sandbox below. Uses a fresh empty bootstrap app for real runtime infrastructure and an explicit sampled read transaction for tenant configuration. No HTTP server or worker loop starts. |

### Performance controls

`AUDIT_TABLES` selects comma-separated registered names. `AUDIT_MODE=read/write`
selects one mode. `AUDIT_MUTATION=ordinary` toggles customer `Name`, point-change
`PrivateNote`, or job `LastAttemptTime`. `AUDIT_MUTATION=none` saves each row
without editing it; generic map order or first-save encoding conversion can still
change bytes. The default relocates `TenantID` as a stress control. `AUDIT_ROLLBACK=true`
isolates work before commit. `AUDIT_ROWS`
selects 1–4096 evenly spaced rows below 1 MiB (default 128). A one-row case selects
the first row; it is not a statistically representative customer sample.
`AUDIT_FSYNC=true` enables fsync after opening; default false isolates CPU work.
Buffer reuse stays disabled and `GOMAXPROCS=2`. `AUDIT_CPU_PROFILE` names a private
profile prefix. Each run needs a fresh copy of the same deployed-prepared baseline.
Alternate old/new order and compare process CPU as well as wall time. Do not
translate local percentages directly into production request latency.

### Offline runtime configuration load

```sh
/usr/bin/sandbox-exec -p '(version 1)(allow default)(deny network*)'   PRIVATE/runtime-tenants-new PRIVATE/prepared-copy.db   PRIVATE/fresh-bootstrap-dir 0 > PRIVATE/runtime.jsonl 2> PRIVATE/runtime.stderr
```

The executable also rejects all HTTP requests through its transport, counts
attempts, uses fake bootstrap secrets, disables jobs and journals, fixes time and
keeps uploads in memory by default. `AUDIT_RESOURCE_DIR=PRIVATE/resources`
selects captured on-disk `trusted`/`untrusted` resource directories instead.
Runtime tabs then resolve through the actual per-version resource map and file
loader; body size/SHA-256 must match the file metadata. The OS sandbox additionally
denies direct sockets.
The explicit sample transaction supplies tenant/settings/theme/catalog rows.
The bootstrap database starts empty; it does not constitute full production
startup, migration, browser execution, unprovided resource availability or integration proof.
Captured diagnostics remain private. Compare semantic result hashes and classify
every diagnostic difference before claiming equivalence.

`AUDIT_RULES=true` also calls actual `coreexpr.Fill` and `coreconf.Query` for
each loaded tenant. It selects up to 16 sampled customers, preferring those with
sampled orders/stats, their available stats
and up to two sampled orders each, plus nil-customer/order controls. It checks
every configured rule aspect and the four existing order-processing/earning
switches in every runtime market at three dates. Results include the actual
input and query-result hashes, coverage counts and recovered panics. Missing
sampled stats/orders remain missing; no synthetic histories are invented.
For joined coverage, combine the base raw sample with the connected order and
referral cohorts before import, prepare it with the deployed binary, then clone
it and run candidate plugin migrations. Independent leaf samples alone rarely
contain matching customer, order and stats IDs.

### Reproduction boundaries

Pin both Fire revisions and their EDB/MVP dependencies. The baseline tree uses
its own module graph; record commits for every local replacement in either tree.
Never silently use candidate dependencies to build the old control.

A physical leaf sample omits most rows and relationships. Explicit cohorts add
complete bounded index ranges for selected customers/orders/referrals; they still
do not make complete tenant backups. Rebuild indexes before joined probes unless
the fixture explicitly carries original entries needed for a migration. An empty
index is not proof of no live rows. Confirm every missing index or source row.
Record counts, exceptions, unresolved scenarios and exact scope with each run.

`verify-fixed-mutations.py PRIVATE` checks the follow-up fixes against the
previously audited candidate. In a fresh private directory, link `native-old`
to the frozen candidate, `native-new` to the fixed binary, and `prepared-old.db`
to the common deployed-prepared baseline. Run `run-mutations.py` first with
256 rows per table. Build both binaries from the current probe sources; older
probe binaries may lack `AUDIT_SKIP_MUTATIONS`. The verifier accepts only the documented explicit-nil
rejections at exact product/coupon element paths, records their exclusions,
and reruns every completed seed with the same exclusions on both binaries.
All remaining native values, write outcomes and complete index buckets must
match; nondeterministic MessagePack map ordering may still change revision
counts. Original unfiltered results and private exclusion files remain intact.

`native-index-memory.go` counts unique field-lookup slices reachable from the
registered Fire table projections/bindings. Run it from each frozen/fixed Fire
source tree. It reflects over diagnostic metadata and reports counts/bytes
only; it neither opens data nor measures total process RSS.
