# Storage regression probes

Synthetic data only. No live database access. See the
[storage compatibility contracts](../../_docs/STORAGE-COMPATIBILITY.md).

Run the boundary reproductions from EDB:

```sh
go run ./research/storage-audit-2026-11-12/repro.go
```

Repeat the Fire comparisons from EDB, with Fire selecting this checkout through
its local dependency replacement:

```sh
python3 research/storage-audit-2026-11-12/run-fire.py /path/to/fire
# Explicit baseline revision:
python3 research/storage-audit-2026-11-12/run-fire.py /path/to/fire e1924bb21dd7359baa10d69c9081fa272fdfa541
```

Requires Go, Python 3 and the existing `goimports` developer tool. The runner
uses a Go overlay: no Fire source changes. Outputs and generated source remain
in the printed temporary directory. `_fire` and `_tools` stay outside ordinary
`go test ./...`; standalone programs use `//go:build ignore`.

- Extracts index/migration callbacks from the baseline source. Only moved enum
  names and the native-row ledger iterator adapter change in extracted bodies.
- Compares registration metadata and field tags using Go ASTs.
- Compares native writes and encoded reindexing against the extracted indexers.
- Compares complete migration results after persistence normalization.
- Exercises joined refund migrations with filtering and integer saturation.
- Sweeps native/encoded field reads, snapshots, edits, clearing and collections.

This is a diagnostic harness, **not a green release gate**. The collection
matrix deliberately includes invalid domain values and reports their JSON
failures. Field programs print protected aggregate replacement rejections
without a nonzero exit. Read and classify the diagnostics; never equate exit
zero with no findings. Numerical fixtures do not enumerate every valid enum.

Limits: index keys use the candidate's encoder on both sides; extracted callback
method calls resolve to current native helpers. Review those encoding/helper
changes separately. No claim of exhaustive input-space coverage.
