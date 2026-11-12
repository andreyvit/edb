# KVO experiments

Historical codec candidates and an independent writer used to check production
Fast2/Compact2 bytes. Application code should use [`kvo`](../../kvo/README.md).

- [`draft/`](draft): selected research grammar and independent codec control.
- [`experiments/`](experiments): alternative layouts and shared fixtures.
- [Format specification](../_docs/KVO-EXPERIMENT-FORMAT.md): candidate wire rules.
- [Benchmark inventory](../_docs/KVO-EXPERIMENT-BENCHMARKS.md): variants and runners.
- [Research decisions](../_docs/KVO-REPRESENTATION-HISTORY.md): conclusions and history.
- [Production design](../_docs/KVO-DESIGN.md): records, bindings, keys and ownership.

```sh
go test ./research/kvo/...
```

Keep rejected candidates runnable as controls. Some sample-based tests require
local ignored [replay input](experiments/testdata/README.md); they skip when it
is absent. `results/` holds local reports, logs and source archives, not committed
application data. These experiments do not migrate an application database.
