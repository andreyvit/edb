# Typed table/index handles

Measured 2026-11-11: Apple M5 Max, darwin/arm64, Go 1.27.3, bbolt.
Baseline: `52049d4` (shared definitions, original access handles).
Typed API: `ac34973`. Runs were sequential, without concurrent compilation.

`Table[Key]` and `Index[IndexKey]` each contain one definition pointer.
`AnyTable`/`AnyIndex` unwrap at shared API entry points. Typed record reads,
scan iteration, and per-index yields use concrete definitions internally.
Schema lists, identity maps, and transaction caches retain definition pointers.

## Before/after

Median of five 100 ms samples. Joined scans include opening/closing the read
transaction and a primary-key lookup per row; the typed API validates index
ownership at query construction. Allocation counts are unchanged in every case.

| Operation | Before | Typed API | Allocations, both |
| --- | ---: | ---: | ---: |
| 1,000-record MessagePack typed join | 242.4 µs | 248.1 µs | 40 per scan |
| 1,000-record Fast2 typed join | 191.7 µs | 195.5 µs | 40 per scan |
| 1,000-record Compact2 typed join | 200.5 µs | 205.6 µs | 40 per scan |
| Warm typed uint64 lookup | 65.8 ns | 68.7 ns | 0 |
| Warm typed string lookup | 67.0 ns | 68.8 ns | 0 |
| Per-index callback and key construction | 67.6 ns | 67.9 ns | 0 |
| Raw MessagePack record read | 69.1 ns | 73.5 ns | 0 |
| Raw Fast2 record read | 54.0 ns | 57.0 ns | 0 |
| Raw Compact2 record read | 57.0 ns | 60.0 ns | 0 |

These samples show small timing differences, not a large throughput regression.
A longer seven-sample rerun of the one-row Compact2 native join changed its
apparent regression from +13% to −9%; short-case percentages are sensitive to
run conditions. Composite/custom key codecs retain their existing allocations.

## Interface entry cost

Median of seven 200 ms samples in one binary. Both paths open a cursor and read
one row. The interface is hidden behind a noinline function to prevent static
devirtualization. Cursor setup accounts for the allocations below; unwrapping
adds none, and does not occur per row.

| Entry | Concrete | Interface | Bytes / allocations, both |
| --- | ---: | ---: | ---: |
| Table scan | 162.5 ns | 152.1 ns | 552 B / 9 |
| Index scan | 288.6 ns | 294.3 ns | 952 B / 16 |

## Reproduce

Build each revision's root test binary with `go test -c`. Run binaries
sequentially with identical arguments:

```sh
./edb.test -test.run='^$' -test.bench='Benchmark(TypedJoinedScan|TypedKeys|IndexCallback|RecordRead)$' -test.benchmem -test.benchtime=100ms -test.count=5
./edb.test -test.run='^$' -test.bench=BenchmarkHandleScanEntry -test.benchmem -test.benchtime=200ms -test.count=7
```

`BenchmarkHandleScanEntry` exists in the typed API revision. The joined-scan
benchmark uses a prebound index in the baseline and table-owned index queries
in the typed revision. Allocation tests cover warm reads; handle tests cover
ID, string, and composite primary keys across MessagePack, Fast2, and Compact2.
