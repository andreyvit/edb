# Shared KVO list cells

EVM's proven local, read-only scalar/string list accesses carry the same
16-byte `kvo.ListCell` as native Go `kvo.List[T]`. A cell contains a
GC-visible pointer, representation ID and auxiliary length. Shared metadata
describes the encoding and element type; there is no per-list allocation.

Guest syntax stays ordinary Go-like syntax:

```go
for _, tag := range customer.tags {
    if tag == wanted {
        matches++
    }
}
n := len(customer.tags)
firstMatches := customer.tags[0] == wanted
```

Native Go uses `for tag := range customer.Get(Customer_Tags).Iter()`. Its
16-byte struct also exposes `Len()`, `Cursor()` and `Slice()`; Go cannot range
directly over that struct. `Slice()` deliberately materializes an owned slice.

The VM resolves the list at acquisition and keeps traversal state in a reusable
workspace per index site. The workspace caches an expanded `ListValue` view
beside its iterator. That 32-byte access metadata is not the transported value.
Adjacent single-use `len`/index expressions are fused with acquisition, avoiding
an extra VM instruction/register. The fusion preserves evaluation order.

This is an internal storage unification, not a new guest slice ABI. Mutable,
escaping and otherwise unproven uses retain the existing materialization and
ownership rules. Scalar strings borrowed from a list still cannot escape those
rules. Model-level access metadata is reused across script compilations; new
representations are registered on first use of an encoding/access plan.

## Before/after measurement

November 9, 2026; Apple M5 Max, macOS arm64, Go 1.27.3, one logical CPU.
Baseline: EDB `18e9cf0` (benchmark added before implementation). Candidate: the
shared-list-cell implementation accompanying this report. Both binaries used
the same benchmark source, toolchain and machine. Six paired rounds alternated
baseline/candidate ordering. No builds or other test runs overlapped timing.

[`BenchmarkRecordListCell`](../evmcompiler/record_lists_bench_test.go) cycles
through 64 distinct records. Each invocation performs 16 acquisitions plus the
requested range, length, first-index or reverse traversal. Lists have 3 or 32
strings; every invocation verifies the result. Each sample runs for 125 ms after
warming metadata/VM state. Medians below are per VM invocation, including those
16 repetitions. Raw samples: [list-cells.tsv](../bench/list-cells.tsv).

| List size | Backing | Before range, ns | After range, ns | Change |
|---|---|---:|---:|---:|
| 3 | Fast2 | 2,428 | 2,354 | -3.1% |
| 3 | Compact2 | 2,634 | 2,562 | -2.7% |
| 3 | MessagePack | 3,494 | 3,099 | -11.3% |
| 3 | Native struct | 2,623 | 2,495 | -4.9% |
| 3 | Memory | 2,333 | 2,296 | -1.6% |
| 32 | Fast2 | 11,870 | 10,948 | -7.8% |
| 32 | Compact2 | 11,790 | 10,831 | -8.1% |
| 32 | MessagePack | 21,639 | 19,084 | -11.8% |
| 32 | Native struct | 16,996 | 15,903 | -6.4% |
| 32 | Memory | 11,709 | 10,837 | -7.4% |

All 40 EVM workload medians improved: length by 5–13%, first-index access by
1–46%, and reverse traversal by 1–8%. Treat the smallest differences as noise,
not a promised speedup. The six native Go control workloads stayed flat or
improved slightly (0–2.4%); native typed range was 90.07 → 89.96 ns for Fast2
and 137.75 → 134.40 ns for MessagePack. All 552 microbenchmark samples reported
**zero bytes and zero heap allocations**.

These results include the adjacent-consumer fusion; they measure the final
implementation as a whole. An initial prototype added redundant representation
validation/iterator setup and regressed several workloads. It was rejected.
Validation now occurs on a new cell/cache generation, while sequential reads
reuse the iterator. Reverse traversal resets that iterator from the cached view.

## Whole-scan and unrelated controls

The back3 application comparison uses the existing reindexed disposable dev
snapshot: 462,902 customers, 381,014 stats rows, 1,615 tag matches and a 1,400-point
sum. Every path loads stats for every customer before filtering. Six paired
rounds alternate binaries; each sample measures three full scans, with the
reference calculation and warm-up outside timing. Transaction/cursor setup is
inside timing. The public path also includes invocation/session setup.

| Complete scan | Before, ms | After, ms | Allocations before / after | Bytes before / after |
|---|---:|---:|---:|---:|
| Native typed KVO control | 574.34 | 569.56 | 44 / 44 | 2,770 / 2,770 |
| EVM original adapter | 600.76 | 598.43 | 44 / 44 | 2,770 / 2,770 |
| EVM public Scan/Key/Get | 803.93 | 798.54 | 57 / 57 | 4,106 / 4,106 |

These sub-1% differences are within observed run-to-run variation: the full
scan is effectively unchanged. There are still **zero per-row allocations**;
the fixed transaction/invocation allocations remain. Bytes/op are slightly
lower than earlier one-iteration reports; these samples average three scans.
This workload uses stored MessagePack
for both customer and stats rows. Traditional full decoding is unaffected and
was not remeasured for this list-storage change.

[All initial controls and full-scan samples](../bench/list-cells-validation.tsv) include
100 ms scalar record read/write and interface workloads. A noisy interface
control and a +2.8% Fast2 scalar-read median prompted longer confirmation:
six more alternating pairs, one second per workload. Those medians were
23,942 → 23,771 ns for interface access (-0.7%), 52,152.5 → 52,465.5 ns for
Fast2 scalar reads (+0.6%), and 61,134.5 → 61,039.5 ns for Compact2 reads (-0.2%).
Allocations were unchanged. [Longer control samples](../bench/list-cells-controls.tsv).
The short write controls differed by +0.5–1.1%, within their observed variation.

`BenchmarkSmallHost/*/Direct` could not be used as a control: the unmodified
baseline rejects its direct-method binding signature. It was excluded before
the paired validation run; this report makes no claim about that benchmark.

## Reproduce

Build binaries from the baseline commit and candidate in separate checkouts (or
with a Go source overlay), then alternate their order for six rounds:

```sh
go test -c -o /tmp/evm-list.test ./evm/evmcompiler
/tmp/evm-list.test -test.run='^$' -test.bench='^BenchmarkRecordListCell$' \
  -test.benchtime=125ms -test.cpu=1

go test -c -o /tmp/go-list.test ./kvo
/tmp/go-list.test -test.run='^$' \
  -test.bench='^BenchmarkTypedList$/^3$/(Fast2|MessagePack)/(CellHandle|CellCursor|CellRange)$' \
  -test.benchtime=125ms -test.cpu=1
```

Compilation, registration and owned copies are outside this zero-allocation
claim. The tests separately check empty/nil lists, incompatible cell types,
multiple/nested lists, bounds, evaluation order, and materialization across an
invalidating call.

Validation: `go test ./...`, `go test -race ./kvo ./evm/...`, and
`go test -gcflags=all=-d=checkptr=1 ./kvo ./evm/...` passed. Back3's full
`plugins/tenantenv` and `fire/business/superadmin/suscripts` tests and live
`TestCustomerSnapshotAllocations` / `TestCustomerSnapshotPublicAllocations`
gates also passed, retaining 44 / 44 / 57 full-scan allocations.
