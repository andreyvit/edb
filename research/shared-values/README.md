# Shared runtime values: old versus new

KVO record access and EVM use `values.Value`. `row2.Value`, its constructors,
and the scalar bridge helpers between the two runtime carriers are removed.
`row2.Field` is raw codec metadata used for schema-free traversal and copying
unknown fields. It becomes a runtime value only through `Decode(Encoding)`.

Scalars have their native runtime representation: signed integers are already
sign-extended, float32 has float32 bits, and strings have pointer + length.
Fast2/Compact2 storage remains compatible, including float32's historical
float64 wire representation and presence-significant empty payloads.
Collections remain lazy encoded row2 byte spans in KVO. EVM materializes its own
maps/slices when requested; sharing the carrier does not erase that boundary.

## Measurement

Measured 2026-11-08 on Apple M5 Max, macOS arm64, Go 1.27.3, default
GOMAXPROCS=18. Old code is `a0a27a2`; new code is `dc81d7d`. Both use the same
fixtures and workloads. A plain archive of the old commit was built separately.

Six samples per case and version, 100 ms each, alternating old/new order within
each pair. A preliminary sequential run showed substantial background timing
variation even in the unchanged control, so the committed comparison uses
paired runs. No other test suite was deliberately run alongside these samples;
ordinary desktop activity remained. Treat small timing differences as local
measurements, not a universal speed guarantee. Tables show medians.

The comparison covers 110 cases: repeated numeric reads/edits, strings,
byte spans, whole collections, EVM string reads, eager materialization with
multiple access counts, and EVM read/write transactions over 512 rows using
both in-memory storage and Bolt. Single-field fixtures use 64-byte strings
and 256-byte binary data. Registration and fixture construction are excluded;
materialization cases explicitly include decoding. Write transaction timings
include saving and index work, not production IO/fsync behavior.

Raw output: [old access](old.txt), [new access](new.txt),
[old transactions/codecs](old-workloads.txt), [new transactions/codecs](new-workloads.txt).
[All medians and sample counts](medians.json) include every case, including
regressions. `time_ratio` is new / old; smaller is faster.

## Results

Retained native/generic strings remove a 64-byte allocation, and their access
times fall to roughly one-third of the old path. Generic materialization is
more than twice as fast, using five rather than eight allocations. Fast2 scalar
access remains close to baseline. Several encoded-access workloads retain
measurable overhead; this is not an across-the-board speedup.

### Repeated access

| Workload | Old, ns | New, ns | Change | B/op, old → new | Allocs/op, old → new |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fast2: one scalar | 6.41 | 6.07 | -5% | 0 → 0 | 0 → 0 |
| Fast2: nine scalars + child | 77.89 | 79.91 | +3% | 0 → 0 | 0 → 0 |
| Fast2: scalar edit | 14.75 | 15.96 | +8% | 0 → 0 | 0 → 0 |
| Generated native: nine scalars + child | 47.04 | 43.95 | -7% | 0 → 0 | 0 → 0 |
| Reflected native: nine scalars + child | 177.40 | 153.65 | -13% | 0 → 0 | 0 → 0 |
| Generic memory: nine scalars + child | 48.59 | 48.72 | +0% | 0 → 0 | 0 → 0 |
| Generic memory: scalar edit | 13.62 | 9.23 | -32% | 0 → 0 | 0 → 0 |
| Generated native: retained string | 18.43 | 5.99 | -68% | 64 → 0 | 1 → 0 |
| Reflected native: retained string | 27.94 | 13.14 | -53% | 64 → 0 | 1 → 0 |
| Generic memory: retained string | 16.08 | 5.56 | -65% | 64 → 0 | 1 → 0 |
| Fast2: retained string | 16.94 | 21.32 | +26% | 64 → 64 | 1 → 1 |
| Compact2: borrowed string | 9.69 | 11.00 | +14% | 0 → 0 | 0 → 0 |
| Generated native: EVM string read | 149.00 | 123.25 | -17% | 64 → 0 | 1 → 0 |
| Generic memory: EVM string read | 134.90 | 114.60 | -15% | 64 → 0 | 1 → 0 |

The retained-string comparison has equivalent lifetime guarantees: old code
uses `string(r.Get(code).Bytes())`; new code uses `GetOwnedString`. Encoded
sources still require the copy. Borrowed-string cases use a borrowed string on
both versions. Repeated string-edit cases assign an unchanged string: the new
native and generic paths compare before copying. They do not measure arbitrary
changed-string assignments, which still retain their own copy.

### Materialization only

These zero-pass cases isolate `.Fastest()`; subsequent repeated-access cases
are also included in the raw results. Shared immutable model layouts remove
per-row sorting/index construction and save 280 bytes in the generic fixture.

| Workload | Old, ns | New, ns | Change | B/op, old → new | Allocs/op, old → new |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fast2 / generated | 496.85 | 510.00 | +3% | 448 → 448 | 5 → 5 |
| Fast2 / reflected | 847.00 | 878.25 | +4% | 608 → 608 | 17 → 17 |
| Fast2 / generic | 987.90 | 436.25 | -56% | 1,288 → 1,008 | 8 → 5 |
| Compact2 / generated | 707.05 | 817.40 | +16% | 448 → 448 | 5 → 5 |
| Compact2 / reflected | 972.30 | 992.25 | +2% | 608 → 608 | 17 → 17 |
| Compact2 / generic | 1,042.50 | 424.85 | -59% | 1,176 → 896 | 8 → 5 |

### Complete EVM transactions

Each operation processes 512 rows. Allocations remain essentially unchanged;
small pool/GC variations are visible in the raw data.

| Workload | Old, µs | New, µs | Change | B/op, old → new | Allocs/op, old → new |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fast2 memory / Read | 64.26 | 66.43 | +3% | 1,514 → 1,513 | 15 → 15 |
| Fast2 memory / Write | 550.15 | 593.34 | +8% | 845,681 → 845,080 | 4801 → 4801 |
| Fast2 Bolt / Read | 59.83 | 64.51 | +8% | 1,984 → 1,984 | 24 → 24 |
| Fast2 Bolt / Write | 700.47 | 746.51 | +7% | 1,003,285 → 1,005,478 | 8979 → 8980 |
| Compact2 memory / Read | 73.76 | 80.55 | +9% | 1,512 → 1,512 | 15 → 15 |
| Compact2 memory / Write | 895.70 | 1,033.06 | +15% | 831,704 → 830,994 | 5218 → 5218 |
| Compact2 Bolt / Read | 68.03 | 76.00 | +12% | 1,984 → 1,987 | 24 → 24 |
| Compact2 Bolt / Write | 1,068.10 | 1,125.12 | +5% | 1,011,030 → 1,010,332 | 9383 → 9383 |

Remaining measured regressions include roughly 3–15% on these transactions,
8% on repeated Fast2 scalar edits, 14% on borrowed Compact2 strings, and 26%
(about 4.4 ns) on retained Fast2 strings. Compact2 generated materialization
is 16% slower in this run. Full results also include cases that were flat or
faster. The unchanged Fast1 scan control was within 1% between versions.
Applications should measure their complete workloads before selecting an
access strategy.

## API and ownership

Use [KVO access](../../kvo/_docs/ACCESS.md) for the supported API and
[values](../../values/README.md) for the runtime carrier. `row2.Field` preserves
unknown wire data; `values.Value` holds decoded scalar bits or typed payloads.

The comparison used the API of its recorded revisions. Some example signatures
from those revisions have been removed; benchmark reproduction requires those
source trees. Retained strings must be owned on both sides of a comparison.

## Reproduce

Build old and new test binaries from their respective source trees:

```sh
# In each source tree, replacing VERSION with old or new:
go test -c ./kvo -o /tmp/shared-VERSION-record.test
go test -c ./evm/bench -o /tmp/shared-VERSION-evm.test
# From the new checkout (the recorded files provide the case inventory):
python3 research/shared-values/run-paired.py
python3 research/shared-values/compare.py
```

## Validation and migration

EDB full and short suites passed. Focused race tests cover row2, records,
generated bindings, EVM and the compiler. Tests cover signed widths, float32
normalization/overflow and EVM edits, empty-value wire compatibility, unknown
fields, generated/reflected agreement and retained string lifetimes.

MVP tests passed using a temporary module file pointing at local EDB. Back3
callers were migrated in `e6e1c19bb`; modified-package tests, the tagged benchmark
build and `make quicktest` passed. The concurrent Node UI suite had a reward
message timing failure; the case and entire file passed independently, all Node
tests passed with `--test-concurrency=1`, and all 223 Playwright tests passed.
No UI code was changed.

Regenerate record bindings for ABI `record3`. This is a Go API migration and
requires updating consumers before release; it does not require a storage
migration. The work is committed locally, not pushed or deployed.
