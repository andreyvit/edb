# Encoded record reads and writes

Measured 2026-11-12, Apple M5 Max, darwin/arm64, Go 1.27.3, bbolt.
Fixtures and allocation gates: [record_path_test.go](../record_path_test.go).

## Contract

Current-schema records borrow encoded bytes. Warm typed integer/string lookups
and borrowed scalar/list/map access can allocate zero bytes. Scan and transaction
setup have fixed costs. Composite/custom key encoding can still allocate;
materializing strings, collections, JSON or native structs has separate costs.
`Entries`/`Keys` decode owned keys: string and composite primary keys can add
per-row allocations even when the corresponding record read allocates nothing.

Writes stay encoded through `New`, `Edit`, `Save`, record indexers and record
notifications. MessagePack inserts start with an empty encoded map; setters add
fields. Unset fields decode to their Go zero values. Updates preserve unmodeled
bytes. Fast2/Compact2 retain their existing encoded editor paths.

Exceptions: custom whole-object codecs, historical inline aliases, explicit
native notifications and verbose row logging. Declaring a stored primary key
also creates a native seed object on insertion to preserve custom/composite key
mapping. Schema migrations are measured separately from current-schema reads.

## Allocation gates

- Typed lookup plus populated scalar, string-list and dictionary access: zero
  allocations for MessagePack, Fast2 and Compact2.
- Repeated save of an unchanged open edit: zero allocations for all three.
- Insert, edit, per-index callback, old/new record notification and reindex:
  encoded-path regression test disables native MessagePack serialization.
- Direct writes through `Editor.Packed()` remain visible to `Save`.
- Fire: all 108 registered table models borrow canonical MessagePack without
  allocating or eagerly decoding a Go row. This checks row construction; it is
  not a claim that every custom field conversion or business operation is free.
- Fire: two typed lookups of populated customer/stats records, with first name,
  nanosecond timestamp, tri-state enum, tags, AppIDs, tenant check and point
  balance: zero allocations. Both use the production models and indexers.

## Database measurements

One warmed transaction. One string index, two strings (including a 512-byte
payload), an integer, two list entries and one dictionary entry. The changed
save updates the integer; the index key stays the same. Measurements include
key encoding, editor allocation, index processing, envelopes and bbolt updates;
exclude transaction begin/commit and fsync.

Baseline: `d006925` plus the benchmark fixture. Final: `8ef5e3e`.

Median of five 100 ms samples; allocation counts are per operation.

| Operation | MessagePack | Fast2 | Compact2 |
| --- | ---: | ---: | ---: |
| Lookup + integer/list/map access | 366 ns, 0 alloc | 196 ns, 0 alloc | 213 ns, 0 alloc |
| Open edit + unchanged save | 156 ns, 4 alloc | 196 ns, 5 alloc | 266 ns, 5 alloc |
| Changed save | 1.15 µs, 16 alloc | 1.32 µs, 16 alloc | 1.47 µs, 16 alloc |
| New + save + delete | 1.28 µs, 24 alloc | 1.50 µs, 22 alloc | 1.33 µs, 22 alloc |

Before → after allocations: changed saves 19 → 16 across codecs; opening and
saving unchanged records 6 → 4 (MessagePack), 10 → 5 (Fast2/Compact2).
New/save/delete: 21 → 24 for MessagePack, 24 → 22 for Fast2/Compact2.
The MessagePack insertion tradeoff is explicit: the encoded editor removes the
native-row requirement, but adds three allocations in this tiny two-field insert.

Longer five-sample, 300 ms reruns of packed writes gave Fast2 changed saves
1.12 → 1.27 µs and insert/delete 1.19 → 1.16 µs; Compact2 changed saves
1.44 → 1.17 µs and insert/delete 1.23 → 1.32 µs. Allocation reductions are
consistent; this is not a uniform timing improvement.


Writes are not zero-allocation. The allocation profile includes bbolt cursor
stacks/key copies, retained value buffers, key handling, editor/pending-field
storage and rebuilt encoded bytes. Removing native row decoding does not remove
these costs. MessagePack's encoded insertion can use more allocations than a
small native struct; it avoids requiring or serializing the whole struct.

## Fire save boundaries

`SaveCustomer` and `SaveOrder` preserve analytics snapshots and loyalty effects.
Their record branches still construct partial native snapshot inputs and
materialize some collections. They do not fully decode and re-encode the stored
customer/order, but they are not zero-allocation business operations.
`SaveCustomer` also loads customer stats through the native `CustomerBalance`
helper. With inactivity tracking enabled, `SaveOrder` still writes a native
loyalty-action struct and loads its projection state through a native helper.
These application paths remain to be converted; record-only indexers do not
eliminate explicit native API calls in surrounding business logic.

Fire explicitly requests Go-struct notifications for tenants, tenant settings
and users. Questions and plugins request records. Upgrading old schemas removes
read-time migration work; it does not remove these application-side costs.

Median of three 100 ms samples, Fire `5b5cbfadc`, EDB `8ef5e3e`.
All use temporary bbolt databases and populated synthetic fixtures, not a live
tenant database. Each timed loop uses one transaction; commit/fsync is excluded.

| Production-model operation | Time | Bytes/op | Allocations/op |
| --- | ---: | ---: | ---: |
| Customer + stats lookup and borrowed fields | 4.20 µs | 0 | 0 |
| `SaveCustomer` with snapshots and populated stats | 12.17 µs | 7,845 | 60 |
| `SaveOrder` with snapshots and inactivity tracking enabled | 9.16 µs | 5,690 | 68 |

The domain loops change opt-in status and order amount respectively. Loyalty
tracking is enabled; an unchanged loyalty action may itself produce a no-op.

Separate indexing fixtures change tenant ID, rewriting the affected index keys:

| Table | Native Put time / allocations | Encoded Edit+Save time / allocations |
| --- | ---: | ---: |
| Customers | 8.98 µs / 166 | 13.70 µs / 70 |
| Customer stats | 8.86 µs / 207 | 18.52 µs / 39 |
| Products | 3.18 µs / 67 | 3.87 µs / 41 |
| Orders | 5.04 µs / 127 | 6.96 µs / 45 |

These encoded edits use fewer allocations and no full native row conversion,
but are slower in these fixtures. Index key construction and repeated encoded
field traversal still cost time and allocate. The domain and indexing fixtures
change different fields and run different side effects; compare each within
its own workload.

One-row full reindex, including rebuilding index buckets: customers 23.53 µs /
394 allocations; stats 24.52 µs / 245; products 6.63 µs / 142; orders 11.17 µs /
196. This is maintenance overhead, not the steady-state row-read cost.

## Validation

EDB: full and short suites; root/index-helper race tests. Fire: all quick-test
packages in bounded batches, plus all 25 changed packages without `-short`.
MVP tests and the tagged benchmark package pass against the local EDB checkout.
The live database snapshot benchmarks were not run for this measurement.


## Reproduce

Run timing binaries sequentially after compilation and test workloads finish.

```sh
go test -c -o /tmp/edb-record-path.test .
/tmp/edb-record-path.test -test.run='^$' -test.bench=BenchmarkRecordPath -test.benchmem -test.benchtime=100ms -test.count=5
```

From Fire, with its local EDB replacement:

```sh
go test ./fdb -run TestEveryTableBorrowsMessagePack
go test -c -o /tmp/fire-record-path.test ./fdb
/tmp/fire-record-path.test -test.run='^$' -test.bench='Benchmark(RecordIndexWrites|RecordDomainSave|CustomerRecordRead)$' -test.benchmem -test.benchtime=100ms -test.count=3
```
