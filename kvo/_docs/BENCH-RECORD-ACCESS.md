# Choosing a record access strategy

## Retention and independent copies, 2026-11-10

`Own` retains data; `Clone` takes an independent snapshot. They intentionally
have different costs for mutable native/decoded records. The existing small
record-handle fixture, Apple M5 Max, Go 1.27.3, one 100 ms sample per case:

| Representation | Own | Clone |
| --- | ---: | ---: |
| Fast2, borrowed | 33 ns; 176 B; 1 alloc | 33 ns; 176 B; 1 alloc |
| Compact2, borrowed | 18 ns; 80 B; 1 alloc | 18 ns; 80 B; 1 alloc |
| MessagePack, borrowed | 63 ns; 384 B; 1 alloc | 68 ns; 384 B; 1 alloc |
| Native, generated or reflective access | 1.7 ns; 0 allocs | 761–766 ns; 808 B; 25 allocs |
| Decoded memory | 1.7 ns; 0 allocs | 743 ns; 2,456 B; 18 allocs |

Already-owned encoded data also makes `Own` a no-op; `Clone` copies it again.
Native cloning traverses the Go data graph directly without serialization.
Decoded-memory cloning snapshots and reconstructs its fields; this is
an explicit boundary cost, not a per-read cost. The zero-allocation read and list
traversal tests remain in place.

```sh
go test ./kvo -run '^$' -bench '^BenchmarkRecordRetention$' -benchmem
```

See [ownership and collection APIs](ACCESS.md#lifetimes-and-compatibility) before
choosing a copy operation. The access-strategy measurements below are separate
from cloning and retention.

The tables below record the native-binding work before runtime-value unification.
See [the shared-value comparison](../../research/shared-values/README.md) for
current old/new timings, string allocations, and transaction benchmarks.

Generated native access and generic decoded memory are faster than Fast2 for
repeated scalar access through KVO. They have an initial decoding cost; keep
Fast2 for short reads. Shared reflected native access is slower than Fast2 in
this fixture, even after decoding.

`Fast()` keeps an existing Fast2 view and otherwise calls `Fastest()`.
`Fastest()` produces a Go struct when bound, or indexed decoded memory when
unbound. Both return the same `Record` API and leave the stored format alone.
Neither predicts the workload. See [usage, bindings and ownership](ACCESS.md).

## Measurement procedure

Measured 2026-11-08, Apple M5 Max, macOS arm64, Go 1.27.3. Tables contain medians
of five 200 ms runs, using the default GOMAXPROCS (18 in the recorded output).
The benchmarks are single-threaded; no other test suite ran during measurement.

```sh
go test ./kvo -run '^$' \
  -bench '^(BenchmarkDecodedAccess|BenchmarkMaterialization)$' \
  -benchmem -benchtime=200ms -count=5
```

[Raw output](../../research/native-bindings/benchmarks-2026-11-08.txt) and
[machine-readable medians](../../research/native-bindings/medians-2026-11-08.json)
are committed. The benchmark implementation is in [access_bench_test.go](../access_bench_test.go).
Each JSON value is `[ns/op, B/op, allocations/op]`.

The synthetic `statrow.Sample(13)` record includes fourteen root properties:
scalars, a nested profile, timestamps, lists and dictionaries. One scalar pass
reads seven root scalars and two profile scalars, including child navigation.
Generated and reflected strategies use independent rows with identical values.
Registration and fixture encoding are outside the timer. These warm-cache
measurements exclude database transactions, IO, primary-key decoding, index
updates and business logic; benchmark the complete application operation too.

## Access after materialization

Decoding is outside the timer in this table. All paths use the same KVO `Record`
API. Generated native fields use the shared `nativebind` generator; reflected
fields use the same `nativebind` reflection code as EVM. Generic decoded memory
has model metadata and no Go struct binding.

| Representation | One scalar, ns | Nine scalars, ns | Scalar edit, ns |
| --- | ---: | ---: | ---: |
| Fast2 | 5.04 | 60.92 | 13.73 |
| Generated native | 3.90 | 44.58 | 10.63 |
| Shared reflected native | 9.30 | 104.20 | 23.41 |
| Generic decoded memory | 3.28 | 45.34 | 12.51 |

Every cell above is **0 B/op and 0 allocations/op**. The edit benchmark keeps
one editor and repeatedly assigns an existing scalar. It excludes editor
creation, snapshot encoding and saving to the database.

Generated native and generic memory are close on the nine-field workload,
about 1.35× faster than Fast2. Generic memory supplies the useful decoded
fallback without requiring a Go type or a new disk format. Reflected field
access is about 1.7× slower than Fast2 here.

The separate direct-Go one-field baseline measures 0.245 ns/op. It bypasses KVO
dispatch and permits compiler optimizations unavailable to the generic API.
These results do **not** mean KVO field access equals raw Go field access.

## Include decoding once per record

Each iteration starts from encoded bytes and optionally materializes a record,
then reads nine scalars per pass. Times include opening/access dispatch and one
materialization, not one materialization per pass. Zero passes isolates that
setup cost. Keep the returned `Record` to amortize it.

| Source / strategy | 0 passes, ns | 1 pass, ns | 16 passes, ns | 256 passes, ns | B/op | Allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Fast2 / direct | 3.9 | 66 | 1,008 | 16,141 | 0 | 0 |
| Fast2 / generated native | 455 | 502 | 1,232 | 12,651 | 448 | 5 |
| Fast2 / reflected native | 762 | 876 | 2,469 | 27,738 | 608 | 17 |
| Fast2 / generic memory | 824 | 960 | 1,690 | 12,389 | 1,288 | 8 |
| Compact2 / direct | 3.9 | 171 | 2,803 | 43,909 | 0 | 0 |
| Compact2 / generated native | 540 | 606 | 1,278 | 11,849 | 448 | 5 |
| Compact2 / reflected native | 893 | 1,019 | 2,634 | 27,582 | 608 | 17 |
| Compact2 / generic memory | 901 | 977 | 2,215 | 12,770 | 1,176 | 8 |
| MessagePack / direct | 3.9 | 1,652 | 26,358 | 419,357 | 0 | 0 |
| MessagePack / generated native | 1,271 | 1,326 | 1,984 | 12,688 | 720 | 21 |
| MessagePack / reflected native | 1,346 | 1,480 | 3,016 | 27,734 | 720 | 21 |

Allocations are constant across pass counts for these scalar workloads.
MessagePack uses its existing full-row decoder in both native strategies;
generated versus reflected refers to the subsequent field operations. Row2
also uses generated versus reflected decoding. Generic memory is measured
for unbound row2 records; the MessagePack access adapter already has a Go
binding for its field-name and codec mapping.

- **Fast2:** direct wins at one and sixteen passes. Generated native and generic
  memory win at 256 passes. Reflected native never catches direct Fast2 in this
  measured range. A binding alone does not make `Fastest()` faster.
- **Compact2:** direct wins at one pass. Generated native and generic memory
  win at sixteen passes. Reflection only narrowly wins there; generated fields
  reduce the repeated-access cost substantially.
- **MessagePack:** decoding wins even at the first nine-field pass in this
  fixture. This does not imply decoding wins for a single property: direct
  access skips full-row construction and can stop early in the encoded map.

The measured crossover depends on record width, property position, nested
payload sizes, codecs and allocation pressure. The sampled pass counts bound
useful choices for this fixture; they are not universal thresholds.

## Practical guidance

Start with direct access for sparse reads and edits. Choose `Fast()` when the
policy is to preserve Fast2's indexed bytes but materialize other formats.
Choose `Fastest()` when repeated access justifies decoding, or when an encoded
record needs an owned representation beyond its transaction. For eager Go
access, attach generated field bindings or a generated evmbind schema; leaving
only reflection can be slower than keeping Fast2.

`Fast()` retains Fast2's borrowed lifetime. `Fastest()` on an already native
record retains that same object, rather than cloning it. Generated native and
generic decoded editors mutate their respective objects. Direct encoded edits
copy bytes. Saving still requires the normal EDB record-edit or typed-row path.

Provide `WithModel(model)` for eager nested generic indexing. Without model
metadata, `Fastest` owns the encoded bytes and `GetAs` supplies each field's
encoding; attach a model for eager indexing. Generic maps/lists retain encoded
values. Whole-object and collection `Get` on native records can encode the
requested aggregate and allocate; the scalar timings above do not describe
those operations. Cache a requested collection value when reusing it.

Database materialization also decodes the row's primary key when necessary,
including keys omitted from the property model. That cost is excluded from
these standalone-record benchmarks. Notifications requesting Go structs and schema migrations can add decoding
elsewhere in a transaction. Record indexers and record notifications use the
encoded read path.

The performance work removes reflection from generated field operations,
uses bounded property-to-slot indexes, borrows immutable native string bytes,
and gives unbound records indexed decoded memory. The generic materializer
sorts fields without a temporary hash map. It preserves unknown properties and
presence while avoiding a new persistence format. These optimizations are
covered by generated/reflected parity, pointer identity, editing, ownership
and snapshot tests.

## Batched writes and borrowed dictionaries

November 10, 2026, Apple M5 Max, Go 1.27.3; medians of three 150ms samples.
Synthetic records have an untouched 8 KiB payload. Times include serialization;
they exclude database/index/domain-save work.

Sixteen string assignments across four fields, followed by one serialization:

| Codec | Before | Batched | Bytes before → after | Allocations before → after |
| --- | ---: | ---: | ---: | ---: |
| MessagePack | 14.4 µs | 2.8 µs | 152,565 → 11,097 | 65 → 53 |
| Fast2 | 13.3 µs | 2.1 µs | 151,816 → 10,610 | 18 → 22 |
| Compact2 | 14.0 µs | 3.1 µs | 151,816 → 10,610 | 18 → 22 |

Staging adds small metadata allocations but removes repeated copies of large
unchanged payloads. Single-field writes pay some staging overhead. This is an
allocation reduction in bytes, not a claim of allocation-free writes.

For 16 dictionary assignments across four keys in a 32-entry map:

| Codec | Serialize after every assignment | Serialize once | Bytes each → once | Allocations each → once |
| --- | ---: | ---: | ---: | ---: |
| MessagePack | 40.4 µs | 6.7 µs | 174,512 → 13,716 | 296 → 101 |
| Fast2 | 51.5 µs | 4.5 µs | 221,393 → 15,797 | 344 → 59 |
| Compact2 | 46.8 µs | 5.1 µs | 221,389 → 15,797 | 344 → 59 |

Both dictionary controls use the new API. The difference is when a snapshot is
requested. In a write loop, retain `edit.Map(prop)` / `edit.List(prop)` and read
through that editor; save once. `edit.Get(collectionProp)` materializes a read
snapshot of the selected collection, and `edit.Record()` snapshots the parent.

Warmed lookup plus full traversal of the 32-entry dictionary reports **0 B/op,
0 allocs/op** for all three encoded formats: ~308 ns Fast2, ~370 ns Compact2,
~1,335 ns MessagePack. The latter improved from ~7,026 ns by caching small-map
normalized keys on the stack rather than repeatedly parsing subsequent entries;
last-key-wins behavior is preserved. Native inline struct map values and custom
object decoders can still allocate; dynamic JSON entry materialization does too.

```sh
go test ./kvo -run '^$' \
  -bench 'Benchmark(RecordBatchUpdates|DictionaryBatchUpdates)$' \
  -benchmem -benchtime=150ms -count=3
```

The old field-write baseline is commit `6168c89`. The current implementation and
contract coverage are described in [the editing notes](../../research/_docs/KVO-DESIGN.md).


## Property declaration namespace and raw Fast1 access

Typed property constructors and their access types have one implementation
in `kvo`. The former alias comparison (`BenchmarkPropertyDeclarationPackage`)
was removed with the forwarding layer. Its historical paired result was 6.71 ns
through `record.BindSignedProp` and 6.75 ns through `kvo.BindSignedProp`, both at
0 B/op and 0 allocs/op (M5 Max, Go 1.27.3, three 300ms samples). Use
`BenchmarkTypedProperty` to compare the current access paths.

`fast1.BenchmarkTypedWords` retains a candidate generic typed-key wrapper only
as a benchmark. On a one-field map, raw access measured about 1.5 ns versus
3.1 ns through the candidate, both allocation-free. Consequently Fast1's raw
word/dynamic-key interface remains numeric. Resolve property references once
in schema/algorithm registration, and use `.Code()` at remaining raw boundaries.
The ordinary `record.Get(property)` API remains fully typed.

```sh
go test ./kvo -run '^$' -bench '^BenchmarkTypedProperty$' -benchmem -benchtime=300ms -count=3
go test ./kvo/codec/fast1 -run '^$' -bench '^BenchmarkTypedWords$' -benchmem -benchtime=300ms -count=3
```

## Unified package

The [package merge check](../../research/kvo-package-merge/README.md) compares the
former split implementation with the unified `kvo` package, including native
reads and EVM scans. All 78 cases retained their allocation counts and the
record handle stayed 16 bytes.
