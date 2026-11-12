# Record handle layout investigation

The production baseline is `BenchmarkRecordHandle` and
`BenchmarkRecordHandleEVM` in `kvo/handle_bench_test.go`.
Production Record uses a 16-byte typed cell. Shared interface transport is
implemented; EVM immutable record operations use the same cells and compiler
ownership analysis. Mutable editors retain their scoped handle implementation. The original measurements below describe the
earlier investigation, not the status of the current production implementation.

`cell-production.txt` contains five 100 ms samples of the production API after
the cell migration. The fixture's old hidden key attachment is removed, matching
the selected no-PK contract; its ordinary modeled ID remains. Metadata setup and
encoding remain outside timing. Compare read, retention and child access with
`baseline.txt`; Fastest no longer restores an omitted database key. Scalar and
child read/retention allocation assertions live in `handle_test.go`.

[Design decisions](../_docs/KVO-DESIGN.md) describe production typed cells,
compiler ownership and explicit primary keys. The prototypes below are controls;
their synthetic inline keys and runtime scope tables are not production storage.

The fixture contains 256 distinct rows, a nested object, and an omitted primary
key supplied separately with a shared decoder. It covers Fast2, Compact2,
MessagePack, generated and reflected native fields, and generic decoded memory.
The benchmarks report handle/editor sizes, elapsed time, bytes allocated and
allocation counts. Payload encoding and model registration are outside timing.

- `Read`: one scalar from an already opened row.
- `ValueBoundary` / `PointerBoundary`: an opaque function call before that read.
  The latter still calls the existing value-receiver `Get`; it is not a test of
  changing that method to a pointer receiver.
- `FieldsAndChild`: seven root scalars plus two child scalars.
- `OpenRead`: construct a fresh handle and read one field.
- `Retain`: open and retain independent records in a 256-slot ring, then read.
  This catches heap boxing in a future pointer/interface implementation.
- `Fastest`: open, materialize, retain, and read. Already decoded rows are a
  no-op. The historical baseline includes primary-key restoration; the production fixture does not.
- EVM `Scan` / `Retain`: host calls return 256 rows, with two field reads each.
  `Retain` stores the results in a guest slice before reading them. Each sample
  includes a new record scope and its close, so escaping handles do not accumulate
  across samples; guest compilation and VM construction are excluded.

Each retention workload checks row identity after the timed loop. EVM results
are checked against the independently calculated expected sum.

```sh
go test ./kvo -bench '^BenchmarkRecordHandle(EVM)?$' -benchtime=100ms -count=5
```

`baseline.txt` records that command on Apple M5 Max, macOS arm64, Go 1.27.3.
This is a local baseline, not an old/new production comparison. Existing
`BenchmarkSharedValue`, `BenchmarkMaterialization`, and the transaction
benchmarks in `evm/bench` remain necessary validation for any production change.

## Layout experiments

[Recommendation and EVM integration plan](../_docs/KVO-DESIGN.md).

`prototypes/handle_probe_test.go.txt` archives test-only implementations sharing cached
model/encoding metadata: a 40-byte inline handle, a 16-byte packed cell, a
16-byte Go interface and an 8-byte pointer. Interface/pointer variants use
separate 16-, 24- or 40-byte backing structs rather than always allocating the
old 128-byte Record. Their construction was checked to allocate only one
backing object, not intermediate wrappers.

`prototypes/handle_layout_bench_test.go.txt` compared these to the old Record on the same
256 rows. It includes both separate-key and synthetic inline-key fixtures,
retained child records, and a mixed-representation call site. The inline key
is a fixed eight-byte key followed by the payload; this is not a disk format
implementation or a variable-length-key benchmark. Descriptor registration,
encoding and fixture allocation are excluded.

`LayoutDirect` uses ordinary calls. The other layout workloads use the same
opaque generic consumer for every alternative; dictionary dispatch overhead is
included. The `Retain` experiment constructs a representation from already
prepared inputs; production constructor behavior is covered separately by the
baseline's `OpenRead`. Retention checks verify that earlier results survive
opening later rows and children. The static Fast2 control skips runtime type
lookup and does not implement the complete Record API.

The prototypes support the measured scalars, strings and plain object children.
They omit general validation, custom MessagePack fallback handling, editing,
serialization and materialization. These results establish representation
tradeoffs, not a measured speedup of a complete production replacement. The
existing implementation has more general behavior, especially for MessagePack.

`untagged-layouts.txt` is the initial 32-bit descriptor / 32-bit length version.
`tagged-layouts.txt` is the 4-bit tag / 28-bit descriptor / 32-bit length version.
Both contain five 100 ms samples per case, on the same machine as the baseline.
Candidates run sequentially in one binary; these are medians, not paired
confidence intervals. Ordinary desktop activity adds noise. In particular,
sub-nanosecond differences and differences between the two runs should not be
overinterpreted.

The later borrowed-scope experiment is in `prototypes/handle_scope_bench_test.go.txt` and
`scopes.txt`. It preserves the descriptor ID in the outer cell and uses a
fresh scope identity per lifetime. The earlier scope experiment at the end of
`tagged-layouts.txt` instead used generation bits and is superseded: it could
not identify a record's type after the scope discarded its entries. No EVM
opcode or production TypeTable was changed by either experiment. Both scope
designs are now superseded by the compiler-managed ownership direction.

```sh
go test ./kvo -run '^TestHandleProbe' -bench '^BenchmarkRecordHandleLayout' -benchtime=100ms -count=5
go test ./kvo -run '^TestHandleProbe' -bench '^BenchmarkRecordCellScope$' -benchtime=100ms -count=5
python3 research/record-handle/medians.py
```

`medians.json` includes every recorded case, including separate-key allocation
costs and workloads where the cell is slower. Go's integer `allocs/op` output
rounds away fractional allocation rates in the mixed case; its nonzero `B/op`
still matters. The mixed fixture contains three separately keyed encoded cases
out of nine, so the cell allocates approximately once per three rows there.

## Validation

At the scope-prototype checkpoint: `go test ./...` passed. The prototype equivalence,
string, scalar-tag-collision, type-preservation and scope-expiration tests passed
with both `-race` and `-gcflags=all=-d=checkptr=2`. No production behavior or
persisted encoding changed. These earlier test results do not validate the new
implementation. The cell checkpoint separately runs the full suite, record/value
race tests and record checkptr tests. Compiler ownership and direct cell transport additionally pass the full suite,
record/value/compiler race tests and checkptr tests.

## Production EVM cells (2026-11-09)

`evm-cell-final.txt` contains five 100 ms samples on the baseline machine.
`evm-cell-production.txt` preserves the earlier checkpoint, before avoiding a
temporary expanded view when querying native model metadata.
Each EVM operation visits 256 rows and reads two fields from each. Compilation,
metadata and VM setup are excluded; the VM is warmed first. The new fixture
removes the obsolete read scope and declares the lookup nonretaining and
noninvalidating, enabling scoped-container analysis. This measures the intended
API, not merely an isolated change to handle size.

| Strategy | Scan before → after, µs | Retain before → after, µs |
| --- | ---: | ---: |
| Fast2 | 17.87 → 12.07 | 37.61 → 20.01 |
| Compact2 | 21.32 → 15.08 | 37.34 → 22.92 |
| MessagePack | 120.83 → 106.94 | 136.30 → 114.71 |
| Generated native | 16.88 → 11.18 | 35.01 → 19.29 |
| Reflected native | 20.99 → 14.40 | 37.34 → 25.16 |
| Indexed memory | 14.87 → 9.95 | 31.06 → 18.21 |

All scans: 720 B / 5 allocations → **0 B / 0 allocations**.
All temporary-slice retention: 60,528 B / 524 allocations → **4,864 B / 1 allocation**.
An escaping encoded row still requires its payload copy. The million-read search
test limits the execution to exactly one allocation. Desktop timing noise and
separate historical runs limit conclusions about small timing differences.

`db-read-before.txt` and `db-read-after.txt` isolate a cache-hot Bolt lookup and
one string field from a single-row table, with a preboxed scalar key. Five 100 ms
samples show MessagePack **163.8 → 65.88 ns**, Fast2 **153.5 → 52.27 ns**, and
Compact2 **154.7 → 54.19 ns**. Each changes from **8 allocations to zero** after
transaction lookup setup. This is lookup overhead, not a large-database workload.
The before run overlays the three lookup/index call-site files from `6883a3d`;
the benchmark itself is unchanged. Dynamic Go-to-`any` key boxing is excluded;
use preboxed keys or `GetRecordByKeyRaw` to avoid that caller allocation.
