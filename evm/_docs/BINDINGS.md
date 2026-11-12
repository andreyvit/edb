# Shared binding declarations and registration

Host functions and registered KVO methods share signature classification and
scalar wrapper generation. Both use `DirectBinding`; generated calls stay typed
Go calls, without reflection.

## Choose declarations once

- For a Go API package, use `evmbind.New` and generated `Options.Static` bindings.
  Compile with `schema.Package()` and link with `schema.Bind(host)`.
- For helpers over a KVO model, use `evmbind.RecordMethod`, `GenerateMethods`, and
  `RegisterMethods`. This explicitly maps a helper to its model and script name.
- For manual declarations, use `Package.Types`, `Functions`, and `Methods`.
  Types use `values.Shape` for both VM cells and optional native layouts. A small
  call supplies `DirectBinding`; an arbitrary signature supplies `Call`.

Direct functions support up to two arguments; pointer methods support one explicit
argument. Both support zero or one result. Record receivers occupy the first
function argument as a 16-byte pair, leaving room for one explicit argument.
Unsupported direct signatures use the ordinary ABI. Function values, interface
methods and deferred calls retain their normal behavior.

A word-only function uses the same declaration, with a convenience constructor:

```go
next := evm.ExternalFunction{
    Name: "Next",
    Signature: evm.Signature{
        Params:  []*values.Type{values.Uint64},
        Results: []*values.Type{values.Uint64},
    },
    Direct: evm.DirectUnary(func(n uint64) uint64 { return n + 1 }),
}
```

`DirectUnary` keeps the faster word-only instruction internally. It also supplies
an ordinary ABI adapter; an optional explicit `Call` overrides that adapter as
with any other `DirectBinding`. There is no `ExternalFunction.RawUnary` path.
Treat the returned binding as immutable setup metadata.

For a hand-declared VM struct:

```go
text := &values.Shape{Type: values.StringType, Kind: values.StringKind, Cells: 1}
customer := values.NewStructShape("shop.Customer",
    values.Field{Name: "Name", Shape: text})
customerPtr := values.NewPointerShape(customer)
pkg := evm.Package{
    Path: "shop",
    Types: []evm.TypeDecl{
        {Name: "Customer", Shape: customer},
        {Shape: customerPtr},
    },
}
p := customer.New()
customer.SetField(p, 0, values.String("Ada"))
name := customer.GetField(p, 0).AsString()
```

Use `customerPtr.Type` in signatures. These helpers access VM cells. Native Go
pointers require native layouts. Both use `Shape`; retired struct opcodes 36–39
remain reserved.

## Metadata lifetime and memory

`kvo.Bind(goType, model)` and `kvo.Project(goType, model, nil)` return
cached default plans by Go type/model identity. Repeated calls allocate neither
plans nor representation IDs. Full bindings and projections remain distinct.
Explicit field maps and `WithGenerated`/`WithNativeSchema` configurations are
independent: construct once and retain them, rather than constructing per row or
transaction. Models, schemas and typed properties also belong in setup.

Record and collection descriptor tables share registration, growth, locking,
and atomic publication. Hot lookup remains one atomic slice load and a flat
representation-ID index, with no additional Shape lookup. IDs and their reachable
metadata are process-lifetime and never reclaimed. The shared global ID space
still produces unused slots in each table; this change deliberately retains the
existing storage/lookup tradeoff.

On arm64, record entries remain 48 bytes, collection entries remain 64 bytes,
and each registry's lock/publication header is 16 bytes. A record layout lazily
publishes eight representation entries. These sizes exclude spare array capacity,
global Shape slots, Shapes and binding plans; they are not a claim of total
metadata cost. `Record`, `ListCell` and typed list handles remain 16 bytes.

## Measurements

Measured on Apple M5 Max, Go 1.27.3, darwin/arm64. Medians of three 200 ms
samples, with warm metadata. Baseline/current benchmark binaries were built first
and alternated in three rounds, with no task test suites running concurrently.
Times include the benchmark's entire operation;
small changes are descriptive, not statistical significance claims.

| Workload | Before | After | Allocations before → after | Bytes before → after |
|---|---:|---:|---:|---:|
| Generated void function, 256 calls | 9.408 µs | 4.045 µs | 256 → **0** | 24,576 → **0** |
| Generated void method, 256 calls | 10.159 µs | 3.370 µs | 257 → **1** | 24,640 → **64** |
| Generated predicate, 256 calls | 4.646 µs | 4.649 µs | 0 → 0 | 0 → 0 |
| Fast2 record method via EVM, 256 calls | 7.182 µs | 7.216 µs | 0 → 0 | 0 → 0 |
| Fast2 record field read | 5.735 ns | 5.721 ns | 0 → 0 | 0 → 0 |
| Fast2 open + field read | 7.463 ns | 7.475 ns | 0 → 0 | 0 → 0 |
| Generated native record field read | 2.961 ns | 2.894 ns | 0 → 0 | 0 → 0 |
| Fast2 typed list handle, 32 elements | 10.64 ns | 10.68 ns | 0 → 0 | 0 → 0 |
| Repeated default `Bind` | 8,123 ns | **11.94 ns** | 156 → **0** | 15,960 → **0** |
| Word-only specialization, 256 calls | 2.058 µs | 1.992 µs | 0 → 0 | 0 → 0 |
| Struct construction + two host calls | 159.3 ns | 163.2 ns | 2 → 2 | 64 → 64 |

Void functions improved 57%; void methods improved 67%. Existing field/list
access and generated record methods stayed within a few percent, with unchanged
allocation counts. Struct construction was 2.4% slower in this small sample.
The word-only specialization is still worth retaining: its current 1.992 µs
compares with 3.300 µs for the ordinary `DirectFunc` implementation.

An initial unpaired rerun slowed unchanged controls as well as changed code by
roughly 40%. The table uses only the interleaved baseline/current runs above;
allocation conclusions were identical in both runs.

`VoidMethod` constructs one native row per run; its remaining allocation belongs
to that row, not to the 256 calls. The repeat-binding benchmark warms one default
binding and then measures repeated `Bind` for the same Go type/model. It does not
measure the first binding construction or custom configurations.

Commands (run separately from other benchmark/test loads):

```sh
go test ./evm/evmbind -run '^$' -bench '^BenchmarkGeneratedHost$' -benchmem -benchtime=200ms -count=3
go test ./evm/evmbind -run '^$' -bench '^BenchmarkRecordMethods/Fast2/' -benchmem -benchtime=200ms -count=3
go test ./kvo -run '^$' -bench '^BenchmarkRecordHandle$/^(Fast2|Generated)$/^(Read|OpenRead)$' -benchmem -benchtime=200ms -count=3
go test ./kvo -run '^$' -bench '^BenchmarkTypedList$/^(3|32)$/^Fast2$/^CellHandle$' -benchmem -benchtime=200ms -count=3
go test ./kvo -run '^$' -bench '^BenchmarkBindingReuse$' -benchmem -benchtime=200ms -count=3
go test ./evm/bench -run '^$' -bench '^BenchmarkUnaryBinding$' -benchmem -benchtime=200ms -count=3
go test ./evm/evmcompiler -run '^$' -bench '^BenchmarkCompileAndInitialize/Run$' -benchmem -benchtime=200ms -count=3
```

The baseline is commit `4b0e318`; the implementation is `cb72134`.
Raw paired samples are in [bindings-consolidation.tsv](../bench/bindings-consolidation.tsv).
Tests cover generated/general/reflected calls,
void mutation, indirect/deferred calls, cancellation, ownership boundaries,
concurrent registration/reuse, nominal shape identity, aggregate copies, unused
scalar lanes, and linked metadata snapshots. The executable book validates all
29 examples and the complete public instruction table.
