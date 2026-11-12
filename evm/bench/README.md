# EVM benchmarks

Measure execution, allocations and build cost separately. Compile/link/warm-up
stay outside execution timings. Database benchmarks include transaction and
cursor work; check each report's sync and indexer settings before comparing.

```sh
go test ./evm/bench
go test ./evm/bench -run '^$' -bench . -benchmem -cpu 1 -count 5 -benchtime=500ms
```

Run competing binaries sequentially with the same inputs; alternate their order.
Do not benchmark alongside a test/build load. Retain slowdowns and unchanged
controls. A zero-allocation property read is not a zero-allocation transaction.

## Results

- [Execution and interpreter comparisons](../_docs/BENCHMARKS.md)
- [Host bindings](../_docs/BINDINGS.md#measurements)
- [Record methods](../_docs/BENCH-RECORD-METHODS.md)
- [Borrowed list cells](../_docs/LIST-CELLS.md)
- [KVO access and decoding](../../kvo/_docs/BENCH-RECORD-ACCESS.md)
- [Scratch-register experiment and reproduction](compare/scratch/README.md)

Results belong to their recorded revision, hardware and workload. Historical
adapters/codec names do not describe the supported application API.

## Other interpreters

Lua dependencies live in a separate Go module:

```sh
(cd evm/bench/compare/golua && go test -run '^$' -bench . -benchmem -cpu 1 -count 5)
```

For native Lua/LuaJIT on macOS:

```sh
LUA_PREFIX="$(brew --prefix lua)" evm/bench/compare/native/run.sh
LUA_PREFIX="$(brew --prefix luajit)" evm/bench/compare/native/run.sh
```

The runner builds temporary executables and measures 20,000 invocations per
sample. `SAMPLES` and `ITERATIONS` override the counts. Native callbacks and Go
callbacks have different costs; report that boundary with the comparison.
