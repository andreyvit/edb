# GoX execution benchmarks

Direct value conversion, measured 2026-11-10:
`BenchmarkValueRendering` fell from 176.6 µs / 3,928 allocations to 84.4 µs /
2,054 allocations by removing serialization and reparsing before rendering.
See [conversion measurements and methodology](../../evm/_docs/BENCHMARKS.md#direct-value-conversion-2026-11-10).
The application results below describe their original measurement date.

The application suite adapts three production template shapes into an anonymous
corpus. All names, copy, selectors, view models, URLs and example data are new.
The fixtures retain the loops, branches, helpers and component patterns that
determine rendering cost.

| Scene | Small / large | Work exercised |
| --- | --- | --- |
| Workshop | 12 / 48 activity cards | Four card variants, forms, checkboxes, optional icons, formatted amounts, state classes, a shared action component with two captured slots, JSON in a script |
| Journal | 12 / 48 records | Four local rendering closures, table/list layouts, signed amounts, expiry states, copy controls, SVG, JSON attributes |
| Comparison | 4 plans × 8 features / 8 × 16 | Nested comparison matrix plus responsive cards, per-cell helper calls, current-plan classes, SVG, CSS interpolation |

The benchmark names use `/12` and `/48` for the small and large scenes. Comparison
uses those scene sizes to choose its matrix dimensions. The timed journal renders
the table layout; parity tests also exercise the compact layout.

## Measurement

`BenchmarkApplication` compiles and links once, prepares the view models, then
measures complete rendering to an immutable string. GoX uses its pooled VM and
builder. The reference uses cached `html/template`, a reused `bytes.Buffer`, and
a copied output string. Both strings escape into the same benchmark sink.

The Go reference uses typed helper scopes, not dynamically allocated maps.
Compilation, linking, model construction, EVM argument packing and the first
render are outside the timer. Both renderers perform their own formatting,
escaping, component calls and output allocation. Host queries, localization,
network requests and application view-model preparation are excluded.

Each benchmark first checks byte-for-byte output parity. `TestApplicationTemplates`
also compares hostile text and unsafe URL schemes, absent icons, boolean
attributes and alternate layouts against `html/template`.

## Results: 2026-11-06

Apple M5 Max, darwin/arm64, Go 1.27.3, GOMAXPROCS=18. Medians of eight samples per
case, 200 ms per sample. Before/after executables ran sequentially in alternating
order. “Before” is the GoX implementation at the start of this optimization pass,
running the same fixtures. These are warm execution measurements on one machine.

| Scene | GoX before | GoX after | Go template | GoX speedup | GoX vs Go template |
| --- | ---: | ---: | ---: | ---: | ---: |
| Workshop, 12 cards | 154.7 µs | 89.7 µs | 128.7 µs | 1.72× | 1.43× |
| Workshop, 48 cards | 606.1 µs | 351.0 µs | 496.2 µs | 1.73× | 1.41× |
| Journal, 12 rows | 72.8 µs | 49.2 µs | 67.4 µs | 1.48× | 1.37× |
| Journal, 48 rows | 274.6 µs | 183.6 µs | 260.2 µs | 1.50× | 1.42× |
| Comparison, 4 × 8 | 79.8 µs | 39.5 µs | 65.7 µs | 2.02× | 1.66× |
| Comparison, 8 × 16 | 247.9 µs | 120.6 µs | 197.6 µs | 2.06× | 1.64× |

Ratios are time-before/time-after and Go-template-time/GoX-time. GoX uses
27–40% less execution time than the Go reference on these six workloads.

| Scene | GoX before bytes / allocs | GoX after bytes / allocs | Go template bytes / allocs |
| --- | ---: | ---: | ---: |
| Workshop, 12 | 87,691 / 1,719 | 35,002 / 522 | 68,932 / 2,230 |
| Workshop, 48 | 349,409 / 6,853 | 139,464 / 2,106 | 272,008 / 8,766 |
| Journal, 12 | 35,231 / 741 | 16,393 / 246 | 35,689 / 1,071 |
| Journal, 48 | 135,927 / 2,876 | 61,747 / 933 | 138,757 / 4,145 |
| Comparison, 4 × 8 | 44,720 / 771 | 21,875 / 125 | 48,417 / 1,136 |
| Comparison, 8 × 16 | 128,962 / 1,971 | 68,034 / 229 | 153,981 / 3,244 |

GoX allocates 47–60% fewer bytes and 67–88% fewer objects than before. The final
output string remains an unavoidable allocation in this API.

Raw samples: [before](../testdata/bench/results/gox-before.txt),
[after](../testdata/bench/results/gox-after.txt).

## Changes behind the results

- Plan static attributes and opening tags once. Keep an opening tag mutable
  until output begins; helpers invalidate its cached prefix.
- Combine consecutive compiler-generated builder operations. Fold complete
  static subtrees into one write. Expressions, calls and control flow remain
  barriers, preserving evaluation order and contextual checks.
- Reuse attribute storage and collect class fragments without repeated string
  concatenation. Clear pooled references after success and failure.
- Append ordinary escaped strings directly into bounded output buffers. Use
  typed string pipelines for URL filters and typed boolean calls to avoid
  intermediate interface boxes. Trusted values retain the generic path.
- Extend EVM direct host calls to void functions and methods. Tag operations and
  builder helpers read registers directly, avoiding outgoing argument copies.
  Fold script wrappers that only forward unchanged parameters to such bindings.

The string paths reuse Go's replacement table, URL safety check and URL
processor. Differential tests cover random bytes, invalid UTF-8, context-specific
filters and output limits. Context checks, strict nil handling, cancellation and
allocation budgets remain active.

## EVM controls

The existing `BenchmarkEVM`, `BenchmarkSmallHost` and `BenchmarkOptimizer` suites
ran against before/after binaries: 22 workloads, eight alternating samples each,
150 ms per sample. Median changes ranged from −2.6% to +2.3%; allocation counts
and bytes were unchanged. The +2.3% case was recursive Fibonacci, which does not
use the new host-call forms. These samples bound the observed changes; they do
not establish a performance guarantee for every EVM program.

The new void forms are also covered by `BenchmarkHostShapes`. For 128 calls with
pointer/string arguments, direct calls took 1.28 µs versus 2.39 µs through the
general ABI; pointer methods took 1.29 µs versus 2.39 µs. Both allocated nothing
(five 200 ms samples). This isolates call overhead, not template rendering work.

Raw samples: [EVM before](../testdata/bench/results/evm-before.txt),
[EVM after](../testdata/bench/results/evm-after.txt),
[void calls](../testdata/bench/results/void-calls.txt).

## Reproduce

```sh
go test ./gox -run TestApplicationTemplates
go test ./gox -run '^$' -bench BenchmarkApplication -benchmem -benchtime=200ms -count=8
go test ./evm/bench -run '^$' -bench '^(BenchmarkEVM|BenchmarkSmallHost|BenchmarkOptimizer)$' -benchmem -benchtime=150ms -count=8
go test ./evm/bench -run '^$' -bench '^BenchmarkHostShapes/(psv|method.sv)$' -benchmem -benchtime=200ms -count=5
```

For a change comparison, build each revision's test binary with `go test -c`,
then alternate the two binaries rather than running competing benchmarks in
parallel. Inspect CPU and allocation profiles on a representative large scene:

```sh
go test ./gox -run '^$' -bench '^BenchmarkApplication/workshop/48/GoX$' -benchtime=3s -cpuprofile=/tmp/gox.cpu -memprofile=/tmp/gox.mem -o /tmp/gox.test
go tool pprof -top /tmp/gox.cpu
go tool pprof -top -alloc_objects /tmp/gox.mem
```

`BenchmarkRender` remains a small 100-row diagnostic. Use the application suite
for decisions: the original implementation won that simple benchmark while
losing on all three application shapes. Closure capture allocation, dynamic
attributes and formatting remain useful targets for further profiling.
