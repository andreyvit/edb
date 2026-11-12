# EVM execution benchmarks

Dated measurements, not a promise of current performance. Keep each result with
its revision, hardware, inputs and timing boundaries. [Run the suites](../bench/README.md).

## Direct value conversion, 2026-11-10

Measured before replacing the round trips and afterward on Apple M5 Max,
Go 1.27.3, with three 200 ms samples. Medians, rounded:

| Workload | JSON round trip | Direct conversion | Heap allocations |
| --- | ---: | ---: | ---: |
| Record export: 32 nested objects, strings and integer fields | 32.3 µs / 56.9 KB | 11.5 µs / 33.0 KB | 971 → 509 |
| GoX: nested dynamic values in text, attributes and scripts, repeated 16 times | 176.6 µs / 161.3 KB | 84.4 µs / 86.1 KB | 3,928 → 2,054 |

The baseline is commit `d6abdc5`. These synthetic fixtures isolate conversion;
they do not claim an equivalent speedup for templates dominated by scalar reads.
Compile/link and fixture construction are outside the timed region.

Record export uses `Record → owned Go projection → VM values`; the JSON
buffer and second Go tree are gone. The projection is also used by native
`Record.ToJSON`, preserving custom scalar semantics. JSON/text marshalers run
only at custom leaves. The final VM maps/slices and intermediate projection still
allocate. GoX uses `VM values → owned Go JSON data → rendering`; its conversion
shares traversal with the existing JSON encoder and meters output allocations.

```sh
go test ./evm ./gox -run '^$' \
  -bench 'Benchmark(RecordJSON|ValueRendering)$' -benchmem -benchtime=200ms -count=3
```

Regression tests compare direct results with JSON round-trip semantics, including
64-bit integer precision, signed/unsigned dynamic types, nil collections, custom
marshalers and their errors, malformed UTF-8, escaping, size limits, cancellation,
allocation limits and ownership after the original encoded storage is overwritten.
The rendering fixture is compared with `html/template` output.

## JSON dynamic values, 2026-11-07

Unboxed interface headers improve the JSON workloads. Compare `10daf42e5bcf887735ac66779362598fece5db63` with the implementation described in [values](../../values/README.md). Apple M5 Max, macOS arm64, Go 1.27.3, one benchmark CPU. Eight rounds alternate the binary order and rotate workloads; each sample runs for 300 ms. Times below are medians. The change column is the median of paired changes, so it need not equal the ratio of the two timing medians. [Every sample](../bench/json-values.tsv).

The fixture contains 128 customer objects, nested profiles and label arrays, booleans, nulls, strings, signed integers, full-width unsigned integers and fractional numbers. Four inputs rotate during timing. The guest's checksums are checked against native Go execution; tests compare complete JSON output. Parsing and encoding stay unchanged, so these measurements isolate the representation change.

| JSON workload | Before µs | After µs | Paired time change | Go bytes/op, before → after | Go allocs/op, before → after |
|---|---:|---:|---:|---:|---:|
| Build and read | 205.42 | 188.83 | -9.4% | 248,800 → 203,632 | 5,520 → 4,237 |
| Read existing objects | 37.77 | 37.76 | +0.2% | 0 → 0 | 0 → 0 |
| Update and read | 91.04 | 72.55 | -21.1% | 24,608 → 0 | 769 → 0 |
| Decode a KVO JSON property and read | 530.30 | 504.75 | -4.8% | 509,944 → 464,777 | 11,867 → 10,584 |
| Rebox integer/string 1,024 times | 70.33 | 38.86 | -44.9% | 65,600 → 0 | 2,050 → 0 |
| Encode existing objects | 307.43 | 307.97 | 0.0% | 157,609 → 157,610 | 4,766 → 4,766 |

Build/read saves 18.2% of allocated bytes; decode/read saves 8.9%. These are allocation totals, not peak RSS. Maps and backing arrays still allocate. A shared type-table pointer also removes 16 bytes from each VM map object. The decoder still constructs an intermediate native Go JSON tree. Removing that tree is separate work.

The first candidate slowed pointer and function interface loops by 6–7%. The retained version resolves pointer assertion IDs at link time, decodes concrete assertions once, and keeps direct pointer packing and function boxing paths. Existing opcodes suffice; no main dispatch change or JavaScript frontend was added.

| Independent control | Paired time change | Allocation change |
|---|---:|---|
| Pointer-valued any | +0.6% | none; zero allocations |
| Boxed function any | +0.3% | none |
| External scan | -0.5% | none |
| Fibonacci | -2.7% | none |
| Sieve | -0.1% | none |
| Pricing | +0.3% | none |
| Mandelbrot | -0.4% | none |
| Queue | -1.0% | none |
| KVO read | +0.1% | none |
| KVO write | -0.2% | none |
| GoX workshop, 48 rows | +1.6% | 2,106 → 2,048 allocations |
| GoX journal, 48 rows | -0.3% | 933 → 900 allocations |
| GoX comparison, 48 rows | -0.4% | none |

GoX used six paired 250 ms samples. Its workshop timing was noisy (paired changes from -17.7% to +5.6%); the small reported increase is inconclusive. The measurements support retaining the JSON change, with no material regression established by these controls. They do not establish a speedup for every program or CPU.

Reproduce from the EDB root:

```sh
trial=$(mktemp -d)
mkdir -p "$trial/base-src" "$trial/base" "$trial/unboxed"
git archive 10daf42e5bcf887735ac66779362598fece5db63 | tar -x -C "$trial/base-src"
cp evm/bench/json_test.go evm/bench/any_test.go "$trial/base-src/evm/bench/"
cp -R evm/bench/internal/jsonwork "$trial/base-src/evm/bench/internal/"
go -C "$trial/base-src" test -c -o "$trial/base/bench" ./evm/bench
go test -c -o "$trial/unboxed/bench" ./evm/bench
python3 evm/bench/compare/scratch/measure.py "$trial" base,unboxed \
  JSON/BuildRead,JSON/Read,JSON/Update,JSON/DecodeRead,JSON/Rebox,JSON/Encode,AnyControl/Pointer,AnyControl/Function,Scan,Fib,Sieve,Price,Mandelbrot,Optimizer/Queue,KVO/ReadCustomers,KVO/WriteCustomers \
  --rounds 8 --ms 300
```

Full EDB tests, race checks and `-gcflags=all=-d=checkptr=2` passed for the runtime and GoX. Header tests include nil versus scalar zero, defined types, integer/float bits, exact slice capacity through the uint32 limit, compact-header fallback, 16-bit type-ID overflow, and values surviving GC. Bubblehouse's `go test -p 3 ./plugins/... ./fire/core/coregox`, `make quicktest` and all 223 tests in `make uitest` passed with the local EDB replacement and regenerated interface nil checks.

## Cross-interpreter workloads

The five cross-interpreter tests time one complete script invocation on an initialized VM. They rotate through four inputs and check every result during timing. The database workloads time a full transaction.

* **Price** values a portfolio of 32 fixed-rate loans under a supplied discount rate. Each loan repays equal principal over 120 months. The guest calculates the outstanding balance, interest, discount factor and present value for all 3,840 payments, then returns the portfolio value in cents. There are no host calls in the calculation. The expected cents were calculated independently with decimal arithmetic using the opening balance for month *m*, `principal × (121 − m)`, and the discount factor `(1 + rate)⁻ᵐ`.
* **Scan** reads a batch of 128 sensors. For each sensor it calls the host twice, once for the reading and once for the threshold. One branch adds the excess to the total if the reading exceeds the threshold. Each callback reads a prefilled array and returns an integer. The data stays in host memory, so the measurement includes 256 real callback crossings and excludes I/O.

## Compiler and frame optimizations, 2026-11-04

Compare EDB `f819d12` with `493ee9f`. Both use the production compiler and the same checked inputs. The reference already includes specialized external calls and the two-register floating-point loop evaluator. The book gives the before/after times, allocation sizes, Lua comparisons and the limits of each optimization. [Optimizer and results](EVMBook.md#1010-from-repeated-source-to-three-instructions) · [Individual measurements](../bench/optimizer.tsv)

Rebuild both Go benchmark binaries from the EDB root. The seven new workloads live in a standalone package. Copy just those fixtures into the reference checkout so the scripts and their native Go result checks agree on both sides.

```sh
trial=$(mktemp -d)
mkdir -p "$trial/base-src" "$trial/base" "$trial/ship"
git archive f819d12 | tar -x -C "$trial/base-src"
cp evm/bench/optimizer_test.go "$trial/base-src/evm/bench/"
cp -R evm/bench/internal/optwork "$trial/base-src/evm/bench/internal/"
(cd "$trial/base-src" && go test -c -o "$trial/base/bench" ./evm/bench)
go test -c -o "$trial/ship/bench" ./evm/bench
measure=evm/bench/compare/scratch/measure.py
python3 "$measure" "$trial" base,ship Scan,Fib,Sieve,Optimizer/Queue --rounds 8 --ms 240
python3 "$measure" "$trial" base,ship KVO/ReadCustomers,KVO/WriteCustomers,AccountState/EVM/ReadCustomers/SetIndexer --rounds 8 --ms 240
```

The `complete` series covers the original five workloads, seven new integer kernels, three floating-point kernels, external calls, KVO, indexed Bolt transactions and build time. The `extra` series runs more floating-point kernels and unmodified Gonum v0.16.0 code. Set `EVM_GONUM_DIR` to that module's source directory to repeat those tests. Allocation figures are Go heap bytes and allocations per operation. The driver rotates workload order and alternates which binary runs first within each pair.

The `engines` series uses the same reference answers and four inputs for EVM, PUC Lua 5.5.1, LuaJIT 2.1.1792588164 with tracing off and on, Lunar v0.1.3, GopherLua v1.1.2 and Yaegi v0.16.1. Build native Lua under `$trial/lua` and `$trial/jit` with the native harness. The Go comparison dependencies stay in their own modules.

```sh
mkdir -p "$trial/lunar" "$trial/gopher" "$trial/yaegi"
(cd evm/bench/compare/golua && go test -c -o "$trial/lunar/bench")
cp "$trial/lunar/bench" "$trial/gopher/bench"
(cd evm/bench/compare/yaegi && go test -c -o "$trial/yaegi/bench")
LUA_PREFIX="$(brew --prefix lua)" evm/bench/compare/native/run.sh --build "$trial/lua"
LUA_PREFIX="$(brew --prefix luajit)" evm/bench/compare/native/run.sh --build "$trial/jit"
python3 "$measure" "$trial" ship,lua,jit-off,jit,lunar,gopher,yaegi Price,Scan,Fib,Sieve,Mandelbrot --rounds 6 --ms 250
```

The remaining series isolate engineering decisions. `raw`, `packed`, `inline` and `frame` measure raw-frame clear elision, packed local slices, scalar leaf inlining and warm-call setup. `cse` and `pool` measure expression reuse and loop-independent materialization of inlined constants. `slots` and `result` compare block-wide scratch reuse with the retained result-slot rule. `fixed` switches fixed-trip unrolling off and on in the same candidate. `child`, `hoist`, `hb`, `len` and `sites` exercise sibling KVO reads, loop-bound hoisting, predicate/branch fusion, slice and string length lowering, and the linked cursor-site bitmap. In `readproof` the four binaries are the reference, the first combined candidate (`sites`), the corrected induction step (`latch`), and that correction plus checked slice-read reuse (`reads`). `writes` repeats the four database write controls for ten 600 ms pairs. `txcount` compares the indexed EVM and native Go writers across the same binaries, twelve pairs at `--iterations 40`. This fixes the number of alternating revisions in each database.

## Scalar scratch registers, 2026-11-04

EDB `8a35218` and `40ff912`, Apple M5 Max, macOS arm64, Go 1.27.3. Each binary runs one checked benchmark at a time on one logical CPU. Successive rounds reverse the order within each pair and rotate workload order. Times are median microseconds per invocation. Changes are medians of **within-round** time differences in percent. An isolated change of a few percent can also reflect Go code layout or machine load.

### Coverage outside the two development workloads

The linker recognized six of the twelve local kernels. Both executors ran exactly the same guest source at two lengths. Every row took zero Go heap allocations when warm.

| Kernel | Linked | 8 iterations, time change | 512 iterations, before µs | After µs | Time change |
|---|:---:|---:|---:|---:|---:|
| Filter | yes | -17.6% | 4.48 | 2.53 | -43.4% |
| Oscillator | yes | -25.2% | 8.42 | 4.74 | -43.8% |
| Midpoint | yes | +8.9% | 5.09 | 4.92 | -3.5% |
| Chebyshev | — | -0.8% | 5.01 | 5.03 | +0.1% |
| Horner | yes | -16.4% | 3.64 | 1.81 | -50.2% |
| Dot | yes | +0.2% | 2.40 | 1.90 | -20.5% |
| Kahan | — | +2.3% | 7.89 | 7.88 | +0.2% |
| Variance | — | -0.7% | 6.58 | 6.55 | -0.5% |
| Calibrate | yes | -0.1% | 3.02 | 2.61 | -14.0% |
| Autocorrelation | — | +1.0% | 4.56 | 4.74 | +3.5% |
| SoftClip | — | -2.2% | 6.06 | 5.80 | -4.6% |
| Newton | — | -5.8% | 0.15 | 0.14 | -4.6% |

Six paired 180 ms samples per length. Newton exits on convergence before either limit. Midpoint performs an integer-to-float conversion and the plan mirrors four intermediate results into the frame per iteration. Its eight-iteration case paid more in setup and stores than it saved in dispatch.

The first out-of-project check used ten unmodified Gonum reductions. The original counted-loop pass selected none of them. Inspection showed that a slice range keeps its previous index in an extra register. After adding a rule for this back edge, we tested five more unmodified transforms with the linker frozen. It selected three. All fifteen results follow. The vector length is 512. The strided reductions visit every other element.

| Gonum v0.16.0 | Linked | Before µs | After µs | Time change |
|---|:---:|---:|---:|---:|
| DotUnitary | yes | 3.87 | 2.12 | -46.4% |
| DotInc | — | 2.69 | 2.78 | +2.8% |
| L1Norm | — | 7.63 | 7.54 | -0.8% |
| L1NormInc | — | 3.92 | 3.86 | -1.2% |
| L1Dist | — | 8.75 | 8.90 | +1.8% |
| LinfDist | — | 15.64 | 15.47 | -1.2% |
| Sum | yes | 3.18 | 1.39 | -56.2% |
| L2NormUnitary | — | 16.42 | 16.44 | 0.0% |
| L2NormInc | — | 9.03 | 9.01 | 0.0% |
| L2DistanceUnitary | — | 17.63 | 17.25 | -1.7% |
| AxpyUnitaryTo | yes | 4.78 | 4.10 | -14.5% |
| ScalUnitaryTo | yes | 3.74 | 2.01 | -46.2% |
| DivTo | yes | 4.30 | 3.30 | -23.1% |
| CumSum | — | 4.79 | 4.91 | +3.3% |
| CumProd | — | 4.80 | 4.75 | -0.9% |

The seven vector comparisons in the first series ran for eight pairs, the other eight for six. All used 180 ms Go samples. The pass declines strided loads and the neighbor access in the cumulative transforms. The norm and distance routines also show the limit of this approach on loops that call functions and branch.

### Original workloads, database controls and build cost

Eight paired 220 ms samples. The smaller KVO runs use 512 records. The indexed account-state transaction uses 4,096. Read/write timings include storage and index work.

| Workload | Before µs | After µs | Time change |
|---|---:|---:|---:|
| Price | 24.09 | 19.97 | -17.0% |
| Mandelbrot | 217.50 | 177.86 | -18.4% |
| Fibonacci | 45.77 | 45.53 | -0.4% |
| Sieve | 99.87 | 102.50 | +2.7% |
| External scan | 3.29 | 3.27 | -0.4% |
| Raw unary scan | 1.81 | 1.73 | -4.2% |
| Scalar predicate | 2.62 | 2.64 | +0.7% |
| Scoped cursor | 7.78 | 7.80 | +0.4% |
| Memory KVO read | 46.74 | 46.35 | -0.7% |
| Memory KVO write | 539.53 | 534.52 | -0.3% |
| Bolt KVO read | 47.67 | 47.45 | +0.2% |
| Bolt KVO write | 709.56 | 692.00 | -0.9% |
| Indexed account-state read | 387.93 | 392.67 | +1.1% |
| Indexed account-state write | 13856.05 | 13918.23 | +0.5% |
| Compile twelve math kernels | 603.02 | 615.46 | +0.4% |
| Link twelve math kernels | 7.01 | 10.75 | +51.6% |

The scan, pricing and Mandelbrot scripts still allocate zero. Sieve still allocates one 81,920-byte backing array. A memory KVO read took 15 allocations, a Bolt read 24, and the indexed account-state read 24 in both versions. The linker allocates plans once per program. Linking these twelve kernels grew from 24,168 bytes in 101 allocations to 31,744 bytes in 152. Compiling the source is a separate benchmark.

### Four Lua and EVM configurations

Six rounds in rotating order. PUC Lua 5.5.1, LuaJIT 2.1.1792588164. Each column uses the same fixture, seeds and expected results. Only LuaJIT's last column enables tracing.

| Workload | EVM µs | PUC Lua µs | LuaJIT off µs | LuaJIT on µs |
|---|---:|---:|---:|---:|
| Price | 19.94 | 27.74 | 24.08 | 4.66 |
| Mandelbrot | 176.25 | 196.64 | 405.84 | 21.59 |
| Fib | 45.37 | 20.67 | 12.96 | 3.13 |
| Sieve | 101.93 | 93.15 | 108.42 | 11.85 |
| Scan | 3.27 | 2.94 | 2.51 | 2.06 |

### Which parts of scratch paid for themselves

Medians in µs. The first group hand-scheduled four Go locals in the main switch. The next group hand-scheduled integer scratch. Both groups widened dispatch even for routines that never used a scratch instruction.

| Main-switch variant | Price | Mandelbrot | External scan | execute text, bytes |
|---|---:|---:|---:|---:|
| Original | 23.84 | 216.95 | 3.30 | 17,184 |
| Four floats, destructive | 90.12 | 589.38 | 4.10 | 18,576 |
| Three operands | 83.56 | 494.17 | 4.12 | 20,112 |
| Three operands plus mixed | 53.20 | 364.52 | 4.44 | 21,088 |

| Integer variant | Sieve µs | Fib µs | External scan µs | execute text, bytes |
|---|---:|---:|---:|---:|
| Original | 101.72 | 45.88 | 3.30 | 17,184 |
| Two integer locals | 167.54 | 61.29 | 4.13 | 18,304 |
| Four integer locals | 173.91 | 63.91 | 4.22 | 41,808 |

The first hand schedules also broke existing fused instructions into their constituents. With rounded multiply/add and loop fusion restored, a hand-scheduled four-float helper took 15.29 µs on Price and 135.34 µs on Mandelbrot. The frame-only helper took 22.78 and 206.75 µs respectively. The automatic linker experiment then varied *only* the number of locals and corresponding dispatch forms.

| Scratch locals in arithmetic-only helper | Price µs | Mandelbrot µs | Helper text, bytes |
|---:|---:|---:|---:|
| 0 | 22.41 | 197.70 | 1,040 |
| 1 | 20.72 | 174.43 | 4,976 |
| 2 | 17.98 | 155.42 | 15,248 |
| 3 | 16.68 | 144.20 | 35,936 |
| 4 | 15.92 | 138.01 | 72,176 |

The retained two-local helper with moves, conversions, slice reads and writes is 16,672 bytes. The main evaluator is 17,248 bytes. An arm64 indirect jump selects a precomputed case. Go emits scalar floating-point instructions inside the case and spills both locals at private dispatch. Go stack traffic still exists. Looking up the location of each operand inside the arithmetic loop cost more than selecting the case at link time.

Untimed counters below use scenario 0. “Folded” counts the loop-control words executed inside the helper without another dispatch. The entry loads and final stores copy between Go locals and the ordinary VM frame. Neither representative plan needed an extra spill for a temporary sharing a local. Midpoint does, as the short-loop timings above show.

| Entry and schedule | Ordinary dispatch | Private steps | Folded | Scratch loads | Stores | Moves | Extra spills |
|---|---:|---:|---:|---:|---:|---:|---:|
| Price, original | 19,798 | 0 | 0 | 0 | 0 | 0 | 0 |
| Price, four floats destructive | 50,582 | 0 | 0 | 15,424 | 3,840 | 3,840 | 0 |
| Price, four floats three-operand | 46,742 | 0 | 0 | 15,424 | 3,840 | 0 | 0 |
| Price, four floats mixed | 27,606 | 0 | 0 | 96 | 32 | 0 | 0 |
| Price, retained | 1,133 | 14,932 | 3,733 | 212 | 212 | 0 | 0 |
| Mandelbrot, original | 118,265 | 0 | 0 | 0 | 0 | 0 | 0 |
| Mandelbrot, retained | 18,342 | 87,724 | 12,199 | 1,896 | 1,896 | 0 | 0 |
| Sieve, original and retained | 68,938 | 0 | 0 | 0 | 0 | 0 | 0 |

[Reproduction commands](../bench/compare/scratch/README.md) · [Every timed sample in the tables](../bench/scratch.tsv) · [Worked plan and arm64 dispatch in the book](EVMBook.md#109-keeping-two-floats-across-loop-iterations)

## Specialized external calls, 2026-11-04

EDB `6bda256`, Apple M5 Max, macOS arm64, Go 1.27.3. General and Direct run the same checked script in the same binary with the same data. Six rounds alternated which binding ran first. Times are medians in µs per invocation. The percentage column is the median of the within-round time changes. Go allocations come from `-benchmem`.

| Workload | General, µs | Direct, µs | Paired change | Go allocs, general → direct |
|---|---:|---:|---:|---:|
| Generated scalar predicate | 12.43 | 3.98 | −68.0% | 256 → 0 |
| Generated tag check on a Go struct | 66.44 | 7.19 | −89.1% | 2,049 → 1 |
| Handwritten scalar predicate | 2.81 | 2.63 | −6.2% | 0 → 0 |
| Pointer getter and balance method | 6.19 | 4.88 | −21.2% | 0 → 0 |
| Pointer getter and tag check | 7.32 | 5.88 | −19.6% | 0 → 0 |
| Cached context and string getter | 7.20 | 6.04 | −16.2% | 0 → 0 |
| Cursor over scoped KVO rows | 8.16 | 7.85 | −3.9% | 0 → 0 |

Each invocation handles 256 entries. The first two use real generated wrappers in both modes. The tag script constructs one guest struct before its loop. The five handwritten cases compare against callbacks that already access the correct R/P slots directly. The cursor traverses prebuilt encoded rows in a warm record scope, ends on nil, and adds a property from each row. The database controls below include opening transactions and scanning the storage engine. The cached getter charges string length against the VM budget in both modes, even when Go reuses the backing string.

The signature sweep runs 128 calls per invocation and compares each opcode against the same callback using `Call.Argument` and `SetResult`. All 39 function and 12 method forms reduced median elapsed time, by 11.2–35.5%. All took zero Go allocations after warmup. These comparisons use a common wrapper body to isolate the register convention. The application rows above show how much survives when the wrapper has other work to do.

Run the paired workloads with one logical processor. The signature sweep used 75 ms samples and the application workloads used 250 ms samples. To reproduce the alternation, run the General and Direct subtests separately and reverse their order on even rounds.

```sh
go test ./evm/bench -run '^$' -bench '^BenchmarkHostShapes$' -benchtime=75ms -benchmem -cpu 1
go test ./evm/bench -run '^$' -bench '^BenchmarkSmallHost$' -benchtime=250ms -benchmem -cpu 1
go test ./evm/evmbind -run '^$' -bench '^BenchmarkGeneratedHost$' -benchtime=250ms -benchmem -cpu 1
```

The existing workloads compare `34f9fcc` with `6bda256` in separate binaries. Each pair runs the same checked workload on both revisions. Eight rounds used 400 ms samples, alternating the revision that ran first. Negative changes mean less time per invocation.

| Existing script | Before, µs | After, µs | Paired change |
|---|---:|---:|---:|
| Price | 23.85 | 23.93 | +0.5% |
| Sensor scan | 3.35 | 3.33 | −1.0% |
| Sensor scan, raw unary | 1.77 | 1.74 | −1.4% |
| Recursive Fibonacci | 46.28 | 46.13 | +0.2% |
| Sieve | 102.75 | 104.04 | +0.5% |
| Mandelbrot | 215.45 | 216.47 | +0.7% |

| Existing database path | Rows | Read change | Write change |
|---|---:|---:|---:|
| Fast, memory | 512 | −2.4% | −2.3% |
| Compact, memory | 512 | −1.8% | −2.7% |
| Fast, Bolt | 512 | −3.6% | −2.2% |
| Compact, Bolt | 512 | −2.1% | −1.0% |
| Indexed KVO table | 4,096 | −3.7% | −2.5% |
| Indexed MessagePack table | 4,096 | −0.1% | −0.1% |

Both revisions used 15 allocations for the 512-row memory read, 24 for the Bolt read and 24 for the 4,096-row indexed KVO read. The TSV includes times, bytes and allocations from every pair. [Per-round measurements](../bench/host-calls.tsv)

## Interpreter comparison, 2026-11-04

Eight rounds on an Apple M5 Max, macOS arm64, Go 1.27.3. Each cell is the median in µs for one checked guest invocation. EVM before is `096ba40`, which adds the workloads to the `2bf855c` interpreter. EVM after is `e3a6829`. The Go embeddings are Yaegi v0.16.1, Lunar v0.1.3 and GopherLua v1.1.2. Native runs use PUC Lua 5.5.1 and LuaJIT 2.1.1792588164.

| Workload | EVM before | EVM | Yaegi | Lunar | GopherLua | PUC Lua | LuaJIT off | LuaJIT |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Price | 36.68 | 28.07 | 184.0 | 53.75 | 277.0 | 29.62 | 27.02 | 5.00 |
| Scan | 4.43 | 3.84 | 40.99 | 9.57 | 17.67 | 2.89 | 2.74 | 2.29 |
| Fib | 90.89 | 53.43 | 733.9 | 60.31 | 130.1 | 21.26 | 16.09 | 3.43 |
| Sieve | 396.7 | 118.7 | 606.0 | 298.0 | 834.9 | 101.8 | 124.5 | 13.67 |
| Mandelbrot | 437.1 | 254.6 | 2,563.7 | 465.9 | 1,743.7 | 222.0 | 445.0 | 24.56 |

Ordinary Scan uses the existing Go callback ABI. With both sensors registered with the word-only specialization (now `DirectUnary`), the same script took 2.00 µs. EVM allocated zero on Price, Scan, Fib and Mandelbrot after warmup. Sieve allocated one 81,920-byte backing array per call, before and after.

Each round ran the eight configurations sequentially, rotating the first one. Go binaries used `-cpu 1 -benchtime 200ms -benchmem`. Native samples ran 6,000 Price, 40,000 Scan, 8,000 Fib, 2,500 Sieve or 1,200 Mandelbrot calls. Fresh runtimes ran 32 checked warmup calls before each timed sample. EVM enforced instruction and allocation limits. The Lua engines ran without an instruction quota. Native Scan calls two C functions per sensor. The Go runtimes call Go. JIT-on and JIT-off used the same executable with different trace settings. A dash in the raw allocation columns means the native harness did not measure allocations.

### Database controls on the same revision

The first eight workloads scan 512 customers or edit 410 of them. The last four use the 4,096-row indexed synthetic fixture with a KV indexer. Each round runs the two EVM binaries consecutively for the same workload, swaps their order across rounds and rotates which workload runs first. Eight rounds, `-cpu 1 -benchtime 450ms -benchmem`, Bolt testing options (sync disabled). Paired change is the median of the eight within-round time ratios. The before and after columns are separate medians. Write allocation counts can fluctuate with the Bolt tree and the number of iterations.

| Table workload | Before, µs | After, µs | Paired change | Allocs before → after |
|---|---:|---:|---:|---:|
| KVO/ReadCustomers | 53.47 | 53.58 | -3.3% | 15 → 15 |
| KVO/WriteCustomers | 627.0 | 622.9 | -0.9% | 4,799 → 4,799 |
| KVOCompact/ReadCustomers | 61.92 | 62.22 | -0.5% | 15 → 15 |
| KVOCompact/WriteCustomers | 804.3 | 822.5 | +2.4% | 5,216 → 5,216 |
| KVOBolt/ReadCustomers | 58.62 | 56.89 | -1.9% | 24 → 24 |
| KVOBolt/WriteCustomers | 827.2 | 858.8 | +2.9% | 8,977 → 8,977 |
| KVOCompactBolt/ReadCustomers | 65.16 | 65.98 | -0.4% | 24 → 24 |
| KVOCompactBolt/WriteCustomers | 981.0 | 1,079.8 | +1.8% | 9,384 → 9,384 |
| 4,096 / EVM/ReadCustomers | 431.4 | 458.5 | +3.1% | 24 → 24 |
| 4,096 / EVM/WriteCustomers | 15,268.8 | 15,993.8 | -5.3% | 92,747 → 92,751 |
| 4,096 / MessagePackEVM/ReadCustomers | 7,469.5 | 7,265.9 | -1.4% | 77,889 → 77,889 |
| 4,096 / MessagePackEVM/WriteCustomers | 31,063.4 | 31,168.2 | +1.8% | 285,896 → 285,894 |

The three largest paired slowdowns were rerun for ten rounds with 900 ms samples. The indexed KVO read was +0.7%, the Fast Bolt write +1.8% and the Compact memory write +0.6%. All three changed direction across pairs. Their median differences are small compared with the variation across pairs. A one- or two-percent regression remains unresolved.

[Individual samples](../bench/interpreter-kernels.tsv) include a separate earlier run under heavy compilation load (`*-contended`). The unchanged pricing baseline rose from about 30 µs to 92 µs in that run. For the comparison above we waited until fewer than three compiler, linker or maintenance processes were present and two baseline pricing probes were under 37 µs. No individual samples were dropped from either set. The [workload descriptions and upstream sources](BENCHMARKS.md#interpreter-kernels) document the three ports.

## Draft KVO rows

Six rounds on an Apple M5 Max, darwin/arm64, Go 1.27.3. The baseline binary came from `b8f7487`. Each round ran the legacy word map, Fast and Compact sequentially in rotating order, with `-cpu 1 -benchtime 400ms`. All three ran the same guest script, checked the same result, and committed the same 410 updates per write transaction. Bolt used a temporary file with sync disabled.

| Median per 512 customers | Legacy word map | Fast | Compact |
|---|---:|---:|---:|
| Scan, memory | 42.47 µs | 41.22 µs | 50.64 µs |
| Update, memory | 321.88 µs | 259.32 µs | 399.43 µs |
| Scan, Bolt | 45.88 µs | 44.10 µs | 51.30 µs |
| Update, Bolt | 410.52 µs | 341.84 µs | 486.04 µs |
| Go heap bytes per memory update | 413,444 | 204,928 | 187,832 |
| Go allocations per memory update | 2,543 | 2,133 | 2,550 |
| Go heap bytes per Bolt update | 454,329 | 259,751 | 266,468 |
| Go allocations per Bolt update | 4,265 | 3,834 | 4,235 |
| Encoded bytes per initial customer, excluding DB key | 100.8 | 54.2 | 15.1 |

The largest gain was on repeated writes. A Fast editor copies each borrowed row on its first scalar change and then patches the second cell in that copy. A Compact editor rebuilds the containing row on each assignment, reusing the writer's metadata buffer. The legacy editor staged modifications and packed its word arrays on Put. The first write has a different cost because it inserts the previously absent revision key. In 12 fresh-fixture samples (`-benchtime=1x`), the in-memory write took 279 µs with legacy, 264 µs with Fast and 318 µs with Compact.

All three scans allocated 16 times per memory transaction and 22 on Bolt. Fast and legacy timing ranges overlap. To isolate lookup, `BenchmarkCustomerRowCodec` scores the same 512 rows directly from Go, without a VM, cursor or transaction. The medians were 5.52 µs for legacy, 10.32 µs for Fast and 21.10 µs for Compact, all with zero allocations. The old tiny maps already held decoded words. Fast consults key width and scalar/span metadata, and Compact walks key deltas and lengths to reach a property. The end-to-end tests also include the cursor wrapper, scoped handle and VM dispatch.

## Instruction fusion

Six paired rounds on an Apple M5 Max, darwin/arm64, Go 1.27.3. The baseline was `8ead9f6`, which already used one step per instruction and the 255 polling mask. The binaries ran sequentially with one logical processor and 400 ms samples. Their order alternated between rounds. Both used the same guest code, host wrappers, data and result checks. [Individual samples](../bench/fusion.tsv)

| Workload | Before | Fused | Go allocations before → after |
|---|---:|---:|---:|
| Price, 3,840 payments | 40.97 µs | 32.44 µs | 0 → 0 |
| Scan, 256 external calls | 4.45 µs | 4.08 µs | 0 → 0 |
| Read 512 KVO customers, memory | 47.09 µs | 44.42 µs | 16 → 16 |
| Update 410 KVO customers, memory | 345.85 µs | 348.24 µs | 2,545 → 2,543 |

The pricing loop dispatches seven words per payment after fusion, down from ten. Each sensor call dispatches one word, down from three. Cursor reads still use the normal record scope. Their cached handles can share an initial slot when instructions move during compilation. The scope probes its eight slots before using its overflow map. The write medians differed by less than 1%, within the spread of the samples.

## Optimization results

Each cell is the median of six samples on an Apple M5 Max, darwin/arm64, Go 1.27.3, with one logical processor. The baseline is EDB `e01e7fb`. Baseline and optimized test binaries ran one at a time, alternating which ran first in each pair. Times are for one complete invocation. [Individual samples](../bench/optimized.tsv) include elapsed time, bytes and allocations.

| Workload | Time before → after | Speedup | Bytes before → after | Allocations before → after |
|---|---:|---:|---:|---:|
| Price, 3,840 payments | 105.03 → 46.25 µs | 2.27× | 0 → 0 | 0 → 0 |
| Scan, 256 external calls | 8.95 → 4.54 µs | 1.97× | 0 → 0 | 0 → 0 |
| ReadCustomers, memory | 105.99 → 44.37 µs | 2.39× | 1,856 → 1,400 | 22 → 16 |
| WriteCustomers, memory | 685.77 → 329.07 µs | 2.08× | 839,634 → 413,845 | 13,591 → 2,545 |
| ReadCustomers, Bolt | 107.25 → 44.63 µs | 2.40× | 2,256 → 1,768 | 28 → 22 |
| WriteCustomers, Bolt | 724.69 → 418.82 µs | 1.73× | 847,852 → 454,735 | 13,674 → 4,267 |

The first four rows used 450 ms samples; Bolt used 350 ms. The two KVO backends run identical guest code, wrappers, checks and data. Bolt uses a temporary file with EDB's testing options (no fsync and no freelist sync). The baseline Bolt binary used those same open options in the old benchmark. The Lua results below come from an earlier experiment; Lua was not rerun in this comparison.

The guest scripts, dataset, checks and transaction boundaries did not change. One line in the example Go write wrapper did: it passes `row.Record()` to `KVPut`, which accepts the mutable record directly. The old wrapper passed `row.Record().Pack()`, boxing a packed slice on each update. Keeping the old wrapper with the optimized libraries measured 329.94 µs and 2,955 allocations in memory, and 426.05 µs and 4,677 on Bolt. The wrapper change accounts for exactly 410 allocations and about 9.8 kB per transaction on either backend.

## Lua comparison after optimization

Median µs per complete guest invocation on an Apple M5 Max, macOS arm64, Go 1.27.3 (2026-11-03). Six rounds ran sequentially. Each runtime occupied each position in the execution order once. The EVM revisions used identical scripts, inputs and benchmark code. Every invocation checked its result. [Individual samples](../bench/lua-comparison.tsv)

| Runtime | Price, 3,840 loan payments (µs) | Scan, 256 external calls (µs) |
|---|---:|---:|
| EVM before (`e01e7fb`) | 105.60 | 9.15 |
| EVM optimized (`8e14177`) | 47.21 | 4.53 |
| Lunar 0.1.3 | 48.15 | 8.57 |
| GopherLua 1.1.2 | 233.12 | 15.74 |
| PUC Lua 5.5.1 | 27.31 | 2.88 |
| LuaJIT 2.1.1792588164, tracing enabled | 4.64 | 2.10 |

EVM and Lunar overlap on Price across the six rounds (46.41–47.89 µs for EVM and 46.86–50.96 µs for Lunar). On Scan, EVM ran 1.89× as fast as Lunar and 3.47× as fast as GopherLua. EVM and Lunar recorded zero Go allocations on both workloads.

The Go benchmarks used one logical processor and 450 ms samples. PUC Lua and LuaJIT used the C harness below, 100,000 iterations per sample except for PUC Lua Price (20,000). For Scan, EVM, Lunar and GopherLua call Go for each reading and threshold. PUC Lua and LuaJIT call C. EVM ran with its default instruction budget. The Lua runs used no instruction quota.

## Original Lua comparison (historical)

Apple M5 Max, macOS arm64, 2026-11-03. Median microseconds per guest invocation across five samples. The samples and Go allocation counts are in [baseline.tsv](../bench/baseline.tsv).

| Runtime | Price (µs) | Scan (µs) |
|---|---:|---:|
| EVM | 103.24 | 6.85 |
| Lunar | 47.06 | 8.57 |
| GopherLua | 234.76 | 14.43 |
| PUC Lua | 27.62 | 2.77 |
| LuaJIT | 4.69 | 2.11 |

Go 1.27.3, one logical processor for the Go benchmarks. The pure-Go comparisons use [Lunar v0.1.3](https://github.com/mmcdole/lunar) and [GopherLua v1.1.2](https://github.com/yuin/gopher-lua). Lunar publishes competitive interpreter benchmarks and was the faster pure-Go Lua engine on both workloads here. Native runs use the same C embedding program, built with Apple clang 21 at `-O2`, linked separately to PUC Lua 5.5.1 and [LuaJIT 2.1.1792588164](https://luajit.org/luajit.html). The LuaJIT harness checks that compilation is enabled and a trace exists after warmup.

All engines execute on one thread. Each sample warms its state with 32 calls before timing. The timer includes host entry, guest execution, return and the result check. It excludes compilation, linking and startup. EVM uses its normal named entry and default execution budget. The Lua hosts cache the function and use protected calls without instruction quotas. The Go embeddings call Go callbacks. The native embeddings call C callbacks directly. Native Scan timings therefore describe the cost of embedding in C, not the cost of a C-to-Go callback.

## Run

From the EDB repository root:

```sh
go test ./evm/bench -run '^$' -bench . -cpu 1 -count 5 -benchtime=500ms
(cd evm/bench/compare/golua && go test -run '^$' -bench . -cpu 1 -count 5 -benchtime=500ms)
```

The second command runs in a separate Go module (Go 1.24 or later). The EDB module has no Lua dependencies. Omit the benchmark flags to run the result checks on their own.

For the native comparison on macOS, install the Homebrew packages `lua` and `luajit` and run:

```sh
LUA_PREFIX="$(brew --prefix lua)" evm/bench/compare/native/run.sh
LUA_PREFIX="$(brew --prefix luajit)" evm/bench/compare/native/run.sh
```

The script builds a temporary executable against each specified installation and times 20,000 guest invocations per sample with a monotonic clock. Set `SAMPLES` or `ITERATIONS` to change those counts. Run the engines sequentially on the same machine. For compiler changes, rerun the EVM benchmark before and after the change and compare multiple samples for each workload.

## KVO customer scans

The two EVM-only workloads run the [customer script](../bench/testdata/customers.go)
against an EDB table of 512 customer records. Each workload has an in-memory and a Bolt variant. `BenchmarkKVO` uses Fast rows, and `BenchmarkKVOCompact` uses Compact rows.
The schema declares the guest record types. The host binds operations to
`m.Customer`. The guest calls `m.Customer.Scan(tx)`, `m.Customer.Edit(tx, id)` and
`tx.Put(id, edit)`. The table supplies the cursor and edit types. The transaction
records which table and key produced each edit and uses that origin for Put.
`ReadCustomers` opens a read transaction, scans every row and computes a
score from the ID, balance and rate of each active customer.
`WriteCustomers` opens a write transaction and passes a writer to a guest
callback. For each of the 410 active customers the callback opens an editor,
sets the root revision and nested balance, and puts a snapshot back. It commits once. The revision alternates between 1 and 2 so every
invocation changes the stored data.

| Original EVM result (historical) | Median per transaction | Go allocations per transaction |
|---|---:|---:|
| ReadCustomers | 104.28 µs | 22 |
| WriteCustomers | 682.43 µs | 13,591 |

Five samples on an Apple M5 Max with one logical processor. Timing includes
VM reset, transaction setup, guest and host execution, scope cleanup and
the checksum check. Compilation, linking and populating the table happen
before timing. These figures include both the VM and EDB's memory backend.
In the optimized memory run, 16-row and 512-row reads have the same allocation count. Those allocations
open the transaction and cursor, create the scope, and initialize reusable
row handles. Scalar property reads and subsequent visits to those
sites allocate nothing. A script that saves a row can still allocate a handle
to keep that row distinct from later cursor results.

The transaction test reads all 512 packed rows after a successful commit
and a subsequent aborted write, verifying that only the commit took effect.

```sh
go test ./evm/bench -run '^$' -bench '^Benchmark(KVO(Compact)?(Bolt)?|CustomerRowCodec)$' -benchmem -cpu 1 -count 5 -benchtime=500ms
```

## Indexed account-state table

The figures in this section predate direct MessagePack access (2026-11-07).
MessagePack property views borrow encoded bytes and support explicit
decode-once hints. See [current access strategy measurements](../../kvo/_docs/BENCH-RECORD-ACCESS.md).

`BenchmarkAccountState` uses 4,096 synthetic Bolt rows and one secondary index on the nested balance. Each row has counters, timestamps, an inline totals object, ID lists and sparse per-key histories and progress objects. The cardinalities are deterministic fixture choices. No customer data is needed. The reader visits every row and scores the active ones. The writer updates the revision and balance of 3,277 active rows in one transaction and moves their index entries. It alternates revision 1 and 2. Setup, compilation and final full-row verification are outside the timer.

| Stored format | Access path | Read ms | Read allocs | Write ms | Write allocs |
|---|---|---:|---:|---:|---:|
| MessagePack | Go structs | 5.932 | 77,882 | 24.44 | 249,563 |
| MessagePack | Go property codes | 6.267 | 77,882 | 26.19 | 285,855 |
| MessagePack | EVM property selectors | 6.886 | 77,890 | 28.39 | 286,012 |
| Fast | Go property codes, format-independent record | 0.226 | 16 | 13.23 | 111,304 |
| Fast | Go property codes, encoded-only view | 0.182 | 16 | 12.65 | 94,870 |
| Fast | EVM property selectors | 0.429 | 24 | 14.63 | 110,600 |
| Fast | Go structs, reflected binding | 4.313 | 66,821 | 22.93 | 266,721 |
| Fast | Go structs, generated binding | 2.491 | 22,726 | 17.40 | 150,181 |

Medians of five 250 ms samples on an Apple M5 Max, Go 1.27.3, one logical CPU, Bolt testing options (sync disabled). All paths use the same logical rows, indexed updates and checksums. The final check compares timestamps as instants because MessagePack restores them in the local time zone.

On MessagePack, the Go property-code reader and ordinary struct reader both allocate 77,882 times. Each cursor advance decodes the same entire struct, including its maps and lists. Resolving properties adds 0.335 ms per scan here. The EVM script uses the same record and edit operations as the Go property-code path. MessagePack writes encode the edited Go struct through the original table writer.

Fast rows have selected Compact children. Their record and encoded-only views skip maps and lists that the score never touches. In this earlier run, saving an edit materialized one Go struct for the typed indexer using the generated binding. The struct writers also re-encode the whole struct.

```sh
go generate ./internal/statrow
go test ./evm/bench -run '^$' -bench '^BenchmarkAccountState' -benchmem -cpu 1 -benchtime=250ms -count=5
```

Earlier measurements of draft KVO rows used the separate experimental bucket. The current record benchmarks use normal tables and include their row envelopes.


### Production-shaped replay

Set `EDB_ACCOUNT_STATE_JSON` to a local JSON array of `statrow.State` rows with sequential surrogate keys starting at 1. With no variable set the benchmark uses the original 4,096 synthetic rows. Each path checks its checksum during timing and compares every persisted row with the input afterward.

The following replay used 576 shapes from three stores. It retained counter values, list and map cardinalities, absent collections and the association between these fields in each row. Keys and timestamps were synthetic. The reader scanned 576 rows. The writer updated 208 active rows and maintained the balance index. Six alternating before/after runs, median per transaction, Go 1.27.3, Apple M5 Max, one logical CPU, 250 ms samples, local Bolt with sync disabled. The baseline was `fd53085` with the same input loader.

| Packed KVO access | Read µs, before → after | Read allocs | Write µs, before → after | Write allocs |
|---|---:|---:|---:|---:|
| Go encoded view | 18.5 → 17.4 | 16 → 16 | 669 → 663 | 5,767 → 4,846 |
| Go record view | 23.3 → 22.7 | 16 → 16 | 703 → 706 | 6,808 → 5,887 |
| Go struct, reflection | 279 → 227 | 3,270 → 2,118 | 1,195 → 849 | 12,957 → 6,697 |
| Go struct, generated | 195 → 144 | 1,877 → 725 | 982 → 704 | 9,428 → 3,730 |
| EVM | 50.5 → 47.2 | 24 → 24 | 856 → 841 | 6,855 → 5,937 |

The native struct loop now reuses one destination with RowInto and writes the row it scanned. Generated writes reuse the writer's directory scratch. A small map sorts its keys on the stack. Both packed Go views and the EVM call the packed balance indexer. The script and its edit-by-key API stayed the same. A packed EVM write allocated 564 kB per transaction, down from 682 kB. The index callback saved allocations on all three packed edit paths. Write times on those paths changed little.

On the same shape replay, the generic MessagePack EVM control took 1.82 ms per update both ways and allocated 11,867 times. These generic rows contain ordinary Go integers. The host application's separate MessagePack benchmark uses complete original row types and their custom codecs.

### Same writer, different indexer

A second sample contained 576 rows, of which 204 were active. Each pair below uses the same row data, writer, binding and balance index. Only the index callback changes. Each iteration commits one transaction. After timing, the benchmark checks every stored row and every index key. Medians of six rounds on an Apple M5 Max, Go 1.27.3, one logical CPU, 250 ms samples. The order of the indexers rotated each round.

| Writer | Struct index, µs | KV index, µs | Heap kB, struct → KV | Allocs, struct → KV |
|---|---:|---:|---:|---:|
| KVO encoded view | 664 | 597 | 561 → 447 | 5,628 → 4,750 |
| KVO record | 695 | 668 | 615 → 501 | 6,649 → 5,770 |
| KVO EVM | 835 | 823 | 668 → 554 | 6,700 → 5,820 |
| Go struct on KVO, reflected | 821 | 821 | 496 → 496 | 6,472 → 6,472 |
| Go struct on KVO, generated | 636 | 646 | 447 → 447 | 3,637 → 3,637 |
| Go struct on MessagePack | 1,157 | 1,165 | 476 → 476 | 6,206 → 6,206 |
| MessagePack record | 1,556 | 1,562 | 807 → 807 | 11,475 → 11,475 |
| MessagePack EVM | 1,770 | 1,814 | 860 → 860 | 11,526 → 11,526 |

The packed-only callback already avoided a struct decode. It took 593 µs for the encoded Go view and 820 µs for EVM, with the same allocation counts as the KV indexer. EVM ranged from 812–845 µs with the struct callback, 804–879 µs with the KV callback and 807–845 µs with the packed callback. A Go struct writer already owns the struct. Changing the indexer saves allocations when an edit originates in packed bytes.

On the larger synthetic fixture (4,096 rows, 3,277 updates, six 200 ms samples), the encoded Go writer went from 11.91 to 11.56 ms and from 93,854 to 76,038 allocations. EVM went from 14.33 to 14.18 ms and from 110,538 to 92,707 allocations. Row validation, Bolt writes and secondary index maintenance still run in both cases.


A full rebuild clears the balance index and visits all 576 stored KVO rows. With the struct callback it took 824 µs, 1,091 kB and 7,192 allocations. With the KV callback it took 673 µs, 911 kB and 6,516 allocations. The packed-only callback also took 673 µs. The latter two validate and reuse each stored payload. These are medians of six 350 ms samples per callback.

```sh
go test ./evm/bench -run '^$' -bench '^BenchmarkAccountState$/.*/^WriteCustomers$' -benchmem -cpu 1 -benchtime 250ms -count 6
go test ./evm/bench -run '^$' -bench '^BenchmarkAccountStateReindex$' -benchmem -cpu 1 -benchtime 350ms -count 6
```

## Interpreter kernels

All Go engines compile the same source. All Lua engines load the same Lua file.
Each entry takes a scenario number from 0 to 3. During timing every engine checks
every return against the integers in `testdata/expected.txt` under the workload package.

| Entry | What one invocation does | Upstream algorithm |
|---|---|---|
| Fib | Recursively computes F(14) through F(17), visiting both subtrees with no cache. Checksums come from the iterative recurrence. | [Yaegi’s recursive fixture](https://github.com/traefik/yaegi/blob/3fbebb36621c03b2981ccee03246ea40455bd792/_test/fib.go) |
| Sieve | Fills 5,000–5,093 flags and strikes multiples starting at twice each prime. A trial-division count supplies the answers. Includes the fresh slice or Lua table. | [Are We Fast Yet sieve](https://github.com/smarr/are-we-fast-yet/blob/74306fec151070fd07157cefeacf19e7e0bcdc89/benchmarks/Lua/sieve.lua), used in [Lunar’s suite](https://github.com/mmcdole/lunar/tree/v0.1.3) |
| Mandelbrot | Counts interior pixels of four 24–30 pixel square grids, using the upstream coordinate grid, initial orbit and 49 updates. A complex-number implementation supplies the counts. Omits bitmap packing and I/O. | [RaptorJIT’s Lua kernel](https://github.com/raptorjit/raptorjit/blob/1d83bb4ec7581cc1715725af1514297ca3f3ac64/testsuite/bench/mandelbrot.lua) |

These are scaled ports, so compare the engines on these inputs. The Go-in-Go
comparison uses Yaegi v0.16.1. It resolves each typed entry once before warmup.
Neither comparator module adds a dependency to EDB. EVM's Sieve benchmark gives
the session a larger cumulative allocation allowance so an arbitrarily long
sample can run. Each call retains the normal 64 MiB and one-million-allocation
limits. The other entries retain all default limits.

```sh
(cd evm/bench/compare/yaegi && go test -run '^$' -bench . -benchmem -cpu 1)
WORKLOADS='Fib Sieve Mandelbrot' ITERATIONS=1000 LUA_PREFIX="$(brew --prefix lua)" evm/bench/compare/native/run.sh
LUAJIT_MODE=off LUA_PREFIX="$(brew --prefix luajit)" WORKLOADS=Fib evm/bench/compare/native/run.sh
```

The native harness disables LuaJIT before loading the script in interpreter-only
mode. With tracing enabled, it verifies that warmup produced a trace.

To run all engines in rotation and capture individual samples, from the EDB root:

```sh
python3 evm/bench/compare/kernels.py \
  --lua-prefix "$(brew --prefix lua)" \
  --luajit-prefix "$(brew --prefix luajit)" > /tmp/evm-kernels.tsv
```

The driver builds the Go and C binaries in a temporary directory. Pass
`--baseline-bin /absolute/path/to/old-bench.test` to include a previous EVM.
Build it from a checkout with the same guest code and answers using
`go test -c -o /tmp/old-bench.test ./evm/bench`. By default each runtime
takes the first position once. Use `--rounds 1` for a smoke run.
