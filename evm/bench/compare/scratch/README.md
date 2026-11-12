# Scalar scratch register experiment

Hand schedules test whether local Go registers speed up particular loops. They
apply only to the exact Price, Mandelbrot or Sieve bytecode and run in benchmark
overlays. The production linker sees no fixture names, hashes or schedules.

## Compare the automatic linker with the original evaluator

Run from the EDB root. Give both binaries the same tests and inputs.

```sh
trial=$(mktemp -d)
mkdir -p "$trial/base-src" "$trial/origin" "$trial/ship"
git archive 8a35218 | tar -x -C "$trial/base-src"
cp evm/bench/math_test.go evm/bench/gonum_test.go "$trial/base-src/evm/bench/"
cp -R evm/bench/internal/mathwork "$trial/base-src/evm/bench/internal/"
(cd "$trial/base-src" && go test -c -o "$trial/origin/bench" ./evm/bench)
go test -c -o "$trial/ship/bench" ./evm/bench
measure=evm/bench/compare/scratch/measure.py
python3 "$measure" "$trial" origin,ship Price,Mandelbrot,Fib,Sieve,Scan
python3 "$measure" "$trial" origin,ship Math/Filter/8,Math/Filter/512,MathBuild/Compile,MathBuild/Link
python3 "$measure" "$trial" origin,ship KVO/ReadCustomers,KVO/WriteCustomers,AccountState/EVM/ReadCustomers/SetIndexer
```

The optional upstream corpus reads complete function declarations out of a pinned Gonum checkout. It adds no module dependency to EDB. The first cohort contains ten scalar-returning dot products, norms, distances and sums. The second contains five destination-writing transforms. Kernel names select the *test inputs*. Run both cohorts, including the routines the linker declines.

```sh
go mod download gonum.org/v1/gonum@v0.16.0
export EVM_GONUM_DIR="$(go env GOMODCACHE)/gonum.org/v1/gonum@v0.16.0"
go test ./evm/bench -run 'Test(Math|Gonum)Kernels' -v
go test ./evm/bench -run '^$' -bench '^Benchmark(Gonum|Math)' -cpu 1 -benchmem
python3 "$measure" "$trial" origin,ship Gonum/DotUnitary,Gonum/Sum,Gonum/L1Dist,GonumTransform/AxpyUnitaryTo
```

For example, the dot benchmark compiles the loop exactly as Gonum wrote it. The expected answers and slice arguments come from the local harness. The transform benchmarks check every destination cell before and after timing. [Gonum dot products at v0.16.0](https://github.com/gonum/gonum/blob/v0.16.0/internal/asm/f64/dot.go)

The Lua harness checks all four inputs for each script. With tracing enabled it also verifies a trace was compiled. It measures native C callbacks for Scan. The Go benchmark measures calls into Go.

```sh
native=evm/bench/compare/native/run.sh
LUA_PREFIX="$(brew --prefix lua)" "$native" --build "$trial/lua"
LUA_PREFIX="$(brew --prefix luajit)" "$native" --build "$trial/jit"
python3 "$measure" "$trial" ship,lua,jit-off,jit Price,Mandelbrot,Fib,Sieve,Scan
```

## Recreate the hand-scheduled variants

`build.py` generates opcode definitions, operand checks, the dispatcher and disassembly with `go -overlay`. It verifies all workload results after each build. The four float locals have destructive, three-operand and mixed forms. A mixed instruction has at most one ordinary frame register, in any operand position. In destructive form the destination is also the first input. The integer trial has separate operations for signed and unsigned division, comparisons and right shift. The schedules stop at calls. Adding `--slim` generates only the handlers touched by the trial to measure the cost of enlarging the switch.

```sh
experiment=evm/bench/compare/scratch
export SCRATCH_EDB="$trial/base-src"
python3 "$experiment/build.py" "$trial/none-dest" none dest
for form in dest three mixed; do
    python3 "$experiment/build.py" "$trial/f4-$form" f4 "$form"
done
for bank in u2 u4; do
    python3 "$experiment/build.py" "$trial/$bank-mixed" "$bank" mixed
done
python3 "$measure" "$trial" none-dest,f4-dest,f4-three,f4-mixed Price,Mandelbrot,Scan
python3 "$measure" "$trial" none-dest,u2-mixed,u4-mixed Sieve,Fib,Scan
```

The early hand schedules expand existing multiply/add and loop superinstructions. To separate the cost of breaking fusion from the cost of scratch registers, repeat with `--island --fused` and with `--island --fused --plain` (ordinary frame operands). The committed linker retains the intermediate float rounding in those superinstructions.

Count and disassemble in separate builds. Timing runs have no counters.

```sh
python3 "$experiment/build.py" "$trial/counted" f4 mixed --counts
(cd evm/bench && "$trial/counted/bench" -test.run '^TestScratchCounts$' -test.v)
python3 "$experiment/count.py" "$trial/counted-current"
go tool nm -size "$trial/ship/bench" | grep -E '\(\*VM\)\.execute$|\(\*floatLoop\)\.run$'
go tool objdump -s '\(\*floatLoop\)\.run$' "$trial/ship/bench"
```

The current counter prints workload, scenario, ordinary dispatches, private steps, original loop-control words folded, scratch loads, final stores, scratch moves and extra frame spills. Original metered instructions equal ordinary dispatches + private steps + folded words − extra spills. A scratch load or final store is setup/teardown and has no guest instruction cost. Assembly spills chosen by Go are separate from explicit frame spills in the plan.

[Measurements, including the routines that stayed on ordinary dispatch](../../../_docs/BENCHMARKS.md#scalar-scratch-registers-2026-11-04)
