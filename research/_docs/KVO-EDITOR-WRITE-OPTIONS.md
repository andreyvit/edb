# Editor property API and write options

The implemented API split is `GetValue(code)` / `SetValue(code, value)` versus
`GetPathValue(path)` / `FastSetPathValue(path, value)`. Keep ordinary write policy in
the model, including explicit zero preservation for presence-significant and
stored-primary-key properties. Explicit path options still have distinct
semantics, notably omission/deletion versus explicit zero.

There is no SetValueOpt. The direct setter resolves the model's policy; the fast
path setter consumes supplied options unchanged. Its Try and WithWriter variants
use the same naming. EVM and the model cache share record.WriteOptions, including
stored-primary-key zero preservation. The direct packed setter combines policy
and encoding resolution in one metadata lookup. The measurements below document
the decision before the API split; they are not measurements of the final direct
setter. Absence of an options argument does not discard the model policy.

## Native comparison

`BenchmarkEditorWriteOptions` uses the same existing setter for all variants:

- Explicit: the caller supplies precomputed flags.
- Cached: fetch flags from shared field metadata, then call the setter.
- Schema: query Model.PropFlags and convert flags on every call.

Cached deliberately pays a separate metadata lookup. It is a conservative
prototype, not the proposed final direct setter, which could share its field
lookup with policy resolution. Scalar reads are not changed. Write options fit
in existing modelField padding (40 bytes total on this arm64 machine), and are
already used by child opening/creation instead of repeated schema lookups.

The benchmark covers six representations, dense/sparse codes, and both
omitted-zero and stored-zero policies. Setups use the real generated statrow
field accessors; sparse cases rebind those Go fields to different property codes
without reusing a codec generated for the old codes. Values alternate 0/1 so
every iteration mutates the field. Encoding, registration, and initial pending
storage allocation are outside the timed loop.

Go 1.27.3, Apple M5 Max, darwin/arm64. Five samples, 150 ms each. Representative
medians for the stored-zero case, ns/write:

| Codes | Representation | Explicit | Cached | Schema |
| --- | --- | ---: | ---: | ---: |
| Dense | Fast2 | 38.80 | 40.59 | 45.96 |
| Dense | Compact2 | 52.83 | 54.88 | 62.05 |
| Dense | MessagePack | 70.23 | 71.35 | 76.53 |
| Dense | Generated struct | 14.08 | 15.03 | 18.16 |
| Sparse | Fast2 | 52.84 | 63.82 | 60.15 |
| Sparse | Compact2 | 65.92 | 75.30 | 71.73 |
| Sparse | MessagePack | 84.45 | 93.88 | 89.64 |
| Sparse | Generated struct | 35.58 | 47.03 | 42.83 |

All three strategies have the same allocation counts: zero for these scalar
writes except MessagePack, which retains its existing 24 B / 2 allocations.
Outputs: `/tmp/edb-editor-options-bench.txt` and
`/tmp/edb-editor-options-tests.txt`. The full EDB suite passes.

## EVM comparison

Measured on the same machine and Go version, five interleaved samples of 200 ms
per case. Modes run sequentially, rotating their order for each sample.

The guest source is [write_options.go](../../evm/bench/testdata/write_options.go);
the harness is [record_options_test.go](../../evm/bench/record_options_test.go).
Compilation, linking, binding, encoding, and editor warmup are outside timing.
Each invocation performs 256 alternating zero/one assignments, reads the field
back, accumulates a checksum, and validates it. Nested holds a child handle and
writes through it. Timings below include the guest loop, read, and accumulation,
plus an amortized scope setup/closure per invocation, not just the setter.

The comparison script uses temporary Go source overlays:

- Explicit: the existing EVM linker precomputes Step.Options.
- Cached: resolve every path step from cached modelField metadata on each
  assignment, overwriting the caller's options before calling the same setter.
- Schema: resolve every path step using Model.PropFlags on each assignment,
  overwriting the caller's options before calling the same setter.

These are separate compiled binaries, with no runtime benchmark-mode switch.
Read paths stay unchanged. Cached and Schema remove the write's dependence on
caller-supplied flags while preserving the model's policies; they do not remove
the Step.Options member itself. As in the native prototype, they intentionally
pay an additional metadata lookup rather than combining it with the setter's
existing field resolution. The results quantify that extra work, not a lower
bound for an optimized setter without an options argument.

Dense uses the original property codes. Sparse multiplies root codes by 4096,
forcing the existing binary-search fallback; child property codes stay dense.
Each comparison uses identical models, data, and guest bytecode. Generated uses
the generated native field accessors, rebound to the benchmark's model.

Median ns per guest iteration (one assignment and read):

| Codes | Representation | Access | Explicit | Cached | Schema |
| --- | --- | --- | ---: | ---: | ---: |
| Dense | Fast2 | Direct | 69.65 | 69.43 | 74.31 |
| Dense | Fast2 | Nested | 103.40 | 103.60 | 120.70 |
| Dense | Compact2 | Direct | 127.20 | 132.40 | 130.40 |
| Dense | Compact2 | Nested | 117.70 | 115.70 | 130.00 |
| Dense | MessagePack | Direct | 111.50 | 109.30 | 116.10 |
| Dense | MessagePack | Nested | 135.30 | 140.70 | 156.40 |
| Dense | Reflected struct | Direct | 59.30 | 61.24 | 65.09 |
| Dense | Reflected struct | Nested | 74.87 | 78.46 | 93.02 |
| Dense | Generated struct | Direct | 35.01 | 38.25 | 43.81 |
| Dense | Generated struct | Nested | 52.65 | 58.68 | 70.93 |
| Dense | Decoded memory | Direct | 34.64 | 37.76 | 41.42 |
| Dense | Decoded memory | Nested | 50.08 | 54.87 | 69.13 |
| Sparse | Fast2 | Direct | 78.95 | 93.12 | 86.93 |
| Sparse | Fast2 | Nested | 95.17 | 111.10 | 115.90 |
| Sparse | Compact2 | Direct | 149.00 | 159.40 | 160.80 |
| Sparse | Compact2 | Nested | 108.10 | 124.50 | 131.10 |
| Sparse | MessagePack | Direct | 137.20 | 152.30 | 152.80 |
| Sparse | MessagePack | Nested | 134.20 | 150.10 | 151.70 |
| Sparse | Reflected struct | Direct | 99.86 | 112.40 | 112.30 |
| Sparse | Reflected struct | Nested | 73.51 | 88.38 | 92.14 |
| Sparse | Generated struct | Direct | 72.32 | 85.41 | 81.76 |
| Sparse | Generated struct | Nested | 53.40 | 68.16 | 70.95 |
| Sparse | Decoded memory | Direct | 52.06 | 65.30 | 60.72 |
| Sparse | Decoded memory | Nested | 49.18 | 63.95 | 68.26 |

All strategies have identical allocations per invocation. Fast2, Compact2,
reflected/generated structs, and decoded memory use 608 B / 5 allocations for
Direct and 720 B / 8 allocations for Nested. These are fixed invocation/scope
costs, with no per-assignment allocation in the warmed loop. MessagePack keeps
its existing two allocations per assignment: 517 total for Direct and 520 for
Nested, approximately 6.8 KB per invocation. Flag resolution adds none.

### Database workload

The existing AccountState WriteCustomers benchmark scans 4096 synthetic
customers in a real bbolt database, updates revision and nested balance for
active customers, persists changes, and maintains a SetIndexer. It validates
the checksum, persisted rows, and balance index. This is not a measurement of the
live Bubblehouse customer database.

Median milliseconds per full invocation:

| Storage | Explicit | Cached | Schema |
| --- | ---: | ---: | ---: |
| Fast2 | 11.763 | 11.907 | 11.827 |
| MessagePack | 17.297 | 17.252 | 17.482 |

The cached medians differ by +1.2% for Fast2 and -0.3% for MessagePack. Sample
ranges overlap substantially: Fast2 explicit 11.50–12.36 ms versus cached
11.78–12.56 ms; MessagePack explicit 16.97–17.56 ms versus cached 17.08–17.32 ms.
This run does not establish a meaningful end-to-end speedup from flags.

These persistence workloads have substantial existing allocation costs. Median
bytes/allocations per invocation (Explicit / Cached / Schema) were:

| Storage | Explicit | Cached | Schema |
| --- | ---: | ---: | ---: |
| Fast2 | 14,103,444 B / 122,338 | 14,024,365 B / 122,349 | 14,199,340 B / 122,352 |
| MessagePack | 16,131,428 B / 127,585 | 15,743,461 B / 127,437 | 15,970,560 B / 127,488 |

Iteration counts, revisions, and bbolt allocation behavior vary with benchmark
calibration. The fixed-size EVM loops above isolate allocation effects; the
database allocation variation should not be attributed to flag lookup.

## API conclusion

Precomputed flags avoid runtime metadata work. The benefit is visible in cheap
decoded writes and with sparse codes: the EVM cached prototype adds roughly
10–17 ns per iteration in sparse cases. Dense Fast2 shows no resolved difference
in this run. Repeated schema traversal is especially costly for nested writes.
A cached binary search is not universally faster than the schema's map lookup.

Keep the normal application setter simple: SetValue(code, value) derives
its write policy from the model. Keep the EVM's precomputed policy on its
lower-level path operation so it does not throw away information already known
at link time. Property identity need not contain mutation flags, and this
measurement does not justify another ordinary SetValueOpt API. The implemented
packed direct setter combines default resolution with the encoding lookup.
Explicit deletion bypasses the preserve-zero policy through FastSetPathValue.

## Reproduction and validation

After the API split, five 150 ms samples of BenchmarkEditorPropertyAccess gave
the following medians. Before is the original one-step path API; after is the
direct code API, including model-policy resolution on writes:

| Representation | Get before/after, ns | Set before/after, ns |
| --- | ---: | ---: |
| Fast2 | 9.76 / 8.80 | 38.34 / 38.66 |
| Compact2 | 30.20 / 30.49 | 92.37 / 92.07 |
| MessagePack | 207.10 / 208.10 | 67.76 / 72.16 |
| Reflected struct | 10.41 / 9.03 | 25.72 / 28.05 |
| Generated struct | 4.61 / 3.32 | 13.15 / 15.28 |
| Decoded memory | 4.61 / 3.35 | 13.18 / 14.89 |

Allocations are unchanged. The direct packed setter avoids a second lookup;
native and decoded setters still pay for policy resolution. The EVM benchmark
was rerun for all six representations, dense/sparse, direct/nested. Its fixed
allocation counts are unchanged. Representative Fast2 medians were 64.02 ns
dense direct and 77.34 ns sparse direct, versus 69.65 and 78.95 ns before.
These sequential before/after runs are a regression check, not proof of a
precise speedup. Logs: `/tmp/edb-editor-after.txt` and
`/tmp/edb-evm-editor-api-after.txt`.

```sh
go test ./kvo -run '^$' -bench '^BenchmarkEditorWriteOptions$' -benchmem -benchtime=150ms -count=5
python3 research/benchmark_editor_options.py --output /tmp/edb-evm-write-options --samples 5 --benchtime 200ms
```

The script runs evm, evmcompiler, and evm/bench tests for every mode before
timing, including the existing compact-child/presence-significant-zero contract
test. All pass. The complete unmodified EDB suite also passes after the benchmark
addition. Raw EVM samples and test logs are in `/tmp/edb-evm-write-options`.
