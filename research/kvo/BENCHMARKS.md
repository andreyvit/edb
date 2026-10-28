# Rerunning the KVO experiments

Keep every considered approach runnable during research. A rejected format is a useful control, and a result on one CPU is not a result on every CPU. Production cleanup is deferred.

The suite uses generic fixtures. It never reads an application database. Bolt benchmarks create isolated temporary files. Timings are reported in nanoseconds; human-readable summaries group large integers with spaces. Raw Go output and CSVs keep machine-readable numbers.

## Local inputs and results

Collected data and generated artifacts are kept locally, outside Git tracking:

- `research/kvo/results/`: reports, raw measurements, profiles, validation logs and source snapshots.
- `research/kvo/experiments/testdata/workload-shapes.json.gz`: the optional anonymized replay input (189 row shapes).

The experiment code and handwritten synthetic fixtures remain versioned. A checkout without the collected replay still builds and runs the available tests; tests and benchmark families that need replay data explicitly skip. No synthetic replacement is silently substituted for the measured workload. Restore the local replay file and rebuild to enable those cases. A malformed present replay fails rather than skipping.

A local build embeds the replay when it exists, so portable test binaries remain self-contained. `build.txt` records whether it was embedded. The generated source bundle explicitly includes the ignored replay when used, plus its tracked README, so the bundle reproduces its inputs; keep bundles under `results/` or outside the repository. Historical result links in these documents refer to local files and are unavailable in a fresh clone until those artifacts are restored separately.

## Build and run

From the EDB repository root:

```sh
# Native bundle, including exact dirty research sources.
research/kvo/bench.sh build /tmp/kvo-native
research/kvo/bench.sh run /tmp/kvo-native /tmp/kvo-native-results

# Portable Linux x64 binaries; the target needs no Go installation.
research/kvo/bench.sh build /tmp/kvo-linux linux amd64
```

Upload the complete Linux bundle to a scratch directory on the authorized host. Then run its included script:

```sh
bash /tmp/kvo-linux/bench.sh run /tmp/kvo-linux /tmp/kvo-server-results
```

The bundle contains test executables for `edb`, `kvo`, `mmap`, `draft`, and `experiments`. The experiments binary includes the production Immutable implementation as a linked benchmark baseline. It also contains all format alternatives below. The runner executes one process at a time, first checks correctness, then runs benchmarks.

Defaults are `GOMAXPROCS=1`, five repetitions, and `150ms` per repetition. The broad suite takes several minutes. Go calibrates each case, so wall time exceeds repetitions multiplied by the requested duration. Keep other CPU-heavy work off the measured host during a run.

```sh
# Quick harness check; not a timing result.
KVO_COUNT=1 KVO_BENCHTIME=1x research/kvo/bench.sh run /tmp/kvo-native /tmp/kvo-smoke

# Focus one family, optionally pinning one CPU on Linux.
KVO_PACKAGES=experiments KVO_BENCH='^BenchmarkMmapRead$' \
  KVO_CPU=24 bash /tmp/kvo-linux/bench.sh run /tmp/kvo-linux /tmp/kvo-mmap
```

Other settings are listed by `bench.sh` with no arguments. `KVO_TEST_ONLY=1` runs correctness checks without timing. `KVO_TMPDIR` chooses the temporary-file directory. Full storage benchmarks include a durable fsync variant; use a filesystem representative of the intended deployment when interpreting those results.

`environment.txt` records CPU, OS, memory, affinity, runtime settings, time, and load. `build.txt` records Go, target architecture, Git revision, and dirty file names. `source.tar.gz` freezes the actual source files, including uncommitted experiments; `SHA256SUMS` identifies it and every binary. A Git revision alone is insufficient for this working tree. `completed.txt` appears only after all requested tests and benchmarks pass. The runner refuses to overwrite a prior run.

Keep raw `.txt` repetitions. `summarize.py` creates ordinary time/allocation `.csv` files and `.metrics.csv` files for all custom metrics. Medians summarize observations; they are not significance tests. Investigate direction changes and overlap before interpreting small differences. Historical result files are never overwritten to make them look current.

## Variant inventory

All paths below are in [`experiments/`](experiments/) unless noted. A benchmark selector is its top-level Go name; subcase names identify layout, width, fixture, strategy, and operation.

| Approach | Source | Benchmark families |
| --- | --- | --- |
| Existing KVO Immutable and mutable update/pack | Production `kvo/`; `bench_test.go`, `extra_bench_test.go` | `Read`, `Scan`, `Nested`, `Update`, `RawScan`, `CacheRead`, `MmapRead`, `Storage` |
| Original custom compact, full index, offset index | `codec.go`, `codec_test.go`, `bench_test.go` | `Read`, `Scan`, `Dictionary`, `Nested`, `Update`, `Bitmap` |
| Dense fixed-word numeric object and stack overlay | `words.go`, `extra_bench_test.go` | `Words`, `CacheRead`, `EditDensity` |
| MessagePack library, borrowed view, owned writer, typed decoder | `msgpack_view.go`, `schema_compact_test.go`, `config_test.go`, `hybrid_test.go` | `Read`, `Scan`, `Dictionary`, `TagEncode`, `MPGeneratedDecode`, `MPHybrid`, `Configuration` |
| Schema-required, bitmap, gaps, all-present framing | `schema_compact_test.go` | `SchemaCompact`, `SchemaConfiguration` |
| Initial overlay and logical change versions | `editor.go`, `changes.go`, `changes_test.go` | `Editor`, `ChangeEncode`, `ChangeVisit`, `ChangeReader`, `ChangeV2` |
| Previous public draft, compact/indexed/shared classes | `../draft/`, `draft_test.go` | `Draft`, `DraftRead`, `DraftDictionary`, `LengthPreviousEncode`, `LengthPaired` |
| Length-only tags; unified immediate tags; scalar/span split; denser partition | `tagged.go`, `tagged_bench_test.go` | `TagEncode`, `TagCold`, `TagDeep`, `TagRead`, `TagScan` |
| Buffered children, prefix backpatch, trailing independent tags | Same tag files | `TagEncode`, `TagCold`, `TagDeep` |
| Two-bit numeric-key fusion with independent tags | Same tag files | `TagFused` |
| Unchanged previous wire bytes with improved skipping | `wire_controls_test.go` | `WireControl`, `LengthListControl` |
| Backward writer producing the previous bytes | `reverse_writer_test.go` | `ReverseWriter` |
| Old wire classes in trailing metadata | `class_catalog_test.go` | `ClassCatalog` |
| Literal scalar lengths in trailing metadata | `class_catalog_test.go` | `RawLengthCatalog`, `LengthIndexedPatch` |
| Numeric incremental Begin/End writer | `catalog_stream_test.go` | `CatalogStream` |
| Kind in count header instead of footer | `retained_count_test.go` | `RetainedCountKind` |
| Universal indexed word cells, including dictionaries | `retained_words_test.go` | `RetainedUniversalIndex` |
| Capacity hints and generic versus width-dispatched indexed search | `retained_implementation_test.go` | `RetainedImplementation` |
| Current common writer/view, scalar/span bitmap, entry offsets | `length_*_test.go` | `LengthCommon`, `LengthPaired`, `LengthKeyDistributions`, `LengthRowCache`, `MmapRead` |
| Explicit-zero schema policy and unknown-zero preservation | `schema_presence_test.go` | `SchemaPresence` |
| Selected direct / bitmap-packed slots, generic / known / evolving access | `fixed_slots_test.go`, `fixed_slots_evolution_test.go` | `SelectedSlots`, `SelectedSlotsEncode`, `SelectedSlotPolicy`, `SelectedSlotSpan`, `SelectedSlotStreaming`, `SelectedSlotWriter`, `SelectedSlotPatch`, `SelectedSlotResolved` |
| Sample-shaped literal4 / rounded3 / tag4-8 selectors and wide-span tags | `selector_catalog_test.go`, `selector_trusted_test.go`, `selector_lists_test.go`; optional locally retained replay fixture | `SelectorCatalog`, `SelectorAccess`, `SelectorListIndex`, `SelectorTrustedGet`, `SelectorLiteralControl`, `SelectorLists` |
| Same change paths with four/three/two-bit value selectors | `selector_changes_test.go` | `SelectorChanges` |
| One-bit eight-byte escape, positive/length tags, optional Object/Dictionary counts | `one_bit_test.go`; first checked-iterator version in `results/source-onebit-20261016.tar.gz` | `OneBit` |
| Current pointer-free overlay | `length_edit_test.go`, `length_compare_test.go` | `LengthEdit`, `LengthPaired` |
| Current whole-owned-object decoder | `length_compare_test.go` | `LengthConfiguration` |
| Inline length-coded and trailing-catalog changes | `length_changes_test.go` | `LengthChanges`, `LengthCatalogChanges` |
| Wide/marked/type-directed values, slice/pointer views | `packed_value_test.go` | `PackedValues`, `PackedView` |
| Within-line and split-line loads/stores, padded directories | `packed_value_test.go` | `Alignment`, `AlignmentStore`, `AlignmentIndex` |
| Real Bolt transaction writes, pack once/twice, copy/patch | `storage_test.go` | `Storage` |

Prefix the family with `Benchmark` in a selector. Some controls intentionally violate the final requirements (for example, schema-only decoding cannot skip unknown fields without its old schema). Keep them to measure the price of the required guarantees, not as silently interchangeable candidates.

`go test ./research/kvo/experiments -run '^TestSelectorReplayByteAccounting$' -count=1 -v` reproduces the byte decomposition in `selector_sizes_test.go` for all four selector dialects. It accounts for actual encoded payloads, Dictionary key bytes, entry metadata, headers and footers, recursively, and checks that every root's parts sum to its size. This is deterministic size analysis, not a latency benchmark.

The kind-in-count and universal-cell files reconstruct layouts whose temporary source was overwritten before the retention request. Their historical size outcomes are reproduced by tests, but newly measured timings must be attributed to the reconstruction. Most other format alternatives remain in their original parameterized implementations. Earlier commits and result logs retain superseded API/error policies; those policies are not additional proposed storage dialects.

## What the mmap comparison measures

`BenchmarkMmapRead` writes generic rows to a temporary Bolt file, closes it, reopens read-only, and keeps a read transaction alive. Values are borrowed from that mmap. No row payload is copied into a Go-heap array for timed reads.

The fixture set has 128 local keys, 128 high global keys, and nested numeric maps. Existing KVO supports those same values. String maps and arbitrary lists do not have an equivalent existing-Immutable implementation in this test; do not imply otherwise.

Each format is tested with one row and 32 768 rows. The operations separate:

- `navigation`: choose a previously located borrowed row, then read a field.
- `bucket`: locate the row through Bolt's B-tree in an existing transaction, then read a field.
- `transaction`: begin/end a read transaction, locate the row, then read a field.

Historical setup located each row through Bucket.Get, but did not explicitly touch every payload page. On the server, overflow payload pages could still fault inside timing. The newer Fast attribution, FinalFast, shape-read and mutation-mmap controls explicitly touch every payload page before timing; process-local fault measurements document the difference. Neither version drops OS caches or establishes physical disk-cold I/O. Larger sets stress CPU caches and TLBs. File sizes include Bolt's page layout and spare pages; `row-B` is the encoding size. `unaligned-percent` measures actual row-start alignment in the mapping, independent of relative alignment inside each format.

The same fixture, key sequence, and operation scope are used for every format. Views are trusted after fixture creation; the old draft uses trusted child views too, so it is not uniquely charged for repeated child validation. Navigation includes constructing the borrowed view for the selected row. Compact children under an indexed root are measured separately from indexing every child. Existing Immutable remains a primary control, not only the previous experimental draft.

These navigation controls store raw codec rows in a temporary bucket. Production EDB's outer value framing and index-key material are outside the timed operation and can change alignment. The experiments establish controlled warm-memory comparisons, not a complete production throughput forecast.

## Validation and future additions

Run `go test ./...`, `go test -race ./research/kvo/...`, and focused `checkptr=2` checks locally after changing pointer-backed readers. Test binaries include deterministic cases and fuzz seeds; long fuzz campaigns are separate Go-tool runs. No race instrumented binary is used for timing.

Three-bit and two-bit selector experiments are retained alongside the four-bit baseline, including the wider short-span tag partition. The selected format is a recommendation, not permission to remove controls. Production-informed fixtures must contain only anonymous structural distributions or generated values, never raw tenant strings, keys, or records. Keep both the sampling method and its limits with the results.


## Sample-derived synthetic replay

`experiments/testdata/workload-shapes.json.gz` is embedded into the portable experiment binary. It contains 189 generic shapes derived from a bounded read-only sample, not original records. Scalars retain bit widths, text/blob values retain lengths, and numeric map keys retain key-gap bit widths. Declared fields become ordinal numbers; dictionary keys are synthetic unique byte strings of the same lengths. Original values, IDs, names, and key prefixes are absent. See `results/workload-distributions-20261016.md` for sampling exclusions and model choices.

Replay roots are not frequency weighted. Aggregate size is a sum across selected small/median/large shapes. The encoding benchmark selects deterministic root-size quantiles, while the correctness test covers all replay roots. Dictionary prefix behavior is not represented. Keep this distinction when comparing a row-size total with latency on one synthetic fixture.


## Performance iteration inventory

All names below are benchmark families, prefixed with `Benchmark` when passed to `KVO_BENCH`.

| Family | Scope / source |
| --- | --- |
| FastAttribution, FastPlacement, FastSearchKernel, FastFaults | Dispatch, header preparation, scalar handling, key loads/search, key-width/physical-footprint controls, and explicit payload faults |
| FastReaders, FastHeaderControls | Fused getters, hit/miss mixes, matched general/simple front/back parsers |
| BatchOverlay, BatchCold, BatchRefinements | Sorted, append, lazy sort, upsert, hash and ordered-tail policies; staging/read/export and allocations |
| FastShapeReads, TreeMutation, OwnedMutation | Four whole-row replay shapes, sparse/deep/70%/empty construction; owned fixed-size patches and fallbacks |
| DenseWriter, ShapeBuilder, ShapeBind | Forward encoding, explicit prepared value slots, preparation and path-to-slot binding costs |
| ImmutableMutation | Compatible numeric Object trees against existing Mutable/Immutable; excludes unsupported Dictionary/List comparisons |
| FastCursor, FinalFast | 80/24/16-byte preparation alternatives, repeated reads, exact-query Immutable comparisons |
| StackScratch, PatchStack | Actual heap escape behavior of large local arrays versus a small flat patch plan |
| CompactMatched | Trusted Object-only Compact readers with matched assumptions; avoids crediting specialization to the wire grammar |
| FrontShape, UnifiedRows | Full front-header writer, then mixed Fast/Compact framing with the existing discriminator moved to the front |
| MutationMmap | Complete sparse/deep/dense writes from actual Bolt mappings, one or 1 024 source rows, old/unified framing and patch-or-rebuild |
| KeysLast, KeysLastShape, KeysLastMmap | Object key directory beside its trailing footer; same byte count, complete mixed-layout writer/view/editor, fused front control and mmap mutations |
| ByteSearchKernel, ByteSearchMmap, ByteSearchShape | Standard-library byte search versus binary search for one-byte key directories, with both old and adjacent-key layouts |
| LeafMerge | Direct sorted leaf-change merge versus recursive edits; patch-first fallback, cold/reused editors, full construction and membership/size changes; rejects ancestor overlaps |
| BulkDirectory | One final directory reservation, pointer frame, and key-width dispatch outside the loop; byte-identical Fast output, cold/reused and mixed/Compact controls |
| PatchLookup | Original parsed-header planning versus fused lookup and within-call prefix reuse; complete owned copy/patch or rebuild from mmap rows |
| TreeChildIndex | Linear sibling list versus sorted child-number arrays and a global child map; shallow/deep/70%/construction/interleaved writes, reused and cold editors on actual mmap sources |
| ShapeBindIndex | Linear, binary and reusable hash path binding; index setup and complete prepare/bind/write, retaining the previous shape writer for an isolated comparison |
| TinyGet | Direct linear search up to one/two/four entries versus binary and byte search; sampled paths and larger mmap working sets |

Source archives named `source-*-20261016.tar.gz` accompany each iteration. Incomplete runs retain their logs and are identified in STATE.md. Use a result's source archive to reproduce that exact implementation; the latest tree adds validation fixes and later candidates. Read [the performance report](results/performance-iteration-20261016.md) for workload scope and the review list of faster options with additional costs.


The selected prototype at the performance closeout uses the adjacent-key Fast layout and the four-entry TinyGet refinement through `keysLastView.Get`. `GetBinary` and `GetByte` retain both controls. `source-performance-close-20261016.tar.gz` freezes the fully validated code after that selection; `performance-close-amd64-20261016/` is correctness evidence, not a new timing run. The final timing sources and results retain their earlier names and hashes. Human-readable logs have trailing whitespace normalized without altering numeric output.


The continuation's `TreeChildIndex` cold cases allocate a fresh editor but reuse writer and output; do not label them a completely cold encoding pipeline. All row-mutation cases include final materialization. `ShapeBindIndex` assumes the canonical leaf-path catalog already exists (the same input as the old binder), and explicitly measures optional index construction. Its prepare/bind/write cases include shape-plan construction while reusing writer/output buffers. The retained server CPU profile `kvo-tree-before-20261016.pprof` uses the final closeout binary identified in `performance-close-amd64-20261016/SHA256SUMS`.


The continuation read controls `BaseKey` / `BaseKeyShape` compare an optional stored key base against full keys and Immutable over Bolt mappings. `ContainerFast` / `ContainerFastShape` isolate returning the Dictionary payload offset from search and direct indexing of canonical all-scalar Lists. Their complete code lives in `base_key_test.go` and `container_fast_test.go`. The stored-base reader is a separate format pilot; current Open/editor APIs must not consume its bytes. Synthetic global-key clusters are controls, not inferred production frequencies. `appendDirectoryWriter` preserves the append-per-entry writer after bulk reservation becomes the default. `boundShapeEditsPrevious` preserves the quadratic dynamic binder; the historical `ShapeBind` benchmark explicitly calls it.


`PatchReserve` compares ordinary append growth, one reservation after the first eligible leaf, and pointer-free eight-byte offset/size plans whose values remain in the input edit batch. It times complete mmap copy/patch or tree rebuild, including early/late eligibility failures. Cold refers to plan scratch only; writer, fallback editor and output are reused to isolate this choice. `PatchLookup/fused-before-containers` retains the earlier fused Dictionary/List locator after selected accessors eliminate the duplicate key decode and canonical scalar-List offset load.


The final continuation checkpoint is `d6c6826`, with code-identical native/Linux archives named `source-continuation-final-{native,linux}-20261016.tar.gz`. The `continuation-final-{arm64,amd64}-20261016` results contain 426 cases per host, five 100 ms repetitions, plus all five package correctness runs. Reproduce its selection with:

```sh
KVO_COUNT=5 KVO_BENCHTIME=100ms \
KVO_BENCH='^Benchmark(TinyGet|ContainerFastShape|PatchLookup|PatchReserve|LeafMerge|BulkDirectory|ShapeBindIndex)$' \
research/kvo/bench.sh run /path/to/bundle /path/to/new-results
```

Use the server CPU/scratch settings documented above when comparing that host. This matrix verifies the selected implementations; all earlier alternatives retain their own benchmark names and exact source archives. A fresh plan/editor label does not imply a fresh writer/output or a durable database commit; each family documents its timed scope.


## 2026-10-17 selected direct-tag/signed candidate

The main candidate is `current_*_test.go`. Previous `keysLast`, `length`, `oneBit` and `draft` implementations retain their exact mappings. The selected Compact codec uses counted positive/byte-length tags; the prior candidate used literal lengths. The scalar-specific comparison holds the counted tag grammar fixed and changes only signed mapping.

| Family | Scope |
| --- | --- |
| `CurrentScalars` | Signed positive/negative/boundary Lists, current two’s complement versus retained ZigZag tags; reused encoding and byte counts |
| `CurrentMmap` | Ordinary Fast read versus the non-inlined scalar accessor (`value-call`), lookup wrapper (`wrapped`), preceding Fast and Immutable; local/global/nested rows, 32 768 warm Bolt mmap rows |
| `CurrentRows` | Four representative replay shapes; Fast/Compact encoding, few shallow/deep mutations, 70% updates and construction; complete materialization from 1 024 mmap rows with reusable scratch/output |
| `CurrentChangeScratch` | Validated change opening plus signed reading with reused versus fresh eight-step path scratch |
| `CurrentIteration` | Hot root iteration with the original 280-byte wrapper, fused 280-byte control and selected 208-byte iterator; complete mmap mutation costs remain in `CurrentRows` |

```sh
go test ./research/kvo/experiments -run '^TestCurrentReplaySizes$' -count=1 -v
KVO_COUNT=5 KVO_BENCHTIME=100ms KVO_BENCH='^BenchmarkCurrent' \
  research/kvo/bench.sh run /path/to/bundle /path/to/new-results
```

The replay preserves scalar widths as unsigned words, not original signed semantic types. Use the controlled signed fixtures to interpret signed mapping costs. Historical 2026-10-16 results do not measure this candidate.

The completed [signed/tag report](results/signed-tags-20261017.md) distinguishes the paired checkpoints: `09ee9a8` for unchanged mmap reads/change-reader scratch (103 cases), `659b8fe` for iterator controls (110 cases), and `ebd5da7` for final affected row writes/scalar encoding (86 cases). Each has five repetitions on both hosts and all five correctness binaries. The full current family at HEAD contains 127 cases; the report does not claim they were all measured in one final combined cohort. Source archives and earlier zero/iterator/width probes are retained.


## 2026-10-17 native Go mapping

`NativeAccess` compares direct key reads, a single traversal, reflected GetFast, a handwritten generated-decoder control and owned Get. It includes one/four/sixteen native access passes, late-field projections, Fast/Compact and widths/lengths from the anonymous replay. `NativeCollections` measures fresh/reused decoding and encoding, plus a two-property native Patch versus equivalent key edits. Both families read temporary Bolt mmap data. `NativeAggregateMapValues` isolates a hot row with nested slice/pointer map values: selected key-index reuse, sorted snapshots and allocating rebuild.

The [native report](results/native-mapping-20261017.md) documents contracts and scope. Final checkpoint `73d3a73` has 112 cases × five 100 ms repetitions on arm64 and amd64. The `5c96236` 110-case initial cohort remains available. Both source archives, raw repetitions, hashes, environments, correctness runs, fuzz/pointer validation and all rejected controls are preserved. Timing excludes plan compilation; cold writer cases include scratch/output allocation. No durable DB write is timed.

```sh
KVO_COUNT=5 KVO_BENCHTIME=100ms KVO_BENCH='^BenchmarkNative' \
  research/kvo/bench.sh run /path/to/bundle /path/to/new-results
```

GetFast's allocation guarantee is conditional on reuse/capacity and live borrowed strings; see the README. A fresh destination and newly inserted nested map keys can allocate. Allocation assertions run without pointer instrumentation; semantic pointer-safety checks run separately.


## 2026-10-17 dictionary entry policy

`DictionaryPolicy` covers default zero elision and `KeepDictionaryZeroValues`, Fast/Compact, 32-entry string dictionaries, native reused Get/Put, schema-bound key reads and Set-zero/Delete with materialization. Nonzero widths come from the anonymous wide-row replay; 13-byte keys and an injected 25% zero sensitivity case are explicit fixture choices, not claimed production frequencies. Reads cycle through 1 024 temporary Bolt mmap rows. The no-zero cases store identical bytes under both policies. Presence-aware key reads consume the boolean as well as the value. Plan compilation and initial storage warming are excluded.

There are 34 policy cases, including two retained general-directory presence lookup controls. The initial policy cohort had 32. Re-run them with the 112 existing native controls using `KVO_BENCH='^Benchmark(Native|DictionaryPolicy)'`. The prior exact source archives retain the old unconditional native-map policy. Existing native fixtures that intentionally require zero membership now declare the flag, preserving their prior comparison contract.


Final dictionary checkpoint `6743de6`: 146 cases × five 100 ms repetitions on each CPU, including all 34 policy cases. Initial checkpoint `24ab3b8`: 144 cases. Exact sources, raw outputs and both general-directory and specialized lookup paths remain available. See the [policy report](results/dictionary-policy-20261017.md) for size, allocation and timing comparisons. The full paired cohorts each pass all five correctness binaries.
