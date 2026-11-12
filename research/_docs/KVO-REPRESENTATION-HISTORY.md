# KVO representation experiments

These experiments informed Fast2/Compact2. They are not the production API.
Use [`kvo`](../../kvo/README.md) and [`kvo/codec/row2`](../../kvo/codec/row2)
for application code. The independent research writer remains a wire-format
control.

## Decisions

- Separate logical types from storage layout and access strategy.
- Use stable numeric property codes. Unknown fields must remain skippable and
  preservable without loading an old schema.
- Share scalar semantics and container framing between Fast2 and Compact2.
  Fast2 favors access; Compact2 favors size. Children can choose another layout.
- Keep presence separate from zero. Dictionary entries can require explicit
  zero values; model defaults determine ordinary property writes.
- Keep native Go mappings optional. Direct encoded access avoids full decoding;
  generated or reflected bindings support workloads that need native objects.
- Preserve old formats until an explicit migration. Logical changes, row
  envelopes, indexes and payload encoding are different concerns.

## Controls worth keeping

The suite compares MessagePack, the original Fast1 word map, historical draft
layouts, schema-directed encodings, fixed slots, selector widths, dictionary
policies and decoded/native access. Rejected layouts remain runnable: different
row shapes and access patterns can reverse a result.

Size accounting includes directories, selectors, alignment and nested payloads.
A small leaf-value win does not imply a smaller database. Read comparisons must
separate sparse access, full traversal and decoding. Write comparisons must
include reconstruction, allocation, index work and persistence where applicable.

The replay input records shapes and distributions, not customer values. Results
on one CPU or synthetic distribution do not establish universal rankings.

## Reproduce

[Experiment inventory and commands](KVO-EXPERIMENT-BENCHMARKS.md) list retained
variants and measurement artifacts. [Research format specification](../kvo/README.md)
describes the historical candidate; [current design](KVO-DESIGN.md) describes
production access and ownership.

Local reports and source archives under `research/kvo/results/` are ignored
measurement artifacts. A clean clone does not contain them. Preserve their
source hashes and hardware labels when sharing results.
