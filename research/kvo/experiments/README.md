# Representation experiments

`kvolab` contains comparison codecs and generic workloads. It is not a supported
library or production format. [`draft`](../draft) supplies the independent
reference grammar; `draft_test.go` checks every field and child against the same
fixtures before reporting sizes. Fixture construction is outside timing.

`SCShape` needs a historical writer descriptor to find field boundaries. It
cannot skip unknown fields without their semantic schema. Keep it as a measured
control, not a candidate for that requirement. Other retired layouts also retain
their own grammars and limits.

```sh
go test ./research/kvo/experiments
```

[Variant inventory](../../_docs/KVO-EXPERIMENT-BENCHMARKS.md) lists portable runners,
mmap controls and source bundles. [Replay input](testdata/README.md) explains
sample-dependent skips. Historical reports live locally in `../results/`;
[research history](../../_docs/KVO-REPRESENTATION-HISTORY.md) points to the full log.
