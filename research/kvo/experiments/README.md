# Representation experiments

This directory preserves all comparison implementations and generic workloads, including the common-container reference described in the [current KVO specification](../README.md). Its `kvolab` package is research code, not a supported library or a production format.

The files were originally under `research/kvo/`. They moved here after the checkpoint commit so that the reference implementation in `../draft/` has one coherent grammar and API. Historical commands and file references in `../STATE.md` refer to those original locations; use `./research/kvo/experiments` for the same package now.

The schema-directed `SCShape` codec requires a historical writer descriptor to find field boundaries. It does **not** meet the requirement to skip unknown fields without their semantic schema. It remains as a measured comparison, not the selected compact encoding. Likewise, earlier compact/indexed codecs and MessagePack views have their own grammars and limitations.

`draft_test.go` adapts the existing fixtures to the new draft and compares the same values. Fixture construction is outside benchmark timing. It verifies every field and child before reporting sizes. The MessagePack library is used only by these comparison experiments; the current `draft` package imports only the standard library.

Run the experiments with:

```sh
go test ./research/kvo/experiments
```

The full history remains versioned in [`../STATE.md`](../STATE.md). Prior results remain locally in the Git-ignored [`../results/`](../results/) directory. The collected replay fixture is also local and ignored; without it, sample-dependent tests and benchmark families explicitly skip. See [local replay input](testdata/README.md).

See [BENCHMARKS.md](../BENCHMARKS.md) for the complete variant inventory, portable test-binary runner, real Bolt mmap controls, and exact-source bundle format. Do not remove rejected alternatives during research.
