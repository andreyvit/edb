# KVO package merge measurements

2026-11-11, Apple M5 Max, Go 1.27.3, darwin/arm64.

Models, records, typed properties, and Go bindings have their definitions
in `kvo`. The former `kvo/model`, `kvo/record`, and root forwarding layer are
removed. Codecs, `values`, and `nativebind` keep their package boundaries.
This changes source imports and canonical Go type paths; it does not change
wire formats or handle layouts.

## Allocation and layout check

The [before](before.txt) and [after](after.txt) runs cover 78 cases, with three
200ms samples each. Every case kept the same B/op and allocs/op. Record handles
remain 16 bytes. Typed reads, ordinary record reads, and EVM scans remain at
0 B/op and 0 allocs/op across all six representations. Decode-once hints and
retained EVM copies keep their existing allocation costs.

```sh
# Before: use ./kvo/record instead.
go test ./kvo -run '^$' -bench 'BenchmarkTypedProperty|BenchmarkRecordHandle' \
  -benchmem -benchtime=200ms -count=3
```

## Interleaved timing check

The first sequential run showed substantial timing drift late in the run.
Other development jobs were running on this shared machine. To distinguish
that drift from the package change, compile both test binaries and alternate
old/new order over six samples. No compilation runs inside these samples.
The old source is commit `5786ff0`; the new source is `d7e64f2`.

[Old samples](before-paired.txt) and [new samples](after-paired.txt) show median
changes from about -5% to +4%, with overlapping sample ranges throughout.
These measurements show no consistent regression; they are not evidence of a
speedup. All cases below allocate zero. EVM scans process 256 rows per operation.

| Benchmark | Before, ns/op | After, ns/op | Change |
| --- | ---: | ---: | ---: |
| `TypedProperty/Fast2/Value` | 15.815 | 16.335 | +3.3% |
| `TypedProperty/Fast2/TypedCode` | 16.525 | 16.575 | +0.3% |
| `TypedProperty/Fast2/DirectCode` | 15.980 | 15.360 | -3.9% |
| `TypedProperty/Generated/Value` | 7.726 | 7.865 | +1.8% |
| `TypedProperty/Generated/TypedCode` | 9.147 | 8.812 | -3.7% |
| `TypedProperty/Generated/DirectCode` | 7.998 | 7.615 | -4.8% |
| `TypedProperty/Memory/Value` | 9.157 | 8.826 | -3.6% |
| `TypedProperty/Memory/TypedCode` | 10.050 | 9.754 | -2.9% |
| `TypedProperty/Memory/DirectCode` | 8.702 | 8.884 | +2.1% |
| `RecordHandle/Memory/OpenRead` | 5.174 | 5.389 | +4.2% |
| `RecordHandle/Memory/Retain` | 5.628 | 5.450 | -3.2% |
| `RecordHandleEVM/Fast2/Scan` | 17,933.500 | 17,118.000 | -4.5% |
| `RecordHandleEVM/Generated/Scan` | 15,671.500 | 15,457.000 | -1.4% |

Reproduction uses `go test -c ./kvo -o /tmp/kvo-after.test` (and the old
`./kvo/record` package for `/tmp/kvo-before.test`). Run each binary with
`-test.run='^$' -test.benchmem -test.benchtime=200ms -test.count=1` and these
filters, alternating binary order on each of six repetitions:

```text
^BenchmarkTypedProperty$/(Fast2|Generated|Memory)$/(Value|TypedCode|DirectCode)$
^BenchmarkRecordHandle$/Memory$/(OpenRead|Retain)$
^BenchmarkRecordHandleEVM$/(Fast2|Generated)$/Scan$
```
