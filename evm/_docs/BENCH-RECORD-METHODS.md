# Explicit record method calls

Measured on Apple M5 Max, darwin/arm64, Go 1.27.3:

```sh
go test ./evm/evmbind -run '^$' -bench '^BenchmarkRecordMethods$' \
    -benchmem -benchtime=500ms -count=3
```

The helper reads earned, redeemed and expired properties and returns
`earned - redeemed - expired`. Each benchmark operation performs 256 calculations;
the table reports median **nanoseconds per calculation**, amortizing VM entry.
Compilation, binding, encoding, materialization and VM warm-up are excluded.
These are property-access/call benchmarks, not database scans.

| Record representation | Native Go helper | EVM registered helper | EVM inline arithmetic |
| --- | ---: | ---: | ---: |
| Fast2 | 14.58 | 28.14 | 34.55 |
| Compact2 | 28.40 | 42.24 | 47.62 |
| MessagePack | 116.60 | 132.30 | 138.70 |
| Native struct, generated fields | 9.63 | 23.91 | 30.36 |
| Native struct, reflected fields | 27.37 | 41.49 | 46.39 |
| Memory | 9.72 | 23.27 | 28.79 |

Every measured path reports **0 B/op, 0 allocs/op**, including the complete
warmed EVM invocation. Tests also assert zero allocations for 1,000 registered
method calls in one invocation across all six representations.

The EVM registered-helper column always uses generated method bindings.
“Reflected fields” describes native struct property access, not reflective host
function invocation. The latter is an optional compatibility fallback and is
not allocation-free. String results are copied at the method boundary and are
outside the allocation-free scalar calculation claim.

The generated method saves approximately 5–7 ns per calculation versus three
separate EVM property reads and arithmetic instructions. Compared with direct
Go, the EVM loop and host-call boundary add approximately 14–16 ns. Registration
selects the method at compile/link time; the record retains its existing
representation. There is no per-call method-name lookup, full decode, boxing or
implicit conversion to a native struct.
