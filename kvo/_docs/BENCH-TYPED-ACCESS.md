# Typed access and collection representation

Measured November 9, 2026 on Apple M5 Max, macOS arm64, Go 1.27.3.
Implementation: EDB 8faf318. Five samples at 150 ms each; medians below.
[Raw output](../typed-access-bench.txt) retains every result. All variants report
0 B/op and 0 allocs/op after metadata initialization.

## Scalar property representation

Each operation reads two integer properties and subtracts them. The 256 distinct
source records and runtime property codes are shared by all variants. The raw
baseline is GetValue(code).AsInt(), matching the old application's runtime codes.

| Storage | Raw Value, ns | Descriptor Get, ns | Typed code kvo.Get, ns | Typed code property.Get, ns |
|---|---:|---:|---:|---:|
| Fast2 | 11.28 | 15.75 | 11.65 | 11.10 |
| Compact2 | 59.50 | 62.71 | 59.44 | 59.19 |
| MessagePack | 378.50 | 383.20 | 383.50 | 382.20 |
| Generated | 5.60 | 9.60 | 6.67 | 5.64 |
| Reflected | 16.92 | 20.66 | 17.85 | 16.77 |
| Memory | 5.54 | 9.30 | 6.36 | 5.60 |

The selected scalar API uses an eight-byte typed code with direct Go conversions.
property.Get(record) matches the raw path closely. kvo.Get(property) adds up
to about 1 ns per two accesses on the cheapest decoded representations. The
callback descriptor costs about 4 ns more there, so it remains available for
custom conversion and object views rather than being the default scalar carrier.
There is no reflection in Value-to-T or T-to-Value conversion. Native field
access still uses whichever shared host binding is attached: generated or reflected.

## Collection handle versus initialized cursor

Get returns a 16-byte List[T] or RecordList containing a tagged values.Value.
The alternative control returns the original 312-byte initialized scalar cursor.
Neither has a per-value backing allocation. The cell points directly at encoded
bytes or an existing native collection header; static element metadata is shared.
Metadata registration is once per property/layout, outside steady-state measurements.

The handle-only measurements assign the returned value to a global sink. The
small view defers parsing traversal state until requested, while the larger
cursor initializes that state immediately. This is an intentional API difference,
not just a comparison of copying 16 versus 312 bytes.

Three-string lists:

| Storage | Return cell, ns | Return large cursor, ns | Traverse raw, ns | Traverse large typed cursor, ns | Cell + typed cursor, ns | Cell + range, ns |
|---|---:|---:|---:|---:|---:|---:|
| Fast2 | 10.50 | 59.50 | 58.30 | 83.24 | 86.72 | 90.09 |
| Compact2 | 13.31 | 66.97 | 58.59 | 85.45 | 89.85 | 93.44 |
| MessagePack | 29.70 | 67.53 | 100.40 | 129.40 | 131.40 | 135.20 |
| Native | 11.41 | 43.74 | 64.81 | 93.50 | 97.09 | 101.60 |
| Memory | 7.58 | 56.20 | 55.49 | 80.53 | 84.64 | 89.75 |

Keep the 16-byte view: returning/storing it is substantially cheaper, while
complete manual traversal costs only 2–4 ns more than the large typed cursor.
Typed traversal is still slower than the raw Value iterator (roughly 25–40 ns
for these short lists), due to typed conversion callbacks and cursor bookkeeping.
It is not an across-the-board speedup over raw iteration. Keep ListValue().Iter()
available for loops where those nanoseconds matter. All compared traversal paths
allocate zero; the live customer benchmark measures the total application effect.
The raw output also includes empty and 32-element collections.

List.Len() reports total elements; Cursor.Len() reports remaining elements.
Cursor() holds traversal state locally. Iter() is single-pass and resumes after
a break if the same sequence is called again. A new Iter() starts independently.
Slice() explicitly allocates its result; borrowed strings/records still refer to
the source storage. Immediate range use is allocation-free; an escaping iterator
closure may allocate, like other Go closures.

Reproduce:

```sh
go test ./kvo -run '^TestTypedCollectionSize$' -v
go test ./kvo -run '^$' -bench 'BenchmarkTyped(Property|List)$' \
  -benchtime=150ms -count=5
```

Functional coverage includes MessagePack, Fast2, Compact2, native and decoded
memory; typed editing; wrong-model rejection; sequence resume/exhaustion; zero
views; and concurrent cold metadata registration. Full EDB tests and race checks
pass. checkptr=1 passes. checkptr=2 forces extra heap escapes and therefore fails
the exact-allocation assertions; it reported no invalid-pointer diagnostic.
