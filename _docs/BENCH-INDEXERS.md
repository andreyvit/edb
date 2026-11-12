# Index callbacks

Measured 2026-11-11, Apple M5 Max, Go 1.27.3. Warm pools; synthetic native
record, one uint64 index key. These are callback costs, not whole database writes.

| Callback | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Table `SetIndexer` | 75 | 8 | 1 |
| Per-index `AddIndex` | 67–72 | 0 | 0 |

Typed yield avoids boxing the index key. Pooled emitters reuse yield closures;
pooled builders retain the encoder workspace. Pools may allocate on first use
or after GC. Composite index keys still use the existing flat encoder.

Primary-key cells reuse the key already decoded by the write path:

| Operation | ns/op | allocs/op |
| --- | ---: | ---: |
| Wrap uint64 or string key | 1.8 | 0 |
| Wrap addressable composite key | 6.3–6.7 | 0 |
| `IndexPrimaryKey[AB]` composite access | 1.8–2.0 | 0 |
| Decode `AB` again from tuple bytes | 33 | 1 |

Non-addressable aggregate keys need one addressable copy per indexed write,
shared by its callbacks. Native row writes and keys decoded from storage already
have addressable key storage. Cells, records, and yield callbacks are borrowed;
do not retain them. Use `pk.AsUint()`, `pk.AsString()`, or
`edb.IndexPrimaryKey[ExactGoKeyType](pk)` for composite/custom key types.

```sh
go test . -run '^$' -bench 'BenchmarkIndex(PrimaryKey|Callback)' -benchmem -count=3
```
