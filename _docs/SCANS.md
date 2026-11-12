# Resumable scans and key-only queries

Rows and records share scan rules: a scan stays in one transaction, a chunk
bounds yielded entries, and an owned bookmark resumes in another transaction.
Tables and unique/nonunique indexes support both directions, strings and
composite keys.

## Bounded reads with joins

`ReadChunks` opens a read transaction, yields a nonempty chunk, and closes the
transaction before starting the next chunk. No rows are buffered. The query
factory receives the new transaction each time and must return the same query.

```go
var total int64
for chunk := range edb.ReadChunks(db, 500, func(tx *edb.Tx) edb.RecordScan[uint64] {
    return CustomersByTag.Exact(tx, "vip")
}) {
    for id, customer := range chunk.Entries() {
        // The chunk implements Txish, so both reads use its transaction.
        today := DailyStats.Get(chunk, StatsKey{id, "2026-11-10"})
        if !today.Missing() {
            total += today.Get(Stats_Orders)
            writeCustomerSummary(id, customer.Get(Customer_FirstName), today.Get(Stats_Orders))
        }
    }
}
```

The [tested customer/statistics example](../examples/loyalty/joins_test.go) contains
all model declarations and a key-only version of this join. If the initial
customer fields are unnecessary, iterate `chunk.Keys()` instead of `Entries()`;
this avoids fetching those customer rows entirely.

Returning `edb.RowScan[Customer, uint64]` from the factory instead gives native
chunks with `Rows()` and `(key, *Customer)` entries. Both forms have `Keys()`,
`Count()`, `Bookmark()`, `More()`, and `Tx()`.

- The limit is a positive number of **entries yielded by the scan**, including
  entries an application filter rejects. Internal index-boundary filtering can
  inspect more physical entries; the limit does not bound those or bytes read.
- `break`, `return`, and panic in the outer loop close the current transaction.
  Check a request's cancellation/deadline inside the loop as usual.
- Breaking an inner iteration ends that chunk early; automatic iteration
  resumes after the last consumed entry. This also allows a time budget within
  each chunk. Leaving a chunk entirely unconsumed stops the outer iterator.
- Each chunk is single-pass. Iterating it again continues from its next entry,
  within the original limit. Ordinary scan iterators remain reusable.
- Joins must use `chunk` or `chunk.Tx()`, not a context holding some other reader.
  Do not close or replace the transaction during iteration.
- The loop body holds a reader. Perform writes outside it; use explicit chunks
  for read-then-write batches.
- Records, raw keys, strings, and collections borrowed from a record must not
  escape the chunk's transaction. Use `kvo.Own()` or the collection's owned
  operation before retaining them. Decoded native rows and typed keys are owned.

**Each chunk sees a new snapshot.** Inserts behind the bookmark are missed;
entries moved ahead of it can be seen again; deleted entries disappear. A
multi-valued index can also yield the same primary key at different index
positions even without concurrent writes. Chunking is suitable for maintenance
and streaming that accept these semantics. Use one read transaction when a
consistent whole-query snapshot is required.

## Explicit continuation and application-owned transactions

Use `Chunk` directly when the application controls reader lifetimes, output
flushes, or work between chunks:

```go
var after edb.ScanBookmark
for {
    more := false
    db.Read(func(tx *edb.Tx) {
        page := CustomersByTag.Exact(tx, "vip").Resume(after).Chunk(500)
        for id, customer := range page.Entries() {
            writeCustomer(id, customer) // Consume borrowed fields here.
        }
        more = page.More()
        after = page.Bookmark()
    })
    flushOutput() // No read transaction is held here.
    if !more {
        break
    }
}
```

`More()` finishes the chunk and probes one further matching position without
fetching its row. It preserves the bookmark of the last **consumed** entry. Do
not iterate the chunk after calling `More()`. Repeated `More()` and `Bookmark()`
then use the cached result. An empty chunk retains its starting bookmark.
`More()` describes the current snapshot, not writes that happen afterward.

`ScanBookmark` owns the exact encoded storage position. For a nonunique index,
that includes **both the index key and primary key**. A last customer ID alone
cannot resume a general index scan. The zero bookmark starts from the beginning;
resumption is exclusive in the original traversal direction. Deleted bookmark
entries are supported. Table/index or direction mismatches panic. Query bounds
remain in force and may be narrowed when resuming.

Bookmarks hold table/index pointers used to validate their scope and have no
serialization format. This makes the current API process-local; the position
itself is owned key bytes, independent of the transaction. Bookmarks are not
durable pagination tokens, transaction snapshots, or authorization boundaries.
Capture a bookmark while its cursor's transaction is open. Copying it afterward
is safe and does not allocate. Creating it copies the position once; ordinary
iteration does not copy positions per row.

Traditional cursors share the same implementation:

```go
cursor := tx.IndexScan(customersByTenant, edb.ExactScan(tenantID).Resume(after))
for count := 0; count < 500 && cursor.Next(); count++ {
    // cursor.Record(), cursor.Row(), or key-only cursor.UintKey()/RawKey().
}
after = cursor.Bookmark() // Capture before closing tx.
```

The [tested manual example](../examples/loyalty/scans_test.go) shows the typed form.
Append `.Reversed()` to the query before resuming a reverse bookmark.

### Logical primary-key bounds versus continuation

`PrimaryKeyRange` restricts primary keys at the logical index range's endpoints.
It does not mean "continue after this physical entry." Use `Resume` for that,
keeping the original query bounds across pages.

The distinction matters for variable-width keys. In an index range from `"b"`
through `"c"`, a lower primary-key bound of `"z"` at `"b"` still admits the row
`(index="ba", primary key="a")`. Its stored payload `"baa"` precedes `"bz"`, so
seeking directly to the complete lower-bound key would miss it. Filtering
unwanted rows after that seek cannot recover the skipped match.

Fixed-width index and primary keys retain direct composite seeks. A range with
equal index endpoints can also seek directly when component boundaries are
unambiguous. Variable primary keys use bounds without the outer index footer.
The upper bound widens only when an embedded tuple-count footer could let a
shorter primary key sort beyond it; logical endpoint checks discard extra
candidates. Ordinary printable string IDs still seek directly in both
directions. Wider variable-index ranges retain the traversal needed for
correctness. No preflight scan or storage-format migration is used.

`Resume` instead seeks after the complete stored entry, then preserves the
query's logical checks. This avoids repeatedly walking earlier pages even for
variable-width ranges. A bookmark continues storage order, which can differ
from decoded index/primary-key tuple order. It also works if the bookmarked
entry was deleted between transactions.

## Filter keys before fetching rows

`scan.Keys()` reads primary keys without fetching or decoding row payloads.
`scan.KeyCursor()` exposes a single-pass stream for set operations:

```go
db.Read(func(tx *edb.Tx) {
    vip := CustomersByTag.Exact(tx, "vip").KeyCursor()
    staff := CustomersByTag.Exact(tx, "staff").KeyCursor()
    subscribed := CustomersByTag.Exact(tx, "newsletter").KeyCursor()

    for id := range vip.Union(staff).Intersect(subscribed).Keys() {
        customer := Customers.Get(tx, id) // First row fetch for this candidate.
        sendNewsletter(customer)
    }
})
```

`Union` and `Intersect` deduplicate results and compare **encoded primary keys**.
They do not materialize sets or decode rejected keys. They use O(number of
inputs) memory and allocate nothing per merged key. Decoding an emitted string,
composite, or custom key can still allocate; unsigned built-in keys do not.
Use `Next()` and `RawKey()` with `GetRaw` to avoid decoding emitted keys too.

Exact index streams with non-unsigned primary keys receive a key-only ordering
check before merging. The current tuple layout can change primary-key order
when variable-length keys contain embedded tuple-footer bytes. The check rejects
such inputs before emitting any set results; use materialized sets for those
key domains. Ordinary string/composite keys work, but merging their exact-index
streams takes an extra pass. Built-in unsigned keys have fixed width and avoid
that pass. Table streams and already-composed streams also avoid it.

Inputs must be from the same table, transaction, and traversal direction, and
ordered by primary key: table scans or exact full-index-key scans. A general
index range or composite-prefix scan is ordered by index key first and is
rejected. Reverse every input to merge in reverse order. Set composition consumes
its inputs; do not reuse or share them between other iterators. A merged stream
has multiple source positions and does not expose a single scan bookmark.

See the [tested union/intersection example](../examples/loyalty/scans_test.go).

## Update keys first when changing an index

Do not save or delete rows while walking an index those writes can change.
Materialize owned keys first, then reload and save through the domain helper:

```go
db.Write(func(tx *edb.Tx) {
    ids := slices.Collect(CustomersByTag.Exact(tx, "vip").Keys())
    for _, id := range ids {
        edit := Customers.Edit(tx, id)
        if edit == nil {
            continue
        }
        edit.List(Customer_Tags).Remove("vip")
        SaveCustomer(tx, edit) // Preserve snapshots and other domain behavior.
    }
})
```

This uses the same write transaction but finishes the cursor before modifying
its index. `SaveCustomer` is your application's save boundary, not an EDB API.
The [runnable example](../examples/loyalty/scans_test.go) uses generic `Customers.Save`
because that example has no additional domain side effects.

For large jobs, collect a bounded chunk of keys, capture its bookmark, close the
reader, then open a writer and reload those keys. Recheck any business predicates
that may have changed. If writes can move entries ahead in the scanned index,
scan by stable primary key instead or stage the complete initial key set; a
bookmark cannot make a changing index into a snapshot.

## Measurements

Local bbolt, Apple M5 Max, Go 1.27.3, 2026-11-10. These are synthetic fixtures,
not the backend's customer database. Each operation includes opening/closing
its reader(s). Medians of three runs, rounded:

| Operation, unsigned primary keys | 1,000 entries | Allocs | 10,000 entries | Allocs |
| --- | ---: | ---: | ---: | ---: |
| Ordinary key-only scan | 22.0 µs | 26 | 215 µs | 26 |
| Union of two identical key streams | 54.6 µs | 51 | 525 µs | 51 |
| Intersection of two identical key streams | 61.9 µs | 51 | 594 µs | 51 |
| Key-only chunks of 1,000 | 23.7 µs | 29 | 238 µs | 308 |
| Record scan plus primary-key join, chunks of 1,000 | 191 µs | 43 | 1.97 ms | 448 |

Union/intersection traverse two source streams; they are not doing the same
amount of index work as a single scan. Allocation counts for those streams stay
fixed as the number of keys grows. Chunk allocations grow with the number of
transactions, not the number of entries within each chunk. Record payloads are
never decoded into native structs in these record/key-only paths.

The existing 1,000-entry joined-scan benchmark measured roughly 190–192 µs and
40 allocations before/after this change; its key-only counterpart remained
about 22 µs and 26 allocations. These sequential runs are a regression check,
not evidence of a small speedup. Cursor/bookmark state adds 48 bytes of total
setup allocation in that benchmark, with no additional allocations per row.

String/composite exact-index intersections include the ordering preflight. Using
`RawKey()` to avoid materializing Go keys, 1,000 entries took about 104–105 µs
and 93 allocations. At 10,000 entries they took about 1.02–1.03 ms: strings had
200 allocations, composites varied from 140 to 392 with page layout.
Allocation profiling identified the additional small allocations in **bbolt
v1.3.7's `page.fastCheck` variadic assertion arguments**, which box larger page
IDs even on successful checks. They are storage-page overhead, not row decoding
or materialized key sets. This change does not modify the pinned storage engine.
Calling typed `Key()`/`Keys()` additionally allocates owned string/composite keys
as described in [the record API measurements](RECORD-API.md#measurements).

Reproduce with:

```sh
go test . -run '^$' -bench 'Benchmark(ResumableScan|VariableKeyMerge)$' -benchmem -count=3
```

[Benchmarks and allocation contracts](../opscan_chunks_bench_test.go) cover both
scan lengths, chunked joins, key-only operation, and set composition. Behavioral
tests cover deleted bookmark positions, narrowed bounds, reversed scans,
duplicate index values, string/composite keys, reader cleanup, and malformed
row payloads that key-only queries must never read.
