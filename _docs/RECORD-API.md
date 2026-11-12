# Typed rows and records

`DefineTable` returns `*Table[Key]` with record CRUD and scans. `AddIndex` returns
`*Index[IndexKey]`. Obtain native row access from the table:

```go
customers := customersTable
customerRows := customersTable.Rows[Customer]()
for id, customer := range customers.ExactIndexScan(ctx, customersByTenantIndex, tenantID).Entries() {
    // id has the table's primary-key type.
}
```

Table and index handles contain one definition pointer. `AnyTable` and
`AnyIndex` expose only `TableDef()` and `IndexDef()`; low-level operations unwrap
them at entry. Schema lists, caches and identity maps use definition pointers.
`TableDef.Typed[Key]()` validates and binds a dynamically obtained definition.
Records are 16 bytes on 64-bit platforms.

| Operation | Record handle | Native row handle |
| --- | --- | --- |
| Read by primary key | `customers.Get(ctx, id)` | `customerRows.Get(ctx, id)` |
| Explicit error and metadata | `customers.TryGet(ctx, id)` | `customerRows.TryGet(ctx, id)` |
| Read encoded primary key | `customers.GetRaw(ctx, raw)` | `customerRows.GetRaw(ctx, raw)` |
| Replace whole value | `customers.Put(ctx, id, r)` | `customerRows.Put(ctx, row)` |
| Delete | `customers.Delete(ctx, id)` | `customerRows.Delete(ctx, id)` |
| Iterate values | `customers.Scan(ctx).Records()` | `customerRows.Scan(ctx).Rows()` |
| Iterate key and value | `customers.Scan(ctx).Entries()` | `customerRows.Scan(ctx).Entries()` |
| Iterate primary keys only | `customers.Scan(ctx).Keys()` | `customerRows.Scan(ctx).Keys()` |

`ctx` can be a transaction or anything implementing `DBTx() *edb.Tx`. Record
reads return a missing record when absent; native reads return nil. Ordinary
operations panic on errors. `TryGet` and `TryLookup` also return `ValueMeta`.

`customers.Lookup(ctx, index, indexKey)` returns `(primaryKey, record)`;
`LookupKey` returns `(primaryKey, found)`. **Nonunique lookup means the first
match in storage order.** Missing lookup returns the zero key and a missing
record/nil native row. Native row handles provide the same operations.

Primary-key scans provide `Scan`, `Exact`, inclusive `Range`, `From` (>=),
`After` (>), `Through` (<=), `Before` (<), and `Prefix`. Index scans use
`IndexScan`, `ExactIndexScan`, `RangeIndexScan`, `IndexScanFrom`,
`IndexScanAfter`, `IndexScanThrough`, `IndexScanBefore`, and `PrefixIndexScan`,
with the index as the second argument. Each checks table ownership.

Append `.Reversed()` for descending order. Prefixes match tuple components,
not text prefixes. Zero or a count at least as large as the key's arity uses
the whole key. `Raw()` opens a low-level cursor.

Each scan iterator opens a fresh cursor, including when reusing an iterator
after an early break. Keep the scan and its iterators inside their transaction.
Records borrow storage; use `Own()` before retaining encoded records beyond the
transaction. Decoded keys are independent of the borrowed record. A record never
implicitly stores the database primary key: retain the argument to `Get`, the
key from `Lookup`, or the key from `Entries`.

For bounded iteration, both scan forms support `.Resume(bookmark).Chunk(limit)`.
`edb.ReadChunks(db, limit, query)` manages a fresh reader per chunk, and each
chunk implements `Txish` for same-snapshot joins. `scan.KeyCursor()` also supports
`.Union(...)` and `.Intersect(...)` before row fetching. See [resumable scans and
key-only queries](SCANS.md) for complete examples, snapshot semantics, and the
keys-first update pattern for writes that change an index.

`customers.Edit(ctx, id)` opens a private edit (nil if missing), `New` prepares
an insert, and `EditOrCreate` does either. `Save` checks the table, transaction,
and intervening writes, then returns whether persisted data changed. No revision
metadata is needed for ordinary business code. `customers.Key(edit)` returns
the correctly typed primary key even when the model does not store it.

```go
db.Write(func(tx *edb.Tx) {
    edit := customers.Edit(tx, customerID)
    if edit == nil { return }
    edit.Set(Customer_Email, "alice@example.com")
    edit.Map(Integration_AppIDs).Set(integrationID, "external-123")
    edit.List(Customer_Tags).AddUnique("vip")
    changed := customers.Save(tx, edit)
    _ = changed
})
```

Keep application save helpers around this operation when they also maintain
snapshots, loyalty data, or other domain behavior. Generic `Save` only performs
the database write and its configured indexes/hooks.

Whole-record `Put` replaces unconditionally, like native Put. For optimistic
replacement across transaction boundaries, retain metadata from `TryGet` and
call `PutChecked(ctx, id, replacement, expected)`. Zero metadata means insert.
`TryPutChecked` returns updated metadata and an error. Low-level transaction
equivalents are `PutRecordChecked` and `TryPutRecordChecked`.

MessagePack, Fast2 and Compact2 batch property/collection edits until save. An
`edit.Record()` snapshot serializes pending edits at that point. Native editors
write through to their private decoded row. See [collection editing](../kvo/_docs/ACCESS.md#dictionaries-and-attached-editors).

See the [runnable loyalty application](../examples/loyalty/model.go) and the
[tested customer/daily-statistics join](../examples/loyalty/joins_test.go), which
combines a string index, unsigned customer IDs, and a composite statistics key.

## Migrating codec-specific adapters

The public record path works with MessagePack, Fast2, and Compact2:

| Removed API | Replacement |
| --- | --- |
| `GetKVO` / `TryGetKVO` | Typed `Get` / `TryGet`, or `Tx.GetRecord` / `Tx.TryGetRecord` |
| Cursor `KVO` / `TryKVO` | Cursor `Record` / `TryRecord`, or typed scan iteration |
| `EditKVO` / `TryEditKVO` | Typed `Edit` and `Save`, or universal callback `Tx.EditRecord` / `Tx.TryEditRecord` |
| `PutKVO` / `TryPutKVO` | Typed `Put`, or revision-checked `Tx.PutRecordChecked` / `Tx.TryPutRecordChecked` |
| `KVOModel` | `Model` |
| `UpgradeKVO` / `TryUpgradeKVO` | `UpgradeRecord` / `TryUpgradeRecord` |
| `TableBuilder.Indexer` / `TableBuilder.KVIndexer` / `RecordStorage.Index` | `TableBuilder.SetIndexer(func(Key, kvo.Record, *IndexBuilder))` |

Choose one indexer: native `Indexer` or universal `SetIndexer`. The latter
receives the database primary key separately and can read MessagePack directly;
there is no packed-view callback or temporary encoding solely for indexing.
`UpgradeRecord` explicitly converts an existing MessagePack row to the table's
configured Fast2/Compact2 storage; ordinary reads do not convert storage.

Fast1's legacy `KVTable`, `KVIndexer`, and analytics APIs are unchanged.

## Measurements

Local bbolt, Apple M5 Max, Go 1.27.3, 2026-11-10. These are small synthetic
fixtures, not the backend's customer database. Run:

```sh
go test . -run '^$' -bench 'BenchmarkTyped(Keys|JoinedScan)$' -benchmem -benchtime=200ms
```

`BenchmarkTypedJoinedScan` opens/closes a read transaction, scans 1,000 entries
of a nonunique string index, reads each row, and joins by primary key. The second
lookup uses the same table to isolate API costs. All three access variants
verify the same names and row count. Times below are rounded single-run results:

| Storage | Native rows | Universal record cursor | Typed record entries |
| --- | ---: | ---: | ---: |
| MessagePack | 725 µs; 17,008 allocs | 234 µs; 39 allocs | 239 µs; 40 allocs |
| Fast2 | 535 µs; 15,008 allocs | 175 µs; 39 allocs | 189 µs; 40 allocs |
| Compact2 | 564 µs; 15,008 allocs | 181 µs; 39 allocs | 196 µs; 40 allocs |

The cursor baseline joins using the already encoded primary key. Typed entries
decode/re-encode the unsigned key, adding roughly 14 ns per row for Fast2 and
Compact2 in this run. Both record paths allocate **zero per row**. The total
includes transaction/cursor setup; typed records use 37 allocations for one
row and 40 for 1,000, with the small difference coming from storage/cursor setup.
Typed index `Keys()` alone takes about 22 µs and 26 allocations for 1,000 keys.

Key-only table scans and warmed individual `Get` calls isolate key handling:

| Key type | Record Get | Scan 1,000 primary keys |
| --- | ---: | ---: |
| `uint64` (also named unsigned IDs) | 64 ns; 0 B; 0 allocs | 9.5 µs; 1,040 B; 15 allocs |
| String | 65 ns; 0 B; 0 allocs | 20.9 µs; 17,040 B; 1,015 allocs |
| Composite `{int, int}` | 95 ns; 16 B; 1 alloc | 40.5 µs; 17,040 B; 1,015 allocs |
| Custom binary-coded unsigned key | 85 ns; 16 B; 2 allocs | 25.9 µs; 9,040 B; 1,015 allocs |

String iteration allocates an owned string per key, so keys can outlive the
transaction. Composite and custom keys use the shared reflective flat
codec; their encoding/decoding can allocate. This is key handling, not payload
decoding. `Records()` avoids decoding unused primary keys; `GetRaw` and low-level
`RawKey()` avoid a decode/re-encode round trip for joins with identical key
encodings. Custom codecs and schema migrations may also require decoded rows.
Zero-allocation tests cover warmed unsigned and string Get across all three
storage formats.
## Change notifications

`Tx.OnChange` chooses payloads per table:

```go
tx.OnChange(map[*edb.TableDef]edb.ChangeFlags{
    customersTable.TableDef(): edb.ChangeFlagNotify | edb.ChangeFlagIncludeKey |
        edb.ChangeFlagIncludeOldRecord | edb.ChangeFlagIncludeNewRecord,
}, func(tx *edb.Tx, change *edb.Change) {
    id := change.Key().(CustomerID)
    if change.HasOldRecord() {
        removeCachedEmail(id, change.OldRecord().Get(Customer_Email))
    }
    if change.HasNewRecord() {
        cacheEmail(id, change.NewRecord().Get(Customer_Email))
    }
})
```

| Mutation | Old value | New value |
| --- | --- | --- |
| Insert | Missing | Inserted |
| Update | Previous | Saved |
| Delete | Deleted | Missing |

Only requested payloads are populated. Record flags require a KVO model.
MessagePack, Fast2 and Compact2 use the ordinary encoded read path; migrations
or unsupported native codecs may require decoding. Records borrow transaction
data; call `Own()` before retaining them beyond the transaction. `RawKey()` is
owned. Request `IncludeKey` explicitly for the decoded `Key()`.

`ChangeFlagIncludeOldGoStruct` / `ChangeFlagIncludeNewGoStruct` request independent
`*T` snapshots through `OldGoStruct()` / `NewGoStruct()`. These contain the primary
key and all persisted fields, including fields outside a KVO projection. They
never alias the struct passed to `Put`. Mutating a snapshot does not save it.
Both record and Go struct payloads may be requested together.

Handlers run after data/index mutation, before commit; the transaction may
still roll back. No-op saves and absent deletes do not notify. Reindexing does.
Unreadable old values are omitted and decoding errors logged when possible, so
repairing/deleting damaged data remains possible. New-value decode errors panic.

Notification allocation check (Go 1.27.3, November 11, 2026): a string primary key,
128-byte name and 100 string tags; already encoded old/new payloads. This isolates
the notification boundary, excluding storage writes and index construction.

| Requested payload | MessagePack | Fast2 | Compact2 |
| --- | ---: | ---: | ---: |
| Key only | 4 allocs, 176 B | 4 allocs, 176 B | 4 allocs, 176 B |
| Key + old/new records | 4 allocs, 176 B | 4 allocs, 176 B | 4 allocs, 176 B |
| Key + old/new Go structs | 216 allocs, ~7.4 KB | 214 allocs, ~7.4 KB | 214 allocs, ~7.4 KB |

Record payloads add no allocations above the event and owned-key overhead in this
fixture. Reproduce with `go test . -run '^$' -bench '^BenchmarkChangeNotifications$' -benchmem`.
