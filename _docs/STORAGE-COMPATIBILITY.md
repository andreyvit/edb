# Storage compatibility

## Formats and keys

Ordinary tables default to MessagePack row payloads. Registering record access
or a model does not change persisted storage. Fast2/Compact2 require explicit
storage configuration; converting existing rows is a separate operation. Fast1
snapshots and application-defined KV payloads retain their own formats. See
[record APIs and codec migration](RECORD-API.md#migrating-codec-specific-adapters).

The database primary key is separate from the record payload. Index and migration
callbacks receive it explicitly, and native row reads restore it. If a storage
model declares a primary-key property, `New` and `EditOrCreate` initialize that
property for MessagePack, Fast2 and Compact2. A keyless model stays keyless;
materializing a record does not invent a key property.

Preserve table names, schema versions, persisted field tags, numeric wire values,
key encoding and index declarations when reorganizing Go packages. A Go type move
alone is not a storage migration. Changing an enum writer from text to integers
requires readers for the old representation, including MessagePack binary text
when `MarshalText` produced it. Forward reads and old-binary rollback reads are
separate compatibility requirements. Test enum map keys as well as scalar fields:
multiple wire encodings of one logical key must not produce duplicate entries
when editing or deleting a map entry.

## Edits and custom codecs

Attached MessagePack edits preserve unmodeled fields. A partial projection does
not promise lossless whole-object replacement or conversion to another codec.
Use attached child editors for partial nested updates; protected aggregate
replacement and unsupported inline deletion can be rejected.

Custom MessagePack collections retain their native conversion plan and serialize
through their own codec. This fallback can decode the selected collection without
decoding the enclosing record. Test nested changes, untouched entries, custom map
keys and unmodeled child fields. A custom root codec can legitimately emit an
explicit nil token; decoding must use an addressable destination and distinguish
that token from absent input.

Presence is distinct from zero. Named timestamp types use the same conversion
rules for reads and presence checks. Native and MessagePack JSON conversion
traverse collections directly and preserve nil child entries as JSON null;
present empty containers remain empty. `TryToJSON` returns conversion errors.
Packed storage has its own representability rules and rejects nil collection
entries. JSON support does not imply that a value can be converted to row2.

Application write validation must enforce domain-specific requirements such as
nonnil product variants or coupon elements. Generic record access can represent
a missing child; it cannot infer whether a downstream native consumer requires
that child to exist.

## Indexes, transactions and ownership

Data writes, index generation and index mutations share the transaction. Index
callbacks still run for unchanged rows because their keys can depend on transaction
context. Per-index callback sets are isolated for nested invocations and clear
transaction references before reuse. `IndexOptDeprecatedUnique` retains its
last-write-wins behavior; it does not enforce uniqueness.

Tuple component identity matters for exact and prefix scans. Variable-length
string ranges may need a wider physical scan followed by logical bounds checks;
the tuple format does not make every logical interval contiguous. See
[scan contracts](SCANS.md) for direction, endpoints and continuation semantics.

Records and collection views can borrow transaction storage. Own retained encoded
values or clone for independent native graphs, including unmodeled fields. Copy
at the retention boundary; ordinary reads do not check transaction expiry.
[Ownership and editing](../kvo/_docs/ACCESS.md) describe these distinctions.

Change handlers run before commit and can observe a transaction that later rolls
back. Queue stable keys or owned values rather than borrowed records. Native
notification snapshots are requested explicitly. See
[change notifications](RECORD-API.md#change-notifications).

## Compatibility checks

Run both storage backends after storage or scan changes:

```sh
go test ./...
go test -short ./...
```

Durable regression coverage includes:

- [Mixed writes and index integrity](../record_regression_test.go): native and
  encoded edits, deletes, reindexing, repeated saves and rollback.
- [Stored primary keys](../record_put_test.go): encoded inserts and readback.
- [Scan oracle](../opscan_oracle_test.go): logical bounds and component identity.
- [Retention](../record_retention_test.go): page reuse and database closure.
- [Custom collection edits](../kvo/edit_fallback_test.go),
  [custom nil roots](../kvo/messagepack_root_test.go) and
  [JSON collections](../kvo/json_collections_test.go).

[Cross-version persistence harness](../research/storage-compat/run.py),
[synthetic storage probes](../research/storage-audit-2026-11-12/README.md) and
[application replay probes](../research/safe-deploy-regressions-2026-11-12/README.md)
provide additional controls. Build each version against its own dependency graph.
Compare complete native values and index contents after commit and rollback;
classify raw-byte or revision differences rather than silently ignoring them.

A bounded sample cannot validate complete tenant history or full startup
migrations. For application upgrades, repeat restore, migration, read and write
checks on a disposable recent backup, and verify the release binary's actual
module versions. Local replacements and passing library tests do not establish
which code a release contains. Measure native write CPU, allocation, GC and
commit/fsync costs separately on representative workloads.
