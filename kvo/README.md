# KVO models, records and storage

`kvo` defines logical models, typed properties, record views, editors, and Go
bindings. A model can access encoded MessagePack/Fast2/Compact2 rows or native
Go structs through the same API. Direct access and the `Fastest` decode-once
hint are independent of storage format; see the [access guide](_docs/ACCESS.md).

Declare typed properties through `kvo.NewUnsignedProp`, `NewSignedProp`,
`NewStringProp`, `NewTimeProp`, `NewListProp`, and the corresponding map/record
constructors. Model builders use one generic `b.Prop(property, options...)`
method accepting typed references and schema declarations. Bare numeric codes
do not compile; dynamic schemas resolve them with `schema.MustPropByCode(code)`.

```go
var Customer_Points = kvo.NewSignedProp[int64](schema, 18, "customer:points", kvo.TInt64)

// During model registration:
b.Prop(Customer_Points)

// During access/editing:
balance := customer.Get(Customer_Points) // int64
edit.Set(Customer_Points, balance + 10)
```

Use `kvo.Model`, `Record`, `Editor`, `List[T]` and `Map[K,V]`. `MapOf` and `ListOf`
declare logical collection types. `NewScalarProp` accepts a custom
`values.Codec[T]`; `NewProp` declares an untyped schema property.

Reuse canonical numeric types (`TInt64`, `TUint32`, `TFloat64`, etc.) for ordinary
Go fields. Use named scalar types for domain semantics. `TUintptr` supports native
and Fast2/Compact2 access; Go MessagePack does not encode `uintptr` fields.
`NewTimeProp` preserves nanosecond timestamps; `NewLegacyTimeProp` preserves the
older offset-microsecond encoding.

Use `.Code()` in raw codec/dynamic analytics loops. Resolve typed references at
setup with `CodeOf` where an API needs a number.

## Packages

| Package | Responsibility |
| --- | --- |
| `kvo` | Models, typed properties, record and collection views, editors, Go bindings and their generator |
| `values` | Shared cells, types, shapes and scalar conversions |
| `nativebind` | Shared reflected and generated Go field access |
| `kvo/codec/fast1` | Original word-map encoding, borrowed views and mutable edits |
| `kvo/codec/msgpack` | Borrowed MessagePack views, copy-on-write edits, pooled struct encoder/decoder |
| `kvo/codec/row2` | Shared Fast2/Compact2 wire format, views, writers and edits |
| `edb` | Tables, transactions, row envelopes, indexes and storage migration |

**MessagePack and Fast1 have the same status: supported legacy formats that we
intend to transition away from.** Fast2 and Compact2 are the current formats.
There is no automatic migration. Existing `KVTable` data remains Fast1, and
ordinary Go tables remain MessagePack unless explicitly configured otherwise.

Fast2 and Compact2 belong in one package: they share scalar semantics and
container framing, and a Fast2 row can contain Compact2 children. Layout hints
such as `kvo.Compact` remain supported schema metadata; they describe storage,
not different logical property types. Borrowed views remain transaction-scoped.

The low-level row2 writer uses its `Fast2` and `Compact2` layout constants.
Pass the model after the name in `edb.DefineTable`. Inside its callback, select a named `edb.StorageFormat`:

```go
b.SetRecordStorage(edb.RecordStorage{
    Format: edb.Fast2, // or edb.Compact2
})
```

`RecordStorage` requires Fast2 or Compact2 explicitly. For property access on
existing MessagePack rows, pass the model and omit `SetRecordStorage`. Set
`MessagePackInserts: true` on `RecordStorage` during a reader-first rollout.
Existing MessagePack keys remain MessagePack until `UpgradeRecord` converts them;
ordinary Put does not silently migrate those keys. All readers must support the
new format before enabling its writes. The format names do not change existing
wire tags, row-envelope versions, bucket names or memento bytes.

## Source migration

| Previous API | Current API |
| --- | --- |
| `kvo.ImmutableRecord`, `MutableRecord`, `ImmutableMap`, `MutableMap`, `Packable` | Same names under `fast1` |
| `kvo.LoadRecord`, `LoadRecordData`, `NewRecord`, `UpdateRecord` | Same names under `fast1` |
| `kvo/kvrow` (`kvrow`) | `kvo/codec/row2` (`row2`) |
| `kvo/kvbind` (`kvbind`), `kvo/record` (`record`), `kvo/model` (`model`) | `kvo` |
| `kvo.Map(keyType, valueType)` | `kvo.MapOf(keyType, valueType)` |
| `record.NewProp[T](...)` | `kvo.NewScalarProp[T](...)` |
| `KVOAccess`, `SetKVOAccess`, `RecordAccess`, `SetRecordAccess` | Model argument to `DefineTable(schema, name, model, configure)` |
| `KVOStorage`, `SetKVO` | `RecordStorage`, `SetRecordStorage` |
| `KVOStorage{Fast: true}` | `RecordStorage{Format: edb.Fast2}` |
| `KVOStorage{Fast: false}` or omitted `Fast` | `RecordStorage{Format: edb.Compact2}` |

Regenerate Go bindings after updating imports. The old `kvo/model` and
`kvo/record` paths are removed, including their aliases and forwarding functions.
The source move leaves storage bytes, record sizes, and the Fast1 `KVTable` API
unchanged.

## Research history

`research/kvo/draft` is a historical candidate, not the current codec. The
selected research design was promoted into `kvo/codec/row2`; the independent
research writer still checks its bytes. Historical measurements and draft
names remain in research reports. They do not describe current API status.
