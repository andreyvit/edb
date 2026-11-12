# Storage and access are separate choices

`Record` provides `Get`, `Has`, `Child`, `ToJSON`, `Snapshot`, and `Edit` over
MessagePack, Fast2, Compact2, a Go struct, or decoded memory. Choosing an access strategy does
not change the stored format. EDB attaches the table's binding automatically.

Operations panic on error by default. Use `TrySet`, `TrySetValue`, `TrySetAs`, `TryToJSON`,
`TryEncode`, `TryDecode`, `TryDecodeProperty`, `TryValidate`, and `TryGenerate`
when errors need explicit handling. Codec packages follow the same convention:
`Open` returns a view, while `TryOpen` returns a view and error. Database
`Record`/`GetRecord` return only the record; their `Try...` forms also return
`ValueMeta` and an error.

Build bindings during setup. `Bind(goType, model)` and
`Project(goType, model, nil)` cache default plans; full bindings and projections
remain distinct. Custom field maps and `WithGenerated`/`WithNativeSchema`
configurations create separate plans: construct once and reuse.

Representation IDs and metadata are never reclaimed. Per-row or per-request
binding construction leaks permanent metadata. Reads use flat descriptor tables.

## Typed properties (Go 1.27+)

Declare scalar references with `NewSignedProp[T]`, `NewUnsignedProp[T]`,
`NewStringProp[T]`, `NewBoolProp[T]`, `NewFloat32Prop[T]`, `NewFloat64Prop[T]`, or
`NewBytesProp[T]`. `Bind…Prop` attaches access to a property already declared in
KVO. Registration validates the Go type; reads and writes use direct conversions
without reflection. Named integer/string types retain their declared Go type.

```go
points := kvo.NewSignedProp[int64](schema, 1, "customer:points", kvo.TInt64)
tags := kvo.NewListProp(schema, 2, "customer:tags",
    kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())

n := r.Get(points) // int64, not values.Value
n = points.Get(r) // same result; fastest scalar syntax
edit.Set(points, n + 1)
tags.SetSlice(edit, []string{"vip"})
for tag := range r.Get(tags).Iter() { _ = tag }
list := r.Get(tags) // kvo.List[string], exactly 16 bytes
_ = list.Len()
copyOfElements := list.Slice()
_ = copyOfElements
```

Scalar refs contain only an eight-byte code. `PropRef[T]` is the alternative
shared descriptor for custom conversion and object views; `NewRecordProp` returns
`Record` without decoding, and `NewRecordListProp` returns `RecordList` whose
items are ordinary `Record` values. Object list access does not require a native
Go type parameter. `NewScalarProp`/`BindProp` accept a `values.Codec[T]` for custom scalar
conversion. Only conversion is reflection-free; native access still follows the
attached generated or reflective host binding.

`List[T]` and `RecordList` each contain one tagged `values.Value`: a GC-visible
pointer, representation ID, and 32-bit byte length (native slices point to their
existing header). They do not allocate a per-value box. Shared representation
metadata is registered once per property/access layout on first use. Warm access
and immediate `range` allocate nothing for ordinary scalar and object elements.

`Cursor()` creates stack traversal state; `Next()` advances it and `Len()` reports
remaining elements. `Iter()` returns a single-pass range-over-function sequence:
breaking stops immediately, and reusing that sequence resumes it. Call `Iter()`
again for an independent traversal. `Slice()` allocates a result slice; strings,
bytes and child records still borrow the original storage. Retaining an iterator
closure can allocate like other escaping Go closures. The source transaction must
outlive all borrowed views, elements and iterators.

Use `list.OwnedSlice()` to copy strings and byte spans as well as the slice.
`RecordList.OwnedSlice()` applies `Own()` to each record; native objects retain
their identity. Use `RecordList.CloneSlice()` for independent records. The same
methods are available on cursors and copy their remaining elements without
advancing the original cursor.

Use `tags.SetSlice(edit, items)` or `edit.Set(tags, tags.FromSlice(items))` for
list assignments; encoding/editing allocates. Generic `Editor.Get/Set/TrySet`
operate on a direct property. Untyped `GetValue(code)` and `SetValue(code, value)`
also address one property; `SetValue` derives compact/zero-preservation policy
from the model. `GetPathValue(path)` reads a nested path.

`FastSetPathValue(path, value)` accepts `[]row2.Step` with precomputed options.
EVM computes them at link time to avoid model-policy lookups on every write.
Use `kvo.WriteOptions(model.PropFlags(code))` when preparing a step; options
are used as supplied, without merging model defaults. `TryFastSetPathValue`
returns errors, and the `WithWriter` variants accept reusable encoding scratch.
Ordinary edits should use typed `Set`, untyped `SetValue`, or attached child
editors. `Delete` explicitly removes a field even when the model preserves zero.
`ValuePropRef` remains available for
raw encoded aggregate access; prefer typed dictionaries for ordinary code.

## Dictionaries and attached editors

```go
appIDs := kvo.NewMapProp(schema, 3, "integration:app_ids",
    kvo.MapOf(kvo.TUint32, kvo.TMachineString),
    values.UnsignedCodec[IntegrationID](), values.StringCodec[string]())

ids := customer.Get(appIDs) // kvo.Map[IntegrationID, string], 16 bytes
externalID, found := ids.Lookup(integrationID)
for integration, externalID := range ids.Iter() { _, _ = integration, externalID }
_ = ids.Len()
_, _ = externalID, found

edit := customer.Edit()
edit.Map(appIDs).Set(integrationID, "external-123")
edit.Map(appIDs).Delete(oldIntegrationID)
edit.List(tags).AddUnique("vip")
edit.List(tags).Remove("inactive")
```

`Get(key)` returns zero when absent; `Lookup` distinguishes absence from a
present zero. `Keys()` traverses without decoding values. Map reads borrow the
original bytes without converting MessagePack to another encoding. Native
scalar maps and encoded scalar/record maps support allocation-free warmed
lookup and iteration. Native maps containing struct values must copy a selected
entry into addressable storage; that case can allocate. Custom object codecs
can also require decoding. Retain the owning record with `Own()` before keeping
a map beyond the source transaction.

`NewRecordMapProp` returns `Map[K, Record]`. `NewNestedMapCodec` combines reusable
map codecs; its `.Prop(schema, code, name)` declares a nested map property.
`NewJSONMapProp` returns `Map[K, any]`, decoding only the requested dynamic value;
object/array materialization allocates. `Len` and `Keys` do not decode those
values. There is no JSON serialization round trip for MessagePack lookup.

Typed map declarations preserve zero entries by default, including in nested
maps. `Set(key, zero)` retains an entry; `Delete(key)` removes it. On a JSON
dictionary, `SetNull(key)` stores a present null, returned as `(nil, true)` by
`Lookup`. `Clear()` keeps an empty collection. `edit.Delete(prop)` removes an
encoded property or resets a native field; native zero/absence distinctions
follow the model and its Go field type. `Missing()` distinguishes a missing/nil
collection from a present empty one. Native `omitempty` serialization can omit
empty collections; direct encoded Clear retains them.

```go
profile := edit.EditOrCreate(profileProp) // Edit returns nil if absent; New requires absence
profile.Set(displayNameProp, "Alice")

subscription := edit.Map(subscriptionsProp).Edit(subscriptionID)
if subscription != nil { subscription.Set(enabledProp, true) }

venue := edit.List(venuesProp).Edit(0)
venue.Set(enabledProp, true)
fresh := edit.List(venuesProp).New() // appends and returns an attached record editor
fresh.Set(enabledProp, true)
```

Map entry `Edit`, `New`, and `EditOrCreate` follow the same explicit creation
rules as record children. Nested dictionaries use `kvo.EditMap(outer, key)`,
`NewMap`, or `EditOrCreateMap`; these free functions infer both nested key types.
Changes stay attached through snapshots and saves. Replacement/deletion
invalidates old child editors. Structural record-list changes (append/remove/
clear) invalidate element editors because native slice storage can move.

Edits coalesce before serialization in MessagePack, Fast2, and Compact2. Typed
map/list editor reads see pending changes without rebuilding the enclosing row.
`edit.Get(collectionProp)` produces a read snapshot of that collection; use the
collection editor's lookup/iteration methods in a mutation loop. `Snapshot`,
`MessagePack`, and `Record` cache immutable encoded output until another change.
Native editors write through; native/generic-memory collection mutation can
allocate. The older `EditEncoded` wrapper deliberately writes through to its
caller-owned codec editor; use `Record.Edit()` for ordinary batched editing.
See the [batching measurements](BENCH-RECORD-ACCESS.md#batched-writes-and-borrowed-dictionaries).

[Typed access measurements and representation tradeoffs](BENCH-TYPED-ACCESS.md).

## Read a few properties directly

```go
r := tx.GetRecord(accounts, key)
if r.Missing() { return nil }
name := r.GetOwnedString(nameCode).AsString()
balance := r.Child(row2.Step{Key: profileCode}).GetValue(balanceCode).AsUint()
_ = name
_ = balance
```

The same code works with `cursor.Record()` in table and index scans, including
scans containing both MessagePack and row2 rows. Pass the model after the name in `edb.DefineTable`. MessagePack is the default;
`b.SetRecordStorage` selects Fast2/Compact2. The model and its registered native
mapping determine which properties are exposed.

MessagePack access follows encoded map entries using the Go fields' `msgpack`
names. `Child` borrows the child's encoded bytes. Reading ordinary scalar
properties does not construct a Go row, map, slice, or string. MessagePack must
scan field boundaries, including nested values it skips; Fast2/Compact2 have
more efficient field addressing. None of these strategies performs a database
format conversion.

`GetValue` returns the shared `values.Value` runtime carrier. Integers are normalized,
float32 uses float32 bits, and strings use the same pointer and length as EVM.
`GetValue(code).AsString()` borrows the record's lifetime. Use
`GetOwnedString(code).AsString()` when retaining a string beyond the transaction;
it copies encoded strings and directly returns native/owned-memory strings.

`GetValue` on a whole object, map or list returns a byte span containing a row2
container (Fast2 or Compact2, as identified by the container's trailer).
For MessagePack it transcodes only that aggregate, without first building its
Go containers. For a native record it materializes that aggregate from the Go
value. Use `Child` to navigate objects when only a few descendant properties
are needed. Repeated collection `GetValue` calls can repeat this work.

## Borrow lists and inline fields

Use `ListValue` for scalar/string list access without constructing a Go or guest slice:

```go
it := customer.ListValue(tagsKey).Iter()
for {
    tag, ok := it.Next()
    if !ok { break }
    if tag.AsString() == wanted { /* match */ }
}
```

`ListValue.Len`, `ListValue.At`, and `ListValue.Iter` work across MessagePack, Fast2,
Compact2, native structs and decoded memory. Use the iterator for sequential
access: MessagePack and Compact2 indexed reads may scan earlier elements.
Borrowed strings share the source lifetime. To retain a list beyond its source
transaction, own the record first. Aggregate/custom-codec elements can still
materialize; this API does not promise zero allocations for arbitrary decoders.

Use `kvo:",inline"` on a value-struct field to expose its properties directly on
the containing KVO model. Include the same declarations with
`b.Inline(childModel)`; codes, local/qualified names, and storage policies are
preserved. There is no container property:

```go
type CustomerState struct {
    CustomerStatValues `kvo:",inline" msgpack:",inline"`
}
// In the CustomerState model builder:
b.Inline(customerStatValuesModel)
// Access the promoted property directly:
points := state.Get(PointsEarned)
```

Field paths and native offsets are resolved at registration. Generated bindings
include the embedded structs' accessors, so decoded reads/writes still use the
shared native binding implementation. MessagePack reads stay borrowed when the
embedding also shares its enclosing wire map. Pointer inlining is rejected;
value fields stored under a separate MessagePack key use the existing decoder
fallback. Duplicate property codes/names panic during model registration.

Without the KVO inline tag, ordinary value-struct embedding, including
`msgpack:",inline"`, keeps a logical KVO child that borrows the enclosing map;
reads and edits resolve its flattened field names. Untouched parent fields are
preserved. Pointer embeddings, conflicting names, and unusual payloads using
nested embedding aliases retain the standard decoder's semantics via fallback.

For custom scalar codecs, register `kvo.RegisterMessagePackScalar` during
initialization, before binding the Go type. Its direct decoder must match every
supported wire form, including historical forms and nil. The callback is cached
in the access plan; reads perform no registry lookup. Edits still use the Go
encoder. Unregistered custom codecs retain the ordinary decoder fallback.

EVM specializes local read-only scalar/string lists used through `range`,
indexing, `len`, or `cap`. It uses the same 16-byte `ListCell` as `List[T]` and
`RecordList`, with one reusable iterator workspace per indexing instruction,
allocated with the VM. A list expression resolves its property once; range traversal is
linear and allocates nothing per row. The compiler requires all consumers to be
known and rejects mutation, escaping slices/strings, and calls or writes while
a borrowed list/element is live. Those cases keep materialized slice semantics.
This optimization does not remove ownership copies required at scope boundaries.

Guest syntax stays `for _, tag := range customer.tags`; no `.Iter()` is needed.
Adjacent single-consumer `len(customer.tags)` and index reads can be fused into
one instruction. Other local uses transport the list's own cell through R/P;
it carries the data span and shared representation metadata. The VM validates
the declared element type when accepting a new cell. Go still uses `.Iter()`
because its native range syntax cannot iterate an arbitrary struct.

[Paired EVM/Go collection benchmarks](../../evm/_docs/LIST-CELLS.md).


Index scans reuse their tuple and data-lookup cursors. Advancing and accessing
records can be allocation-free once those cursors are initialized. Opening a
fresh database transaction and constructing its cursors still has a fixed cost;
compilation, metadata registration and VM construction are separate startup work.
For joins, pass the cursor's `RawKey()` to `GetRecordByKeyRaw`. Hosts that already
keep typed key storage can use `GetRecordByKeyVal` / `TryGetRecordByKeyVal` with a
reflected reference to it, avoiding boxing a new key into `any` for every lookup.

## Keep Fast2; accelerate other formats

```go
hot := r.Fast() // keep an existing Fast2 view, otherwise use Fastest
```

`Fast` preserves Fast2 without decoding or allocation, including attached model
and binding metadata. Compact2 and MessagePack materialize just as with
`Fastest`. Already decoded and missing records remain unchanged. A retained
Fast2 view still borrows bytes: `Fast` does not guarantee ownership beyond a
transaction. Use `Fastest` when you explicitly need an owned decoded value.

## Decode once when you will reuse many fields

```go
hot := r.Fastest() // Go struct if bound; indexed decoded memory otherwise
for range passes {
    balance := hot.Child(row2.Step{Key: profileCode}).GetValue(balanceCode).AsUint()
    // Read more properties through the same Record API.
    _ = balance
}
```

Keep the returned record: repeatedly calling
`r.Fastest()` on the original encoded record repeats the decode. Calling it on
an already decoded record is a no-op. A bound missing child remains missing.
No binding is required. A missing record remains missing.

This is a concrete `Record` value with internal dispatch, so callers do not
need a new interface or type assertion. Full decoding uses the registered Go
type and existing MessagePack decoder or row2 binding. Generated row2 bindings
are used when attached. It includes unexposed MessagePack fields: the hint pays
for a complete Go row, even if the access model exposes only a projection.

For standalone use, attach the binding when opening bytes:

```go
b := kvo.Bind(reflect.TypeFor[Account](), accountModel)
a := b.MessagePack(messagePackBytes)
z := b.Encoded(row2.Trusted(row2Bytes))
_ = a.Fastest()
_ = z.Fastest()
```

A partial projection uses `kvo.Project(...).MessagePack(bytes)`. For a model
without a Go struct, use `kvo.Encoded(view).WithModel(model).Fastest()`.
This owns the bytes, normalizes scalar words, and eagerly indexes nested model
objects. A plain `kvo.Encoded(view).Fastest()` also works: it owns a copy
of the encoded bytes. Without a model, use `GetAs(code, row2.Signed)` (or
`Word`, `Float32`, `Data`) to interpret a field explicitly. `Child` still
navigates lazily; attach a model for eager nested indexing. Collections retain
their encoded values because `Get` exposes collection bytes.

EDB's `GetRecord` and cursor `Record` attach native bindings for every supported
storage format. `WithModel` attaches logical metadata, not a Go binding.

## Share Go fields and methods with EVM

`nativebind` owns the shared field operations. `evmbind` and the record codec
compiler both emit typed Go getters/setters using its generator. Without
compiled bindings, both use the same reflected implementation. Model property
codes map to these operations once during registration.

```go
binding := kvo.Bind(reflect.TypeFor[Account](), accountModel).
    WithGenerated(&AccountBindings, nativebind.StaticOnly)
// When this type also has an evmbind schema:
binding = binding.WithNativeSchema(hostSchema)
hot := binding.Encoded(view).Fastest()
account := hot.Native().(*Account) // original decoded object; no extra copy
vmArg, err := hostSchema.NativePointer(account)
_ = vmArg
_ = err
```

For tables, call `b.SetNativeSchema(hostSchema)` in the definition callback. `GetRecord` and cursor records then
retain that binding automatically. Traditional Go row APIs still populate the Go primary key. Decoding an encoded
record with `Fastest` cannot invent a key omitted from its stored payload; retain
the lookup ID or obtain it separately from the cursor.

`Projection.WithNativeSchema` works with selected fields and custom property
mappings too. `hot.NativeType()` is the same `values.Type` exposed by that
schema. EVM fields and generated native methods act on the same Go object;
changes are visible through KVO. Methods continue to use evmbind's existing
call wrappers. KVO does not introduce a second method invocation engine.
Go callers can invoke methods directly on `Native()`'s typed pointer.

Host-origin data pointers preserve identity across EVM field addresses, aliases,
and native calls. Structs passed by value remain copies. Guest allocations of registered structs/scalars also use native storage.
Other guest pointers backed by VM cells retain call-scoped temporary/copyback semantics. evmbind's existing
host field-type restrictions still apply; registering a persistent Go binding
does not automatically expose every field/type to guest scripts.

Regenerate record and evmbind bindings after this change. The ABI fingerprints
changed; automatic mode falls back for stale code, while `StaticOnly` rejects it.
Generated field callbacks must be attached, not just generated codec functions.

## Edit without decoding the whole MessagePack row

```go
// Inside a write transaction:
edit := tx.OpenRecordEdit(accounts, key)
if edit == nil { return nil }
edit.Editor().Edit(profileProp).Set(balanceProp, uint64(900))
tx.SaveRecord(edit)
return nil
```

MessagePack edits copy and rewrite encoded maps along the changed path. They
encode the replacement value and preserve every untouched field's bytes,
including fields absent from the access model or Go type and unknown extension
values. Input bytes and earlier snapshots remain unchanged. Edits are not
in-place writes to Bolt memory. Replacing an entire aggregate with unexposed
Go fields is rejected; edit its individual properties instead.

`SaveRecord` keeps the row's format, checks for intervening changes, updates
indexes and revision metadata, and runs the normal notification path. A
property-based `SetIndexer` and record notifications read the edited
MessagePack directly. Notifications requesting Go structs decode at the
notification boundary; they do not force decoding for indexing.
`NewRecordEdit` creates new MessagePack rows through a native Go value because
there is no existing payload to preserve.

For standalone edits, use `e := a.Edit()` and `e.MessagePack()` to obtain the
edited MessagePack bytes. `Snapshot()` returns the exposed properties in row2
format, not a lossless copy of the MessagePack payload. Persisted edits belong
to `RecordEdit`; `r.Fastest().Edit()` alone does not save an EDB row.

## Lifetimes and compatibility

Choose the boundary operation explicitly:

| Need | Operation |
| --- | --- |
| Read the latest state in a later transaction | Keep a primary key or `edb.Ref`, then reload |
| Retain encoded data after the transaction closes | `r.Own()` |
| Modify an independent snapshot for a hypothetical calculation | `r.Clone()` |

```go
saved := customer.Own()         // retains encoded bytes; native identity may be shared
alternate := customer.Clone()  // independent, even if customer is already native
edit := alternate.Edit()
edit.Set(pointsRedeemed, edit.Get(pointsRedeemed) + proposedRedemption)
balance := edit.Get(pointsEarned) - edit.Get(pointsRedeemed)
// This has not changed customer or saved, and has not saved anything to EDB.
```

`Clone()` preserves the encoded format, native binding and unknown fields of
encoded records. Native clones keep their Go type and copy nested pointers,
maps, interfaces, slices (including capacity), strings, and fields outside the
KVO model. Repeated pointers/maps and identical slice views remain shared within
the clone; distinct overlapping slice views are copied separately. Immutable
`time.Time` location metadata is retained. Non-nil functions, channels and unsafe
pointers cannot be independently copied and cause a panic. Native cloning uses
reflection only at this explicit boundary; generated/reflective reads are unchanged.

`Own()` is idempotent and can return an already owned/native value unchanged.
`Clone()` always detaches data. Neither operation adds lifetime checks to reads.

Direct views borrow immutable bytes and are transaction-scoped when read from
EDB. Treat returned byte slices as read-only. Decoding an encoded record with
`Fastest` produces an owned Go value or decoded memory; it can outlive the source transaction.
An already native projection remains a view of its existing Go object, so
`Fastest` does not clone that object. Edits to native projections mutate their
Go object; decoded-memory edits similarly update that owned object. Edits to encoded views copy bytes. `Snapshot` freezes decoded memory into independent row2 bytes.

Types with custom MessagePack or binary/text methods, embedded fields, struct-array
layouts, aliases, interning and duplicate field names retain decoder semantics.
A custom child or a map with custom key codecs is decoded only when accessed or edited; a custom root is decoded
when opened. JSON and time values use their native property codecs. Some `Has`
checks (such as fixed arrays and JSON values) decode that property to preserve
Go zero-value semantics. EDB schema migrators use the same editor over encoded
records or decoded Go structs; migration alone does not require full decoding. These compatibility cases never
silently reinterpret a recognized custom encoding as an ordinary map. Out-of-band
`msgpack.Register` handlers are not discoverable through Go reflection. If such
a handler changes the ordinary wire layout, expose codec methods on a named
Go type or use `Projection.Record` over a row decoded by the existing handler.

## Choosing an access strategy

Start with direct access. Use `Fastest` explicitly around a workload that reads
many fields repeatedly, then measure the complete operation including decoding.

- **MessagePack:** direct reads and edits help sparse access. Decoding once wins
  when repeatedly consuming many properties; it may win on the first full pass.
- **Fast2:** direct scalar access is already very cheap. Generated-native and
  indexed-memory access can be faster after decoding, but the initial decoding
  cost requires enough repeated reads to pay back.
- **Compact2:** decoding can pay back across repeated passes; generated fields
  reduce the threshold substantially. Sparse one-pass reads still favor direct
  access. `Fast` requests decoding here; retain the original record when you
  know the access will be sparse.
- **Collections:** native `Get` still materializes the common collection value.
  Decoding is not an automatic cache for those values. Retain a requested
  collection value if you will traverse it repeatedly.

See [measurements and reproduction commands](BENCH-RECORD-ACCESS.md). `Fastest` names an
explicit full-decode hint; it neither predicts your workload nor automatically
chooses the faster strategy.

## Low-level codec access

Use `values.Int`, `values.Uint`,
`values.Float32`, `values.Float`, `values.String`, and `values.Bytes`. The
`values.Bytes` span has pointer + byte length; it is distinct from the mutable
slice representation used inside EVM. The declared type supplies its meaning.

Modeled `Record` APIs choose the wire conversion. Raw `row2.View` and `Writer`
APIs require an explicit encoding:

```go
n := view.Get(code, row2.Signed).AsInt()
writer.Put(code, "", values.Int(n), row2.Signed)
```

`row2.Field` holds raw codec metadata for schema-free traversal. `RawField`,
`RawString`, `RawAt`, and `Iterator.Next` return fields. Call `Decode(encoding)`
to get a runtime value, or `Writer.PutRaw` to copy unknown fields without
reinterpreting their bytes. `Field.Container` opens an encoded child. No wire
markers or deferred integer decoding escape into `values.Value`.

Generated record bindings use ABI `record3`; regenerate them after migrating.
This changes Go APIs, not the stored Fast2/Compact2 formats.

### Property names and native tags

Declare a property once with a stable code and logical type. A qualified name
such as `customer:full_name` is unique within its schema. Models automatically
expose its local name `full_name`; lookups also accept `customer:full_name` and
`customer_full_name`. Duplicate codes, local names and conflicting aliases panic
at schema construction. EVM selectors accept both `row.full_name` and
`row.customer_full_name` on a statically known record model; both compile to the
same property code. An untyped EVM `any` does not support record selectors.

Register native structs with the entity type and use `kvo:"full_name"` or
`kvo:"customer:full_name"` on fields to bind them. Use `kvo:"-"` for excluded
fields. Existing `edb` mapping tags remain supported when no `kvo` tag is present.
Names and tags are resolved during binding, not during record access; MessagePack
wire names remain controlled by the separate `msgpack` tags.

Timestamp properties use `kvo.NewTimeProp(schema, code, name)` and return
`time.Time`, including `IsZero`, `Before`, and the other Go methods. Their logical
type is `kvo.TTimestamp`; lists can use `values.TimeCodec()`.

Fast2 and Compact2 store ordinary timestamps as integer Unix nanoseconds plus
one (zero denotes Go's zero time). Dates outside the signed nanosecond range,
and the reserved -1 ns instant, use two integers: 64-bit Unix seconds and 32-bit
nanoseconds. Reads preserve nanoseconds and the full date range. They normalize
to UTC and do not retain a monotonic clock. MessagePack timestamps remain in
MessagePack's existing 32/64/96-bit integer format and are read directly.
Historical RFC3339 row2 values remain readable; new writes use integers.
`kvo.TTime` retains its original offset-microsecond encoding for Fast1 users.

Normal timestamp access allocates nothing. Reading extended MessagePack or row2
payloads also borrows their bytes without allocation; constructing a wide date
from a native `time.Time` needs one 12-byte payload. JSON still emits RFC3339,
which is a presentation format, not the stored representation. EVM exposes
`time.Time` with `IsZero`, `Before`, `After`, `Equal`, and Unix time accessors.

Typed scalar dictionaries use `kvo.NewMapProp` with a key codec and a value
codec, just as lists specify an element codec. The returned `kvo.Map[K, V]`
is a 16-byte borrowed view supporting `Get(key)`, `Lookup(key)` (value/presence),
`Len()`, and `Iter()` for `for key, value := range view.Iter()`.

For example, declare an ID-to-string property with
`values.UnsignedCodec[IntegrationID]()` and `values.StringCodec[string]()`. Then
`customer.Get(AppIDs).Get(integrationID)` reads directly without materializing a
Go map. Zero values remain distinguishable from missing entries through `Lookup`.
MessagePack key adapters normalize historical encodings before lookup and apply
Go's last-key-wins rule. Direct lookup scans MessagePack once; use Fast2 for large
maps needing repeated random access. Iteration handles duplicate normalized keys
without allocation, with quadratic lookahead in the MessagePack case.

`GetValue` on a whole collection requests a row2 encoding and can allocate. Prefer
typed list/map views for access. As with lists, retain the source record with
`Own()` before carrying its borrowed strings or collection views beyond a transaction.
