# KVO design decisions

KVO separates a model from its data. One property API reads MessagePack, Fast2,
Compact2, native Go structs and decoded memory. Storage format does not force an
access strategy. See [usage](../../kvo/_docs/ACCESS.md) and
[measurements](../../kvo/_docs/BENCH-RECORD-ACCESS.md).

## Types and bindings

- `kvo.Model` defines properties, stable codes, logical types and storage policy.
- `values.Type` defines runtime type identity; `values.Shape` describes storage.
- `nativebind` supplies reflected or generated Go field operations. KVO and EVM
  share these operations; a native pointer still points to the original object.
- `kvo.Bind` requires a complete persistence mapping. `kvo.Project` permits a
  subset. `WithNativeSchema` attaches the same native type/fields used by EVM.
- Models and bindings belong in setup. Default bindings are cached; explicit
  field maps and generated/native configurations should be constructed once.

Use `kvo:"local_name"` or `kvo:"namespace:name"` to map Go fields. Inline fields
belong directly to the parent model. A field excluded from persistence must not
appear only through decoded access.

Typed properties bind a code to an access type. `ModelBuilder.Prop` accepts
property declarations, not bare numbers. Raw codecs and dynamic Fast1 keys keep
numeric codes; extract `.Code()` outside hot loops. A shared semantic property
uses the same declaration across models. Registration rejects collisions.

## The 16-byte record

A record is a typed pair: a GC-visible pointer plus a word containing a tag,
representation ID and 32-bit auxiliary value. Encoded records use that value for
length. Native records use the pointer directly. No wrapper allocation is needed
merely to pass a record through an EVM pair register.

Shared metadata selects access operations by representation ID. KVO uses flat
record/collection tables; hot reads do not walk a chain of Shape extensions.
Representation IDs are process-lifetime metadata, never transaction IDs, and are
not reclaimed. A 16-byte handle does not imply that its payload or metadata is
16 bytes.

`Fastest()` returns a replacement representation. It does not attach a decoded
cache to the original handle. A bound model can produce a native struct;
otherwise it produces indexed decoded memory. `Fast()` preserves an existing
Fast2 view. Measure decoding plus access before choosing either.

The rejected pointer/interface handles needed separately allocated backing
objects for escaping encoded views. Larger inline handles improved some isolated
reads but increased copy/retention costs. [Layout controls](../record-handle/README.md)
retain the measurements, including cases where the cell was slower.

## Keys

The database key is separate from the record payload. A record exposes it only
when the model declares an ordinary property with `kvo.PrimaryKey` and the payload
stores it. `Fastest()` does not reconstruct an omitted key.

Carry identity through `Entries()`, cursor keys, lookup results or an explicit
argument. Edit handles retain their write key. Explicitly keyed writes work with
keyless models; operations deriving a key from a keyless record panic. A stored
key must match the supplied key. Adding a primary-key declaration does not
backfill old rows.

This avoids a second byte span and per-record key adapter. Synthetic inline-key
experiments are controls, not a database wire format.

## Ownership

Borrowed records and their strings, lists and dictionaries expire with their
transaction. Reads perform no expiry check. Do not retain them across transaction
closure, page reuse or database unmapping.

- Keep the key and reload for current data.
- `Own()` retains encoded data; native data may remain shared.
- `Clone()` makes an independent snapshot, including native data.
- `OwnedSlice()` owns borrowed elements as well as the slice header.
- `CloneSlice()` independently clones record elements.

EVM's compiler tracks declared borrowing effects. Borrowed results taint values
and containers; retention or invalidation boundaries require owned copies. Place
copies near the escape where analysis permits, so a search can copy its one
result rather than every row. Unknown effects require conservative copying.
Mutable editors retain explicit runtime lifetime checks.

`BorrowOnly`, `NoRetain` and `NoInvalidate` are host promises. Incorrect promises
can defeat the analysis. They are not inferred from arbitrary Go code. See
[effect declarations](../../evm/README.md#declaring-borrowing-effects).

No borrowed tag, transaction-specific representation ID or per-read slot table
is part of this design. Those approaches required lifetime bookkeeping or safe
ID reclamation for handles that callers never explicitly release.

## Runtime values and codecs

`values.Value` carries interpreted runtime values. `row2.Field` carries raw codec
metadata. Decode only the selected field with its declared encoding; copy unknown
fields as raw fields. Signed widths and wire float conventions belong at the
codec boundary. Presence is separate from the scalar value.

Record and collection edits batch changes before encoding. Snapshotting an edit
flushes pending changes; repeated snapshots can defeat batching. Keep application
save helpers around database writes when they maintain additional domain state.

## Evidence

- [Record access and decode cost](../../kvo/_docs/BENCH-RECORD-ACCESS.md)
- [Typed properties and collections](../../kvo/_docs/BENCH-TYPED-ACCESS.md)
- [Shared-value comparison](../shared-values/README.md)
- [Editor option measurements](KVO-EDITOR-WRITE-OPTIONS.md)
- [Storage compatibility](../../_docs/STORAGE-COMPATIBILITY.md)
