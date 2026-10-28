# KVO: a model, two storage layouts, and logical changes

**Local research evidence:** `results/` (reports, measurements and source snapshots) and the collected replay fixture are intentionally ignored by Git. Links to results below refer to the locally retained evidence. The experiment code, specification and state/history remain versioned. Fresh checkouts build without the replay file and explicitly skip sample-dependent experiments; restore the local input to run them. See [benchmark setup](BENCHMARKS.md#local-inputs-and-results).

KVO separates the meaning of data from its storage. The model gives fields names and types. The bytes give each value a boundary. An unknown field can be skipped, copied or preserved without its old schema.

This is an experimental specification for extending EDB. The codec is **not integrated into production EDB storage**; native registration metadata is available in `kvo`. The main candidate is implemented in [`current_codec_test.go`](experiments/current_codec_test.go), with [typed values](experiments/current_value_test.go). Fast reuses the measured adjacent-key directory; Compact now uses direct positive tags and byte lengths. The preceding `keysLast`, literal-length and ZigZag experiments remain runnable comparisons. Names and bytes are not frozen. A [further performance pass](results/performance-continuation-20261016.md) records broader mutation navigation, path binding and writer improvements. Source snapshots identify every checkpoint. The 2026-10-16 combined checkpoint validates the preceding candidate on both CPUs with 426 cases per host and five repetitions; those timings are historical controls, not measurements of the new signed/tag candidate. The [previous specification](results/README-before-performance.md), source snapshots, and every benchmark alternative remain available.

There are two persistent layouts:

- **Fast** keeps a searchable directory and eight-byte cells for frequent partial reads and small updates.
- **Compact** stores consecutive payloads and a trailing catalog for cold data and whole-object work.

Layout is normally chosen per row. A Fast row can mark a cold child Compact; Compact propagates through its descendants. The top-level row is always an Object. Both layouts use the same value semantics and forward writer strategy. A decoded Go struct or bound value array is an optional in-memory view, not another stored encoding.

The measured recommendation keeps trailing framing and puts Fast Object keys beside their header. This changes no byte counts and requires no payload copying or size prepass. For one-byte keys, the getter compares directories of up to four entries directly and uses standard-library byte search above that. Wider keys use binary search. Dictionary search returns the matched payload offset directly. An all-scalar Fast List addresses its contiguous eight-byte values without loading the stored offsets. It avoids the large temporary parsed header. Front framing remains a measured alternative: it helps flat reads but loses on nested server workloads. No fixed slots are needed for these improvements.

For mutation, the selected planner now fuses structural lookup and uses the same narrow-key search as reads; the [continuation report](results/performance-continuation-20261016.md) measures the complete resulting writes. Maintain a small overlay, patch fixed-size cells after copying borrowed bytes into owned output when eligible, and rebuild when shape changes. For construction or broad replacement from known values, use the forward builder. A prepared dense view can make repeated broad work much cheaper, but its setup and path binding must be paid explicitly. Canonical path binding now uses binary search; a prepared path map is an optional faster choice. A one-shot `currentLeafMerger.Append` avoids building an editor tree for a completed leaf batch. The preceding adapter measured substantial cold allocation savings on deep rows; the selected adapter keeps the same algorithm. It rejects overlapping ancestor changes and is not the interactive editor.

The 2026-10-17 decision selects direct positive tags and two’s-complement signed payloads, with no ZigZag. Compact keeps container counts. It does not retain shared scalar widths: every value is either a positive immediate or a bounded byte payload. Signed payloads preserve their sign at the stored width; unsigned payloads zero-extend. The old one-bit measurements used ZigZag and are retained as controls, not predictions for this revision. Every unusual rule must earn its complexity through a concrete correctness requirement or measured benefit.

The [direct-tag and signed-value report](results/signed-tags-20261017.md) records the completed 2026-10-17 implementation and paired measurements. Selected Compact bytes are 0.307% larger than the preceding literal catalog on the unsigned replay. Fast mmap reads remain within about 2% of the preceding candidate on the server after inlining; the smaller iterator recovers most of the new adapter’s small-row rebuild cost. Reused reads and writes allocate zero. Remaining write regressions, signed-negative size costs and fresh scratch allocation are reported explicitly.

The [native mapping report](results/native-mapping-20261017.md) records the new struct APIs and 112 paired benchmark cases. Reading every field once is cheaper with direct traversal in the measured flat fixture; repeated native access pays back decoding. GetFast reuses storage and borrows strings. Nested-map storage reuse removes allocations at a measured CPU cost; faster allocating and generated controls remain available.

The [performance report](results/performance-iteration-20261016.md) contains attribution and the initial paired results. The [continuation and consolidated review list](results/performance-continuation-20261016.md#consolidated-review-list) records later gains, costs, and every faster alternative retained for review. The [state file](STATE.md) retains requests, decisions, intermediate findings and corrections. The [benchmark inventory](BENCHMARKS.md) explains how to rerun every variant.

## Model and storage contract

A stored value is either a scalar word or a byte span. Zero needs no payload. A span can contain a string, binary data, a container (Object, Dictionary, or List), or an application-defined encoding. The model supplies its meaning. Structural decoding never guesses whether arbitrary bytes contain an object.

| Container | Keys | Purpose |
| --- | --- | --- |
| Object | Sorted, unique unsigned 64-bit numbers | Record properties and numeric dictionaries |
| Dictionary | Sorted, unique byte strings | Runtime string-keyed maps |
| List | Positions starting at zero | Lists |

The top-level row is always an Object. Dictionary ordering is lexicographic byte order. Empty keys and non-UTF-8 bytes are representable; a string model can impose UTF-8 separately. Numeric record properties and numeric dictionary keys have the same physical representation.

Ordinary fields follow Go zero-value semantics. Missing, null, and zero are indistinguishable. Zero numbers, false, empty strings, empty byte slices, and empty modeled objects are omitted from maps. Setting an ordinary map field to zero clears it. Dictionary entries also omit zeros by default, independently of whether they are accessed by key or through a native Go map. A Map property marked `kvo.KeepDictionaryZeroValues` retains zero entries when key identity matters. Lists retain zero positions, so `[0, 7, 0]` still has three elements. Change records retain an explicit zero assignment because clearing a field differs from leaving it unchanged.

`edbschema.PresenceIsSignificant` on a property retains explicit zero values and makes missing distinct from zero. It does not introduce null as a third state. A presence-aware accessor can return `(value, present)` or expose `Has`; ordinary access still returns the value without a presence result. Setting zero stores a present zero on a marked property; `Delete` makes it absent. An empty string or container can use a zero-length representation because the schema supplies its type. Lists always retain positions. The existing binary layouts already represent zero. The [presence prototype](experiments/schema_presence_test.go) adds schema-directed retention, `Lookup` / `Has`, explicit deletion, and owned zero patching without changing those bytes. The frozen common checkpoint still elides map zeros. Unknown entries, including unknown zeros, must survive a rewrite without consulting their former schema.

### Selected fixed slots

`edbschema.FixedSlot` is a proposed opt-in for selected fields in a Fast Object. These are fields needing the cheapest access or fields present in almost every row. Unmarked fields remain in the normal keyed area. Slot assignment must be stable across schema changes; a removed field cannot silently donate its slot to a different meaning. A Compact subtree does not acquire slots.

Direct slots and bitmap-packed selected slots have been compared, including sparse fields, `PresenceIsSignificant`, mixed access, encoding, and mmap reads on both CPUs. **Direct slots are the recommendation for fields explicitly marked `FixedSlot`.**

A scalar can occupy a word cell; a string or child still needs a bounded span reference. Generic enumeration and preservation must work without knowing the former field type. The [selected-slot prototype](experiments/fixed_slots_test.go) puts selected cells first, retains their field numbers, and adds presence and scalar/span bitmaps. Direct cells trade reserved space for stable offsets; the bitmap-packed control removes absent cells and pays for rank calculation. Both retain the ordinary Fast keyed area for unmarked fields. The bitmap-packed form remains a measured control.

`FixedSlot` affects placement in the containing Fast Object; it does not choose the layout of a child. A property may therefore combine a fixed span-reference slot with a Compact child. Inside a Compact ancestor, the slot hint is inactive. The global layout default remains an open API decision; it does not add a wire format.

### Field identities and evolution

Prefer small field numbers local to each record type. Existing global numbers remain representable. A separate [key-base experiment](results/performance-continuation-20261016.md#read-follow-up-results-and-format-review-decision) substantially improves clustered global keys by storing one base and narrower relative keys. It remains a review option: scattered keys do not benefit, current validators/editors do not accept that extension, and its descriptor allocation has not been reconciled with FixedSlot. A database catalog may allocate numbers for runtime declarations, but must persist assignments and never reuse retired numbers for a different meaning.

An unknown field can be enumerated, skipped, copied, and relocated without a semantic schema. An older writer preserves unknown fields during a rewrite. Removing a model declaration does not require retaining its old type just to read other fields. An incompatible change of meaning still requires a new field number or an explicit migration.

The catalog remains necessary for names, validation, plugin declarations, and historical rendering. Structural framing recovers boundaries, not lost meaning.

For Go row binding, the first struct field is the primary key and belongs in the database key. This applies only at the row root. The first field of a nested struct remains an ordinary field. A binder or generator implements that rule; the codec itself does not depend on MessagePack annotations or reflection.

## Native Go mappings

Native registration is available in `kvo`; the encoder/decoder remains an experimental layer alongside the candidate codec. Register each allowed struct projection when declaring its entity type:

```go
type WidgetSummary struct {
    Count int64 `edb:"count"`
    Cache any   `edb:"-"`
}

TWidget := kvo.NewEntityType(schema, "widget", reflect.TypeFor[WidgetSummary]())
MWidget := kvo.NewModel(schema, TWidget, func(b *kvo.ModelBuilder) {
    b.Prop(KCount)
})
```

Every struct field must have an `edb` tag, including excluded/unexported fields. The name must be explicit and must belong to that object's model. Duplicate bindings, unknown options, missing tags and unregistered projections panic with `kvo.ErrNativeMapping`. `edb:"details,compact"` selects a Compact child; `edb:",compact"` has no property name and is invalid. Anonymous fields are explicit nested properties, never implicitly flattened. The schema supplies `kvo.Compact`, `kvo.PresenceIsSignificant` and `kvo.KeepDictionaryZeroValues`; tag options cannot invent property identities or presence semantics.

`kvo.ListOf(itemType)` describes positional values; `kvo.TBytes` describes binary data. Scalar declarations retain their native Go type, so a binder distinguishes signed, unsigned, Boolean, floating-point and time values without a persisted semantic tag. These declarations do not add native encoding or List support to the legacy Immutable codec.

The experimental native layer offers allocating Get, reusable GetFast, Put and Patch. Put replaces the modeled object; Patch overwrites only the supplied struct's declared properties, including clearing a declared property when its value is zero. A declared nested property is replaced as one value; Patch does not implicitly merge its descendants. Use a key-based deep edit for that operation. Unknown and unrepresented sibling properties survive Patch. Object mapping does not silently omit its first Go field; a future database row adapter owns primary-key exclusion/injection, with explicit `edb:"-"` available now.

The executable implementation is in `experiments/native_*_test.go`:

```go
codec := bindNative[WidgetSummary](TWidget) // compile once after all models exist
owned := codec.Get(view)                  // owns its strings and collections
var row WidgetSummary
codec.GetFast(view, &row)                 // reuse row; strings borrow view
encoded := codec.Put(dst[:0], &row, true) // true selects Fast at the root
patched := codec.Patch(dst[:0], view, &row)
```

The codec object owns writer and map scratch and is used by one goroutine at a time. Sparse Fast projections (at most one quarter of the stored properties) use direct lookups; fuller projections and Compact rows use one ordered traversal. Explicit scan/lookup controls remain in the benchmarks; the threshold is a replaceable read policy, not a wire rule. Plans are compiled once, validate semantic type compatibility, and sort struct properties once. Payloads stream directly into the candidate writer. There is no intermediate generic object tree or per-child payload buffer. Destination storage for Put/Patch must not overlap the base row or native input spans.

GetFast borrows strings, including string map keys, for the lifetime of the input row/transaction. It copies writable byte slices into caller storage and reuses slice/map capacity. The allocating Get owns all data. Excluded fields are untouched by decoding; mapped fields absent from the input are reset, so reused projections do not retain stale properties. Nil and empty collections are equivalent. Time values use existing KVO microseconds and decode in UTC, without monotonic-clock or original-location metadata.

`Optional[T]{Value: value, Present: true}` is required on a property marked `kvo.PresenceIsSignificant`; an absent optional has `Present: false`. This representation is in memory only. Explicit zero/empty containers use the existing present-zero encoding, and absent optionals delete their declared property during Patch. Ordinary pointers are convenient nested values but do not introduce a separate null value.

Dictionary semantics belong to the schema, including for native Go maps. Zero-valued entries are omitted by default. `b.Prop(KLabels, kvo.KeepDictionaryZeroValues)` retains zero-valued entries in that property; generated entry getters then return `(value, isPresent)`. The option also applies to numeric-keyed Maps. It does not imply `PresenceIsSignificant` for the containing property, and does not propagate into nested dictionary values. A dictionary with retained zero entries is nonempty; an empty dictionary is still an ordinary zero unless its property is presence-significant. Enabling retention later cannot recover key identities previously omitted with zeros. Lists always retain positions. Current supported native values include named numeric/string types, Booleans, floats, `time.Time`, bytes, registered nested structs, pointers, slices/arrays, string-keyed maps and unsigned-integer-keyed maps. Arbitrary interfaces, implicit flattening, custom scalar converters and optional List elements are not implemented; incompatible registrations panic during binding. Numeric narrowing is checked rather than silently truncating.

A schema declaration decides the dictionary entry API:

```go
b.Prop(KCounts)                                // Counts.Get(key) int64
b.Prop(KNamedCounts, kvo.KeepDictionaryZeroValues) // NamedCounts.Get(key) (int64, bool)
// Add PresenceIsSignificant independently if the dictionary property itself
// must distinguish absent from present-empty.
```

Both forms return zero for an absent key. The second also returns false for absence, and returns true for an explicitly stored zero. Setting zero deletes an ordinary entry; setting zero in a retaining dictionary keeps its key. Explicit Delete removes the key under either policy. Native Put/Patch and Get/GetFast use these same rules, including omitting stored zero entries when read under an ordinary dictionary declaration. Native `map[key]` lookup therefore exposes retained zero membership only when the schema permits it.

The selected-format implementation and executable handwritten equivalents of generated getters are in `experiments/current_dictionary_test.go` and `experiments/native_dictionary_test.go`. `bindCurrentDictionary(model, property)` provides schema-bound Get/Lookup, iteration, writer emission and Edit/Set/Delete/Append operations, including creation from an absent dictionary; only a retaining policy permits Lookup. Ordinary raw views and iterators remain schema-free, so unknown zeros survive opaque copying. There is no generator yet: the two Get signatures above are its specified output, exercised by the handwritten wrappers. Presence-aware Fast dictionary lookup uses the same specialized directory search as ordinary lookup; the generic-directory implementation remains a benchmark control. Neither the flag nor these APIs add a wire tag or stored metadata. The [paired dictionary measurements](results/dictionary-policy-20261017.md) verify zero allocations after warming and near-equal ordinary/presence-aware lookup on the tested Fast fixture. Invalid use of the flag on a scalar, List or entity Object panics with `kvo.ErrPropOption` during model declaration.

Zero allocations require warmed scratch and sufficient destination capacity. Reused scalar structs, strings, slices of structs and scalar-valued maps can meet this contract. Growing storage allocates. For maps containing mutable values, GetFast snapshots addressable old values and uses a reusable key index to retain each matching entry's storage. Stable keys and sufficient child capacities can therefore avoid allocations too. New keys, growth, and storage dropped by an empty row can allocate. Prior borrowed map keys must remain valid through reuse: normally reuse the destination within one read transaction, or clear those maps before ending that transaction. The allocating Get has no borrowed lifetime. A rebuild control and a sorted-snapshot control remain runnable; avoiding allocations can cost additional CPU.

## Error policy

Expected external failures return errors. Misuse and unexpected failures panic with defined errors. Setters grow their storage as needed.

| Operation or condition | Result |
| --- | --- |
| Read an absent map field | Zero value |
| Set a valid value | Complete; allocate if scratch capacity is exhausted |
| Use an undeclared property through a record model | Panic with `ErrUnknownKey` |
| Invalid property option | Panic with `kvo.ErrPropOption` |
| Invalid native declaration, tag or binding | Panic with `kvo.ErrNativeMapping` |
| Structurally incompatible accessor or wrong container kind | Panic with `ErrType` or `ErrKind` |
| Invalid order, index, value, or encoding limit | Panic with the corresponding defined error |
| Validate malformed external bytes | Return `ErrFormat` |
| Try an owned-cell optimization that cannot apply | Return false; rebuild through the editor |

The reference uses the defined errors in `draft/`. Low-level numeric dictionaries accept arbitrary data keys. A record model supplies the distinction between an absent declared property and an unknown property, and checks semantic types. A short opaque payload can be interpreted as a word by the low-level reader; framing alone cannot reject every semantic type mismatch. The earlier `draft.RecordModel` demonstrates membership checks; the native binder now checks registered projections and compatible scalar/container families, while a combined public key-based model facade remains integration work.

`currentOpen` validates one container, including order, lengths, and offsets. It leaves spans opaque. `currentTrusted(value.Bytes())` is a **trusted reinterpretation**: use it for known child bytes already validated or emitted by this codec. The older `lengthValue.Object()` helper belongs to the retained earlier layout and must not open this candidate. Validate an untrusted child explicitly with `currentOpen(value.Bytes())`. Trusted access is not a second corruption boundary.

## In-memory representation

On 64-bit Go, both values and views occupy 16 bytes:

```go
type currentValue struct {
    ptr *byte
    num uint64
}

type lengthView struct {
    ptr *byte
    n   int
}

// The candidate adds methods, not storage.
type currentView struct { lengthView }
```

A value is one pointer plus one word. Nil plus `num` holds an opaque eight-byte Fast word or positive immediate. A borrowed pointer plus length holds an opaque payload, preserving its exact byte width. Nil plus zero is the ordinary zero value. Empty spans normalize to zero. `Int()` sign-extends payloads; `Uint()` zero-extends them. Interpretation belongs to the schema.

Two real static marker pointers distinguish signed construction with `currentInt(n)` and unsigned construction with `currentWord(n)`. They let the writer shorten typed values without allocating a temporary byte array or adding a third word. Unknown Fast words retain eight bytes; values 0–127 need no hint. These are construction hints only; neither adds a wire tag. The common Fast scalar read is a nil-pointer check and word return, small enough to inline; borrowed payload decoding lives in a separate helper. The pointer stays visible to Go's garbage collector. No integer is disguised as a pointer, and no heap pointer is stored in a `uintptr`.

A view stores the address and length of one bounded object. Slice access reconstructs a borrowed slice with capacity equal to length. It needs no root-object pointer because all child references are local to the containing object. A detached subtree remains self-contained.

Typed wrappers need no model pointer per instance:

```go
type Widget struct { data currentView }

func (w Widget) Count() uint64 { return w.data.Get(1).Uint() }
```

A generic model-aware wrapper may carry a model pointer separately. A decoded struct or flat projection can remove repeated navigation when most fields are used repeatedly. It is an application view, not a third persistent encoding. Automatic projection or representation switching is not implemented.

An optional prepared view can also fit in 16 bytes by packing the count, directory offset, width, mode and kind beside the pointer. The retained `fastPacked` experiment uses the earlier directory order; its cached-read measurements must not be presented as timings for the new placement. It demonstrates the size and preparation tradeoff, but is not an accessor for `currentWriter` bytes. A cache also retains 16 bytes per row and inherits the transaction lifetime.

The small-overlay editor stores a borrowed base view, sorted pointer-free edit records, and a byte arena. The recursive editor creates overlay nodes only for changed containers. Each edit record is 24 bytes. Scalar edits need no allocation with sufficient scratch; byte assignments copy into the owned arena. Arena references are 64-bit offsets to length-prefixed bytes, so cumulative edits do not inherit a 32-bit arena limit. Unused ranges from replaced values remain until the editor is discarded or reset.

Borrowed views do not extend database transaction lifetimes. Retaining an interior pointer retains its Go allocation, but cannot make a released database mapping valid. Returned byte slices are read-only by contract. Owned patching requires exclusive ownership. Output used for a rebuild must not overwrite the borrowed base while it is being read. Bolt may retain bytes passed to `Put` until the transaction commits, so output reuse belongs to the transaction's ownership policy.

## Forward construction

For signed assignments use `currentInt(n)`; for unsigned bit patterns use `currentWord(n)`; for bytes use `currentBytes(b)`. These constructors do not allocate. `Put` and editor `Set` retain the signed construction hint until serialization. A schema-free rewrite passes the borrowed value through unchanged; it must not turn it into an unsigned word first.

Write every payload directly into one destination. Keep only keys, lengths, and scalar/span flags in reusable scratch. On closing a container, append its catalog and footer, then record its final extent in the parent.

A later large value cannot widen already-written payloads. Numeric key widths and Fast scalar/span bitmaps are chosen when the container closes. This needs a pass over buffered metadata, not another traversal of the input tree. There is no intermediate child payload buffer and no copy of a child at every ancestor.

Destination growth can still copy the accumulated output. Reserve output capacity when a useful estimate exists. Metadata scratch is bounded by entries in open containers; retained per-depth capacities and dictionary-key arenas must also be counted. It is not merely the largest scalar payload.

The actual experimental builder accepts both key forms to keep this skeleton small:

```go
var w currentWriter
w.Reset(dst[:0])
w.Begin(0, "", 0, true)       // Fast Object root.
w.ReserveEntries(2)          // Optional capacity hint, not a size prepass.
w.Put(1, "", currentWord(7))
w.Begin(2, "", 1, false)      // Compact dictionary child; length unknown.
w.Put(0, "region", currentBytes([]byte("west")))
w.End()
w.End()
encoded := w.BytesWritten()
```

Container kind numbers are `0` Object, `1` Dictionary, `2` List. The checkpoint parameter `indexed` selects Fast; old source and benchmark labels retain their historical names for reproducibility. The numeric key is used for numeric parents; the string key for dictionaries; both are ignored for sequence positions. Keys must arrive sorted and unique, including keys whose zero values will be omitted. Empty modeled children are omitted in maps and occupy a zero position in lists. Raw spans remain opaque.

`End` reserves the known Fast directory extent once and emits it with a width-specific loop. The old append-per-entry implementation remains `appendDirectoryWriter` for comparison. `Reset` reuses frame and metadata storage. `ReserveEntries` uses a known source count only as a capacity hint; forward producers without that information can omit it. `Put`, `Begin`, and `End` have no error results.

## Reading and editing

The following examples use actual package-local names. They are executable API sketches, not an importable public next-generation API. [`current_mutation_test.go`](experiments/current_mutation_test.go) checks construction, mixed-layout reads, signed edits, presence and borrowed-base preservation; [`current_changes_validation_test.go`](experiments/current_changes_validation_test.go) covers logical changes.

```go
view, err := currentOpen(encoded)
if err != nil { return err }
count := view.Get(1).Uint()
region := currentTrusted(view.Get(2).Bytes()).GetString("region").Bytes()
```

Missing numeric or string keys return zero. An absent child can be navigated as empty. `At` checks list bounds and panics for an invalid index. An iterator returns `(numericKey, stringKeyBytes, value, ok)`; `ok` marks iteration end, not ordinary field presence.

On 64-bit Go, the selected iterator carries 208 bytes of transient state. It retains only the Fast header/index and Compact cursor; the old 280-byte adapter also carried unused state from the preceding codec. Both the larger wrapper and its fused-loop variation remain benchmark controls. Row views and individual values stay 16 bytes.

```go
var scratch [8]classMeta
var arena [256]byte
editor := newCurrentLengthEditor(view, scratch[:0], arena[:0])
editor.Set(1, currentWord(editor.Get(1, "").Uint() + 1))
updated := editor.Append(&w, dst[:0], true)
```

Reads consult the overlay before the base. Assigning zero records an explicit clear. Unknown base values survive serialization without interpretation. `SetString` and `DeleteString` edit dictionaries. List `Set` and `Delete` use **current logical positions**: deleting element zero shifts later indices. The overlay translates those positions to stable base positions internally. This flat editor supports shifting deletion. The separate recursive path editor supports leaf replacement, List construction and whole-container replacement; List insertion/splicing is not yet integrated there.

The optional `view.Patch(numberKey, stringKey, word)` changes an existing scalar in exclusively owned Fast bytes. It returns false for absent keys, spans, or a zero map assignment. List zero assignments can patch in place. It does not insert, resize, or make a database write durable. Ordinary `Set` succeeds by rebuilding when this optimization cannot apply.

For a nested edit, the current skeleton accepts a resolved path. `perfStep.kind` identifies the parent: 0 Object, 1 Dictionary, 2 List. The model should construct these paths or generated property handles; application code need not repeat this metadata in the final API.

```go
var edit currentTreeEditor
edit.Reset(view, "sorted", true)
edit.Set([]perfStep{{kind: 0, key: 2}, {kind: 1, text: "region"}},
    currentBytes([]byte("east")))
updated := edit.Append(&w, dst[:0])
```

`SetPresent` records an explicit zero for a property whose schema makes presence significant. Unchanged children remain opaque. A parent replacement invalidates previously staged descendant edits. The low-level physical patch planner accepts resolved leaf updates; ordered ancestor replacements must go through this semantic editor.

For an already resolved, independent leaf batch, the preferred physical plan uses eight bytes per change and no GC pointers. It reads values from the unchanged input batch. The semantic layer can hide this optimization and fall back to the direct merge when membership or size changes:

```go
var local [8]patchOffset
plan, ok := currentPatchPlan(view.Raw(), changes, local[:0])
if ok {
    updated = append(dst[:0], view.Raw()...) // Own the bytes before writing.
    currentPatchApplyOwned(updated, plan, changes)
} else {
    var merge currentLeafMerger
    updated = merge.Append(&w, dst[:0], view, changes, nil, true)
}
```

The output buffer must not alias base or input values. The plan is valid only for that base and that unchanged batch; it is not a persisted change set. `currentLeafMerger` sorts input indexes once, uses the last duplicate leaf, and rejects ancestor/descendant overlaps. Its optional `keepZero` flags retain presence-significant zero assignments. Ordered ancestor replacements and interleaved reads use the tree editor. Known shape changes can skip the physical probe and merge directly. Repeated broad work can instead bind canonical paths with `bindShapeBinary`, or explicitly prepare `shapePathIndex` and dense values; changing Dictionary keys or List lengths invalidates that prepared shape.

| Capability | Actual reference API |
| --- | --- |
| Construct values | `currentInt`, `currentWord`, `currentBytes` |
| Validate / trust bytes | `currentOpen`, `currentTrusted` |
| Inspect view | `Raw`, `Kind`, `Indexed`, `Len` |
| Read | `Get`, `GetString`, `At`, `Iter` |
| Interpret value | `Int`, `Uint`, `Float`, `Bool`, `Bytes`, `IsZero`; open known children with `currentTrusted` |
| Build | `currentWriter.Reset`, `Begin`, `ReserveEntries`, `Put`, `PutPresent`, `End`, `BytesWritten` |
| Edit | `newCurrentLengthEditor`, `Set`, `SetString`, `Delete`, `DeleteString`, `Get`, `Append` |
| Patch owned word | `currentView.Patch` |
| Plan/apply resolved leaf batch | `currentPatchPlan`, `currentPatchApplyOwned` |
| Merge resolved leaf batch | `currentLeafMerger.Append` |
| Bind prepared shape | Retained previous-format controls: `bindShapeBinary`, `shapePathIndex` |
| Encode changes | `currentChangeWriter.Append`, or `Reset` / `Put` / `Delete` / direct payload / `RecordSpan` / `Finish` |
| Validate/read changes | `openCurrentChanges`, `NextChange`, `Key`, `Reset` |

## Binary conventions

The enclosing row supplies the encoding kind/version once. A partial rewrite retains that version: an unknown span may contain children using it. Changing a row codec requires a migration that can identify such children. Fast/Compact layout changes within the same codec remain possible, including preserving an unknown child in its previous layout. Objects have no version byte. Each object is a bounded byte span; its final byte distinguishes Compact from Fast. Scalar multi-byte words and fixed-width offsets use little-endian order. Unsigned varints use the usual seven payload bits per byte and a continuation high bit.

A **reversed uvarint** stores those varint bytes in reverse order. Read it from the end of a bounded object. This permits a variable-size footer without a separate footer-size field.

Unsigned values use their bits directly. Signed values use ordinary two’s complement, never ZigZag. A Fast scalar occupies eight bytes. A Compact signed payload uses the shortest whole-byte width that preserves its sign; reading it sign-extends from that width. Unsigned payloads omit leading zero bytes and zero-extend on reading. Floating-point values use their bits and unsigned interpretation; both signs of floating zero become zero. The schema supplies interpretation.

Positive immediate tags represent 0–127 directly, for signed and unsigned fields alike. Thus signed +64 uses tag `40`, not a mapped value. Examples of signed payload bytes in little-endian order: −1 is `ff`, −128 is `80`, −129 is `7f ff`, and +128 is `80 00`. Unsigned 128 uses payload `80`. Minimum signed byte-width thresholds match ZigZag; the gain is simpler semantics and twice the positive signed immediate range.

An unknown value retains its byte boundary. A reader must not zero-extend a short opaque payload and then discard its original width. In particular, an unknown eight-byte Fast word is kept eight bytes when rewritten into Compact, unless it is 0–127, whose immediate interpretation agrees for both signedness choices. A schema-aware rewrite can read `Int()` and write `currentInt(n)` to minimize its signed width. No sign bit or signed type tag is stored.

Current experimental limits are 1 048 576 entries and 67 108 864 bytes per object (`tagMaxEntries` and `tagMaxBytes`). These are checked prototype limits, not a promise that allocating such objects is practical. Fast directories use a 32-bit entry count. Numeric keys cover the entire unsigned 64-bit range.

### Compact

```text
payloads | catalog | reversed-uvarint footer

footer = (catalog_byte_length << 3) | (kind << 1) | 0
kind   = 0 Object, 1 Dictionary, 2 List
catalog begins with uvar(entry_count)
```

A value has one of two forms: a direct positive immediate, or a bounded byte payload. The schema determines whether those bytes mean a signed integer, unsigned bits, text, binary data or a child container.

| Tag | Meaning |
| --- | --- |
| 0–127 | That integer directly; no payload bytes |
| 128–254 | Payload byte length `tag − 128`, from 0 through 126 |
| 255 | Length `127 + uvar(extension)`; extension follows in the catalog |

Objects store a numeric key delta combined with one low bit. Dictionaries store a key byte length combined with the same bit, followed by the key bytes. Bit 1 means the value has exactly eight payload bytes and no tag follows; bit 0 means a normal tag follows. Lists have no key metadata and always use a normal tag. An eight-byte payload may contain any bits; the flag is a length shortcut, not a semantic scalar type.

The first numeric delta is relative to zero. The combined numeric delta/flag occupies up to 65 bits in a maximum of ten varint bytes, so all uint64 field numbers, including zero and MaxUint64, remain available. Its first byte carries the flag and six delta bits; subsequent bytes carry seven delta bits each. No reserved field number or exceptional full-key escape is needed.

Payloads follow entry order. Their lengths let a reader skip them without interpreting their schema. Tags and keys live in the trailing catalog, so a child writes directly into the final destination and its length is recorded only after it finishes. There are no varint scalar payloads, negative immediate tags, shared width classes or per-object versions. Every container retains its count. Compact List positional reads walk earlier tags; applications needing direct positional access choose Fast.

For example, an Object containing signed field 1 = −1 is:

```text
ff | 01 02 81 | 18
     │  │  │    └ catalog length 3, Object, Compact
     │  │  └ one payload byte
     │  └ delta 1, ordinary tag follows
     └ one entry
```

The eight-byte shortcut is retained because wide words are common in the measured workload; it removes a tag for those entries. Counts retain constant-time cardinality and bounded validation. The length extension covers arbitrary large values with one rule. The schema and byte boundary provide signedness and width, so separate signed tags do not earn a place in this format.

The previous literal-length catalog and shared-width List experiments remain in `length_*`, `class_catalog_test.go` and the source snapshots. They are comparisons, not additional row-by-row choices in the selected codec.

### Fast

Fast Objects use a word directory:

```text
span payloads | eight-byte cells | optional bitmap | fixed-width keys |
uint32 entry_count | descriptor
```

Keys are adjacent to the footer so a lookup fetches its count/descriptor near the directory it searches. Cells use the same sorted key order. A scalar cell contains its word. A span cell contains `uint32 offset, uint32 length`, relative to the beginning of this object. Numeric keys share a width of 1, 2, 4, or 8 bytes, chosen from the largest key.

Fast Dictionaries and Lists use entry offsets:

```text
complete entries | fixed-width entry-start offsets | optional bitmap |
uint32 entry_count | descriptor
```

A dictionary entry is `uvar(key_length), key_bytes, value`. A List entry is just its value. Scalar values occupy eight bytes. A span's end is the next entry's offset, or the beginning of the directory for the final entry. Offset width is chosen from the largest actual **start offset**, not total payload size; a large final blob need not widen earlier offsets.

| Descriptor bits | Meaning |
| --- | --- |
| 0 | 1, Fast |
| 1–2 | Container kind |
| 3–4 | 0 all scalars, 1 all spans, 2 mixed bitmap; 3 invalid |
| 5 | Reserved, zero |
| 6–7 | Width exponent: 0/1/2/3 means 1/2/4/8 bytes |

For mixed containers, bitmap bit `i` is 1 for a span and 0 for a scalar. The bitmap occupies `(count+7)/8` bytes. Unused high bits are zero. Shared scalar/span containers have no bitmap.

An Object containing `{1: Uint(7), 2: Bytes("hi")}` in Fast form is 26 bytes:

```text
68 69                           span payload
07 00 00 00 00 00 00 00         scalar cell: 7
00 00 00 00 02 00 00 00         span cell: offset 0, length 2
02                              bitmap: entry 1 is a span
01 02                           keys
02 00 00 00 11                  count 2; Object, mixed, one-byte keys
```

The directory shape follows container kind; it is not another application setting. Universal eight-byte cells were measured for dictionaries too, but added an offset/length pair where adjacent entry boundaries already supplied the same information. The retained rejected prototype allows that trade-off to be retested.

No alignment padding is serialized. Portable access uses little-endian loads/stores; measured amd64/arm64 readers also use native-width unaligned key slices, with bounds and lifetime checks at the trust boundary. Ordinary unaligned access and deliberate cache-line crossings are benchmarked separately. Relative padding alone cannot guarantee absolute alignment of bytes borrowed from a database page. These operations are not atomic synchronization primitives.

### Selected slots in Fast Objects (experiment)

Only properties marked `FixedSlot` receive stable slots. Other fields keep the ordinary keyed directory. Direct slots currently use:

```text
selected eight-byte cells | span payloads |
slot keys | slot presence bitmap | slot scalar/span bitmap |
ordinary keys | optional ordinary scalar/span bitmap | ordinary cells |
uint16 slot_count | uint32 ordinary_count | descriptor
```

All keys share the width selected from the largest field number. Slot keys are in stable slot order; ordinary keys are sorted. A slot scalar occupies its word cell. A slot span uses the same local `uint32 offset, uint32 length` pair as an ordinary Fast cell. The two slot bitmaps have one bit per declared slot. An absent direct slot reserves eight zero bytes; that is the cost explicitly requested by `FixedSlot`. `PresenceIsSignificant` controls the presence bit independently of the word.

Descriptor bit 5 identifies this experimental extension. Bit 2 remains zero; bit 1 selects the bitmap-packed comparison, which stores only present slot cells and locates them by bitmap rank. Other descriptor bits retain ordinary key width and ordinary-cell mode. There is no new version byte. The prototype limits selected slots to 64 so the packed control can use one machine-word bitmap; this is not a selected production limit.

Slot assignments are append-only and never reused. A resolved property handle or generated accessor knows its key and slot ordinal. It checks the layout and stored slot count, falling back to keyed lookup for a row written before slots existed or before that property's promotion. Older code that knows only the key can also read a newer slotted row. During a rewrite, preserve the stored slot directory, including unknown assignments and present zeros; do not regenerate it only from the current semantic schema. Arbitrary key lookup still searches. A known scalar slot can be assigned or cleared in owned bytes, including insertion into an absent reserved slot; spans and missing slots fall back to rebuilding. Borrowed Bolt bytes remain read-only.

The standalone prototype has its own reader and builder. It is not yet dispatched by the common `currentView` reader. Complete integration and final descriptor allocation remain production work; the prototype already tests old-row fallback and schema-independent rewriting. This retained slot prototype still uses the preceding scalar and Compact rules; it must not read the new row codec. Direct-slot helpers accept only validated slot rows; the resolved `slotProperty` wrapper dispatches ordinary Fast, Compact, direct-slot, and packed-control rows.

### Schema flags and API use

The proposed public spellings are `edbschema.PresenceIsSignificant`, `edbschema.FixedSlot`, and `edbschema.Compact` (or `FastAccess` if Compact becomes the default). There is no importable `edbschema` package in this experiment. Native binding now uses the real `kvo.Compact` and `kvo.PresenceIsSignificant` schema flags; `FixedSlot` remains a separate prototype. Flags belong to the model; reusable property handles hold resolved key/slot information outside each row view. No schema pointer needs to be carried by a generated typed row wrapper.

The earlier standalone presence prototype demonstrates the model API below. Its `encoded` input uses that prototype's retained `lengthWriter` format; do not pass adjacent-key bytes to it. The current recursive editor separately tests `SetPresent` and unknown-zero preservation. A single public schema-bound facade remains integration work:

```go
spec := objectSpec{{key: 2, flags: propertyPresenceIsSignificant}}
row := schemaView{data: lengthTrusted(encoded), spec: spec}
value, present := row.Lookup(2)
_ = value
_ = present

var writer lengthWriter
editor := newPresenceEditor(row, edits[:0], arena[:0])
editor.Set(2, lengthWord(0)) // Explicit present zero in this retained prototype.
editor.Delete(2)            // Absent; different from the preceding assignment.
updated := editor.Append(&writer, dst[:0], true)
```

Ordinary `Get` and `Set` have no presence or error result. `Lookup` and `Has` are available for marked properties. Decoding to a projection needs presence bits only for those marked properties; an ordinary Go scalar field alone cannot retain the absent/present-zero distinction.

The separate selected-slot builder and resolved handle look like this:

```go
plan := slotPlan{{key: 1}, {key: 4, presenceIsSignificant: true}}
var writer selectedSlotWriter
writer.ResetSlots(dst[:0], plan)
writer.PutSlot(0, lengthWord(7)) // Property 1.
writer.PutSlot(1, lengthWord(0)) // Property 4 is explicitly present.
writer.Put(20, "", lengthBytes([]byte("text"))) // Ordinary keyed area.
encoded := writer.FinishSlots()

count := (slotProperty{key: 1, index: 0}).Word(encoded)
value, present := (slotProperty{key: 4, index: 1}).Lookup(encoded)
```

These package-local skeletons demonstrate the behavior. A schema compiler or Go generator must produce the plan and handles, validate property types, and connect presence editing to the selected-slot builder. The selected-slot prototype retains its earlier ordinary-area ordering; it has not been combined with the adjacent-key placement experiment. The reference does not yet provide one combined public editor for those pieces.

## Logical changes

A change batch has a base revision, a later revision, and an ordered sequence of assignments or deletions. Paths contain numeric keys, dictionary keys, and list positions. Numeric record fields and numeric dictionary entries share one path representation; the model distinguishes their roles.

An empty path addresses the root. A zero assignment is explicit. Deletion is a distinct operation, particularly for lists. Operations retain their order; the codec does not sort changes or infer commutativity. The applier must interpret list positions against the state at each operation.

The selected construction places replacement payloads directly in the batch output and buffers path metadata:

```text
replacement payloads | metadata | reversed-uvarint(metadata_byte_length)

metadata:
  uvar(base_revision), uvar(next_revision), uvar(operation_count)
  for each operation:
    uvar(common_path_prefix_count)
    uvar((new_suffix_count << 1) | delete_flag)
    suffix path steps
    normal value tag if this is Set
    uvar(length - 127) if that tag is 255
```

Set uses the same direct positive/byte-length tag as Compact, including minimal two’s-complement signed payloads. Delete has no value tag or payload. A Set-zero uses tag 0 and remains distinct from Delete. There is no separate change-value type code or signed-number mapping. The first path has a zero common prefix. Each later path reuses a prefix of the previous path and supplies the remaining steps.

| Path step | Bytes |
| --- | --- |
| Numeric key up to `MaxUint64 >> 2` | `uvar(key << 2)` |
| Full-width numeric key | `01, uvar(key)` |
| String key | `uvar((length << 2) | 2), key_bytes` |
| List index | `uvar((index << 2) | 3)` |

The index has 62 usable bits, above the container-count limit. The numeric escape reserves no key. The reference accepts `draft.FieldStep` and `draft.NumberStep` as construction aliases and normalizes them on read.

`openCurrentChanges` validates a full batch and resets its cursor. `NextChange` then returns a value and iteration flag without pervasive error results. Paths borrow caller-supplied scratch, growing when needed; a returned path is valid until the next iteration. Span values borrow the input. A checked streaming pass is also benchmarked separately from full validation followed by reading, which necessarily visits metadata twice.

An unknown-size replacement object can be built directly into the batch output with `currentWriter`, then recorded with `RecordSpan(path, start)` and its resulting extent. No child payload buffer is needed. The previous literal-length catalog and inline change formats remain runnable controls; their historical timings describe those formats. Reused writer/output and reader path scratch are allocation-free. A fresh caller-local path array currently escapes through the returning reader API; it is not claimed to be a zero-allocation stack path.

The outer WAL/transaction envelope must supply row identity, encoding version, integrity checks, atomicity, durability, and revision enforcement. `next > base` is only a codec check. Correct replay must check the expected base and handle duplicate delivery. Change application, conflict policy, WAL integration, high-level audit grouping, and compaction are not implemented here. High-level reasons and actions can wrap these low-level changes.

## Protected values and plugins

The model may distinguish ordinary and protected strings while both occupy spans. A sealing layer can replace a span or subtree with an authenticated ciphertext envelope. Separately stored keys can then be discarded without changing outer framing. Unknown-field copying preserves ciphertext without decryption.

Encryption and key management remain separate work. They must include changes, indexes, backups, and snapshots. Cleartext dictionary keys can expose information even if their values are sealed.

Runtime plugins can declare models, obtain field handles, and call host operations by key or path. The host validates values and controls transactions and permissions. Go pointers and borrowed database slices do not cross that boundary. Plugin execution and its ABI are not part of this codec experiment.

## Performance and limits

The audit covers all 100 registered backend tables/maps. A bounded typed sample supplies 189 anonymized replay shapes. Synthetic keys preserve lengths and uniqueness; original strings, IDs and Dictionary prefixes are absent. Selected rows preserve scalar widths, sparsity, Lists, Dictionaries and nesting. Access and edit sequences are explicit scenarios, not measured production frequencies.

The current cross-format row-lookup comparisons use actual borrowed Bolt mmap values and touch all payload pages before timing. Finding a value through Bolt alone does not guarantee its overflow pages are resident; that mistake materially affected an early server run and is documented with process fault counters. Current comparisons measure warm memory behavior, not physical disk-cold I/O.

The two layouts and existing Immutable differ materially in size. The nearly equal Compact selector experiments do not imply every representation is the same size. Strings, binary payloads and Dictionary keys account for 84.93% of bytes in the selected replay, so tiny selector changes have limited scope. Eight-byte scalar words and short strings are common; rare large spans still account for substantial volume. There is no universal size or speed guarantee against MessagePack.

The preceding 2026-10-16 candidate established the following results. These numbers belong to that frozen implementation; the direct-tag/signed candidate has a separate [2026-10-17 comparison](results/signed-tags-20261017.md):

- Fused reads avoid large returned header structs and substantially improve ordinary Fast access without fixed slots. Smaller keys help cache locality; forcing eight-byte keys does not cure the original gap.
- Moving the Object key directory next to the trailing header improves the server's flat reads without the front header's nested regression. It preserves byte counts, unknown-field handling and incremental writing. The final paired server run measures Fast / Immutable at 151 / 258 ns for local128, 266 / 263 ns for global128, and 173 / 209 ns for nested numeric reads. The global-key result is close in this matched control; other cohorts and the differently structured key-base controls show larger gaps, so universal parity is not established.
- Fused planning halves the complete deep 70% copy/patch cost on the server: 319 097 → 158 114 ns. A pointer-free eight-byte plan is nearly as fast under reuse and reduces fresh plan allocation from 119 392 B / twelve calls to 9 472 B / one call. Failed eligibility and rebuilding remain included in their own controls.
- Sorted small overlays remain competitive after full materialization. Appending is cheap to stage but needs normalization later; sorting after every interleaved read is expensive.
- A completed leaf batch avoids constructing an editor tree: server deep 70% with a cold editor improves 854 488 → 488 792 ns, and construction improves 1 070 485 → 585 297 ns. Construction allocations fall from 1 833 to one. These cases reuse writer/output. Wide shallow batches can lose, so the interactive editor remains available.
- Prepared shapes offer a separate repeated-work path. Deep binding + writing is 425 378 ns with binary path lookup and 145 206 ns with a prepared path map. Shape and sized-arena index preparation + binding + writing costs 724 413 ns. No preparation cost is hidden in the recommendation.
- Reused writers and overlays can allocate zero. Fresh editors allocate. Large local scratch arrays may escape to the heap; the small eight-edit patch-plan control stays allocation-free with reusable output.

Validation and the current candidate’s paired timings are recorded in the [signed/tag report](results/signed-tags-20261017.md). Exact byte tests cover positive immediates, sign extension, unknown widths and zero/delete semantics. These checks validate the research implementation, not production integration.

All human-readable timing tables use ns and space-grouped integers. Exact repetitions, allocation counts, encoded sizes, source archives and machine details are in the [performance report](results/performance-iteration-20261016.md), [continuation report](results/performance-continuation-20261016.md) and [benchmark inventory](BENCHMARKS.md). The [earlier complete comparison](results/common-cross-hardware-20261016.md), [selector/slot comparison](results/next-iteration-20261016.md), [typed distribution study](results/workload-distributions-20261016.md), and [design lessons](results/workload-design-lessons-20261016.md) retain their original scope.

## Remaining integration work

The research implements views, bounded validation, forward construction, native struct mapping, sparse recursive edits, dense controls, owned patching, presence/selected-slot sketches, and logical change encoding/reading. It does not yet provide one production package combining those pieces.

Production work includes integrating the native binder and model facade with database transactions, stable schema catalogs, a public schema-bound view/editor, selected-slot integration, ordered change application and WAL, migration, sealing, and a plugin ABI. The physical patch controls accept resolved leaf updates; they are not a substitute for an ordered change applier handling ancestor replacement and List splices. Borrowed ownership and revision checks must remain explicit.

Adoption needs a distinct row encoding identifier and an explicit migration. Removing ZigZag changes signed scalar meaning even where the Fast directory bytes are unchanged. Old signed values must be decoded under the old row codec and rewritten using their model; an unknown old scalar cannot safely be guessed signed or unsigned during that conversion. Retaining the old row codec remains necessary when migration lacks the required model. Unknown spans may contain child containers in the old row codec, so changing that codec cannot simply copy arbitrary unknown spans into a new version and assume they changed too. No production EDB bytes are modified by these experiments. The research recommendation remains open to review, including every faster option recorded with additional costs.
