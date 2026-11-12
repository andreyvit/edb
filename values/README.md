# Shared values

`Value` is two words: `R uint64` and `P unsafe.Pointer`. The operation supplies
its `Type`; presence is separate. `Type` defines nominal identity and the
Raw/Pointer/Pair representation. `Shape` describes storage.

**P must be a real GC-visible pointer or nil.** Never hide pointers in R or put
integer payloads in P.

## Payloads

- Integers: decoded bits in R; narrow signed values are sign-extended.
- Floats: IEEE bits in R; float32 uses the low 32 bits.
- Strings/immutable bytes: length in R, backing pointer in P. Borrowing allocates
  no wrapper; the backing storage must remain valid and immutable.
- Mutable slices: uint32 length/capacity in R, backing pointer in P. Header copies
  preserve aliases.
- Functions: program-local reference index + 1 in R; zero means nil. Closures
  retain immutable capture cells in P; mutable captured variables share cells.
- Time: Unix nanoseconds plus a reserved zero sentinel when they fit; otherwise
  a 12-byte integer payload preserves Unix seconds and nanoseconds. The instant
  immediately before the epoch uses a shared payload to avoid the zero sentinel.
  `AsTime` returns UTC and preserves precision/range, not timezone or monotonic
  readings. Legacy string timestamps remain readable.
- KVO records/collections: compact typed pairs with a physical representation ID.

`TypedPair` packs tag `0110`, a 28-bit representation ID and a 32-bit auxiliary
value in R, with the payload pointer in P. Register immutable Shape metadata
once; IDs are process-lifetime and never reclaimed. An encoded byte span uses
its auxiliary word for length. Concrete scalar code must not interpret arbitrary
numeric bits as tags: its declared type wins.

## Interfaces

`TypeTable` packs/unpacks exact dynamic types. Scalars use marker pointers in P
and retain all 64 payload bits in R. Check markers before interpreting tags.
Only `(R=0, P=nil)` is a nil interface; typed nil, false and numeric zero are not.
Use `Unpack`; P need not point to a box.

| Prefix | Remaining R payload |
|---|---|
| `0000` | 60-bit concrete type ID; P is the direct pointer or fallback box |
| `0100` | 60-bit plain string length |
| `10` / `11` | 32-bit length and 30-bit spare capacity for the selected byte/any slice type |
| `0001` | 16-bit exact type ID and 44-bit string length |
| `0011` | 16-bit exact type ID, 12-bit spare capacity and 32-bit length |
| `0101` | 16-bit exact type ID, 22-bit spare capacity and 22-bit length |

| `0110` | 28-bit representation ID and 32-bit auxiliary value |

Implicit slice tags select exact linked types. Capacity is length + spare.
Values that do not fit use a Value box; functions also box in interfaces.
JSON maps/arrays still allocate their storage even when their interface header
needs no box. Equality/hash respects named types, comparability, NaNs and signed
zero. Program-local type IDs and markers cannot be copied into another program
without repacking.

## Aggregate storage

`NewStructShape` and `NewPointerShape` create nominal identities; construct and
reuse them during setup. The linker snapshots their graph. VM structs/arrays
use flattened, GC-scanned cells. Register aggregate values point to immutable
snapshots; loads/stores copy aggregate cells. Pointer moves preserve aliases.
An extra cell keeps trailing zero-sized field addresses inside the allocation.

`Shape.Stored` borrows a view; snapshot it before exposing an immutable value.
`Shape.Store` copies aggregates and clears unused scalar halves. Never mutate
an aggregate snapshot. Map keys/values must be snapshots, though referenced
maps, slices and pointers retain normal aliases.

Shape helpers operate on VM cells, **not arbitrary native Go pointers**.
`nativebind` supplies Go layouts and field operations. KVO uses these same native
operations. Raw codec fields remain codec metadata; convert them to `Value`
with the declared encoding.

See [EVM](../evm/README.md) and [KVO access](../kvo/_docs/ACCESS.md).
