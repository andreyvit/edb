# EDB replication-log consumer contract

This document specifies admission, synchronous staging of borrowed changes,
ordered publication, consumer-owned memory, durable storage, filtering, and
replay. It describes the current EDB API; it does not implement a
replication-log package or define a final byte format.

## 1. Model and responsibility boundary

The source has an asynchronous **post-commit replication log**, not a source
write-ahead log:

```text
Reserve an ordered consumer slot
    → stage each borrowed change in consumer-owned memory
    → validate and commit the physical EDB transaction
    → publish transaction ID and schema revision
    → append and sync the committed replication log
```

Staging occurs during the source write transaction, after a successful primary
data mutation and before its leaf returns. A later error can still roll back the
whole physical transaction. Staged data must not become a committed/replayable
log group before `Publish`.

EDB owns:

- Detecting persisted puts/deletes and exposing their borrowed encoded bytes.
- Grouping changes by physical Bolt transaction, including batched callbacks.
- Transaction IDs, record ordinals, and pre-commit failure/rollback guards.
- Post-commit metadata publication or aborted-attempt cancellation.
- One committed replication transaction ID per database.
- Schema name hashes and collision checks.
- Atomic replay of data, destination-local indexes, and the checkpoint.

The consumer/controller owns:

- Ordered reservation slots and all memory used to retain staged changes.
- Copying or encoding borrowed bytes synchronously during `Stage`.
- Per-group, queued-byte, slot, encoding, and retention limits.
- Reclamation and pooling of staged, cancelled, published, and encoded buffers.
- Log framing, versioning, checksums, append, sync, recovery, rotation, and
  retention.
- History identity, subscription continuity, projection, transport, and
  authentication.
- Application job gating, valid writer handoff, and gap recovery policy.

Capture/apply require Bolt. The consumer owns pending replication storage;
EDB owns its normal row-encoding and transaction buffers. Raw Bolt escape-hatch
writes bypass this contract.

## 2. Configuration and entry points

Relevant definitions in package `edb`; unrelated open options are omitted:

```go
type Options struct {
    LocalWrites   LocalWritePolicy
    RowMigrations RowMigrationPolicy
    Replication   *ReplicationOptions
}

type LocalWritePolicy struct {
    Mode    LocalWriteMode
    Include func(ReplicationObject) bool
}

type LocalWriteMode uint8

const (
    AllowLocalWrites LocalWriteMode = iota
    RejectLocalWrites
)

type RowMigrationPolicy uint8

const (
    AllowRowMigrations RowMigrationPolicy = iota
    SkipRowMigrations
)

type ReplicationOptions struct {
    Capture *ReplicationCaptureOptions
    Apply   *ReplicationApplyOptions
}

type ReplicationPoisoner interface {
    Poison(error)
}

type ReplicationCaptureOptions struct {
    BestEffort bool
    NextTransactionID func(lastCommittedID uint64) (uint64, error)
    SchemaRev uint64
    Include   func(ReplicationObject) bool
    Shard     func(ReplicationObject, []byte, ReplicationValue) ([]byte, error)
    Sink      ReplicationSink
}

type ReplicationApplyOptions struct {
    SchemaRev        uint64
    SupportsRevision func(uint64) bool
}

func Open(path string, schema *Schema, opt Options) (*DB, error)
func (db *DB) ApplyReplication(group *ReplicationGroup) error
func (db *DB) ReplicationStatus() (committedID uint64, err error)
func (db *DB) AllowsLocalWrites() bool
func (db *DB) AllowsRowMigrations() bool
```

Capture/apply are independent capabilities. Neither changes local-write or
row-migration policy implicitly. Zero policies allow both. Nil replication
config, or both capabilities nil, disables replication. Policies also work
without replication, including on memory storage.

Capture requires a sink and shard resolver. Nil `Include` includes all covered
objects. Consumer admission and staging enforce capacity.

Optional `NextTransactionID` replaces the default committed-checkpoint-plus-one
allocator. It runs once per captured physical attempt, after sink reservation,
under Bolt's serialized writer. It receives the committed checkpoint visible
in that transaction and must return a strictly greater ID. The new checkpoint
is persisted atomically with data; aborted attempts do not advance it. The
callback must be synchronous, memory-only, and must not reenter EDB. In the
default strict policy, an error, panic, zero ID, or non-advancing ID forces
rollback even if caught or paired with `CommitDespiteError`. Apply preserves upstream IDs without calling this
allocator. EDB imposes no timestamp encoding and does not import an app's ID
package. Applications can supply a checkpoint-seeded time/node ID allocator.

`BestEffort` is an explicit, non-fatal capture policy. It requires
that Sink implement `ReplicationPoisoner`. Capture-only callback errors/panics
are recovered, poison the sink, and do not abort source writes. Real source
storage errors and incomplete source mutations still fail normally. Poison
handlers must disable durable publication of partial groups, be memory-only,
and must not reenter EDB; handler panics are contained. Optional
`ReplicationError() error` on the sink lets EDB observe hidden/asynchronous sink
failures on source writes.

`DB.ReplicationPoisoned()` reports runtime poison only. Legacy stored poison
flags are ignored. If capture fails before allocating an ID, the write still
advances the checkpoint so a missing group can be detected after restart.
`ReadReplicationStatus(path)` reads the checkpoint without schema/migration
writes for controller preflight. BestEffort publication, cancellation, and
drain panics/errors poison/report instead of escaping. The controller must
check the source/log gap on restart. No automatic reseed is provided.

An already-poisoned BestEffort sink may return an owned shard-only snapshot,
omitting key/value payload bytes that will not be stored. This avoids copying
large values merely to discard them. Shard ownership/validation still applies;
such snapshots must never be treated as replayable records. Strict snapshots
and the normal successful capture path keep the full contract below. Configuration is
copied at open; callbacks, schemas, and descriptors must remain immutable while
in use.

The sink must be ready before `Open`: startup row writes may be staged before
open returns. Sink callbacks must not depend on obtaining a database handle from
the completed open call.

A read-only replica normally selects `LocalWritePolicy{Mode: RejectLocalWrites}`,
`SkipRowMigrations`, and apply options explicitly. In reject mode, an optional
`Include` predicate admits local mutations on matching objects only. A denied
mutation aborts the whole transaction even if the panic is recovered.
`AllowsLocalWrites()` reports unrestricted writes, not individual exceptions.
The zero policy permits unrestricted writes; an `Include` predicate with allow
mode is invalid. Local admission does not filter capture or replay. Omit local-only
objects from source capture separately. Writes on an apply-only DB do not advance
the replay checkpoint. Write fencing alone does not disable
otherwise-allowed startup migrations. The app separately gates its jobs and
external side effects. Local DDL and index-only maintenance remain allowed.

## 3. Changes and groups

```go
type ReplicationGroup struct {
    TransactionID uint64
    SchemaRev     uint64
    Changes       []ReplicationChange
}

type ReplicationChange struct {
    Object  ReplicationObject
    Op      ReplicationOp
    Ordinal uint64
    Key     []byte
    Value   ReplicationValue
    Shard   []byte
}

type ReplicationValue struct {
    Present   bool
    Flags     uint64
    SchemaVer uint64
    ModCount  uint64
    Data      []byte
}

type ReplicationOp uint8

const (
    ReplicationPut ReplicationOp = iota + 1
    ReplicationDelete
)

type ReplicationObject struct {
    Table   *TableDef
    KVTable *KVTable
    Map     *KVMap
}

type ReplicationObjectKind uint8

const (
    ReplicationInvalidObject ReplicationObjectKind = iota
    ReplicationRowTable
    ReplicationKVTable
    ReplicationSingletonMap
)

func (o ReplicationObject) Kind() ReplicationObjectKind
func (o ReplicationObject) NameHash() uint64
```

`ReplicationGroup` is a consumer-owned replay container. Copying a group,
change, or value copies slice headers, not their bytes. The consumer controls
lifetime and reclamation.

One source transaction means one physical Bolt write transaction, not one app
callback. Callbacks sharing a physical batch share one reservation, ID and
ordinal space. Retries use fresh attempt state and reservations. Aborted
proposed IDs can be reused.

Changes follow actual primary data-mutation order, including nested handlers.
Repeated changes to one key are not coalesced. Unfiltered ordinals start at
zero. Existing row no-ops and missing normal row deletes emit nothing; raw APIs
retain their existing semantics. Excluded-only, status-only and index-only work
allocate no capture ID.

Publication follows physical commit, not the app's returned error.
`CommitDespiteError` can commit and publish while the app receives an error;
capture or incomplete-mutation failures still force rollback.

| Field | Meaning |
|---|---|
| `SchemaRev` | Global compatibility revision for the transaction. |
| `SchemaVer` | Source row's stored table-schema version. |
| `Flags` | Source row codec/envelope flags. |
| `ModCount` | Source row modification count. |
| Row `Data` | Exact encoded payload, without the source index tail. |
| Raw-KV/singleton `Data` | Exact raw value bytes. |
| `Present` | Distinguishes an absent value from an empty/nil put. |
| `Shard` | One resolved shard; successful empty attribution means global. |

`Op`, not nilness/length, determines put versus delete. Raw-KV nil input
deletes; singleton nil input puts. Singleton delete is unsupported. A codec
upgrade may emit a put without changing `SchemaVer` or `ModCount`; do not
deduplicate by those counters alone. Log format version, schema revision, row
version and codec are distinct.

## 4. Sink and reservation interface

```go
type ReplicationSink interface {
    Reserve() (ReplicationReservation, error)
    Drain() error
}

type ReplicationReservation interface {
    Stage(ReplicationChange) (ReplicationChange, error)
    Publish(transactionID, schemaRev uint64)
    Cancel()
}
```

Use one ordered sink queue per database. All methods must tolerate concurrent
worker activity, publications and cancellations. Calls for separate physical
attempts can overlap after writer-lock release. Each attempt needs an
independent token and staging state; a single mutable token reused across live
attempts is invalid.

`Stage` is synchronous, memory-only pre-commit work. `Publish` is metadata-only
post-commit work. The consumer can encode directly into its eventual log
representation during staging; no generic intermediate group copy is required.

## 5. Borrowed input and snapshot result

The `Stage` argument is a borrowed view. `Key`, `Value.Data` and `Shard` can
reference cursor storage, EDB encoding/key pools, editor storage or resolver
scratch. Their validity is limited to the staging call. No byte slice may be
retained for later copying or encoding.

Before returning successfully, the consumer must copy or encode every needed
byte into storage it owns. Schema descriptors are shared immutable objects;
encoding `Kind` and `NameHash` avoids retaining them in the stored
representation.

The returned change is a read-only consumer-owned snapshot of the submitted
change. In particular:

- Returned `Shard` must equal the submitted shard and must not alias borrowed
  input.
- Snapshot bytes must remain valid until the next `Stage` on that reservation or
  a terminal `Publish`/`Cancel` call.
- EDB currently reads the returned `Shard` immediately to validate an
  overwrite's old shard attribution. It finishes that check before the next
  ordinary staging call.
- Returning the original argument unchanged is invalid when its slices still
  borrow source memory.
- A consumer can return slice views into its encoded buffer. The result does not
  require a second generic payload copy.

EDB does not retain the snapshot as a pending group. Once the immediate
validation finishes, the consumer remains the sole owner of all retained change
storage.

## 6. Pre-commit staging lifecycle

On the first included mutation in a physical attempt, EDB reserves a token,
allocates an ID in the same DB transaction, and registers the physical commit
handler. It stages each successful primary mutation before the leaf returns and
before its notification handler runs. Later index/storage/handler work can still
fail the attempt.

A typical lifecycle is:

```text
Reserve
Stage(change 0) → owned snapshot → EDB validation
Stage(change 1) → owned snapshot → EDB validation
...
DB commit
Publish(transaction ID, schema revision)
```

Failure instead produces:

```text
Reserve
Stage(change 0)
Stage(change 1) → error or panic
DB rollback
Cancel
```

A staging error, panic or returned-snapshot shard mismatch poisons the physical
attempt. Catching the panic or requesting `CommitDespiteError` cannot make it
commit. A consumer may have partially encoded the failing change; `Cancel` must
discard the whole attempt, not only that record.

Exactly one terminal call follows each successful reservation in normal
execution. A crash can interrupt the RAM-only lifecycle. Manual transactions
must be closed after rollback/commit failure so ownership cleanup can finish.

## 7. Ordered reservations and publication

**EDB calls `Reserve` synchronously while holding Bolt's serialized writer.
Successful reservation order is physical capture-attempt order.** The consumer
must enqueue a pending slot inside `Reserve`, before returning. EDB reserves at
most once per nonempty attempt.

Staging does not make a slot ready for append. Commit callbacks can arrive out
of order after Bolt releases the writer:

```text
Reserve A → slot 1 pending; stage A
Commit A
Reserve B → slot 2 pending; stage B
Commit B
Publish B → slot 2 ready; slot 1 still pending
Publish A → slot 1 ready
Append    → A, then B
```

Cancellation can also arrive after a later publication:

```text
Reserve A → slot 1 pending
Abort A
Reserve B → slot 2 pending; stage B
Publish B → cannot pass slot 1
Cancel A  → retire slot 1; B becomes eligible
```

Slots are ordering tokens, not persistent transaction IDs. An aborted attempt
consumes no log transaction; a retry receives a new slot and may reuse the
proposed ID. Cancellation can be delayed until physical-attempt or outer-batch
ownership cleanup proves abort.

Valid transitions are:

```text
pending/staging → published(transactionID, schemaRev)
pending/staging → cancelled
```

A terminal call is not a new queue position. Slot order, not callback arrival
order, establishes the safe append prefix.

## 8. Method requirements

### Reserve

- Admit bounded staging capacity and enqueue a pending slot synchronously.
- Return a non-nil token with nil error, or an error with no accepted slot.
  Clean up partial admission before returning an error.
- Reject if failed/stopping/out of capacity. Do not perform I/O, reenter EDB or
  depend on commit/drain of the current attempt.
- No payload size is supplied. Admission can reserve a per-attempt allowance or
  use bounded incremental accounting during `Stage`.

### Stage

- Copy/encode borrowed bytes synchronously; return the owned snapshot before
  input expires.
- Enforce group, shared-byte, queue and encoding limits before exceeding them.
- Return an error rather than depending on the current uncommitted attempt's own
  drain. The attempt cannot publish until staging and validation succeed.
- Perform no disk/network I/O or DB reentry. CPU work executes under the source
  writer; heavyweight compression can be deferred after ownership is secured.
- Keep staged data uncommitted/invisible to replay and durable acknowledgements
  until `Publish`.
- Errors may leave partial staging, but `Cancel` must reclaim it. Valid
  implementations should report normal failures as errors rather than panic.

### Publish

- Supply only the final transaction ID and schema revision to the existing
  token.
- Seal/fill reserved header/framing metadata, mark the slot ready, signal the
  worker and return.
- No payload ownership transfer occurs here: the consumer already owns it from
  staging.
- No I/O, DB reentry, queue-full/encoding-size rejection, or dependency on
  earlier slot progress is allowed. Capacity for metadata finalization must
  already exist.
- Valid publication cannot fail or panic, including during shutdown.

### Cancel

- Discard all staged data and reclaim its memory and admission budget, including
  after a partial `Stage` failure.
- Mark the ordering slot cancelled and signal progress. Append nothing and
  advance no logical/durable cursor.
- Reclaim a cancelled attempt's capacity even when an earlier slot is pending. A
  retry must not be blocked by budget that belongs to an already-cancelled
  attempt.
- Bound queue bookkeeping too: unlink cancelled slots safely or use bounded
  tombstone storage. This must not change surviving slot order.
- No I/O, DB reentry or dependency on earlier slot progress is allowed. Double
  termination is a programming error, not a normal retry.

## 9. Consumer memory and worker rules

The consumer owns staged and published payload storage. `Stage` is the
ownership boundary; source transaction buffers cannot serve as consumer
storage.

The consumer bounds uncommitted staging, published queues, encoded overhead,
reader retention and pools. It decides its per-group/global limits and retention
caps. `ErrReplicationGroupSize` and `ErrReplicationOverload` are available error
values for consumer rejection; EDB does not impose their thresholds.

Retire or pool owned buffers only when no worker, journal or reader references
them. Source input can be reused as soon as staging returns; published consumer
data must remain valid until its consumers finish.

Only the ordered queue head may advance:

| Head state | Worker action |
|---|---|
| Pending/staging | Park; do not pass it. |
| Cancelled | Retire and continue. |
| Published | Append the complete group; retire after success. |

A single append worker is simplest. Do not hold the queue lock during I/O. Keep
the head logically in flight until append succeeds so another worker cannot pass
it. A ready prefix may share one sync; durability advances only through the
complete, ordered, successfully synced prefix.

Do not infer readiness from available IDs, numeric contiguity, timeouts, an
empty published-only list or `DB.ReplicationStatus()`. The DB checkpoint can
advance while an earlier publication is pending. Neither source commit nor
metadata publication is a durable-log acknowledgement.

Parallel post-commit work is allowed only over consumer-owned buffers and must
preserve append/durable-cursor order. Forwarded upstream log data and writer
handoffs need controller serialization; this sink alone does not merge arbitrary
producers.

## 10. Schema-owned IDs and encoding

```go
func (scm *Schema) NameHash(name string) uint64
func (tbl *TableDef) NameHash() uint64
func (tbl *KVTable) NameHash() uint64
func (mp *KVMap) NameHash() uint64
func (scm *Schema) TableByNameHash(hash uint64) *TableDef
func (scm *Schema) KVTableByNameHash(hash uint64) *KVTable
func (scm *Schema) KVMapByNameHash(hash uint64) *KVMap
```

Name ID is xxHash64, seed zero, over UTF-8 `strings.ToLower(name)`. Row tables,
KV tables and singleton maps share one case-insensitive namespace. Hashes are
cached at declaration. Registration/`Include` panic on collisions without
partial publication; there is no automatic salting. Typed lookup returns nil for
unknown/wrong-kind IDs. Renaming changes identity.

Encode `change.Object.Kind()` and `change.Object.NameHash()`, not pointers or
repeated name strings. Hash identity is not schema compatibility. Local index
IDs, definitions and tails are not replicated.

A consumer can stage directly into a final log byte buffer, reserving header
space for metadata supplied at publication. Alternatively it can copy changes
into an owned structure and serialize later. A plain replay container can use
`ReplicationGroup`; no EDB intermediate copy is mandatory.

An illustrative stored change carries kind/hash, operation, original ordinal,
key, shard, presence, codec/row metadata and exact value bytes. The final format
must bound allocation, expose `SchemaRev` before row decode, validate
framing/checksums, and recover only complete groups. Truncated/uncommitted tails
must not become replayable partial transactions. Format version and history
identity belong to consumer metadata, not extra EDB fields.

## 11. Shards and subscription projection

One change has one shard. Put resolves the new value's shard; delete resolves
the old value or key. Missing raw-KV delete still calls the resolver with
`Present == false`: resolve from key or return an error. Successful empty
attribution means global; unknown attribution must fail.

For an overwrite, membership must stay fixed while the object/key exists. EDB
compares old attribution with the owned `Stage` snapshot and returns
`ErrReplicationShardChange` on mismatch. An explicit delete followed by put
represents a move; each event has its own shard.

The consumer owns table/shard projection and filter continuity. Include global
events where required. Preserve original transaction ID, revision, ordinals and
relative order. A filtered group can be empty and still advance replay's
checkpoint. Do not split one group into independently committed row batches or
invent replacement IDs. Apply does not route storage by shard or recompute
source attribution.

## 12. Replay

Before apply, validate history identity, filter definition, complete framing,
checksums, size limits and increasing group order. Resolve kind/hash to
destination schema descriptors. Unknown/wrong-kind mappings are errors, not
valid empty descriptors.

Reconstruct one `ReplicationGroup` and call `ApplyReplication`. Do not replay
through normal business put/save APIs. Caller-owned bytes must remain valid
until return; the decoder can then reuse its storage according to its own
lifetime rules.

EDB applies the group in one physical local transaction, preserving keys,
payload, codec, `SchemaVer` and `ModCount`. Data, local indexes and checkpoint
commit atomically. Apply does not recapture, run row migrators, allocate a new
source ID or invoke app change handlers.

Deliver groups in increasing ID order. EDB does not reorder: IDs at/below the
checkpoint are ignored, including groups delivered too late. Numeric next-ID
contiguity is not required. An empty projected group is valid.

On mapping/schema/codec/index/apply failure, retain the blocked group for retry.
Do not skip it and advance as though it succeeded. Source and destination
snapshots carry the same single checkpoint used for deduplication.

## 13. Schema gate and local indexes

Apply rejects revisions newer than configured `SchemaRev`. Without
`SupportsRevision`, only equal revision is accepted. With the callback,
equal/older revisions require callback acceptance; newer revisions still fail.
Table/codec validation remains separate.

Keep unsupported groups for retry after software/schema upgrade and reopen. Do
not run local row migrators to force data to fit. Receive/spooling policy is
consumer-owned.

Indexes are local only. Put/replace computes destination entries, removes stale
entries and builds a local tail; delete cleans entries. Raw-KV apply uses local
KV indexers. This is affected-row maintenance, not a full-table rebuild per
event. Definitions/ordinals may differ from source. Typed indexers can require
decode; record indexers can use encoded records. Source payload/metadata remain
exact.

Any required index failure rolls back the whole group/checkpoint. Index-only
rebuilds emit nothing. Source unsafe delete skips source index work but its
replicated delete cleans destination indexes. DDL is local through open.
Requested startup builds finish before open returns; reindex runs inside its
write transaction. Readers use normal transaction snapshots.

## 14. Shutdown and failures

Normal shutdown stops new write admission, completes/closes active transactions,
resolves accepted reservations, drains the ordered published prefix, and syncs
before success. EDB tracks admitted writers and invokes sink `Drain` after
admission stops. Drain must also tolerate final terminal callbacks and must not
mistake an empty ready list for completion.

Already-reserved valid publications remain acceptable during shutdown or
consumer failure. New reservations/stages can reject capacity before commit; an
already-committed publication cannot be rejected. Return a drain error if
accepted work cannot be durably completed. Current `DB.Close` has no error
result and panics on drain error. A hard timeout/termination is not a clean
drain.

Post-commit I/O failure cannot roll back the DB. Account for and retain/report
accepted unwritten data, surface failure, and let the controller stop admission
or choose recovery. Never acknowledge failed/unsynced work as durable.

## 15. Checkpoint and recovery policy

```go
committedID, err := db.ReplicationStatus()
```

There is one committed replication ID per database. Capture allocates next;
replay preserves incoming. Both update the same small KVO checkpoint atomically
with data. Uncommitted/aborted attempts are invisible. Missing status is zero.
Status is readable with replication disabled on Bolt; memory status is
unsupported.

Keep these facts distinct:

- DB checkpoint: the data transaction committed.
- Successful staging: the consumer owns an uncommitted representation.
- Publication: that representation is committed and eligible in reservation
  order.
- Append: the complete representation reached the logical log/file buffer.
- Durability: the complete ordered prefix was successfully synced.

No EDB operation reports external log durability. Crash after DB commit and
before log sync can lose history while DB data survives. RAM staging is not a
persistent outbox. The checkpoint cannot reconstruct missing payloads.

The controller validates history and compares the DB ID with its recovered
durable log cursor, including rotated history. It chooses
continue/pause/recover/reseed. EDB accepts no durable-head input and does not
block open for a gap. Filter changes and valid single-writer handoffs are
controller responsibilities. A cascade forwards upstream IDs/log data, not a new
recaptured sequence.

## 16. Error surface

```go
var (
    ErrLocalWriteRejected     error
    ErrReplicationOverload    error
    ErrReplicationGroupSize   error
    ErrReplicationSchema      error
    ErrReplicationShardChange error
    ErrReplicationTransactionID error
    ErrReplicationPoisoned error
)
```

Consumer admission/staging errors and panics, shard mismatch and required
storage failures poison the physical attempt. They cannot be overridden by
`CommitDespiteError`. Pre-write business validation/stale-edit errors retain
normal transaction policy.



## 17. Required consumer tests

- Reversed publication callbacks preserve reservation append order.
- A pending head blocks a later published slot; late cancellation unblocks it.
- Stage input key/data/shard are borrowed; mutating/reusing source buffers
  cannot change consumer-owned data or returned shard snapshots.
- Stage error/panic after partial encoding rolls back all DB data/checkpoint and
  triggers whole-token cancellation, including under `CommitDespiteError`.
- Cancel reclaims memory and admission capacity behind a pending head; retries
  use fresh storage and can reuse a proposed ID without deadlock.
- Queue metadata and buffer pools remain bounded under repeated failures.
- Concurrent publish/cancel/drain does not lose records or report success early.
- Publication finalizes only metadata; no queue-full failure or I/O occurs
  there.
- Header placeholders, count/length fields and exact payloads encode
  consistently before and after publication.
- Hash-only row/KV/map mapping rejects unknown/wrong-kind IDs.
- Explicit nil put/delete semantics, codec upgrades, stable shards and explicit
  delete/put moves survive projection.
- Empty filtered groups, original ordinal gaps, snapshots, duplicate replay and
  noncontiguous IDs remain safe.
- Local index/schema failure leaves data/checkpoint unchanged and allows retry.
- Torn tails and append/sync failure never produce false durability
  acknowledgements.
- DB-ahead-of-log status stays readable; gap policy remains outside
  EDB.
