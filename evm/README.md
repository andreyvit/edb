# EVM

Run Go plugins inside a Go application. Compile against the APIs you expose,
link their implementations, then create a VM session. Swap linked programs when
you activate a new plugin; existing sessions retain their program.

EVM supports sequential Go: structs, collections, closures, interfaces,
concrete generic instances, range functions and defer/panic/recover. It excludes
goroutines, channels, `unsafe` and arbitrary native imports. Source is limited
to 1 MiB. Unsupported constructs fail compilation.

## Compile, link, run

```go
module, err := evmcompiler.Compile("bonus.go", `
func Bonus(points int64) int64 {
    if points < 1000 { return 0 }
    return points / 10
}`)
if err != nil { return err }
program, err := evm.Link(module)
if err != nil { return err }
vm := evm.New(program)
var out [1]values.Value
err = vm.Run(ctx, "Bonus", []values.Value{values.Int(1500)}, out[:])
if err != nil { return err }
bonus := out[0].AsInt() // 150
```

A linked program is immutable and shareable. A VM owns registers and globals;
it is neither concurrent nor reentrant. Initialization runs once before the
first entry. Later calls retain globals. `Reset()` clears session state and
makes the next call initialize again. Execution failure invalidates the session
until reset. Results are written only on success; host side effects are not
rolled back by the VM.

Arguments/results use [`values.Value`](../values/README.md). Their types come
from the entry signature. Keep program-local interfaces and function references
with their program; repack them through the destination program's APIs when
crossing that boundary.

## Host APIs

Use [`evmbind`](evmbind/README.md) for a Go API package:

```go
module, err := evmcompiler.Compile("plugin.go", source, schema.Package())
if err != nil { return err }
program, err := evm.Link(module, schema.Bind(host))
```

`schema.Package()` supplies declarations; `schema.Bind(host)` supplies their
implementations. Reuse the schema so nominal type identities match. Generated
bindings call Go directly; reflection is the fallback. Native pointers retain
their identity and mutations. The host must synchronize concurrent access.

Manual APIs use `Package.Types`, `Functions` and `Methods`; see
[binding declarations](_docs/BINDINGS.md). Only supplied exports satisfy imports.
A `Call` and its argument/result windows are borrowed for that callback. Read
arguments before overwriting overlapping result slots; never retain the windows.
Host callbacks must honor cancellation themselves.

A package may also supply Go `Source`. Its shared scalar types and methods can
compile as guest code; see [shared scalar source](#shared-scalar-source).
`PackageSource`, `PackagePaths` and `PackagesSource` print the script-facing API
for tooling or an agent writing a plugin.

## Limits and errors

`VM.Limits` sets session limits. `RunWithBudget` sets invocation limits, including
initialization. Zero fields select defaults or inherit the session limit.
`Budget.Duration` and the context bound elapsed time cooperatively; native calls
must return or observe cancellation. Instruction, depth, register, allocation
and output limits cover VM execution. Allocation accounting is cumulative until
reset and includes transient capacity; it is not live heap size. Native APIs
remain responsible for their own work and allocations.

Script panics and language errors can be recovered by a direct deferred
function. Uncaught errors retain source positions and unwrap their cause.
Budget, cancellation, invalid-value and native callback errors are fatal and skip
script defers. Native Go panics propagate after VM cleanup. The host owns rollback
and resource cleanup.

Bytecode and native bindings are trusted implementation inputs. Link validates
operands and layouts; it cannot prove arbitrary pointers safe. Accept script
source through the compiler, not uploaded raw bytecode. EVM is not a process
isolation boundary.

## Standard library and output

Imports expose selected `fmt`, `log`, `math`, `strings`, `strconv` and `errors`
APIs. Unlisted symbols fail type checking. Formatting supports flags, aggregates,
custom formatting methods and wrapped errors. `errors.As` is not exposed.
`log` has no process-exit or configuration APIs.

`VM.Output func(string) error` receives print/log output; nil uses Go's logger.
Sink errors abort execution. `Limits.OutputBytes` defaults to 1 MiB and caps both
a formatted value and total output per invocation, including initialization.
Library callbacks share the VM's execution limits.

## KVO records

Create a model package once:

```go
models, err := evm.ModelPackage("m", schema)
```

A customer model exposes `m.Customer` and `m.CustomerEdit`. Selectors come from
the model, including local and qualified aliases. The host supplies lookup,
scan and save operations; model registration does not grant database access.

A cursor wrapper can call `call.SetRecordResult(model, cursor.Record())`.
Immutable records travel as 16-byte pairs, including through `any`. Field reads
use their shared representation metadata; they need no read-record arena or
expiry check. Generated and reflected native records use the same script API.
Use [registered record methods](evmbind/README.md#explicit-record-methods) to
expose a Go helper as `customer.PointBalance()`.

Mutable editors require `evm.WithRecords(ctx)` inside the transaction. Register
an editor through `scope.EditRecord`; recover/check it through
`scope.MutableRecord` in the save wrapper. Flush pending materialized collections
before saving. Saving and committing remain host operations.

`ToJSON()` returns detached JSON-compatible data. It traverses records directly,
preserves integer precision and invokes custom JSON/text marshalers at scalar
leaves. It does not serialize and reparse the whole record. `Program.JSONValue`
and `Call.JSONValue` convert VM `any` to an owned Go tree; numbers are
`json.Number`. The call form charges the allocation budget. Their byte limit
counts equivalent escaped JSON output. `EncodeJSON` writes actual JSON bytes.

## Declaring borrowing effects

Host declarations can make ownership analysis more precise:

```go
readEffects := &evm.OwnershipEffects{
    NoRetain: true, NoInvalidate: true,
    Borrowed: []evm.BorrowSource{{Result: 0, Domain: "db.tx", ScopeParameter: 1}},
}
endEffects := &evm.OwnershipEffects{
    NoRetain: true,
    Ends: []evm.ScopeEnd{{Domain: "db.tx", ScopeParameter: 1}},
}
writeEffects := &evm.OwnershipEffects{
    NoRetain: true,
    Callbacks: []evm.ScopedCallback{{Argument: 1, Domain: "db.tx"}},
}
```

Attach these to `ExternalFunction.Ownership` or `ExternalMethod.Ownership`.
Results and callback arguments are zero-based; scope parameters are one-based,
including a receiver, with zero meaning the enclosing scope. Only promise
`NoInvalidate` when the operation cannot close or replace existing borrowed
storage. A cursor advancing to another transaction must not promise it.
`NoRetain` forbids keeping arguments beyond the synchronous call. Effects are
part of the compiled binding contract: Link rejects mismatches.

Register another borrowable host type with `values.NewOwnedType` and its
budgeted ownership callback. Immutable record types provide this automatically.
Unknown calls conservatively retain arguments and invalidate live borrows.
Simple read-only guest helpers are analyzed. Scope-local flat arrays, slices
and maps remain borrowed when all aliases stay local and no call invalidates
the scope. Other containers own their borrowed leaves when inserted; opaque
borrowed types copy at acquisition. `Function.OwnershipSites` explains inserted
operations. Scope matching treats all potentially aliased instances
of a domain conservatively; nested callbacks remain safe through leaf copying.

Read-only KVO handles represent snapshots, not stable database identity. Owning
a snapshot can change its representation/address; use an explicitly carried
primary key for identity or map keys that must survive an ownership boundary.
Explicit Go struct copies (`copy := *row`) allocate VM struct storage and own
their payload; ordinary handle assignment and interface packing do not box.
Returned strings are conservatively copied even when their originating record
was already owned. Materialized collection fields own their contents.

Raw Go access and handwritten bytecode do not get compiler ownership analysis.
Use `kvo.Record.Own()` before retaining encoded bytes beyond a transaction.
Host effects must describe actual invalidation; no runtime lease or per-record
transaction bookkeeping can repair an incorrect declaration.

## Shared scalar source

A package can provide `Source` instead of native declarations. Its functions
and methods compile as guest Go code. `SharedTypes` preserves a native scalar's
type identity across host signatures and KVO fields:

```go
idType := values.NewType("ids.ID", values.Raw)
ids := evm.Package{
    Path: "ids",
    Source: idsSource, // the same Go scalar/method source used by the host
    SharedTypes: []evm.SharedType{{
        Name: "ID", Native: reflect.TypeFor[ID](), Type: idType,
    }},
}
```

Register `ids` with the model package for compilation and linking. A KVO word
whose native type is `ID` appears as `ids.ID`, including inside collections.
Its methods work even when the script imports only the model package. Shared
source must use supported EVM imports and cannot declare globals or init
functions. The shared type's scalar kind must match its native declaration.
Keep host-specific integrations in separate files; embed the shared source.

## Internals and tests

[Inside EVM](_docs/EVMBook.md) covers values, instruction encoding, compilation,
calling conventions, the evaluator and optimization. [`evmasm`](evmasm) is the
assembler for tests and diagnostics; bytecode has no persisted compatibility
format. [Benchmarks](bench/README.md) separate execution, compilation and database
costs.

```sh
go test ./evm/... ./values/... ./nativebind/...
go run ./evm/EVMBook-rebuild.go
```
