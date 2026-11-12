# Source-defined host bindings

Build one schema with `New(guestPath, sourceFS, options)`. Supply a directory of
self-contained Go API declarations. New parses and type-checks its Go files,
registers exported types, fields, methods, constants and package functions,
and validates the native signatures. A host receiver implements each package
function as a method with a leading *evm.Call. Options.Types supplies native
type roots that cannot be discovered through host signatures, such as an entry
interface. Concrete context types register with Pointer[T]().

Compile scripts against schema.Package(). For each distinct host receiver,
link the module against schema.Bind(receiver). Create as many sessions from a
linked program as needed. A schema owns type identities. A program owns the
numeric dynamic type IDs used during execution.

## Generate bindings

Generate direct Go wrappers in a separate package:

    go run github.com/andreyvit/edb/evm/evmbind/cmd/evmbind \
        -dir ../api -path demo -native example.org/project/api \
        -package apibindings -o bindings_gen.go

Pass &apibindings.Bindings in Options.Static. Automatic selects generated
wrappers when the source fingerprint and complete set of wrapper names match.
Missing or stale bindings select reflection. StaticOnly panics on mismatch.
The evmreflect build tag excludes generated Go references, so native API
renames/removals can be developed before regeneration. A package imported in
both builds can supply a zero Static under that tag, as Fire's bhbindings does.

## Native values

Pass a host interface argument with schema.Interface(program, ptr). The concrete
type must have been registered and implement a declared interface. R contains
the program's nonzero type index. P holds the original, GC-visible Go pointer.
A typed nil keeps its type index. Both dispatch paths expose only declared
interface methods, and the generated path calls the native interface directly.
Context values passed back to native code must originate from a registered host
implementation. Repack using the destination program when crossing programs.
Use `PointerForCall` when a receiver needs the current VM call to open or
close a budget scope. It can return a shallow receiver copy for that call;
the original pointer stays in the guest interface. Embedded host interfaces
can be passed to procedures accepting just the smaller method set.

The data boundary supports bools, integers, floats, strings, named
scalars, nested data structs with exported fields, and pointer fields to data
structs or scalars. Data structs cross by value. Host pointers retain their
original Go address: guest fields, aliases, escaped field addresses, and native
methods read and mutate the same object. `schema.NativePointer(ptr)` supplies
a registered struct pointer as a guest entry argument. This borrows the native
object; callers must synchronize concurrent access.

Guest allocations of registered data structs/scalars and their globals use
native Go storage too. Other guest addresses backed by VM cells retain the
call-scoped temporary/copyback boundary. Host interfaces carry opaque pointers
and expose methods.
Host arguments may also be `[]string` and `map[string]any`. Both bindings
give the host an independent copy. The map accepts JSON values up to 1 MiB
per argument and decodes numbers as `json.Number`. Other collections,
collection results, collection/interface fields, embedded data fields, variadic host
functions and generic host declarations require additional codecs. Unsupported declarations
fail schema creation. These limits concern host bindings; guest scripts retain
the VM's existing language and collection support.

## Direct calls and fields

A generated wrapper qualifies for
a function with up to two arguments or a pointer method with up to one explicit
argument and zero or one result. Host functions and registered record methods
share the same signature classifier and scalar conversion generator. Supported
values are words (including named scalars and float bits), strings and true pointers. The compiler emits the instruction
for the wrapper’s declared signature. Reflection and older generated wrappers
continue to use their existing call ABI. Interface methods use virtual dispatch.
For a handwritten binding, `Direct` alone also supports function values and
deferred calls. Supply `Call` alongside it to provide your own window wrapper.

A generated pointer wrapper passes native addresses directly. Only VM-cell
arguments need a temporary and copyback. The VM charges a
returned string once and keeps its Go data pointer visible to the GC. For a
handwritten direct binding that returns a KVO row, use `Call.RecordValue` to
construct its typed pair. A direct callback may return a conversion
or execution error. Expose fallible application operations through `Native`.

Generated output also supplies native struct field operations. `NativeFields`
returns the shared `nativebind.Struct`; `NativeType` returns its nominal
`values.Type`. KVO `Binding.WithNativeSchema` and `Projection.WithNativeSchema`
reuse both, so property access and host methods operate on one Go object.
The record codec generator uses the same field generator for types that need
persistence bindings without script exposure. Generated field access, including
escaped scalar addresses, uses typed Go operations; fallback bindings share
nativebind's reflection implementation. Regenerate after changing declarations.

## Explicit record methods

Ordinary Go helpers can become methods of a specific KVO model. The Go symbol,
model and script method name are independent; no naming convention or prefix
stripping controls registration. For example, in a native model package:

```go
func Customer_PointBalance(r kvo.Record) int64 {
    return r.Get(PointsEarned) - r.Get(PointsRedeemed) - r.Get(PointsExpired)
}
```

Declare the script-facing mapping in a separate package:

```go
func RecordMethods() []evmbind.Method {
    return []evmbind.Method{
        evmbind.RecordMethod(nm.MCustomerStatsOverall, "PointBalance",
            nm.Customer_PointBalance, evmbind.BorrowOnly),
    }
}
```

A small `go:generate` program imports that list, calls
`evmbind.GenerateMethods("bindings", RecordMethods())`, and writes the result
to a Go file in package `bindings`. The output package must differ from the
helpers' packages. Pass its generated `MethodBindings` to registration **before
compiling scripts or publishing the package**:

```go
pkg, err := evm.ModelPackage("m", nm.Schema)
if err != nil { panic(err) }
err = evmbind.RegisterMethods(&pkg, RecordMethods(), evmbind.MethodOptions{
    Mode: evmbind.StaticOnly,
    Static: bindings.MethodBindings,
})
if err != nil { panic(err) }
```

Go callers use `nm.Customer_PointBalance(r)`; scripts use
`stats.PointBalance()`. Both execute the same helper over the original 16-byte
record, whether it references Fast2, Compact2, MessagePack, native struct or
in-memory data. Calls do not decode or change representation. The method is
available only on explicitly registered immutable model types; editors have a
separate method set. The same helper can be registered on several models.
Properties, existing methods and duplicate registrations cannot be shadowed.
Registration errors leave the package unchanged.

`BorrowOnly` is a trusted promise: the helper must neither retain its arguments
nor invalidate their backing storage, directly or through calls it makes. Use
it for read-only calculations such as this balance helper. It prevents ownership
analysis from inserting unnecessary copies. Omit it for conservative behavior;
`NoRetain` and `NoInvalidate` can also be specified separately. This metadata is
not inferred from Go code.

Generated adapters call Go functions directly, with no reflection or record
boxing. `StaticOnly` rejects missing or stale adapters; `Automatic` can fall
back to reflective calls, which allocate. Regenerate after changing a helper's
signature. Generation requires exported top-level functions, with
`kvo.Record` first, scalar arguments and scalar results. Supported scalars
are bool, integers (excluding uintptr), floats and strings, including named Go
types. Guest signatures use their underlying scalar types. Variadic functions,
collections, record results, closures and bound Go methods are not supported
by this generator. Signatures beyond the direct argument/result limits use the
ordinary call window; bound methods, method expressions, interface calls
and deferred calls work too.

String results are copied and charged to the VM allocation budget so they can
safely outlive borrowed record storage. Numeric/bool calculations can remain
allocation-free. A helper's own allocations are its responsibility. Generated
files are excluded by `evmreflect`; supply an empty `StaticMethods` and use
`Automatic` under that tag if reflection-only development builds are desired.

See [record-method benchmarks](../_docs/BENCH-RECORD-METHODS.md) and the executable
generation example in `internal/methodfixture/gen`.

See [binding consolidation benchmarks and API guide](../_docs/BINDINGS.md) for
void calls, metadata reuse, and manual Shape/DirectBinding declarations.
