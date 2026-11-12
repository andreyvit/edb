# GoX

GoX combines EVM Go code and HTML. Each renderer is a named Go function with an
explicit first `*gox.Builder` argument. `.go` and `.gox` files share one package;
functions can call each other in either direction. Parameters normally occupy
separate lines, with trailing commas.

```gox
func Customers(
    b *gox.Builder,
    page *Page,
    customers []*Customer,
) {
    heading := fmt.Sprintf("%s (%d)", page.Title, len(customers))
    <section>
        <h1>
            heading
        </h1>
        <ul>
            for _, customer := range customers {
                <li>
                    customer.Name
                </li>
            }
        </ul>
    </section>
}

func countLabel(n int) string { return fmt.Sprintf("%d", n) }
```

## Syntax

- Put markup at the first non-whitespace `<` on a line. Strings and Go comments
  protect their contents; multiline opening tags continue through `>`.
- Inside elements/fragments, standalone expressions render. Assignments,
  conditions and arguments do not. Outside them, write ordinary Go.
- Quote text. Source indentation emits nothing; string whitespace is preserved.
  Use `<p>{name}</p>` for inline values and `{"{"}` for a literal brace.
- Attributes accept quoted strings with interpolation or `{expression}`.
  Boolean HTML attributes omit false; ARIA attributes retain it.
- JS/CSS bodies retain their syntax. Insert Go data with `{=expression}`.
- Inputs are typed. Inside markup, omitted arguments bind by name; explicit
  inputs win and incompatible matching types fail compilation.
- KVO selectors use model aliases. Go fields also accept unambiguous JSON names
  and snake_case (`AppIDs` → `app_ids`, `URLs` → `urls`).

GoX lowers markup before Go parsing, then uses EVM's compiler and budgets.
HTML, text and CSS share that pipeline but use different escaping contexts.

Use a fragment for output that needs no HTML element:

```gox
func Greeting(b *gox.Builder, name string) {
    <>
        "Hello, "
        name
        "!"
    </>
}
```

Fragments must balance within each branch, loop iteration and component. They
do not create a Go variable scope; use a Go block for that. Inside an element,
attribute helpers called through a fragment still modify that element's pending
opening tag. `<>...</>` also works in text and CSS templates.

## Compile, link, render

```go
tmpl, err := gox.Compile("greeting.gox", `func Greeting(b *gox.Builder, name string) {
    <p>
        "Hello, "
        name
        "!"
    </p>
}`)
if err != nil {
    return err
}
program, err := tmpl.Link()
if err != nil {
    return err
}
html, err := program.Render(ctx, []values.Value{
    values.String("Alice & Bob"),
}, gox.RenderOptions{Strict: true})
// html == "<p>Hello, Alice &amp; Bob!</p>"
```

`Compile` accepts the same `evm.Package` descriptions as EVM's compiler. `Link`
accepts their implementations; the two sets must have compatible signatures.
The standard library and supplied packages autoimport by package basename,
including `fmt`. No `package` or `import` declaration is needed in a GoX file.

`Template.Parameters` gives the entry argument order. Arguments use the existing
EVM value ABI, including host types, script structs and KVO models. Compiled
`Module`, original source positions, `GeneratedSources` and class metadata are
available for tooling. Keep the template's rendering plan with its module.

A linked `Program` supports concurrent renders. Each render gets isolated VM
globals and builder state; registered host implementations retain their own
state. Context cancellation and `RenderOptions.Budget` apply to execution.
Output is capped at 16 MiB by default (`MaxOutputBytes` overrides it). Failed
renders return an error and no partial document.

## Ambient context

`evmruntime.Ambient` is a persistent map of `string` to `any`. A nil pointer is
an empty context. `Get(key)` searches from the newest node to its parents;
`CloneWith(map[string]any{...})` copies a delta map and shares its parent.
An explicit nil value hides the parent value. Stored values are not deep-copied.
`Values()` returns a separate flattened map for interop with older extras APIs.

Pass it as an ordinary optional parameter, usually `ambient *evmruntime.Ambient`.
Calls inside tags and fragments inherit it by name like other parameters. A local
override can use `ambient = ambient.CloneWith(...)`; callers keep their original context.

## Components, slots and helpers

`CompileSources(sources, packages...)` compiles a map of filenames to source text.
Call `program.RenderFunc(ctx, "Page", args, options)` to select a renderer; `args`
omits the builder. `CompileFiles(entry, sources, ...)` also selects a default
renderer for `Render`: `entry` can be a function name or a GoX filename (its first
renderer). Ordinary Go files require their own imports. GoX files autoimport
registered packages. An omitted package declaration inherits the package name
from the other files, defaulting to `plugin`.

Function names identify components: `func Button(...)` is `<Button>`;
`func EarnCard(...)` is `<EarnCard>`. They also work as ordinary function
values: `chosen := Button`, `Button(...)` or `<chosen ... />`.
Tags resolve exact, case-sensitive Go names, including lexical function values
and selectors such as `<ui.Button>` or `<card.Render>`. Component attributes keep
parameter spelling, including camelCase. A callable in scope takes precedence
over a same-named HTML tag; prefer capitalized component names to avoid collisions.
Unknown capitalized tags are compile errors. The full Go signature includes the
builder and every input. See the tested
[page](testdata/examples/page.gox) and [button](testdata/examples/button.gox).

`Compile`, `CompileFiles` and `CompileSources` use the same compiler. Every
renderer declares its function name, builder and inputs; anonymous parameter-list
templates are not supported.

```gox
<Button label={customer.Name}>
    <slot-body>
        "Hello, "
        customer.Name
    </slot-body>
    <slot-after>
        <span>{page.Title}</span>
    </slot-after>
</Button>
```

The button declares `body func()` and `after func()` inputs and calls `body()`
and `after()`. Children without `<slot-...>` wrappers supply `body`. A slot typed
`func(item *Item)` exposes `item` inside that slot, while retaining access to the
caller's lexical variables. Repeated or unknown inputs are compile errors.

Local components use Go closures:

```gox
item := func(label string) {
    <strong>
        page.Title
        ": "
        label
    </strong>
}
<div>
    item("one")
    <item label="two" />
</div>
```

For ordinary calls, explicit positional arguments come first and omitted trailing
arguments are filled by name. Component attributes supply named arguments. The
builder argument is inserted automatically when the signature requests one.
Rendering functions containing markup return no values. Ordinary helpers can
return values, including `gox.HTML`; output-position calls render their result.
Use `_ = helper()` to discard a value deliberately.

```gox
func decorate(b *gox.Builder, disabled bool) {
    b.Class("button")
    b.Attr("aria-disabled", disabled)
    b.Attr("disabled", disabled)
}
```

Call `decorate(disabled)` inside a pending opening tag. `class("...")` appends
classes; other attributes replace the earlier value. Adding attributes after
child output fails. `b.Open`, `b.Close`, `b.Text` and `b.HTML` support native-style
render helpers. The builder is valid only during its render. It cannot write
HTML text into a JavaScript, CSS, or attribute context through a hidden callback.

`value | format` means `format(value)`; `value | format(arg)` means
`format(arg, value)`. Integer bitwise OR keeps its Go meaning. Pipelines require
a callable right side and a single result.

## Whitespace and output formats

Indentation and newlines between Go/markup statements emit nothing. Add spaces
and newlines explicitly in strings. Raw strings preserve their contents, useful
inside `<pre>`:

```gox
<pre>
    "first\n  second\n"
</pre>
```

Inline JS/CSS retain their native source whitespace and use `{=value}` for data:

```gox
<script>const customer = {=customer};</script>
<style>.button { color: {=color}; }</style>
```

There is no automatic pretty-print mode: it could change inline text or `<pre>`
semantics. `Options{Format: TextFormat}` renders top-level expressions as plain
text. `CSSFormat` treats literal strings as CSS source and dynamically rendered
expressions as contextually escaped CSS data. Both use the same Go frontend,
type analysis, nil guards and EVM compiler; HTML markup requires `HTMLFormat`.

## Safety and failures

The contextual analyzer and escaping functions are copied from Go's
`html/template`; see [upstream notes](internal/htmltemplate/README.md).
Compilation chooses filters for HTML text, attributes, URLs, srcsets, JavaScript
and CSS. Runtime calls those filters directly; it never executes a Go template.
The copied upstream tests and end-to-end comparisons cover the escaping behavior.

Template source and host packages are trusted code. Data is untrusted.
`gox.HTML`, `CSS`, `JS`, `JSStr`, `URL` and `Srcset` have Go's explicit trusted-content
semantics; converting untrusted data to one of these types bypasses protection.
Dynamic `script`/`style` types are rejected. Runtime attribute helpers cannot
change those types or meta-refresh parsing. Their other attribute names and
values pass contextual validation. Rendering operations also check their input
context at runtime to prevent callbacks from bypassing the compiled filters.

Nil pointer field accesses in output expressions report a `Diagnostic` and skip
that expression or component call. Attribute failures omit the whole attribute.
Inline JS/CSS use a context-safe empty/null replacement so omission cannot join
unsafe tokens. Guards preserve operand order and short-circuiting without defers.
`Strict: true` turns these failures into render errors; other EVM errors always
fail the render. `Report` defaults to `slog.Warn` and can be replaced by the host.
This does not recover arbitrary host panics or silently swallow script failures.

Branches, loop iterations and components must contain complete elements.
`return`, `break` and `continue` close elements they leave. Labeled jumps across
open elements are rejected. Malformed syntax, incompatible input types and
ambiguous escaping contexts fail compilation with source positions.

## Class discovery

`Template.Classes` includes static class attributes, constant interpolations and
constant arguments to `class`, `Class` and `attr("class", ...)`, with source
positions. `bg-{tone}` is dynamic and does not falsely contribute `bg-`.
`DynamicClasses` flags values that need a separate Tailwind safelist or host
policy. Arbitrary host helper output cannot be enumerated by this compiler.

## Validation

```sh
go test ./...
go test -race ./gox/... ./evm/...
go test ./gox -run '^$' -fuzz FuzzCompileGoX -fuzztime=15s
go test ./gox -run '^$' -bench BenchmarkApplication -benchmem -benchtime=200ms -count=8
```

The [application benchmarks](_docs/BENCHMARKS.md) use anonymized production-derived
card, history and comparison templates, each at two sizes. Both engines render
identical output to immutable strings using reused buffers. Compilation, linking
and view-model preparation are excluded.

On Apple M5 Max with Go 1.27.3, GoX uses 27–40% less execution time than cached
`html/template` across these six workloads (eight samples, 2026-11-06). The report
includes allocation counts, before/after samples, EVM regression controls and
reproduction commands. `BenchmarkRender` remains a small diagnostic workload.
