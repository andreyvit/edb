# KVO representation research — state and history

Status: schema-owned dictionary zero handling and native Go struct mapping are implemented, measured and validated (2026-10-17). Signed scalar/tag simplification is implemented, measured and validated. The preceding performance research iteration was completed and validated (2026-10-16). That preceding performance pass had 426 cases per host with five repetitions; the latest dictionary/native pass has 146 cases per host with five repetitions. The selected implementation, exact source snapshots, all tested/rejected alternatives and a consolidated review list are retained. Everything remains experimental, in local WIP commits on `main`; nothing has been pushed. Production storage paths and the reference backend are unchanged; the real KVO schema now has native registration metadata.

Started: 2026-10-15 (Indian/Maldives).

## Read this first

**Artifact policy (2026-10-28, supersedes earlier instructions to commit results):** keep collected data and generated evidence locally, ignored by Git. This includes all of `research/kvo/results/` and, by explicit follow-up decision, `experiments/testdata/workload-shapes.json.gz`. Keep experiment implementations, handwritten fixtures, tooling, README and this state/history versioned. Preserve local artifact contents exactly. Sample-dependent experiments explicitly skip when the optional local replay is missing; available synthetic tests still run. Portable local bundles retain the replay they used. The user requested replacing the one squashed WIP commit with a source-only commit ready to publish; the destination branch/push remains the user's next step.


**Dictionary policy correction (2026-10-17):** schema semantics are authoritative for both native mapping and key access. Omit zero-valued dictionary entries by default. `kvo.KeepDictionaryZeroValues` on a Map property preserves zero entries and requires a generated entry getter to return `(value, isPresent)`. This applies to string and numeric data keys; `PresenceIsSignificant` independently governs the dictionary property's own presence. It does not propagate into nested dictionary values. The earlier native-Go-map-specific assumption is superseded.

- [x] Add and validate the real schema flag and native registration metadata.
- [x] Apply the policy consistently in native Put/Get/GetFast/Patch and selected-format key access/mutation; demonstrate both generated getter signatures.
- [x] Test default/flagged dictionaries, empty children, independent property presence, plan reuse, edits and unknown-field preservation. Retain prior measurements; verify allocation behavior and relevant paired benchmarks.
- [x] Update README/results with the decision, validate and commit locally without pushing.

**Active native mapping task (2026-10-17):** keep codec/binding experiments on the side; put the necessary type/property declaration support in `kvo`. Registered Go structs are projections of an authoritative model. Every field needs an explicit tag, including `edb:"-"` for exclusions. No implicit field-name mapping. Commit each substantial step locally.

- [x] Add native-type registration, strict property/tag metadata and the missing schema descriptions needed for native Lists/bytes/presence/layout. Preserve existing callers.
- [x] Implement experimental allocating Get, reusable GetFast, Put and projection Patch, covering nested structs, maps, slices, scalar values and explicit optionals.
- [x] Check omission/reset behavior, unknown-field preservation, present zero, layout inheritance, ownership, registration errors and round trips.
- [x] Benchmark full traversal versus decode plus one/repeated native passes, projected reads, fresh/reused Get, Put and Patch. Retain raw results and runnable comparisons on both architectures.
- [x] Update the public-facing README with actual APIs, semantics and limits; record findings and finish validation/commits.

Decisions for this implementation: `compact` is an option after an explicit property name (`edb:"details,compact"`); a blank name is rejected under the no-default-key rule. Native objects themselves do not infer a primary key from field order; the future database row adapter owns key injection/exclusion. Explicit `edb:"-"` is available now. GetFast's zero-allocation claim must name ownership and capacity conditions; it cannot promise allocation-free growth of arbitrary maps/slices or fresh strings. Measure rather than assume that decoding before one traversal is faster.

**Simplicity is an acceptance criterion:** every special case and unusual rule must earn its complexity with a concrete correctness requirement or measured workload benefit. Speed alone is not the objective. Keep the reason next to the rule, retain rejected alternatives, and commit substantial steps locally without pushing.

**Completed change (2026-10-17):** the main candidate uses direct positive tags and two’s-complement signed payloads, with no ZigZag. Compact uses the counted one-bit/tag grammar; Fast keeps eight-byte cells. The previous literal-length and ZigZag candidates remain runnable controls.

- [x] Preserve the interrupted full-word-only change and restore the preceding candidate as an exact runnable baseline.
- [x] Implement the selected tagged Compact/Fast builder, typed values, readers and unknown-value preservation.
- [x] Integrate signed values with edits, sparse patches and batch construction; test signed boundaries, zero/presence and opaque rewrites.
- [x] Measure the selected mapping against retained controls, run correctness and allocation checks, update README/BENCHMARKS, and commit the completed step.

**Completed broader audit:** server performance has priority over Apple Silicon. The first checkpoint left reasonable implementation opportunities. The continuation below measures them, adopts the broad wins, retains explicit alternatives for conflicting workloads, and completes fresh paired validation. See the final combined checkpoint and consolidated review list in `results/performance-continuation-20261016.md`.

**Completed optimization audit:**

- [x] Remove repeated sibling scans in broad recursive edits; compare simple sorted/hash indexes, including sparse and cold costs.
- [x] Replace the quadratic dense path binder with a simple bounded-cost alternative; include index construction and binding in end-to-end timings.
- [x] Transfer fused parsing and narrow-key search to physical patch planning; compare sparse/deep/dense paths, eligibility failures and ownership costs.
- [x] Compare a one-shot sorted leaf-change merge with the recursive overlay for broad updates and construction; keep interactive edit/read semantics separate.
- [x] Measure contiguous directory reservation and width-specialized emission for Fast construction, including Dictionaries/Lists and mixed Compact children.
- [x] Check Dictionary/List access for redundant decoding, and wider numeric-key lookup for any remaining simple gain. Evaluate a single stored key base with fixed-width relative keys when global numbers occupy a narrow range; quantify added parsing versus narrower keys and record the option even if not selected.
- [x] Reconcile selected implementations against the full benchmark/usage matrix on both hosts, weighted toward the server; preserve optional alternatives where no reasonable universal choice wins. Update README and this file, validate and commit each substantial step without pushing.

**Active task list (2026-10-16; ordered):**

- [x] Explain why smaller selector classes saved little, with deterministic byte accounting, design implications and sampling caveats; preserve the user's intervening request below.
- [x] Explain the broader design priorities: distinguish nearly equal Compact selector sizes from substantial layout differences, identify common value cases and rare byte-heavy tails, and separate measured implications from new unapproved special cases.
- [x] Test a one-bit eight-byte-cell escape plus positive-immediate/byte-length tags; remove dedicated small-negative tags from this candidate. Retain the four/three/two-bit controls, structural unknown-field skipping, zero/presence behavior, and full-width numeric keys.
- [x] Measure tiny-container framing, including whether Object/Dictionary counts earn their bytes; keep List API implications explicit.
- [x] Attribute ordinary Fast's lookup gap against Immutable without relying on fixed slots. Quantify reader/setup, search, key-width, payload, layout and working-set costs and interactions before optimization. The latest request authorizes continuing into measured improvements in the same run.
- [x] Optimize the Fast reader implementation and evaluate format changes only where attribution supports them; retain each control and mmap-backed comparisons on both CPU families.
- [x] Expand anonymized shape-based workloads: a few shallow keys, a few deep paths, repeated/interleaved edits, about 70% replacement, and full construction from empty. Preserve realistic value widths, span lengths, sparse fields, Dictionaries, Lists and nesting; do not invent a production access-frequency trace.
- [x] Compare sorted insertion, append/dirty-sort, small unsorted overlays, direct owned patches and dense builders; measure staging, read-after-write and final materialization separately, with cold/reused allocations and scratch bytes.
- [x] Validate correctness, unknown/present-zero preservation, borrowed-data ownership, pointer safety and cross-hardware performance. Keep source snapshots, raw repetitions and all variants. Update README/STATE and commit after substantial steps; no push.
- [x] Finish the common-container checkpoint already underway: final verification, paired measurements, and a coherent README covering the actual experimental APIs and formats.
- [x] Preserve runnable code and benchmarks for **every considered approach and variation**, including rejected alternatives and implementation controls. Recover overwritten variants where necessary; mark historical results whose exact implementation cannot be reconstructed. Do not remove extras for maintainability during research.
- [x] Provide a portable benchmark runner, variant inventory, environment capture, and Linux amd64 test binaries. Keep complete raw repetitions and correctness checks alongside summaries.
- [x] Compare the current indexed root directly with existing KVO Immutable using equivalent data, both hot-row and larger working sets read from an actual mmap-backed temporary Bolt database. Separate navigation from transaction/bucket lookup overhead and report stored sizes.
- [x] Follow the backend repository's port-knocking procedure, connect to `bh311.bubblehouse.com`, upload the Linux x64 binaries, and run the prepared correctness tests and comparisons there. Run bounded, single-process benchmarks and record hardware/load so results can be interpreted. Preserve server results separately from Apple Silicon measurements.
- [x] After finishing the current work, inspect backend debugging tools and obtain bounded, read-only Scan2 samples from American Girl, Life Extension, and Milwaukee. Keep raw application data outside this generic research repository; record anonymized aggregate distributions and sampling limitations here.
- [x] Measure scalar ranges/signs, scalar widths, string/span lengths, key gaps, container sizes, nesting, zero sparsity, and shared-width distributions. Do not assume strings are the only material span workload.
- [x] Compare the current four-bit literal-length codes against the proposed three-bit `0/1/2/4/8/span/reserved/mixed` codes and two-bit `normal tag/4/8/mixed` codes. Include encoding, reads, edits, changes, scratch, copying, and size; rerun relevant variants on the server as well.
- [x] Explore further optimizations suggested by the distributions until the remaining trade-offs are understood. Record wins, losses, uncertainty, and reasons to stop; do not claim universal optimality from a finite sample.
- [x] Adopt Object / Dictionary / List and Fast / Compact terminology. Define downward Compact inheritance, an Object-only row root, and a schema-level layout choice; keep the default open until the trade-off is assessed.
- [x] Implement and measure `edbschema.PresenceIsSignificant` semantics: explicit stored zero versus missing on marked properties, ordinary zero elision elsewhere, presence-aware access, and distinct Set-zero / Delete changes. Preserve unknown zeros during rewrites.
- [x] Explore opt-in `edbschema.FixedSlot` fields within Fast Objects. Only selected hot or usually present fields get slots; the rest use the normal keyed area. Compare direct selected slots and bitmap-packed selected slots, presence, schema evolution, generic versus generated access, and mmap reads on both machines. Dense all-field slots may remain a labeled comparison, not the proposed API.
- [x] Update this file and README as decisions change. The comparison is complete; four-bit literal lengths are the measured recommendation, not a frozen production format.

The user explicitly requested this ordering: enqueue the new distribution study and finish existing work first. The server benchmark request is authorized; it does not authorize changing application data or deploying the codec. The latest user instruction is to commit after every substantial step. Keep these WIP commits local; do not push.

**Commit policy:** commit after each substantial step, even while the work is experimental. Preserve the runnable variants, measurements, and documentation together in useful WIP checkpoints. Keep commits local unless the user asks to push.

**Rejected performance options:** record every option rejected for complexity, maintenance, safety, API or format costs when it offers a significant measured gain. Preserve its runnable code and results. Keep a review list with the gain, hardware/workload, cost, reason for rejection and remaining uncertainty; the user will review these choices. Do not silently discard a faster alternative.

The current [review list](results/performance-iteration-20261016.md#review-list-faster-options-with-costs) includes 80/16-byte prepared caches, front framing, prebound dense slots, retained physical patches, append-only staging, ordered insertion shortcuts, native key loads, and count-free one-bit Compact tags. It records measured gains and their narrower contracts, including cases where the apparent gain disappears after setup or materialization.

**Report units:** use nanoseconds throughout timing comparisons and spaces to group large integers, e.g. `19 770 ns` and `46 168 B`. Raw benchmark logs and CSVs retain machine-readable formatting.

**Research policy for interactive iterations:** user proposals are hypotheses to test, not conclusions to agree with. Challenge a proposal immediately when there is a better argument or alternative, and reject it when measurements do not support it. Explain trade-offs and distinguish format costs from implementation costs. This applies throughout the project. Questions and discussion are now welcome. Update both this state file and the README as the work changes; retain valuable intermediate results and rejected alternatives.

**Preceding measured recommendation (2026-10-16; its scalar/Compact choice is superseded by the 2026-10-17 decision):** two persistent layouts with forward payload emission and trailing catalogs. In Fast Objects, place eight-byte cells before the optional scalar/span bitmap and put keys beside the footer. Fuse one-shot access; for one-byte keys, compare up to four entries directly and use byte search for larger directories. Keep binary search for wider keys. This retains 16-byte borrowed values/views, adds no stored bytes and needs no fixed slots or prepared cache. For writes, use small overlays, preflight and copy/patch eligible leaf changes into owned bytes, and rebuild changed containers otherwise; use a forward builder or explicitly prepared values for broad construction. Keep shared four-bit literal lengths in Compact for now, with the simpler count-free one-bit candidate retained for review. PresenceIsSignificant and opt-in FixedSlot remain as previously decided; their standalone prototypes still use the previous ordinary directory placement. The [performance report](results/performance-iteration-20261016.md) records controls, regressions, scope limits and faster options with costs. The README specifies the current research direction and actual APIs.

**Naming:** `edbschema.PresenceIsSignificant` replaces the earlier name `edbschema.ZeroValues`. Historical requests and results below retain their original spelling.

**Latest user decisions:** direct positive tags and two’s-complement signed payloads, no ZigZag; every special case must justify its complexity, and substantial steps must be committed locally. Return errors only for expected failures; panic with defined errors for misuse and unexpected failures. Ordinary properties use Go zero defaults without presence flags or a separate null value, and zero map entries are omitted. `edbschema.PresenceIsSignificant` is part of this design: marked properties retain zeros and distinguish them from missing. Containers are Object (numeric unique keys), Dictionary (string keys), and List; layouts are Fast and Compact. Rows are Objects. Compact propagates downward in the schema; a Fast parent may have Compact children. Fixed slots are opt-in per property (`edbschema.FixedSlot`), for selected hot or usually present fields, not every field. Encoding kind/version belongs at the row boundary, never in each object. Unknown fields must remain skippable without their schema. Historical statements that presence was deferred are superseded by this decision.

**Current direction: one model, two persistent value profiles, one logical change format, implemented by EDB.** The [standalone specification](README.md) defines a concrete structurally framed grammar, with the current Fast reference in `experiments/keys_last*_test.go`, Compact in `length_*_test.go`, and separate presence/slot extensions beside them. The earlier `draft/` remains a comparison. Compact and indexed values expose field boundaries without a semantic schema. The model supplies interpretation of known fields. A numeric editor preserves opaque unknown values; change encoding/reading is separate and is not yet connected to an applier or WAL. Keep decoded structs or flat projections for repeated whole-object use. MessagePack remains a comparison, not a runtime dependency of the draft.

The source audit covers **all 100 registered tables/maps**, including obsolete registrations and the five factory-created tables. String-keyed dictionaries are a first-class part of the design. This is a complete static registration census, not a production workload trace. The experiments use generic synthetic fixtures and include fair zero-allocation MessagePack navigation and generated-style decoding baselines.

Read the [compact follow-up](#follow-up-schema-directed-compact-encoding), [initial overall design](#recommended-design), [initial measurements](#final-measurements), [table-by-table audit](#table-by-table-audit-ledger), and [validation and reproduction](#validation-and-reproduction). The original request and scope additions are retained verbatim; intermediate findings, corrections, rejected ideas and limitations remain below. Raw observations are in `results/`.

The schema-directed experiments show the space available when metadata is removed, but their sizes omit structural information now required for schema-independent skipping. They are comparison points, not measured sizes for a compliant format. The indexed scalar experiment reduces a 128-field object from 2,064 to 1,160 bytes and improves larger-working-set reads, but current KVO remains faster for one hot object. Four string-key assignments fit in an 87-byte logical change stream, before the outer transaction/WAL envelope.

This remains an architecture proposal and research reference, **not production-ready storage**. The current combined grammar and APIs are specified in `README.md`; model binding, catalog allocation, a complete public path editor, ordered change application, WAL, plugin ABI and sealing remain future work. No universal size or speed guarantee against every MessagePack construction is claimed. The cleanup section at the end records fresh measurements for this grammar; earlier results are historical.

## User request — verbatim

My goal is to greatly extend the KVO representation that EDB supports right now.  Use backend project as an example user. Previously, I was only using Klaviyo for analytics snapshots, where the trade-off is that I want them to be read very quickly and updated without decoding. The trade-off is that the on-disk representation is much less compact than msgpack encoding with short keys would be.

Now, KVO is dual purpose. On one hand, it is a modeling framework, meaning that it's a way to describe the schemas and access properties by key generically. This is actually very compelling for me. In fact, this has been a big factor in the previous version 3 of analytics, where we would describe algorithmic computations generically and specify keys to apply them to. I want to extend the usage of the modeling framework. I want to cover almost everything with it so that we could more generically write interfaces that access arbitrary database rows and patch arbitrary values in them. More importantly, I want this to be a basis of a future dynamic environment where we run AI-coded plugins at runtime that declare the schemas using a modeling framework. Those AI-coded plugins would be compiled into something like WebAssembly, or maybe they would be written in a scripted language that has a Go native interpreter. But the point is that they will not really access trucks from the database, right? They will need to, in the end, make the calls to access things by key.

And the second purpose of KVO framework is the actual binary encoding, both in memory and on disk. Here, right now we have two representations which are well related, right? One is the representation with the trade-offs I mentioned before, which is that it allows very fast reads and very fast in-place mutations. In fact, in-place mutations was the key factor. Because whether that allows for the fastest reads is arguable and depends on, you know, the trade-off between size and caching and decoding time.

The second representation we have is a mutable representation, which is very similar to the on-disk one, but does not require sorted keys, so actually has much worse timing characteristics.

Here's what I want to do. I want to define another representation for on disk storage which favors compactness. Now I'm not sure how that's going to work. Maybe it's gonna be either or and you pick it by table. Maybe we even allow you to use compact representation for an object and And as such, maybe any subobject or the entire top-level object can pick their compact representation. So maybe it will be declared at l at like a type level or something.

Now I want a representation that's basically at least as compact as MSG pack encoding so that I could be sure that I can use it across my entire database without any compromise compared to the alternatives. Unlike MSG pack, it will of course not use the key names, and it will only use key codes, which probably helps, but also our key codes are pretty big, so key names that are one letter within a given table, you would find you would have a hard time competing against those. So maybe instead of writing uint sixty fours, maybe we use a varint representation.

I'm not actually sure I want to keep these fixed key codes from the code indefinitely. There is another world where the actual integer codes are allocated within the database based on the strings that are defined in the properties, so that the actual properties would not define the code, the integer code, but the integer code would be picked in place. Now this is worse for stability and data recovery, but it is better for extensibility, which will be a big part of the future. But regardless of this, so don't I mean, what I'm saying is that we might be able to to pick smaller integers, because right now all integers are in a big, big namespace, and so have to keep growing, but if we had per table namespaces, then we could pick very small integers, and those could have very small variant representations. For specialized needs, you might even allow specifying fixed keys for a table, like a set of fixed slots, just like we do for the legacy key value representation, where we have slots, right? And those slots, like there is a fixed number of slots, and then there is there are variable length stuff. There, it's still a fixed slots, but like the idea is that if, say, every customer always has, like, an ID or, like, I don't know, maybe external ID and stuff, we just declare those as fixed slots so that we know that they are always at a predictable offset and there is always space maybe allocated for them or something, but that's not necessary.

Now I realize I still didn't get to the actual task, so here it is. I want you to spend a lot of time and analyze the current use cases and consider the future use cases. Future use cases, but really imagine I convert all the current tables that Bubble House uses. See the backend project. I hope you can find it. All current tables that Bubble House uses. I convert them to KVO system. So consider those use cases and there are very different use cases in there. Like there is customer statistics table, right? which should really be called customer states, which is accessed often, only a small portion of it is accessed at a time, and so it would benefit from a representation that's very quick to decode and that would be able to be mutable in place. So that's one use case. A very sort of opposite use case is the tenant settings table, where we always read it fully. Now, oh basically we always read it fully and also we don't make many mutations to it. I mean saving settings is is okay for it to be a kind of slower operation that pays the decoding price. So there we really want the most compact and flexible format instead of going for quick updates or quick lookups. So you can see how there are trade-offs, right? How there are trade-offs, and these trade-offs, like different tables might benefit from different trade-offs. But I mean, I guess If you come up with a single representation that has like two forks for sub-objects, because I could see that a certain like say customer state could benefit from a compact form for some sub object, right? instead of so maybe we combine them again anyway.

The hero's actual task: analyze all of these use cases and come up with really good representations, or two representations, or three representations if you include in-memory, and if it has to be different, and make sure they actually perform well. So you need to experiment with all of that.

I encourage you to write code in edb. I'm not sure if we even have any tests for Cvpr representations right now, but you are absolutely free to extend that and to mimic Bubblehouse use cases. Do not copy any literal code from Bubblehouse. Or rather, what I should say is that you're free to copy code from Bubblehouse, just change the name so that the particulars are unidentifiable, so that all the names and use cases are generic. There is no mention of loyalty or something, right? So you're free to borrow anything from Bubblehouse, just rewrite, rename, etc.

So what I want you to do is to come up with a very elegant solution here, because look how we try to be very memory efficient, we try to be very simple in the data formats, and this is what we want to keep, right? But the particular decisions made for our mutable representation are not set in stone. Nothing in Klaviyo is set in stone. Consider that anything could change.

So you're free to propose changes to anything there. Like say maybe the current on-disk format, maybe you can come up with a better on-disk format that actually performs better for our use cases and keeps quick lookups and quick in-place mutations. We have this idea that basically an on-disk representation is a list of sub-objects, and each sub-object is a map, or in the future we thought that we would have string sub-objects and byte sub-objects, like byte array sub-objects. We also wanted to split PII strings from non-PII strings and maybe keep personal identifiable information encrypted optionally, and the encryption key so that, you know, encryption key could be dropped and it could be stored separately so that PII protection could operate on a different layer than the actual database. So maybe we have two separate data types for PII strings and non-PII strings.

So again, success criteria is that something is very, like, you know, we have the minimum required number of representations for this data that covers our use cases well, and the plan on how to manage this, and this whole system performs very well, and is elegant and neat, for lack of a better word. Just like, you know, just like, say, UTF-8 in its current incarnation is neat and like much worse ways to encode the same information using similar could actually be the reality, right?

There is a famous story of how UTF-8's current representation was a result of, I think, 1 or 2 persons composing that on a napkin and proposing that instead of the representation that was gonna go into effect, that was much worse, that would not be reasonable midstream. So things like that were like, you know, we have a big space of opportunities and we can pick better or worse ones.

Here are some examples of what I consider neat, but you can, you know, this is not an exclusive list. So one is that the actual representation is easy to explain and easy to spec. It should not be a very complicated spec, right? It's like, you know, it's like that simplicity is the ultimate sophistication idea, and this is what we're looking for here.

So that's one: it's simple to explain, simple to specify, etc. The second is that it provides the best runtime characteristics in terms of both time and space that are possible given the use cases. So it does not require, like, I don't know, carrying unnecessary pointers, adding unnecessary data, etc.

For example, our current representations, some of them are less compact than I would like them to be because they have to carry multiple pointers, but I always thought that in the end we would be writing a wrapper around them for a particular type. Like, say, if we have T widget declared as a type, right? Then there is, like, you can work with it using, like, mutable map, immutable map, any map, which have to carry the type with them. But if we declare mutable widget, immutable widget, any widget specifically for widgets, we could both add typed accessors, but we could also just, you know, because we know at compilation time, we know the type, we don't have to carry it inside the struct at runtime. So in the end it could just carry like 1 or 2 slices. The need to carry two slices is unfortunate, by the way, for me. If we could somehow get around it, that would be great, but it seems fundamental that we have a carrier pointer to the actual object or sub-object, right? And the pointer to the top-level object where like the object catalog resides. I see no way around it, but maybe I don't know. I'd rather say I do see ways around it, but they are too low level for Go language probably.

So anyway, that's the second idea of what NEET looks like. Third idea is that the balance we pick when there are competing goals or competing values is smartly decided for our use cases, and we use what's called machine empathy.

So look at in the backend, there is an AI document called Performance Model or something like that, which tells you that basically, you know, at runtime, what all the time goes, all CPU time goes towards is, aside from IO, but IO can be minimal for reads. So aside from IO, all the time goes into msgpack encoding, decoding, and into garbage collection. So when we are deciding on particular performance characteristics, we want to consider an app where like all the time goes into these things. And so, you know, remove decoding, right? But if that adds to garbage collection, that's still bad. So we want to have minimum or preferably zero allocations where possible, right? And no decoding time ideally for the most compact representations.

But say there are other cases like tenant settings where minimizing decoding time makes no sense. And in fact, when you access all the fields, it actually takes more time to navigate the data structure than to decode into a struct. So maybe in those cases we just, I don't know, we just decode into a struct or at least into, when I say a struct, it could be a Go struct, but for more dynamic use cases, maybe it's like a struct where we have a fixed offset for every field. So basically every field has memory space allocated towards it, something like that. So maybe that's a representation we need to support, and then there is a decoding step.

And the final way this could be neat is if it's as transparent when actually using it as possible. So for example, we could define multiple representations. If we could switch representation at runtime based on access pattern, that's probably even better than if we allow you to pick a representation from the get-go. I mean, yes, that's probably but it should be optional if it has any cost associated with it, and it probably will, right? Because, I mean, jumping representation, you could quite clearly decide when it's beneficial, but supporting a change of representation probably has some wrapping costs. So if that's the case, then we probably make it opt-in, like through a separate type or something. But still, I could see a type that's like fully decoded, like maybe decoded. Like if we have widget, right? Right now we have a mutable widget, but maybe we have decoded widget or like static widget or widget struct or whatever that provides fastest runtime access at the cost of paying the decoding price beforehand.

I want you to research and propose how this works. Feel free to write code, but note that you're not writing production code. You're just doing experiments. It's okay to keep those experiments in, you know, uncommitted. You can write benchmarks. You should probably write tests that show how your proposed code structure would look like in usage and a skeleton of that code structure.

And again, try to avoid like, you know, overengineering, overcomplications, and especially like having avoidable complications, like having two of something that you need to choose from when we could have a smart middle ground one solution that would work for both. So like, try to look for these smart solutions whenever possible, like research what's, you know, research what is the state of the art in this area, what other projects do and use, and what are the smartest and simplest ideas out there related to our use case. So basically, I want you to propose something that's much better than I would come up with myself after like thinking about it for a long time.

And I'm very good at simplifications and like knowing when to simplify and when to allow complexity. And so like basically we're searching for the simplest solution that will cover the use cases well and enable very fast operation. So like zero allocation reads, as low allocation as possible mutations, maybe we even sometimes zero allocation mutations if we can.

Preallocate something on the stack, right? So like there is a lot to research and I'm sure there will be uncertain areas where you can write Go benchmarks, Go code and like run them and measure something and even look maybe at how Go is implemented. So go very deep on this. You have a lot of time.

I want you to create a spec file where you put my request verbatim and then track everything that you do, everything that you try, so that the context and history never gets lost. I suppose it's not literally a spec file, but more like a state file, but it has like user request and then has all the work we did, and it should also record any user decisions I will do later.

I'm going away for a while, so do your best. Don't ask me questions for now. Just make sure you find all the resources first and verify you have access.


## User decisions

- Initial request: no questions for now; inspect resources first; genericize all application examples; experiments may remain uncommitted.
- Later scope additions: audit every table and retain string dictionary semantics; include native changes for WAL/audit and preserve intermediate results here. Both messages are retained verbatim below. No representation choice has yet been approved by the user.
- Compact follow-up: user prefers an EDB-owned implementation and model rules without MessagePack annotations; MessagePack binary format remains acceptable. They challenged whether schema knowledge could improve on self-description. This is a preference and research direction, not approval of the exact experimental grammar.
- Schema evolution requirement: without any semantic schema, or with a field absent from the current schema, the encoding must still support structurally traversing the enclosing object and skipping that field. A dropped field's historical semantic type must not need to be retained for future reads/writes. This supersedes the proposal to require an old writer descriptor merely to locate field boundaries.

## Work log

### 2026-10-15 — resource discovery

- Verified EDB checkout and existing KVO package; working tree initially clean, branch `main`.
- Verified backend checkout at `/Users/andreyvit/Developer/prairie/backend`; read-only reference.
- Found backend `_readme/performance-model.md` and repository instructions.
- Saved this request directly from the current chat transcript without paraphrasing.
- Reading code and workload shapes before selecting candidate formats.

## Plan and completion criteria

1. Inventory every registered backend table and classify payload shapes/access patterns; inspect representative readers/writers and performance model.
2. Audit current KVO format, API, memory layout, transaction lifetime, and mutation costs.
3. Read primary sources for compact, zero-copy, mutable, and schema-driven formats.
4. Implement isolated generic prototypes and fair baselines, including short-key MessagePack.
5. Check correctness, unknown-field/schema evolution, allocation claims, and realistic end-to-end update costs.
6. Benchmark sizes, sparse/full reads, mutation/packing, typed/dynamic access, cold/cache-sensitive workloads where feasible. Save raw output and environment.
7. Recommend the minimum necessary representations with a concise format sketch, API examples, migration plan, rejected alternatives, and uncertainties.
8. Run relevant tests and verify only research files were changed.

## Artifacts

- `STATE.md`: verbatim request, decisions, complete audit, chronology, proposal, results and final validation.
- `README.md`: standalone specification of the current draft, including binary formats, APIs, ownership, examples, and measured limits.
- `draft/`: the current schema-independent reference implementation and its tests.
- `experiments/codec.go`, `experiments/words.go`, `experiments/msgpack_view.go`: earlier compact/indexed layout and navigation experiments.
- `experiments/editor.go`, `experiments/changes.go`, `experiments/change_reader.go`: earlier mutation and logical change experiments.
- `experiments/usage_test.go`: earlier typed/dynamic API skeleton; `experiments/*_test.go`: generic fixtures, comparison benchmarks and real EDB/bbolt controls. `draft/example_test.go` uses the current draft APIs.
- `cmd/census/main.go`: source-only reproducible registration/type-shape census, anonymized by default.
- `results/`: raw tests, benchmark repetitions, escape-analysis observations and CSV summaries. The artifact/reproduction guide at the end identifies authoritative runs.

### User steering — verbatim

Note: look at ALL tables that would be converted; we have more needs, we have maps keyed by strings as well, unfortunately... and I don't think we can get rid of them all, although they could of course be stored as []{key, value} or something

Decision: enumerate all table registrations and their payload shapes; string-keyed maps are first-class requirements, including nested maps, arbitrary key lookup, ordering, updates, and plugin data. Do not replace map semantics with a list-only API.

### Initial audit findings

- EDB baseline `go test ./...` passes; Go 1.27.1, darwin/arm64. EDB base commit `d755f5a2df703d9bc99886781c89d19b91a83c80`; backend base commit `518652f09630779c80ff59a15bfefd03c8ac7a84`.
- Current immutable format is a word-based object directory plus sorted uint64 keys and values. Only maps are implemented, despite reserved kinds for other objects.
- Current mutable representation overlays unsorted changes and repacks; this is not a write directly into Bolt mapped bytes. EDB KVPut currently calls Pack twice for non-nil data; account for this in end-to-end measurements.
- Runtime interfaces carry schema and multiple slices; typed wrappers can eliminate schema from each value. Object-local offsets may eliminate the record-wide directory pointer for tree-only values.
- Backend performance model confirms allocation/decoding, bandwidth, serialized writes, and transaction lifetime constraints. Read source code and historical performance notes; no live production access required.
- Primary sources found: FlatBuffers/FlexBuffers, Cap'n Proto, Protobuf, MessagePack, VelocyPack, Arrow, V8 property storage, bbolt.
- Notion content search tools reference an access-discovery tool that is not exposed in this session; relevant local design docs are accessible. No Notion claims relied on.

### User steering — verbatim

A few more notes. First, I encourage you to keep all valuable intermediate results in that same file. Like say if you do the audit of the tables and use cases, right? Put the result in the file. So first, second, we're definitely gonna have a wall, write a headlog for a dbtable soon. While we could just write, you know, it's a key value storage underneath, so we could just write every value fully, but if we were to define a diff representation of change set representation, if you will, where we can encode efficiently what we changed, that could be a better way to encode writes in the write-ahead log, but also in other cases, like say, for example, we are storing tenant history and we do want to store audit history for other things, and in fact, we have change objects right now in Bubble House, but they carry high-level change information. But we really want to store changes, like a history of changes of key things like customer data in multi-level format where like we have what we ran and call and the reasons at the top level or user actions or API calls, but at the lowest level, we would represent the actual changes in customer data that were done as a result. And so there are many use cases for representing changes in the data encoded using these modeling framework. So if it could have a native diff representation or native change representation, if you will, as a better name maybe, that could be used in for many things, and uh, of course it could have different representation on mem in memory and on disk, and uh, it could have like different characteristics again. Uh, but I think f for diffs on disk, the goal is always to have the most compact representation possible, unless you find a use case that wants a different one. So anyway, consider this as well.


Decision: keep valuable intermediate findings in this file, not only linked reports. Add native change sets for mutation, compact WAL records, and nested audit history. Distinguish in-memory edit representation from durable logical changes.

### Complete table registration census — first pass

- Parsed all non-test Go sources, excluding vendored/unused/testdata directories; did not import or execute the application. Found **96 registration sites**: 52 legacy typed, 30 current typed, 11 KVO, 2 raw, 1 singleton map. One typed site is a factory called five times, so this represents **100 registered storage tables/maps: 86 typed, 11 KVO, 2 raw, 1 singleton map**. These include obsolete tables that remain registered.
- Of 81 directly registered typed payloads, **30 have reachable string-keyed maps**, and **15 have reachable dynamic/interface values**. This is a source-shape census, not a measurement of populated production data.
- Large settings payload: 1,526 fields across distinct reachable struct types, 34 string-map declarations, 8 dynamic/interface declarations. This is not the root field count and does not imply all are populated.
- Frequently updated state payload: 260 fields across reachable structs, nested numeric-key maps, lists, optional values, and times.
- Identity/profile payload: 110 reachable fields, string-to-string and string-to-any maps, optional nested objects, lists of strings, private text.
- Transaction/document payload: 162 reachable fields, list of child objects, strings, dynamic maps, numeric values, state flags, times.
- All payload families matter: small session/status/queue rows; settings/themes/snapshots; profiles/identity/contact data; catalog/content and external entities; transactions and child arrays; state and numeric aggregates; append-oriented events and audit/change rows; imports with bulk arrays/opaque values; existing nested numeric KVO snapshots; raw activity records; singleton maps.
- Generic type substitutions and a small number of external custom scalar/codec types need manual review; do not interpret unresolved source types as unsupported business values.
- Found legacy fixed-slot experiments in the backend. Their string, sensitive-string, list, and string-map goals reinforce this request; they are not assumed to be current working code.
- Storage lifetime finding: bbolt Get bytes live only through the transaction; Put retains the supplied slice until transaction end. Reusing a single output scratch buffer after Put corrupts pending writes. Zero-allocation mutation and zero-allocation durable update are distinct goals.

### First experiment results and corrections

- Implemented isolated recursive compact, fixed-cell indexed, and offset-indexed prototypes; mixed subtree encodings work. All fixture reads allocate zero, including nested string dictionaries. A typed subtree view is one Go slice (24 bytes); current ImmutableMap is 64 bytes.
- Compact numeric entries fuse the key delta and wire kind. A child slice includes that tag, so it remains self-contained without a parent/catalog pointer. An early draft exposed a separate kind-plus-slice body view; replaced it with this single-slice design and tested mixed nesting.
- First sizes: synthetic numeric state 1,112 bytes compact vs 2,270 with short-key MessagePack; synthetic settings 5,169 vs 5,293; ordinary 128-entry string dictionary 2,693 vs 2,563. Thus this compact candidate does NOT meet a universal MessagePack size guarantee. The state fixture currently has clustered runtime IDs; add spaced/random-ID sensitivity before relying on the ratio.
- First hot-cache 128-field scalar reads: current KVO about 6.5ns, indexed prototype 17.2ns, offset index 24ns, sequential compact 622ns, zero-allocation MessagePack navigator 347ns. These are microbenchmarks on this Apple machine, not production predictions.
- One scalar update plus packing at 128 fields: current KVO about 1.20us, 2,536 allocated bytes and 6 allocations; double pack about 2.36us/4,864 bytes/8 allocations; copying and patching indexed owned bytes about 127ns/1,408 bytes/1 allocation. Patching an already owned image about 16ns/zero allocations. bbolt lifetime and full-value copying still apply.
- Important negative result: a generic editor supplied with a local `[8]Edit` still allocates 448 bytes per operation. Compiler escape diagnostics show slice growth/record references escape. A local scratch declaration alone does not establish stack allocation. Test bounded pointer-free edits next.
- Initial benchmark batch had one invalid baseline: MessagePack's default dynamic decoder assumes string map keys and failed on mixed numeric maps. Fixed it by configuring its untyped-map decoder. The failed batch is retained, marked as intermediate; do not quote its failed decoding comparison.
- Existing KVO's iterator-based full scan showed allocations in this benchmark. Add a raw-array scan baseline before blaming the representation.
- Manual census follow-up found composite struct keys (three enum-like components) in configuration maps, plus raw JSON/custom-codec leaves and generic wrappers. The logical model needs a key codec for tuples/opaque stable keys, not only uint64 and strings. Source census unresolved entries are mostly generic type parameters, json.RawMessage, and the external KVO property-code alias.
- Current backend property code maximum is 491, not remotely near uint64 capacity. Narrow fixed keys can save space now without changing stable identities. Per-model field numbers remain useful for future extensibility, but database allocation is not required merely to make today's keys small.
- Native changes prototype supports ordered Set/Delete at typed paths, adjacent-path prefix compression, borrowed payloads, and revision-gated reference replay. Prefix compression helps repeated parents modestly; preserve operation ordering. A 1-edit example is 24 bytes for a three-step numeric path or 30 bytes for a two-step string path, excluding outer WAL identity/checksum/commit framing.

### Table-by-table audit ledger

Every direct storage registration is below. IDs refer to source positions at the recorded backend commit, not table names. Application-specific names and domain terms are intentionally omitted. `A` is the main `fdb/reg-schema.go` registry; `B` is the additional two-table registry; `C` is the period-table factory in `fdb/statschema.go`. The census utility can reproduce the exact private mapping with `-private`; no mapping or application row data is copied into this repository.

Counts are reachable source declarations, with each named struct visited once; they are not populated field counts, memory sizes, or frequency measurements. `S/N` = string/numeric map declarations; `D` = dynamic/interface leaves. Generic type parameters and custom codecs need bindings. Categories summarize source-level purpose; hotness is inferred except where representative reads/writes were inspected.

| ID | Source | Payload/access family | Reachable fields | S/N maps | D |
| --- | --- | --- | ---: | ---: | ---: |
| R001 | B:17 | frequently updated state | 2 | 0/0 | 0 |
| R002 | B:20 | work coordination | 7 | 0/0 | 0 |
| R003 | A:1009 | work coordination | 5 | 0/0 | 0 |
| R004 | A:1016 | configuration | 18 | 0/0 | 0 |
| R005 | A:103 | work coordination | 7 | 0/0 | 0 |
| R006 | A:1037 | frequently updated state | 7 | 0/1 | 0 |
| R007 | A:1044 | event and audit | 10 | 0/0 | 0 |
| R008 | A:106 | event and audit | 14 | 0/0 | 0 |
| R009 | A:1061 | catalog and content | 13 | 0/0 | 0 |
| R010 | A:1078 | frequently updated state | 2 | 1/0 | 0 |
| R011 | A:1081 | frequently updated state | 2 | 1/1 | 0 |
| R012 | A:1088 | frequently updated state | 4 | 0/0 | 0 |
| R013 | A:1091 | dynamic objects | 7 | 1/1 | 1 |
| R014 | A:1101 | profile and identity | 11 | 5/0 | 0 |
| R015 | A:1104 | profile and identity | 18 | 5/0 | 0 |
| R016 | A:1111 | profile and identity | 11 | 0/0 | 0 |
| R017 | A:1118 | profile and identity | 6 | 0/0 | 0 |
| R018 | A:1121 | profile and identity | 7 | 0/0 | 0 |
| R019 | A:113 | event and audit | 27 | 0/0 | 0 |
| R020 | A:1141 | profile and identity | 8 | 0/0 | 0 |
| R021 | A:1161 | work coordination | 7 | 0/0 | 0 |
| R022 | A:1169 | event and audit | 92 | 2/0 | 1 |
| R023 | A:1210 | configuration | 65 | 2/1 | 0 |
| R024 | A:1220 | work coordination | 44 | 2/0 | 0 |
| R025 | A:1232 | work coordination | 32 | 1/0 | 1 |
| R026 | A:1244 | work coordination | 5 | 0/0 | 0 |
| R027 | A:1253 | work coordination | 11 | 0/0 | 0 |
| R028 | A:1262 | catalog and content | 15 | 0/0 | 0 |
| R029 | A:1271 | frequently updated state | 15 | 1/0 | 0 |
| R030 | A:1286 | event and audit | 91 | 1/2 | 1 |
| R031 | A:130 | work coordination | 5 | 0/0 | 0 |
| R032 | A:135 | work coordination | 23 | 0/0 | 0 |
| R033 | A:1368 | profile and identity | 41 | 1/0 | 1 |
| R034 | A:1394 | event and audit | 30 | 1/0 | 0 |
| R035 | A:1426 | event and audit | 57 | 1/1 | 0 |
| R036 | A:1451 | event and audit | 14 | 0/0 | 0 |
| R037 | A:1467 | event and audit | 9 | 0/0 | 0 |
| R038 | A:1500 | event and audit | 5 | 0/0 | 0 |
| R039 | A:1516 | event and audit | 16 | 0/0 | 0 |
| R040 | A:1530 | configuration | 4 | 0/0 | 0 |
| R041 | A:1540 | catalog and content | 32 | 0/0 | 0 |
| R042 | A:1550 | profile and identity | 5 | 0/0 | 0 |
| R043 | A:1560 | profile and identity | 7 | 0/0 | 0 |
| R044 | A:1587 | event and audit | 15 | 0/0 | 0 |
| R045 | A:1602 | frequently updated state | 8 | 0/0 | 0 |
| R046 | A:1608 | numeric KVO snapshot | — | 0/0 | 0 |
| R047 | A:1614 | numeric KVO snapshot | — | 0/0 | 0 |
| R048 | A:1620 | numeric KVO snapshot | — | 0/0 | 0 |
| R049 | A:1628 | numeric KVO snapshot | — | 0/0 | 0 |
| R050 | A:1636 | numeric KVO snapshot | — | 0/0 | 0 |
| R051 | A:1644 | numeric KVO snapshot | — | 0/0 | 0 |
| R052 | A:1652 | numeric KVO snapshot | — | 0/0 | 0 |
| R053 | A:1660 | numeric KVO snapshot | — | 0/0 | 0 |
| R054 | A:1668 | numeric KVO live state | — | 0/0 | 0 |
| R055 | A:1674 | numeric KVO live state | — | 0/0 | 0 |
| R056 | A:1680 | numeric KVO snapshot | — | 0/0 | 0 |
| R057 | A:1688 | event and audit | 12 | 0/0 | 0 |
| R058 | A:1698 | raw activity encoding | — | 0/0 | 0 |
| R059 | A:1703 | raw activity encoding | — | 0/0 | 0 |
| R060 | A:1708 | event and audit | 11 | 0/0 | 0 |
| R061 | A:1731 | event and audit | 7 | 0/1 | 0 |
| R062 | A:1740 | profile and identity | 19 | 3/0 | 2 |
| R063 | A:183 | profile and identity | 46 | 1/3 | 0 |
| R064 | A:203 | profile and identity | 110 | 2/4 | 2 |
| R065 | A:343 | catalog and content | 11 | 0/0 | 0 |
| R066 | A:363 | catalog and content | 46 | 2/1 | 0 |
| R067 | A:406 | configuration | 154 | 2/0 | 1 |
| R068 | A:413 | frequently updated state | 52 | 1/0 | 0 |
| R069 | A:445 | frequently updated state | 35 | 0/9 | 0 |
| R070 | A:448 | frequently updated state | 12 | 0/0 | 0 |
| R071 | A:455 | frequently updated state | 5 | 0/0 | 0 |
| R072 | A:462 | frequently updated state | 5 | 0/0 | 0 |
| R073 | A:476 | frequently updated state | 260 | 0/31 | 0 |
| R074 | A:557 | frequently updated state | 8 | 0/0 | 0 |
| R075 | A:564 | frequently updated state | 5 | 0/0 | 0 |
| R076 | A:566 | frequently updated state | 2 | 0/0 | 0 |
| R077 | A:569 | profile and identity | 7 | 0/0 | 1 |
| R078 | A:576 | event and audit | 162 | 1/13 | 1 |
| R079 | A:632 | event and audit | 15 | 0/0 | 0 |
| R080 | A:668 | event and audit | 88 | 2/0 | 1 |
| R081 | A:71 | singleton values | — | 0/0 | 0 |
| R082 | A:74 | work coordination | 3 | 0/0 | 0 |
| R083 | A:755 | event and audit | 13 | 0/0 | 0 |
| R084 | A:77 | configuration | 87 | 6/0 | 1 |
| R085 | A:783 | event and audit | 9 | 0/0 | 0 |
| R086 | A:79 | frequently updated state | 68 | 1/7 | 0 |
| R087 | A:809 | event and audit | 12 | 0/0 | 0 |
| R088 | A:81 | configuration | 1526 | 34/15 | 8 |
| R089 | A:837 | profile and identity | 57 | 2/0 | 0 |
| R090 | A:883 | event and audit | 4 | 0/0 | 0 |
| R091 | A:896 | profile and identity | 57 | 1/0 | 0 |
| R092 | A:91 | configuration history | 1535 | 34/15 | 8 |
| R093 | A:929 | event and audit | 31 | 2/0 | 2 |
| R094 | A:961 | event and audit | 47 | 0/1 | 0 |
| R095 | A:994 | work coordination | 47 | 2/0 | 0 |
| P01 | C:83, invoked A:1809–1813 | period aggregate | 12 | 0/0 | 0 |
| P02 | C:83, invoked A:1809–1813 | period aggregate | 143 | 0/15 | 0 |
| P03 | C:83, invoked A:1809–1813 | period aggregate | 7 | 0/1 | 0 |
| P04 | C:83, invoked A:1809–1813 | period aggregate | 35 | 0/9 | 0 |
| P05 | C:83, invoked A:1809–1813 | period aggregate | 5 | 0/0 | 0 |

Coverage total: 95 direct registrations plus five period-factory instances = 100. Of these, 86 are typed payload tables, 11 are existing KVO tables, two have bespoke raw activity payloads, and one is a singleton key/value namespace. No registration outside these sources was found by scanning all non-test application Go source. This is a static registration audit; no production schema/database was opened.

Consequences for the logical model:

- Records need missing versus zero/false/empty/null, scalar widths, exact signedness, enums, timestamps, nested optional records, lists and unions. Current KVO zero-as-absence cannot cover this unchanged.
- Numeric dictionaries need signed, unsigned and enum keys independent of schema field numbers. Runtime IDs remain data, never entries in a property-name registry.
- String dictionaries need arbitrary UTF-8/byte keys, empty keys, long common prefixes, and nested heterogeneous values. They appear in identity extras, integration state, imports, themes/configuration, catalogs and audit payloads.
- A composite-key case already has a stable text codec. Preserve that codec initially; optionally bind it to a canonical tuple-key codec later. A list of key/value entries is a physical dictionary representation, not a replacement for lookup/uniqueness semantics.
- Raw JSON, opaque input/output bodies, binary data and existing custom scalar codecs need explicit opaque/extension adapters. Do not silently reinterpret them as generic records.
- Lists of child records need iteration, random access when hot, replacement, and explicit splice semantics. Ordered sets can use list storage with schema-level uniqueness/order rules.
- Existing raw activity rows can acquire model adapters without an immediate storage rewrite. Main keys and secondary index keys remain outside the value model unless explicitly exposed as read-only properties.
- Sparse events have a small populated subset of a broad schema; dense fixed slots on disk would be wasteful. Small queue/status rows can be cheaper to replace entirely than to encode a patch.
- Large configuration rows already become owned runtime objects; amortize decoding across their lifetime. Hot state caches accumulate many edits before finalize; benchmark both first edit and many-edit lookup, not only immutable reads.

### Second experiment results

These are intermediate observations from the runs recorded at the time. The five-repetition final medians below supersede approximate timings here; the intermediate results remain for history.

- A homogeneous scalar map with narrow keys and fixed 64-bit values occupies 1,160 bytes for 128 local fields, versus 2,064 bytes in current KVO. With present global field codes in the 300s it uses 1,288 bytes. Random 64-bit dictionary keys remove almost all key-width savings; do not extrapolate schema-field savings to runtime IDs.
- A 65,536-row random-read experiment reversed the hot-cache ordering: narrow-key words were about 59ns/read versus current KVO's 72ns; on one hot row they were about 30ns versus 25ns. This supports measuring memory footprint and locality together. It does not prove a production-server win.
- A scalar editor with an embedded bounded array of numeric edits is verified at zero allocations, including copying/persisting into caller-provided output space. The general raw-slice editor still allocates. Keep byte payloads in an arena and store integer spans in edit/path metadata.
- Change-reader evolution: owning strings allocated per string-key path; borrowed slice keys still caused the path scratch array to escape because mutable slice fields leaked through the reader. Storing integer offsets/lengths in a pointer-free path array and advancing the input by an integer cursor eliminated allocations. Final measured reader: about 195ns for 16 string-key changes, zero allocations. This is a concrete Go escape-analysis result, not an assumption.
- A smaller dictionary index uses entry offsets rather than 9-byte value cells plus 8-byte key references. For 128 entries: 2,952 bytes and about 76ns lookup, versus 4,742 bytes/41ns for fixed cells, 2,693 bytes/602ns for sequential custom compact, and 2,563 bytes/364ns for zero-allocation MessagePack navigation. This is a useful speed/space middle ground for variable-sized keys.
- Typed configuration benchmark (32 rules, 32 string properties): one full encoded walk about 2.08us/zero allocations; generated-style owning decode plus use 3.43us/102 allocations; MessagePack reflection decode plus use 5.60us/108 allocations; cached Go struct use 0.17us. At eight full uses: encoded walks 16.55us, generated decode once plus use 4.85us, MessagePack decode once plus use 6.99us. Decode-once projection is warranted for repeatedly used configurations.
- MessagePack compact leaves under an indexed root also work with zero-allocation nested access. The adapter uses 2,598 bytes for the state fixture versus 2,202 for all-compact integer-key MessagePack; for the transaction fixture 1,047 versus 1,008. Keep the cost of hot indexing explicit; this is not a universal smaller-than-MessagePack claim.
- Actual EDB/bbolt update measurements include transaction work and preserve output lifetime. Without fsync: roughly 7.47us current double-pack, 6.29us single-pack, 5.18us narrow-key copy/patch. With fsync on this local filesystem: all around 7.1–7.4ms; differences are dominated by commit noise. Codec microsecond wins must not be reported as end-to-end durable-write speedups.
- Dictionary-change encoding v2 folds field/index numbers or string lengths into path tags and folds Set/Delete into suffix length. Four string-key assignments shrink from 99 to 91 bytes with current experimental value encoding. This is a small, explainable saving; arbitrary uint64 map keys still get their full varint. Values can use the chosen compact codec independently of path framing.
- Correctness so far: round trips for all prototype layouts and mixed nesting; presence/zero/null/false/empty distinctions; full uint64 keys and values; negative integers; Unicode/NUL/empty string keys; width boundaries; unknown-child preservation; detached subtree views; changes, duplicate replay reference case, order, truncation. Tree fuzzing completed 414,557 executions; change-reader fuzzing completed 229,781 executions. These are prototype checks, not a production parser/security validation claim.

## Recommended design

Historical initial proposal. The compact-profile recommendation and the cross-generation single-slice view assumption are amended in the [follow-up](#revised-compact-recommendation); the indexed/edit/WAL/audit direction remains applicable.

**Use one logical model, two persistent value layouts, and one logical change format. Keep decoding into a Go struct or a flat slot array as an optional projection.**

The two value layouts are compact MessagePack-based values and indexed objects. They share field identities, scalar semantics, key codecs and change operations. An indexed object may contain compact children. A compact value may contain an indexed child through a reserved extension envelope. Most application code should see a typed view or a bound model, not select a codec on each call.

This is a research recommendation, not a production-ready wire format or an assertion that every requested performance target is proven. The experiments distinguish working mechanisms from proposed integration. In particular, the first custom compact format does not consistently beat MessagePack and the first generic indexed format does not beat current KVO on a hot scalar lookup. Do not ship those prototypes as-is.

The following initial policies cover every category in the registration ledger. They are source-informed defaults for testing, not claims of measured per-table hotness. A table supplies a default; a type/subobject may override it. Existing rows retain an explicit representation discriminator, so a policy change need not rewrite the whole table immediately.

| Ledger family | Initial persistence policy | In-memory access and change policy |
| --- | --- | --- |
| Frequently updated state; numeric KVO live state | Indexed hot records/maps; compact cold children | Bounded editor, then materialize touched objects; sparse assignments |
| Numeric KVO snapshots; period aggregates | Keep indexed for repeated partial analytics; test compact for cold completed periods | Borrowed projections; change stream only while mutable, snapshot on completion |
| Configuration | Compact | Decode/cache once; generic access through generated/model adapters |
| Configuration history | Compact snapshots plus logical changes where smaller | Reconstruct from checkpoints; retain schemas and causal audit envelopes |
| Profile and identity | Compact first; indexed hot counters/subobjects if access data justifies it | Partial projection/patch; explicit private-data boundaries |
| Catalog and content | Compact | Decode/cache or project; indexed large dictionaries only when repeated lookup warrants it |
| Event and audit, including transaction documents | Compact; compact child lists | Append/read sequentially; keep cause metadata above low-level changes |
| Work coordination | Compact for small queue/session/status rows | Whole-row replacement often simplest; indexed only for demonstrated hot mutable state |
| Dynamic objects | Compact with persisted schema/extension identity | Bound runtime handles; preserve unknown values; promote hot objects explicitly |
| Raw activity encoding | Preserve specialized payload with model adapter initially | Expose structured metadata and opaque payload; measure before replacing |
| Singleton values | Choose compact per value by default | Whole-value operations unless a value is itself a large mutable object |

String-keyed dictionaries retain dictionary semantics in every family. Their compact storage is MessagePack; hot instances can acquire an offset index or a decoded map. They do not require global string interning or a different public collection API.

### 1. Separate modeling from storage now

A model describes a record's fields, identity, type, presence/default rules, key codecs, validation and sensitivity. It must work with:

1. An existing Go row and generated accessors.
2. A borrowed compact or indexed view.
3. An edit session.
4. A decoded struct/slot projection.

This permits generic interfaces and patches across the existing database before converting every row's encoding. It also avoids forcing frequently read, already cached configuration into a tree navigator merely to gain modeling metadata.

Distinguish a **record** with declared fields from a **dictionary** whose keys are data. Both offer lookup/iteration, but they have different identity, typing, compression and evolution rules. A field number is not an entity ID or a user-supplied dictionary key.

Minimum value model: null; bool; signed/unsigned integers; float32/float64 with explicit precision policy; text; bytes; record; typed dictionary; ordered list; tagged union/extension. Missing is an absent property, not the integer zero. Default-returning convenience getters may remain, but `Get`/`Has`/`Delete` must preserve presence. Empty and missing containers remain distinct where the model requires it. Sets are ordered/unique list semantics, not a mandatory additional physical format.

Do not automatically reuse the existing microsecond-offset time word for every timestamp. Existing KVO uses an offset Unix-microsecond representation; Go/MessagePack time can preserve more precision. Choose a timestamp semantic type explicitly, retain the current codec for existing fields, and use a precision-preserving extension for new fields where needed. Keep decimal/scaled monetary-like values as typed integers, never silently convert them to floating point.

Custom text/binary/MessagePack codecs need model adapters. Treat raw JSON as an opaque value unless the model explicitly exposes its contents as a dynamic tree. Unknown enum values and opaque extension payloads must survive a read/patch/write cycle.

### 2. Compact profile: start with MessagePack, using local numeric field IDs

A compact record is a MessagePack map from stable model-local field numbers to values. A string dictionary is an ordinary MessagePack string-keyed map. A numeric dictionary retains numeric keys. Lists, strings, bytes and primitive values use their existing compact encodings. A composite key uses its declared canonical text/byte codec; current text-key adapters can remain unchanged.

This choice has a strong property: ordinary string dictionaries do not pay a new per-value header tax. The experimental delta/tag format saved space for clustered numeric state but made the 128-entry string dictionary 5% larger than MessagePack. It also lost to integer-key MessagePack for several mixed fixtures. That is insufficient reason to standardize another scalar/type system.

Specify canonical writer behavior independently of MessagePack's general flexibility: one field/key occurrence, deterministic key order, shortest permitted scalar encoding, exact byte/string distinction, explicit float-width policy, and a bounded extension namespace. Readers may accept the valid older forms required by migrations. Reject duplicate keys at untrusted boundaries rather than giving patches ambiguous semantics.

The compact profile can be navigated without allocating a Go map. The prototype does so. However, ordinary MessagePack containers carry element counts rather than total byte lengths: skipping a nested sibling may scan its contents. This is a real cost, not zero decoding in the strict sense. Large hot parents should use the indexed layout; configurations consumed repeatedly should decode once. A generated one-pass projection can also extract a handful of fields without repeatedly rescanning the same prefix.

**Size guarantee boundary:** the compact payload can match MessagePack for the same logical keys and values because it is MessagePack. This does not prove that an arbitrary versioned KVO row is always byte-for-byte no larger than an independently optimized one-letter-key MessagePack row. Model/version framing, field-number history and incompatible key dictionaries have costs. Measure the whole EDB row, including its existing envelope, and state any new framing overhead. Do not hide it in benchmark denominators. A universal guarantee against every possible alternative key assignment is not established.

Retain the delta/tag prototype as a numerical-compression experiment. If actual analytics datasets later justify packed homogeneous numeric maps, add one narrowly scoped typed extension and measure it against ordinary MessagePack. Do not make it a third general object format now.

### 3. Indexed profile: narrow keys, fixed scalar cells, local child ranges

The promising building block is a self-contained object with sorted keys and fixed cells. Schema-known scalar cells contain raw 64-bit values; reference cells contain a local byte offset and byte length. There is no mandatory record-wide object catalog. Copying a subtree preserves all of its internal references.

An implementation sketch, with tag assignments intentionally not frozen:

- A small fixed header identifies version, key class, cell class, key width, entry count and payload size. Use explicit little endian, not a native-endian `unsafe` cast.
- A numeric-key object has a sorted key array, using the smallest of 1/2/4/8 bytes that fits its keys, followed by fixed scalar/reference cells and child bytes.
- A typed record gets value kinds from its bound schema generation. A homogeneous `Map<K,V>` gets its element kind from `V`. A truly heterogeneous dictionary carries value kinds in its referenced compact values. Do not tax every known integer with a dynamic tag. The fully tagged-cell prototype is a comparison, not a requirement for every typed record.
- A reference cell uses a 32-bit offset plus 32-bit length relative to this object. Set a practical row-size limit below that address space and validate bounds/overflow. This covers current bbolt value limits without 64-bit pointer-sized overhead per offset.
- A string/byte-key dictionary stores sorted key/value entries and an offset index. The offset-index experiment is the initial default candidate for variable-sized entries: materially smaller than fixed cells, still much faster than a sequential scan. A decoded Go map is an optional repeated-access cache, not part of the on-disk contract.
- Variable-sized lists can use the same entry-offset idea when random indexing is needed. Sequential compact lists remain the default. Homogeneous fixed-width scalar lists are contiguous arrays, without per-element references.

Schema-elided cell kinds require the persisted **encoding descriptor**, not only the application binary's current semantic model. A reader that does not know a newly added field must still know whether its cell is an inline word or a reference before rebuilding the parent. Load that information from the recorded schema generation; if it is unavailable, preserve the entire object opaquely or refuse the patch. Do not claim arbitrary unknown-field preservation from an untyped 64-bit cell alone. Nullable/unions may use references to self-tagged compact values; never steal a bit from a full-range uint64 and silently reduce its range.

The `Words` experiment implements the scalar core and its copy/patch mutation. Its narrower keys reduce 128-field size from 2,064 to 1,160 bytes for local field codes. It wins the larger random working-set experiment, but current KVO remains faster on one hot object. Keep that trade-off visible. The 9-byte tagged-cell prototype is an intentionally more general comparison and is not the preferred schema-known scalar layout.

Avoid an independent numeric offset-index layout in the first production version. It adds size and decoding/indirection but cannot provide fixed-cell mutation; the measurements did not establish a useful default niche between compact values and fixed cells. Likewise, do not introduce a third bitmap or fixed-slot disk layout solely because a dense synthetic bitmap lookup is fast. Generated decoded slots provide that access pattern without committing the wire format to an occupancy assumption.

Strings themselves should normally be inline within the subtree that owns them. A global string/object catalog adds references, recovery rules and mutable handle maintenance. Introduce sharing only for a measured workload. The current data model is a tree; entity relationships should remain logical IDs. DAG sharing/cycles are not part of this first design.

### 4. Views and mutation without unnecessary wrappers

A generated `WidgetView` can contain one slice. Its model is known by generated code; it need not carry an interface/type pointer. The experiment's wrapper is 24 bytes on this machine, versus the current 64-byte `ImmutableMap`. An optional dynamic wrapper adds one model pointer, making it 32 bytes. These are measured Go sizes, not an assumption that every API object is that small.

Borrowed strings should first be exposed as bytes or an explicitly lifetime-bound text view. Returning an owning Go string may allocate. Do not build the allocation guarantee on callers retaining an unsafe string after the transaction closes. These are payload-view sizes: a row whose primary key lives outside its value still needs separate key/context storage, or a typed scalar key beside the view. Do not duplicate the key on disk merely to advertise a smaller wrapper. A detached subtree needs one copy; keeping a tiny borrowed slice can retain the entire original backing allocation, even with a three-index slice.

A mutable typed handle should be `editor pointer + node ID`, about 16 bytes, rather than a slice that becomes stale when the editor grows or repacks its storage. The editor owns flat node/edit tables and byte arenas. Static type information stays in generated accessors; dynamic access binds a model once. This editor handle design is proposed, not fully implemented by the experimental `Editor`.

Use a single logical edit session:

- Untouched objects remain borrowed views.
- A few edits use small caller-owned storage. Store scalar cells and byte spans in pointer-free metadata; avoid one allocation or Go interface per edit or child object.
- On an owned indexed scalar object, update the cell directly and record the logical change. Never patch bbolt's borrowed mapped bytes.
- For a compact object or a growing collection, keep edits until a sorted merge/rebuild, or materialize the touched object when repeated reads justify it. Reuse the indexed layout or a decoded slot projection; do not invent a separate unsorted full-record representation.
- Bound the small overlay. Read cost must not grow indefinitely with every write. The candidate crossover depends on object size and access frequency; do not hardcode a universal magic threshold from one benchmark.
- Coalesce final assignments per field for durable output while preserving ordered parent/child semantics. A tiny-row update may use a full replacement if it is shorter than a patch.

The general prototype editor still allocates and performs extra scans. Its role is to verify semantics and unknown-child preservation. The bounded scalar editor and pointer-free change reader prove that genuinely zero-allocation paths are achievable. A production generic arena needs its own escape/allocation regression tests; “scratch allocated locally” is not sufficient.

EDB/bbolt imposes an ownership boundary: bytes passed to `Put` remain borrowed until transaction completion. Use a transaction-owned output arena with non-overlapping ranges for different writes, or one owned output per row. A reusable scratch buffer is safe for calculation but must not overwrite pending writes. Indexed updates still copy the row and bbolt still writes changed pages. Small logical changes only reduce persistent write volume once the planned WAL/storage layer actually consumes them.

### 5. Decoded projections and optional adaptation

Keep generated Go structs for settings and other whole-object workloads. For dynamic models, bind field IDs to slot ordinals once and decode into flat scalar slots plus byte/reference arenas and an explicit presence bitmap. Avoid one pointer/interface per scalar. Do not reserve a uint64 slot for every property in every cold on-disk object.

An immutable typed view should not carry access counters or an adaptive heap wrapper by default. Offer explicit `Decode`, `Project(fields)` or a separate caching/adaptive wrapper. Existing long-lived configuration caches already provide the natural promotion point. An edit session can materialize an object as part of work it already owns.

The configuration measurements support the policy but not a universal numerical threshold: one encoded traversal is cheaper than full owning decode; repeated complete traversals rapidly amortize the decode. A scalar projection may allocate nothing when its storage is supplied by the caller. Owning strings, maps and child slices necessarily have different costs.

### 6. Stable identity without making recovery depend on initialization order

Keep three concepts distinct:

- Stable property identity: qualified semantic identity, with display-name aliases/renames.
- Model-local wire number: compact stable ordinal within the owning record type.
- Runtime field handle/slot: bound once for the current process or plugin session.

Existing built-in field identities can keep their declared numbers. The highest property code found in the audited registry is 491, so 16-bit keys already cover them. Do not force a database-assigned global property allocator merely to solve a size problem that narrow keys solve today.

For runtime-declared models, persist a catalog mapping a qualified model/field identity to model-local wire numbers and type descriptors. Allocate atomically, append only, never reuse retired numbers, and reject incompatible redeclarations. Bind plugins to that catalog once; do not hash strings or consult a database dictionary on every scalar access. Static generated code can use explicit stable local numbers. Dynamic code receives bound handles.

Persist/export the catalog with backups and WAL checkpoints. Record schema generation/dictionary identity where interpretation needs it; write catalog changes before records that reference them, in the same durable transaction. A recovery/export tool must work without the original application binary and must retain unknown fields and extension bytes. Compatible additions need no rewrite. Incompatible meaning/type changes need a new field identity or explicit migration, not silent reinterpretation of a slot.

Changing field names, physical layout or code generators must not change logical field identity. Dictionary strings and runtime entity IDs are not automatically interned into this catalog. Arbitrary strings can have unbounded cardinality and may contain private data.

### 7. Native changes for WAL, replay and audit

Use logical **Set** and **Delete** operations on typed paths. Each path component distinguishes a model field, numeric dictionary key, string/byte dictionary key or list index. Path `[]` can mean a whole-row replacement in the enclosing row operation. Row deletion is an explicit enclosing tombstone. Keep missing and null distinct.

Use final assignments for storage redo. An API-level `Add(3)` can remain an intent operation, but the durable storage change should normally carry the resulting value plus a base revision. Replaying an increment twice is not equivalent to replaying a final assignment. Do not persist physical byte offsets, mutable node IDs or Go pointers in durable changes.

The proposed compact path stream is small:

```
change-set = version schema-generation base-revision next-revision count change*
change     = shared-prefix-length suffix-count-and-op path-suffix payload?
field step = uvarint(field-number << 2)
uint step  = uvarint(1) uvarint(key)
text step  = uvarint(byte-length << 2 | 2) key-bytes
index step = uvarint(index << 2 | 3)
payload    = byte-length compact-value
```

`suffix-count-and-op` is `suffix-count << 1 | op`, with Set=0 and Delete=1. Varints are unsigned base-128. Paths use the preceding path as a prefix source; order is preserved. The full uint64 dictionary key remains separate so the tag cannot overflow or truncate it. For byte/tuple keys, the bound model supplies the canonical codec; a future distinct byte-key tag must be versioned if needed. Values use the compact profile; before-values, if requested, use the same encoding. The codec prototype has both initial and improved encoders, and the improved path reader uses integer spans without allocating strings.

Field numbers and list indices in this grammar must be below 2^62; arbitrary uint64 dictionary keys are unrestricted. Signed dictionary keys can use the bound key codec's ZigZag mapping through the uint step; a given dictionary's declared key type resolves interpretation. There must be no per-call guess between signed and unsigned. The prototype directly tests unsigned and text-key paths; signed/byte/tuple adapters remain part of the production model contract.

For one object with many related changes, a recursive sparse change tree could shave more prefix/count overhead. Do not add that second change grammar yet: the measured prefix stream is simple, incremental and already much smaller than full state for sparse changes. Whole-value replacement remains the fallback for dense changes and tiny rows. Choose by actual encoded size plus replay cost, not by assuming patches always win.

List semantics need an explicit decision before production. Replacing a list or an element is sufficient for a minimal version. Efficient insertion/deletion needs a defined splice operation with start/delete-count/inserted values and a base revision; never reinterpret an element Delete as “shift everything” accidentally. The prototype deliberately rejects list deletion rather than inventing that meaning. Lists with stable child IDs may instead expose a dictionary plus an explicit order list if that matches the application's semantics; do not impose that transformation universally.

The WAL envelope is separate from this value-change grammar. It needs table/row identity, transaction identity or commit sequence, record lengths/checksums, atomic batch boundaries, and an applied-sequence checkpoint. Recovery applies only complete committed batches against the correct base/schema; duplicate commits are skipped by durable sequence/transaction identity. A per-row revision equality check alone is not a complete deduplication protocol. Secondary index changes must be atomic with row changes and generated from before/after values, or logged as part of the same storage transaction. A no-op change should not create spurious audit entries or index writes.

For a WAL above bbolt, define authority explicitly: the external committed log may be replayed into a lagging bbolt checkpoint, and the checkpoint sequence must be stored atomically with the applied rows. Define what happens if the log commits but bbolt application fails; do not acknowledge success and then serve an unmarked stale state. Flush ordering, checkpoint/truncation, torn tails, schema changes and multi-row failure must be tested separately. This research does not implement or prove a WAL.

Audit envelopes attach actor, request/job, high-level action, reason and causality parent to low-level changes. Preserve the distinction between an intent log and a normalized storage redo log. Include before-values only where audit/undo requirements need them; otherwise use periodic snapshots plus forward changes. Full historical reconstruction needs a starting snapshot and retained schemas, not just a list of assignments.

### 8. Private data belongs to the model and encryption boundary

Represent sensitivity in the model, with typed private-text/private-bytes wrappers if useful. A private value may be plaintext under an authorized policy or an opaque sealed subtree. This should not double every string API or require a separate global string catalog.

A sealed subtree carries algorithm/version, key reference, nonce and authenticated ciphertext. Keys live outside the data/WAL/backup set that should become unreadable after key deletion. Use an established authenticated-encryption implementation; this project should not invent a cipher or nonce scheme. Bind associated data to stable tenant/record/field identity. Binding it to a mutable physical offset or every enclosing row revision would force re-encryption on otherwise unrelated edits.

Group values at the desired access/erasure boundary, often one subject's private subobject. Encrypting each tiny string separately has substantial nonce/tag/key-reference overhead. Updating a sealed subobject requires decrypting and re-encrypting that subobject; zero-copy scalar access cannot also apply to encrypted plaintext without a decrypted cache.

Private dictionary keys, indexes, historical before-values, debugging output, caches and exported catalogs can reveal data too. Audit/WAL payloads must protect the private bytes when first written; deleting a live-row key later does not erase plaintext that was already logged. Key deletion only covers ciphertext whose keys and key backups are actually removed. A semantic field annotation alone does not provide cryptographic erasure. Plugins must not be able to downgrade host-enforced sensitivity rules.

### 9. Dynamic/plugin access

Expose schema-bound object/field handles to a plugin, not host pointers or mmap slices. Handles are scoped to a transaction/capability; validate type, bounds, ownership and lifetime. Keep raw binary decoding and index maintenance in the host. Unknown data may be preserved opaquely, but untrusted plugins must not invent unchecked references into host memory.

Offer batched field projection, scans and `ApplyChanges` so a runtime does not cross the host boundary once per scalar in a large loop. The schema/path/change vocabulary should be the same for WebAssembly, an interpreter, admin UIs and native Go. Choosing the plugin runtime is a separate decision; no runtime-specific representation is required now.

### 10. Migration and adoption order

1. Define presence, scalar/key codecs, model identities and typed/generic access over existing Go rows. Add unknown-field preservation where read/patch/write may encounter newer schemas.
2. Define the logical change set and transactional application contract. Use it for an audit proof of concept; keep high-level intent envelopes separate.
3. Add compact numeric-field MessagePack encoding and zero-allocation navigation/generated projections. Preserve existing short-key rows through adapters; do not force an all-table rewrite.
4. Prototype the narrow-key indexed layout with strings/lists/unknown fields and mixed compact children, then benchmark actual representative data on the target server. The current scalar and mixed-node experiments establish building blocks, not a finished universal implementation.
5. Integrate transaction-owned output arenas and a bounded editor. Remove the observed duplicate Pack call in a separately reviewed production change; it is not changed in this research.
6. Migrate one hot state table and one configuration/snapshot table. Compare byte size, allocated bytes, scan CPU, write transaction occupancy, index costs and latency under the actual access mix.
7. Add the WAL using the agreed durability/replay contract. Add sealing only with key lifecycle, backup, audit and index handling in place.

Choose an unambiguous format discriminator before any mixed-version deployment. Existing KVO has reserved header flags, while existing raw tables may contain arbitrary bytes. Do not identify old/new formats by a heuristic first byte that can collide with an old object count. Use an explicit EDB row/table version or a proven reserved header flag/envelope. Preserve a decoder for old rows, rewrite lazily or in bounded maintenance batches, and keep rollback/export tools aware of both. The backend startup migration budget rules out rewriting the whole database at startup.

### Alternatives considered and disposition

| Idea | Finding | Disposition |
| --- | --- | --- |
| One compact sequential format for everything | Small, zero heap reads; sparse access still scans and repeated full walks cost CPU | Use for cold/full-read data, not all hot state |
| Existing KVO everywhere | Excellent hot scalar reads; 16 bytes per key/value plus catalog, no implemented text/list API, zero-as-absence | Preserve as baseline and migration source |
| Custom delta/tag compact format | Good clustered numeric sizes; loses on string dictionaries and several mixed objects | Keep experiment; not default wire format |
| Fixed cells with dynamic type tags everywhere | Flexible but bloated and slower than existing KVO hot reads | Use schema-known kinds; reserve tags for dynamic values |
| Numeric offset index over variable values | Pays both index bytes and scalar decoding; no fixed-cell updates | Do not add as an independent default layout |
| String entry offset index | Meaningful size/speed compromise, zero-allocation lookup | Initial indexed string-map candidate |
| Per-record hash table on disk | Fast possible lookups, but capacity/slack/hash policy/collision complexity and weaker ordered iteration | Prefer sorted index; optional decoded hash cache |
| Bitmap/dense fixed fields on disk | Fast for dense local fields, expensive for sparse/long-lived schemas | Use decoded projections first; revisit with occupancy data |
| Global database-interned property/string codes | Can shrink strings but adds catalog dependence and unbounded data-key cardinality | Only model-local field registry for runtime schemas |
| Global object/string catalog | Supports sharing but adds indirection and relocation/lifetime bookkeeping | Prefer local subtrees; no sharing until measured |
| Transparent adaptation in every view | Adds state/indirection and retention cost even to simple reads | Opt-in wrapper or explicit Decode/Project; editor may materialize |
| Physical byte diffs for WAL | Small for fixed cells but tied to layout/version and unsafe for logical history | Logical typed-path changes |
| Persist increments as storage redo | Replay can double-apply | Persist final assignments with revision/transaction gates |
| Whole-row WAL values only | Simple and ideal for some tiny/dense updates; expensive for sparse large-row edits | Retain as change-set root replacement fallback |
| Compression as the universal answer | Can improve storage, but introduces decompression and changes access granularity | Optional storage/chunk layer; not required by the core model |

## Primary-source research

These sources informed the design; performance claims above come from our own prototypes, not vendor benchmark rankings.

| Source | Useful idea and consequence here |
| --- | --- |
| [MessagePack specification](https://github.com/msgpack/msgpack/blob/master/spec.md) | Compact primitive encodings, integer or string map keys, extensions and timestamps. Reusing it avoids paying a new string/primitive overhead merely to support numeric field names. Its count-based nested containers explain the cost of skipping unknown large subtrees. |
| [FlatBuffers internals](https://flatbuffers.dev/internals/) | Relative offsets and shared schema-driven field access support direct reads. Tables pay a field-offset directory; structs trade evolution for fixed layout. Borrow the relative-offset idea, without forcing every sparse row into fixed slots. |
| [FlexBuffers documentation](https://flatbuffers.dev/flexbuffers/) | Dynamic maps/vectors and width selection demonstrate useful schema-free direct access. Uniform widths can be inflated by a wide scalar, so “automatically choose a width” is not a free compactness solution. |
| [Cap'n Proto encoding](https://capnproto.org/encoding.html) | Separates an addressable layout from optional packing. Packing improves zeros/padding but changes the access path; compression cannot preserve direct random reads without unpacking the relevant region. |
| [Protocol Buffers encoding](https://protobuf.dev/programming-guides/encoding/) | Field numbers, wire types, length-delimited values and ZigZag signed encoding are simple building blocks. Small integers are not all cheap: full-width IDs and negative signed values need deliberate codecs. |
| [VelocyPack specification](https://github.com/arangodb/velocypack/blob/main/VelocyPack.md) | The closest precedent: compact containers and indexed containers in one value system, including string-keyed objects. This supports a small number of container layouts rather than one universal representation. |
| [Apache Arrow format](https://arrow.apache.org/docs/format/Columnar.html) | Validity bitmaps and offset/data buffers are useful for decoded projections and homogeneous lists. Columnar arrays are not automatically the right layout for a mutable single document. |
| [V8 fast properties](https://v8.dev/blog/fast-properties) | Shared shape metadata and fixed slots can make repeated dynamic field access cheap; dictionaries handle sparse/arbitrary properties. Keep shape binding outside each scalar value, and make promotion explicit or opt-in. |
| [bbolt documentation](https://github.com/etcd-io/bbolt) and local v1.3.7 source | Transaction-lifetime borrowing and copy-on-write storage constrain mutation/output ownership. The actual local `Bucket.Get`, `Bucket.Put` and `node.put` implementations were checked; an executable lifetime probe confirms the supplied value is retained. |
| [RocksDB WAL format](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format), [atomic writes](https://github.com/facebook/rocksdb/wiki/Basic-Operations) | Logical operation batches still need separate framing, checksums, ordering and atomicity. A compact change codec alone is not a recovery protocol. |
| [CBOR RFC 8949](https://www.rfc-editor.org/rfc/rfc8949.html) | A useful alternative with integer keys, binary/text distinction and deterministic-encoding considerations. No benchmark here establishes a reason to replace the existing MessagePack dependency with CBOR. |
| [libsodium AEAD guidance](https://doc.libsodium.org/secret-key_cryptography/aead) | Authenticated encryption, associated data and nonce requirements belong at the sealed-value boundary. No cryptographic primitive is implemented by these experiments. |

## Measurement protocol and limits

Final main run: Apple M5 Max, 48 GiB RAM, darwin/arm64, Go 1.27.1; `GOMAXPROCS=1`; five repetitions of each case, 150ms minimum benchmark time. The main run includes 171 benchmark cases. The additional edit-density and generated MessagePack decoder runs use the same settings. Raw observations and medians/min/max are retained under `results/`.

Baselines include current KVO, its actual mutable overlay/Pack path, the observed double-Pack call pattern, zero-allocation MessagePack navigation, typed/dynamic MessagePack decoding, cached Go structs, direct scalar slots, and a raw KVO array scan. We do not compare only against the slowest reflection/map decoder. Fixture construction and index construction are outside read timings. Owning decoded data is deliberately included in decode timings. Reused-buffer and already-owned mutation paths are labeled separately from allocating or durable writes.

Eight generic mixed fixtures cover profiles, state, settings, child-record lists, sparse events, ordinary and long-prefix string dictionaries, and an opaque blob. Additional numeric cases vary field count and local/global/spaced/random key distributions. All fixture values are synthetic; no application records, identities or business literals are copied. The clustered-ID state case is favorable to delta coding, and the key-distribution sensitivity results are part of the evidence, not an optional caveat.

The large working-set test uses 65,536 independently stored 128-field records and pseudorandom row/key selection. It tests cache-sensitive access, not cold disk I/O. The smaller direct-read benchmark repeats a predictable key permutation and must not be mistaken for random production branch behavior. The full scan's 2-allocation KVO iterator result is an API-path effect: the raw KVO word-array scan is zero allocation. Do not attribute that overhead to the underlying byte layout.

Benchmarks are not fully optimized production implementations. In particular, the fixture encoder allocates temporary buffers freely and is not evidence about production encoding throughput. The measured editor/Pack and owning-copy paths are separate implementations. The proposed exact combined schema-aware indexed wire format, code generator, schema catalog, plugin ABI, PII sealing and WAL are not implemented. A full parser validator and hostile-input limits remain production requirements; fuzzing the experimental tree builder/reader is not a substitute.

No target-server CPU, live production occupancy/row-size distribution, secondary-index-heavy workload, sustained GC trace, or real concurrent reader/writer traffic was measured. The bbolt write benchmark updates one row without secondary indexes. Fsync measurements show commit dominance on this filesystem and have broad variance. Do not publish their small differences as a durable-write speedup. The next production decision needs anonymized size/access histograms and a representative target-server replay; this research does not pretend source-level table enumeration supplies those distributions.

A final control strengthens the compact-format choice: generated-style decoding of an integer-field MessagePack configuration uses 1,493 payload bytes and about 3.83us/102 allocations, versus 1,558 bytes and about 3.86us/102 allocations for the custom compact codec. Most of the gain over the reflection baseline comes from access/decoding strategy, not a new scalar wire format.

## EDB integration observations

- Current typed rows already have an envelope containing format flags, schema version, modification count, data length and stored index-key length. Reuse/extend this boundary rather than inventing a contradictory per-row version scheme. KV/raw rows do not currently share this envelope.
- Typed `PutVal` builds new index entries, compares encoded payload/index bytes, stores the output in transaction-owned buffers, and can decode old/new rows for change callbacks. Generic model support must account for those callback/index costs, not only `Get`.
- Typed-row index removals can use saved old index keys; KVO index callbacks currently examine both old and new values. Declared field dependencies could avoid recomputing unaffected indexes, but begin conservatively and prove dependencies before skipping work.
- Re-encoding an unchanged logical row into a new layout changes bytes. It must not automatically become a user-data audit event or an unintended semantic revision bump. Distinguish physical rewrite/schema migration from logical modification.
- Current mutable KVO enumeration counts/lists the overlay, not a merged base-plus-changes object. A generic patch/UI abstraction needs merged iteration and explicit presence semantics. This is an API contract change to design, not a production fix made here.
- Keep index maintenance, change notifications, no-op detection, row/schema revisions and transaction ownership inside EDB's commit boundary. Exposing a modeling layer alone must not allow plugins to bypass those mechanisms.

## Final measurements

All times below are medians of five runs on the environment described above. The 171-case main run plus edit-density, generated MessagePack and improved-change-codec supplements contain **190 benchmark cases**. Min/max, allocated bytes and all five original observations are retained. These are synthetic component measurements unless the row explicitly names EDB/bbolt.

### Reads: size and access pattern matter together

| Workload | Current KVO | Candidate | Allocation/result |
| --- | ---: | ---: | --- |
| 128 numeric fields, hot lookup | 6.66ns | Narrow-key words 10.00ns | Both 0 allocations; current KVO wins |
| Same numeric payload size | 2,064 bytes | Narrow-key words 1,160 bytes | Local field codes; 44% smaller |
| Random reads across 65,536 such rows | 72.89ns | Narrow-key words 56.90ns | Both 0 allocations; smaller layout wins this working set |
| Random reads within one hot row | 25.36ns | Narrow-key words 33.04ns | Same random-access benchmark, opposite result |
| 128 numeric fields, sequential compact lookup | — | MessagePack view 353.8ns; custom compact 628.2ns | Both 0 allocations; no decoded map |
| 128 numeric fields, fully tagged indexed lookup | 6.66ns | 17.43ns | Dynamic tags/dispatch are not free |
| 128 numeric fields, complete raw scan | 34.99ns | Tagged indexed 353.8ns; MessagePack view 804.1ns | All 0 allocations; current raw-word scan is strong |
| Nested mixed-state access | — | Indexed root + MessagePack leaves 104.4ns; all-MessagePack view 1,186ns | Both 0 allocations; indexed hybrid is 2,598 vs 2,202 bytes |
| 128 short string keys | — | Offset-indexed 77.14ns; MessagePack view 370.3ns | Both 0 allocations; 2,952 vs 2,563 bytes |
| Same dictionary, fixed cells | — | 40.91ns | 4,742 bytes; faster but much larger |
| Same dictionary, already decoded Go map | — | 5.76ns | Excludes construction and retained map memory |

The raw KVO scan directly visits its word array; its public iterator benchmark instead reports 187.3ns and two small allocations. The difference is an API/iterator implementation cost, not a reason to discard the underlying layout. Direct-slot and bitmap lower bounds likewise exclude lookup/binding/decoding and are not fair whole-operation replacements.

### Mixed payload sizes

Bytes below include each experimental value's own container framing, but exclude the common EDB row envelope, secondary index keys and any external schema catalog. “Custom mixed” means indexed root with custom compact children; it is not the proposed standard MessagePack compact profile. The all-indexed column is the flexible tagged prototype, not the narrower schema-known scalar core.

| Fixture | Custom compact | All indexed | Custom mixed | Integer-key MessagePack | Short-field-name MessagePack |
| --- | ---: | ---: | ---: | ---: | ---: |
| Profile | 296 | 514 | 357 | 279 | 289 |
| Nested state | 1,112 | 3,542 | 1,530 | 2,202 | 2,270 |
| Settings | 5,169 | 9,958 | 6,630 | 4,972 | 5,293 |
| Transaction with children | 1,020 | 2,046 | 1,053 | 1,008 | 1,141 |
| Sparse event | 51 | 85 | 85 | 48 | 54 |
| String dictionary, 128 entries | 2,693 | 4,742 | 4,742 | 2,563 | 2,563 |
| Long-prefix dictionary, 128 entries | 8,197 | 10,246 | 10,246 | 8,195 | 8,195 |
| Opaque 32KiB blob | 32,787 | 32,798 | 32,798 | 32,783 | 32,785 |

The state fixture's clustered runtime numeric IDs are unusually favorable to delta coding. The key-distribution control is essential: for 128 scalar entries the custom compact size grows from 436 bytes with local keys to 1,443 with random uint64 keys, while narrow-key words grow from 1,160 to 2,056. Current KVO remains 2,064. The production census establishes that both model fields and runtime numeric dictionaries occur; only the former can safely be renumbered within a model.

### Mutation and ownership

| Operation, 128-field object | Median | Allocated bytes / allocations |
| --- | ---: | ---: |
| Current KVO, construct one-edit overlay | 54.47ns | 208 / 4 |
| Current KVO, edit and Pack once | 1,234ns | 2,536 / 6 |
| Current call pattern, edit and Pack twice | 2,416ns | 4,864 / 8 |
| Tagged indexed, allocate copy then patch | 98.09ns | 1,408 / 1 |
| Tagged indexed, copy into supplied buffer then patch | 27.25ns | 0 / 0 |
| Tagged indexed, patch already-owned buffer | 16.27ns | 0 / 0 |
| Bounded scalar editor, edit and copy/pack into supplied output | 22.76ns | 0 / 0 |
| Generic editor over indexed base, merge to compact output | 1,893ns | 448 / 1 |

The generic editor is not yet the desired fast path. Its escaping scratch array and compact output merge are retained as negative evidence. The zero-allocation scalar experiment is limited to existing fields and bounded edits; it does not prove arbitrary string growth, insertion or a full generic editor has zero allocations.

The edit-density supplement compares 1, 8, 32 and 128 edits. With 32 edits, current overlay reads cost 20.42ns vs 10.22ns from the materialized words object; at 128 edits, 30.77ns vs 10.19ns. Current edit-and-Pack takes 2.77us/18 allocations and 9.78us/24 allocations respectively. Copying into preallocated words storage and applying the same edits takes 310ns and 1.19us, zero allocations. This supports bounded overlays followed by materialization, while **the Pack comparison deliberately has different output ownership**: current KVO allocates a fresh output; the words case reuses supplied space. A transaction arena must supply that space safely before those numbers become an EDB guarantee.

### Whole-object configurations

| Configuration operation | Median | Allocations |
| --- | ---: | ---: |
| One complete encoded walk | 2.08us | 0 |
| One owning custom generated-style decode and use | 3.86us | 102 |
| One owning MessagePack reflection decode and use | 6.41us | 108 |
| One owning integer-field MessagePack generated-style decode and use | 3.83us | 102 |
| One use of cached Go struct | 0.164us | 0 |
| Eight encoded walks | 16.67us | 0 |
| Decode custom once, use eight times | 6.26us | 102 |
| Decode MessagePack through reflection once, use eight times | 8.74us | 108 |
| Eight uses of cached Go struct | 1.32us | 0 |

The generated-style MessagePack control occupies 1,493 bytes vs 1,558 for custom compact. Owning strings/maps/slices dominate allocation counts; changing scalar framing does not remove them. Keep the existing configuration cache and supply generic modeling access to it. There is no benefit in navigating the encoded tree on every access merely for consistency with hot state.

### Logical changes and actual storage writes

The improved prefix stream with standard MessagePack values encodes four assignments to string dictionary keys in **87 bytes**, including version/schema/base/next/count, excluding outer row/transaction/checksum framing. The initial codec used 99 bytes with the original custom value format. The improved 16-change stream encodes into reused space in **124.7ns** and reads borrowed paths/payloads in **192.1ns**, both with **zero allocations**. These measurements do not include applying changes, validation against a model, index maintenance or durability.

| One-row EDB/bbolt update, no secondary indexes | NoSync median | Allocations | Durable median |
| --- | ---: | ---: | ---: |
| Current double-Pack call pattern | 7.76us | 48 | 7.57ms |
| Single Pack control | 6.71us | 46 | 7.08ms |
| Narrow-key copy/patch | 5.50us | 41 | 7.12ms |

Durable observations overlap substantially: about 6.5–7.9ms across the latter two cases. Fsync dominates and there is **no demonstrated durable-write speedup**. The NoSync results establish that codec/mutation savings survive the EDB transaction layer in this small test; they do not measure WAL bandwidth, indexing or concurrent application throughput.

## Validation and reproduction

Completed checks:

- EDB baseline suite before experiments; final `go test ./...` and `go test -short ./...` both pass.
- `go test -race ./research/kvo` and `go vet ./...` pass.
- All research Go files are formatted; `git diff --check` passes. Existing tracked EDB files and the backend checkout remain unchanged.
- Correctness covers supported scalar ranges, explicit presence, string/numeric dictionaries, mixed nesting, offset width boundaries, detached subtrees, edits and unknown opaque children, ordered changes, truncation and a reference revision gate. `ExampleWidget` executes the typed/dynamic API sketch.
- Tree fuzzing: 414,557 executions over 10 seconds, pass. Malformed change-reader fuzzing: 229,781 executions over 10 seconds, pass. The parser limitations stated above still apply.
- Zero-allocation assertions cover the bounded scalar editor, nested views and improved borrowed change reader. Real bbolt tests confirm output-buffer retention and update readback.
- The complete initial request and both scope additions were checked against this chat's transcript; the exact text is retained here.

From the EDB repository root:

```sh
go test ./...
go test -short ./...
go test -race ./research/kvo
go vet ./...
GOMAXPROCS=1 go test ./research/kvo -run '^$' -bench . -benchmem -benchtime=150ms -count=5
go run ./research/kvo/cmd/census /path/to/backend
```

The full benchmark command now includes the supplements too. Do not run competing CPU-heavy jobs while collecting timings. Preserve new output under a new filename rather than overwriting the original evidence. Run `python3 research/kvo/summarize.py path/to/new-run.txt` to produce a neighboring CSV with medians and ranges. The census is source-only and anonymizes declaration names by default; `-private` is available for local inspection but private output should not be added to this generic research directory.

Authoritative final measurements: `results/bench-final.txt` / `.csv`, `bench-edit-density.txt` / `.csv`, `bench-msgpack-generated.txt` / `.csv`, and `bench-changes-v2.txt` / `.csv`. `final-sizes.txt` contains the size matrix; `words-tests.txt` contains scalar-key sensitivity checks. `test-full.txt`, `test-short.txt`, `test-race.txt`, `vet.txt`, `fuzz-tree.txt` and `fuzz-changes.txt` contain validation evidence. Earlier runs, including the initial failed nested-MessagePack baseline, remain as documented experiment history; they are not the final result set.

The next implementation work is the staged adoption plan above. Before freezing the indexed wire format, obtain representative anonymized row-size/occupancy/access distributions and repeat these controls on the target server. Remaining decisions are explicit: exact indexed header/tag assignments, persisted physical descriptors, generic arena growth/compaction, list splice semantics if required, plugin binding, WAL authority/recovery and private-data key lifecycle. They do not invalidate the tested building blocks, but this research has not implemented them or claimed their performance.

## Follow-up: schema-directed compact encoding

### User steering — verbatim

> The custom compact codec did not consistently beat MessagePack—an important reason to simplify the proposal.



I would still love to a custom one just to avoid dependency on msgpack impl given that we won't be using a lot of it, and so that we don't have to rely on msgpack annotations for our specifics (say we always skip primary key aka first field on a struct when encoding). But okay to use msgpack binary format if we want... but then it's a self-describing format; are you sure you cannot improve on it?



### Correction and experimental plan

The first conclusion was too broad. A custom implementation, application/schema binding, and the wire grammar are three separate decisions. EDB can own the first two whether or not it retains any MessagePack bytes. Omitting the root row key belongs to the row model/binder, not a MessagePack tag. Nested structs must not automatically lose their first field.

The untested opportunity is schema-directed compact encoding: record presence plus schema-ordered values; typed dictionaries/lists without repeated element tags; length-prefixed variable bytes; schema-known scalars. Compare this to both integer-key MessagePack and a schema-shaped MessagePack baseline that also omits record keys. Keep runtime dictionary keys as data. Test sparse broad schemas, booleans/null/missing, schema evolution/unknown fields, full-width numbers, generated decoding and allocation behavior. Report external schema/framing costs explicitly.

### What was implemented and what changed

Added `schema_compact.go` and `schema_compact_test.go`, plus schema-directed checks in the existing tree fuzzer. The codec package itself imports only the Go standard library. A separate small MessagePack writer in the tests also uses no MessagePack implementation: it writes the required primitive/container bytes directly. It is checked against the existing library baseline. This demonstrates that choosing MessagePack bytes would not require its reflection machinery, encoder pooling or struct tags.

The new compact experiment uses the stored writer descriptor:

- Record values occur in descriptor field order, without field numbers or value-type tags. The descriptor retains stable logical field identities; physical ordinals are local to a particular generation.
- Optional presence is explicit. The prototype chooses between all-present, a bitmap, and sorted ordinal deltas by encoded length. This is one small field-set primitive, not three public object formats or three choices for application code.
- A record whose entire field set is declared required has no presence bytes. This is a schema contract, never inferred just because one observed instance happened to populate every field. Required-field deletion is rejected. Mixed required/optional optimization is not implemented yet.
- Typed strings/bytes use unsigned length plus raw bytes. Typed numbers use unsigned varints or ZigZag; float64 uses eight raw bytes. Booleans currently use one byte; they are not treated as constant merely because the fixture happens to contain only true.
- Typed lists/dictionaries store a count and known-type elements. Numeric dictionaries retain their actual data keys and delta-encode sorted keys. String keys retain their bytes. This is distinct from replacing property names with schema ordinals.
- Nullable values have an explicit null/present discriminator. Truly heterogeneous values retain the earlier self-describing escape in this prototype. A production dynamic grammar could instead borrow selected MessagePack tags; that choice is still open.
- Record/collection skipping is recursive; schema knowledge does not magically make sequential data randomly addressable. The indexed profile remains necessary for hot partial access.

The stronger MessagePack comparison encodes fully populated known records as arrays in schema order, retaining ordinary maps for sparse records and runtime dictionaries. It shares the same descriptor dependency for reconstructing array field names. This removes most record-key overhead without inventing new scalar bytes. The sparse control does not implement a MessagePack array containing a separate presence bitmap; therefore sparse-record wins over that control are not a proof against every schema-shaped MessagePack construction.

### Size results

All numbers below are payload bytes. The primary key is retained in the first table for comparability with the original fixtures. A separate control removes it equally from both candidates. No shared descriptor/catalog storage is charged to an individual row; that cost must be amortized and reported separately. Add explicit version/format framing where the EDB envelope does not already provide it. The raw test output includes a hypothetical +2-byte framing column to make small-row sensitivity visible.

| Fixture | Integer-key MessagePack maps | Schema-ordered MessagePack | Schema compact | Main interpretation |
| --- | ---: | ---: | ---: | --- |
| Profile | 279 | 269 | 268 | Most savings come from omitting record keys |
| Nested state | 2,202 | 2,134 | 1,009 | Clustered numeric dictionary keys strongly favor deltas; not solely tag elimination |
| Settings | 4,972 | 4,651 | 4,653 | Schema order captures almost all the gain; custom is two bytes larger |
| Transaction with child records | 1,008 | 875 | 808 | Record-key removal and typed integer coding both help |
| Sparse event, only its populated fields declared | 48 | 40 | 40 | An optimistic narrow descriptor; broad-schema control follows |
| Short string dictionary, 128 entries | 2,563 | 2,563 | 2,562 | Existing short-string prefixes already combine type and length efficiently |
| Long-prefix string dictionary, 128 entries | 8,195 | 8,195 | 8,066 | Length encoding improves; key-prefix compression is not implemented |
| Opaque 32KiB blob | 32,783 | 32,781 | 32,781 | Bytes dominate; almost no removable information |
| Typed configuration | 1,493 | 1,392 | 1,387 | Mostly record-key removal; small remaining scalar/framing gain |

The nested-state fixture's eight 16-entry numeric maps save 840 bytes just from using key deltas instead of absolute base-128 keys. That component is orthogonal to schema-known types. The same integer transforms could be placed inside a MessagePack-compatible typed extension; merely retaining MessagePack byte syntax does not prescribe the logical container grammar.

Explicit required-field controls reduce configuration from **1,387 to 1,354 bytes**, and transaction with required child-item fields from **808 to 776 bytes**. These save one presence marker per required record. They are alternative contracts for synthetic fixtures, not evidence that all fields in the audited application can be made required. The comparison schema-ordered MessagePack sizes remain 1,392 and 875 bytes because those arrays still require a container header.

Root-primary-key omission control:

| Fixture with row key omitted from all encodings | Integer-key MP | Schema-ordered MP | Schema compact |
| --- | ---: | ---: | ---: |
| Profile | 269 | 260 | 259 |
| Transaction | 998 | 866 | 799 |
| Sparse event | 38 | 31 | 31 |
| Blob | 32,773 | 32,772 | 32,772 |

`ExampleSCExampleRow` encodes an ordinary Go row without annotations. Changing only its ID leaves the payload unchanged; the nested object's first field is retained. This illustrates the binding rule the user requested. EDB's existing `reflectTypeWithoutCache` already identifies the first struct field as the key; a new binder can apply that rule once when creating a row model. The existing generic `encodingMethod.EncodeValue` still uses the MessagePack library; no production behavior was changed.

### Sparse records, numeric boundaries and evolution

A fixed 1,024-field writer descriptor was reused across varying occupancy. The presence encoder selected ordinal deltas for very sparse rows, a bitmap for intermediate/dense rows, and all-present for the full row. Zero and false remain real present values; missing and null remain distinct.

| Present fields out of 1,024 | Schema compact bytes | MP map bytes | Schema-ordered MP control |
| --- | ---: | ---: | ---: |
| 0 | 1 | 1 | 1 |
| 1 | 4 | 4 | 4 |
| 6 | 33 | 46 | 46 |
| 128 | 442 | 768 | 768 |
| 512 | 1,380 | 3,044 | 3,044 |
| 1,023 | 2,522 | 5,962 | 5,962 |
| 1,024 | 2,401 | 5,974 | 3,284 |

Base-128 is not uniformly better than MessagePack's integer widths. Uint 256 takes two bytes instead of three; 2^32 takes five instead of nine; MaxUint64 takes ten instead of nine. ZigZag also grows some small positive signed values. The prototype tests the extrema, but this is not a finished choice of primitive grammar. A bounded nine-byte uint codec or model-declared fixed-width ID representation deserves a focused comparison before freezing it. Do not accumulate ad hoc per-field encoder knobs solely to optimize a single fixture.

The unknown-field test changes a known field while copying a newer field's raw bytes unchanged, using the **stored writer descriptor** to identify spans. This preserves unknown application semantics, not unknown physical layout. Without the descriptor, an old binary cannot generally locate the next field in this encoding. Every referenced writer generation must remain recoverable from the database catalog, backups, WAL/checkpoints and export bundle; never decode historical data using whichever Go struct happens to be compiled today.

A dynamic plugin can still use this format: its schema is declared at runtime and bound once. Dynamic code does not imply every persisted value is untyped. Truly arbitrary `any` leaves require self-description or an explicit union/schema identifier, while the enclosing known record need not repeat all its tags.

### Timing and allocations

Final follow-up run: same Apple M5 Max / Go 1.27.1 environment, `GOMAXPROCS=1`, 150ms, five repetitions, 18 cases. Raw observations: `results/bench-schema-compact-final.txt`; medians/ranges: matching `.csv`. The earlier follow-up run is retained separately.

| Operation | Schema compact | Independent MP map implementation | Schema-ordered MP |
| --- | ---: | ---: | ---: |
| Encode profile into reused output | 163.9ns | 162.3ns | 157.1ns |
| Encode settings into reused output | 3.84us | 3.52us | 3.42us |
| Encode transaction into reused output | 1.26us | 1.20us | 1.16us |
| Encode short dictionary into reused output | 965ns | 1,012ns | 1,020ns |
| Owning generated configuration decode and use | 1.83us | — | 2.06us |

All four encoding workloads report zero allocations for all three candidates with sufficient caller-owned output. The two generated decoders each allocate 5,104 bytes in 102 allocations for owning strings, maps and slices. They use equivalent one-pass typed traversal; the previous map-navigation decoder is not the only baseline. The required-record optimization's size is measured, but these decode timings use the general all-present marker, not the required-record variant.

Numeric lookup in a 128-field compact record measured 312ns with schema direction versus 363ns for the existing MessagePack map view, both zero allocations. That is still much slower than indexed scalar lookup. This prototype improves space and some reads but is not a universal encoding-throughput win. The reused-output encoder is a generic descriptor interpreter, not generated field access; the fixture tree and descriptors are constructed before timing. Dynamic container fallback behavior outside these fixtures is not covered by the zero-allocation claim.

### Revised compact recommendation

Historical recommendation, superseded where it requires a writer descriptor for structural traversal. The later [schema-independent skipping requirement](#schema-evolution-requirement-schema-independent-field-skipping) rules out the unframed schema-directed prototype as the final format. Its measured savings remain useful experimental evidence, but do not include the framing needed to satisfy that requirement.

1. **Own the codec and model binding in EDB.** No MessagePack annotations, reflection encoder, pooling policy or third-party API needs to define KVO semantics. Primary-key omission is a root row-model rule; nested objects keep their first field. Apply exactly the same omission when benchmarking alternatives.
2. **Make the compact profile schema-directed.** Persist the physical writer schema, encode required fields directly, describe optional presence once, and encode values in schema order. Give dictionaries/lists a known element codec. Retain a tagged escape only for genuinely dynamic values. This replaces the initial recommendation to use ordinary MessagePack maps as the default compact representation.
3. **Retain the indexed profile and logical changes.** The compact and indexed profiles share model semantics and writer descriptors. A change's typed path can determine the payload codec for known fields, so change values can use the schema-directed compact profile too; arbitrary/dynamic/root-replacement cases carry the necessary schema identity. The measured 87-byte change result still refers to the prior MessagePack payload experiment and is not relabeled as this codec's result.
4. **Keep the binary sketch provisional.** The tested mechanisms are convincing; exact presence tags, mixed required/optional layout, integer grammar, dynamic escape and container length framing still need joint evaluation. Frame large compact children where constant-time skipping merits its bytes, or use the existing indexed parent; do not silently add a length to every tiny record. The current tests do not implement the exact final mixed compact/indexed grammar.

This changes the compact recommendation, not the number of general storage representations. Schema order offers the largest straightforward simplification; tags only carry information that the bound schema cannot supply. A custom implementation of selected MessagePack primitives remains a valid component where their byte trade-offs win. There is no need to import the full implementation or standardize every KVO object as an ordinary MessagePack map.

**View-size consequence:** fully schema-directed data makes the writer binding more important. A generated view for a fixed writer generation can still be one slice. A generic view spanning multiple historical generations needs a bound layout pointer, an external binding context, or a descriptor identity in accessible bytes plus lookup. For the ordinary bound view, budget one slice plus one pointer (32 bytes here). Do not claim a universal one-slice wrapper with both zero lookup cost and no writer-layout context. A detached compact subtree is not independently interpretable from bytes alone unless its descriptor is supplied; a fixed-generation typed wrapper can supply it statically. This qualifies the earlier single-slice discussion.

The design has an established precedent: [Avro's binary encoding](https://avro.apache.org/docs/1.12.0/specification/#binary-encoding) uses a writer schema to omit field names and type information, and its schema-resolution contract keeps writer and reader schemas distinct. We are not adopting Avro or its complete grammar. The useful lesson is that self-description can move into shared metadata; it does not disappear from the system. [MessagePack's specification](https://github.com/msgpack/msgpack/blob/master/spec.md) remains the comparison for efficient tagged primitives and containers.

### Follow-up validation and remaining scope

- Full suite, in-memory suite, research race check and vet pass; output retained in `test-schema-full.txt`, `test-schema-short.txt`, `test-schema-race.txt` and `vet-schema.txt` under `results/`.
- Extended tree fuzzing completed 243,960 executions over 10 seconds, passing schema-directed round trips as well as the earlier layouts. `fuzz-schema-compact.txt` preserves the output.
- Focused tests cover scalar extrema, bool/null/missing/empty, heterogeneous and nullable lists, sparse/dense presence, required-field rejection, unknown-field byte preservation with a writer descriptor, independent MessagePack writer agreement, zero-allocation borrowed lookup, and the row-key omission example. This is not hostile-input validation for the new decoder.
- `gofmt` and `git diff --check` pass. Only uncommitted research files are present; no production EDB edits were made. The follow-up request was copied directly from the chat transcript.
- The schema catalog, migration binder, fully generic patching across generations, final mixed-format envelope and production validation remain unimplemented. New results support the revised compact direction; they do not establish a universal byte-size or speed guarantee.

## Schema evolution requirement: schema-independent field skipping

### User clarification — verbatim

So I want to document what I wanted to do about the schema. So you can see that the existing EDBQ code balances self-describing and compactness. The idea is that if you don't have a schema or say a particular key is missing, you should be able to correctly parse the overall value while skipping the value of that key, this is for me the balance. So you don't have to parse be it to be able to parse the value of the corresponding key or to interpret it in any way, although we kinda allow it because we say if it's a map or a string, right, in our previous system, so it's like kinda self-describing, but that's not even necessary. It is a nice slightly nice, but I mean, we cannot really format values without without knowing their type, so it's pretty useless. So I would say that that's a balance to strive for. So say we do schema evolution and there is a key that we dropped. We don't have to preserve information on which type it was, because the remaining information in the represent in the encoding itself should be enough to just completely skip that key for future reads and writes.

### Confirmed contract

**The encoding describes structure; the schema describes meaning.** With no semantic schema at all, a reader must still be able to traverse an object's field entries, determine where each value ends, and continue to subsequent entries. With a partial/current schema, it interprets known fields and skips unknown ones. Skipping an unknown field must not require interpreting that field's value, discovering its semantic type, or loading a historical writer schema.

This is deliberately weaker than semantic self-description. The bytes need not distinguish a string from arbitrary bytes, or an integer from some other scalar representation, unless that distinction is useful to the physical format. A reader may know only “these are the bytes belonging to this field.” Correctly formatting an unknown value is not a requirement.

When a property is removed from the current schema, its historical semantic type may be forgotten. Old rows must remain structurally readable and rewritable. Retaining all past type descriptors merely to skip removed fields is outside the accepted design.

### Consequences for the proposed formats

- Field identity/position and value boundaries must be discoverable from the physical format. Stable wire field numbers, key deltas, or a stable wire-defined presence scheme can provide identity; transient ordinals whose mapping exists only in a discarded writer descriptor cannot substitute for that.
- Minimal structural metadata is appropriate: for example, fixed-size cells, a varint extent, a length-delimited span, or an offset directory. These are candidate mechanisms, not a selected grammar. A wire class can describe how to skip bytes without naming their semantic type.
- A whole-object length alone is insufficient when an unknown field precedes another field that must be read. Framing must permit finding the next sibling. An unknown nested object can be skipped as an opaque span; recursively interpreting its contents is unnecessary for that operation.
- Typed collections can still omit repeated semantic tags. When an entire collection is an unknown field, its enclosing extent must be enough to skip it without an element schema. Independent element navigation, where exposed by the collection API, needs its own structural boundary rules.
- Writes must also work without recovering the unknown field's type. Copying an untouched unknown value as an opaque span is a useful default; explicitly retired fields may be omitted under an explicit removal policy. The skipping requirement itself does not authorize silently dropping all unknown fields, since an older writer may encounter fields added by a newer writer.
- An indexed layout must make copying/relocating unknown data safe. If rebuilding an object requires distinguishing inline cells from child references, that distinction must be discoverable from the wire structure, or the layout must preserve their addressing without such interpretation. Depending on a historical semantic type to repair unknown references does not satisfy the contract.
- Schema catalogs may still serve semantic field names, plugin declarations, validation and rendering of historical data. They are not prerequisites for structural traversal or opaque preservation of unknown fields. Stable identity/non-reuse and intentional type-change migrations remain separate concerns; skippability alone does not prevent misinterpreting a known field whose meaning was changed incompatibly.
- Logical change records need the same property: an unknown field's Set payload must have a discoverable boundary so a reader can continue to later changes. The existing change-stream payload length already provides such a boundary; payload interpretation remains schema-dependent.

### Effect on the current research

The unframed `SCShape` prototype cannot skip an unknown field without its writer descriptor. It therefore **does not satisfy the clarified requirement**. The recent recommendation to persist every writer layout as a prerequisite for locating field boundaries is withdrawn. The experiments remain useful measurements of the savings from removing keys/type tags, but a compliant compact candidate must include the necessary structural information and be measured again. No new size or speed claim is made by this documentation update.

The earlier schema-elided indexed proposal also needs review wherever unknown inline/reference interpretation depended on a stored descriptor. The general direction—compact and indexed profiles, a shared model, bounded editing and logical changes—remains compatible with this requirement.

### Acceptance cases for the next candidate

1. Write fields A, B and C. Remove B and all knowledge of B's semantic type from the reader. Read A and C correctly, including when B has a variable-size or nested value.
2. Open the same object with no semantic schema. Enumerate physical field identities and extents and reach the object's end without interpreting values.
3. Change A or C and serialize the object while preserving B's opaque data, without obtaining B's old type. A separate explicit retired-field policy may discard B instead.
4. Exercise compact/indexed nesting and reference relocation so unknown data is not corrupted during rebuilding.
5. Reject malformed lengths/offsets structurally; semantic type validation is not required to detect a boundary outside its container.

Only the research document changed in this turn. Existing prototype code and measurements are preserved as history; no tests were rerun for this documentation-only change.


## Draft checkpoint, cleanup, and standalone specification

### User instruction — verbatim

Okay, I'll let you clean up your implementation now. In fact, commit what you have, but don't push these commits. They are like draft commits. I will probably go and change them later, but commit what you have, then clean up the implementation and write a spec in a format that would be comprehensible, so plain language, simple, but something that could be published, read, shared with others, a spec of how this works, including the binary formats, in-memory representation, and actual APIs, and how the usage looks like. So basically a file that could become a readme of some of the system or something like that.


### Checkpoint and scope

- Created branch `codex/kvo-representation-draft` from the unchanged EDB base. Committed all existing research as **`e90c73c` — `WIP: checkpoint KVO representation research and experiments`** before cleanup. Nothing was pushed.
- The user authorized local draft commits and a standalone, shareable specification. They did not approve a final wire format or a production migration. The grammar below is an implementation choice for review, not a newly attributed user decision.
- Moved the previous Go experiment files without changing their contents into `experiments/`. Kept the audit, requests, history, raw results, and census tool. Historical command paths now need `./research/kvo/experiments`; this is documented there.
- Added `README.md` as the standalone specification. It does not require reading this chronology and contains no application-specific business names or private checkout paths. It covers the model/structure distinction, exact binary grammar, memory layout, ownership, APIs, examples, logical changes, evolution, integration boundaries, and measured limitations.

### Consolidated reference

A coherent `draft/` package replaces the incompatible prototypes as the executable reference. It is deliberately separate from production `kvo/` and imports only the standard library.

- **Structural classes:** null, unsigned varint, fixed 1/2/4/8-byte scalar, and a length-delimited byte span. No text/object/float semantic tag is required to skip an unknown field. Null, missing, zero, false, and empty remain distinct.
- **Compact:** sorted numeric key deltas, normally combined with a three-bit structural class; shared classes for uniform containers; full-width delta escape. String dictionaries keep real keys. Lists and dictionaries use the same body classes, sharing them where possible. Children are opaque bounded byte spans.
- **Indexed:** numeric key arrays plus eight-byte cells and optional packed class bits. Byte references are object-local offset/length pairs, making opaque relocation possible without schema. String dictionaries and lists instead use offset directories over their compact entry bodies, avoiding an eight-byte cell per short value. These are kind-specific layouts within two profiles, not another public representation choice.
- **Views:** one slice per object, including a detached child. No record-wide catalog pointer is necessary for tree data. Sharing/cycles are model-level IDs. Generic semantic binding may carry a model separately; a typed wrapper has just the view.
- **Editing:** sorted numeric overlays and a pointer-free edit array, with a caller-provided byte arena. Scalar and byte edits pass a zero-heap-allocation check with local scratch arrays and reused output. The editor preserves unknown bytes and merges into either layout. It does not yet edit arbitrary nested paths or emit change records automatically. String-map/list changes rebuild the affected container.
- **Owned scalar patching:** a separate operation for existing indexed numeric cells. It never mutates transaction-borrowed bytes. A uniform container cannot change its shared class in place; `Word` provides a stable class for hot counters. A mixed container can update an individual scalar class.
- **Changes:** ordered path-based Set/Delete, prefix reuse, typed path steps, revision header, and independently length-delimited Set payloads. The parser uses offsets for borrowed string steps. No semantic schema is needed to reach subsequent changes. This is a codec, not a revision gate, change applier, durable WAL, or audit engine.
- No extra automatic format-selection wrapper, decoded runtime tree, catalog implementation, generator, or plugin ABI was added. Their responsibilities are stated in the standalone specification.

### Implementation findings and corrections

The first editor implementation caused a local edit array to escape. A cursor that stored borrowed `Value` slices through its receiver made Go's escape analysis conservative. The cleaned-up cursor mutates only integer positions and returns the current entry; it does not store returned slice pointers in the receiver. The editor also retains full scratch capacities and uses counts. Both scalar and byte edits now measure zero allocations in an ordinary optimized build with sufficient scratch/output. Growing buffers still allocates; the output lifetime must still satisfy bbolt's transaction contract.

An initial race-instrumented run reported one editor allocation, while normal optimized builds reported zero. This was an allocation-budget failure, not a race report. Allocation budgets now live in a `!race` test file; functional editing, randomized map comparison, validation, and examples still run under the race detector. The initial output is preserved in `results/test-draft-race-instrumentation.txt` rather than discarded. The subsequent race run passes.

Validation checks counts and byte lengths before narrowing to `int`, including compact headers and dictionary keys. It bounds each container/stream at 64 MiB and each entry count at 1,048,576. `Open` validates only the enclosing object: deliberately invalid nested bytes can still be preserved as an opaque field. Explicitly opening that child is a separate validation step.

The checkpoint's first staged whitespace check exposed two preexisting issues that earlier unstaged checks had missed: CRLF CSV output and trailing spaces in verbatim user requests. CSV files were normalized to LF without changing values, and `summarize.py` now writes LF. A directory-local `.gitattributes` exempts only trailing spaces in `STATE.md` so the original requests remain exact. Historical statements that `git diff --check` passed were narrower than a check of all then-untracked research files; the final staged check includes the additions.

### Fresh size evidence

`experiments/draft_test.go` converts the same generic fixture trees to the reference grammar and checks every field and child in compact, indexed, and mixed forms. Fixture conversion is outside benchmark timing. All candidates retain the same fields, including any synthetic row ID. These measurements include the new structural framing; they do not reuse the incompatible schema-directed sizes.

| Fixture | Compact | All indexed | Indexed root / compact children | Integer MP | Short-key MP |
| --- | ---: | ---: | ---: | ---: | ---: |
| Profile | 285 | 380 | 333 | 279 | 289 |
| Nested state | 1,085 | 3,207 | 1,453 | 2,202 | 2,270 |
| Settings | 5,131 | 8,002 | 6,274 | 4,972 | 5,293 |
| Document with child records | 1,011 | 1,954 | 1,040 | 1,008 | 1,141 |
| Sparse event | 47 | 82 | 82 | 48 | 54 |
| String dictionary, 128 | 2,563 | 2,824 | 2,824 | 2,563 | 2,563 |
| Long-key dictionary, 128 | 8,067 | 8,328 | 8,328 | 8,195 | 8,195 |
| Blob, 32 KiB | 32,783 | 32,793 | 32,793 | 32,783 | 32,785 |

Compact is at most the short-key baseline on these eight fixtures, but not always at most integer-key MessagePack. In particular, settings are 159 bytes (3.2%) larger than integer-key MessagePack. The nested-state benefit comes largely from clustered numeric-key deltas. No universal size guarantee follows from synthetic fixtures. All-indexed storage is predictably wasteful for many cold nested values; independent child layouts retain the useful trade-off.

### Timing, allocation, and limits

The first five-repetition run of the consolidated draft used Apple M5 Max, Go 1.27.1, `GOMAXPROCS=1`, and 100 ms per case. It measured 128-field lookup at 869 ns compact, 14.0 ns indexed, 6.59 ns current KVO, and 353 ns borrowed MessagePack. String-dictionary lookup was 1,231 ns compact and 96.3 ns indexed. All allocated zero. The separate uniform-word fixture measured owned scalar patching at 10.6 ns and four edits plus full indexed serialization into reused output at 4.63 µs, also zero allocations. Two logical changes encoded in 24.8 ns and decoded in 44.8 ns with reused output/path scratch.

The final run after defensive bound checks is retained in `bench-draft.txt`/`.csv`; the initial run remains in `bench-draft-initial.txt`/`.csv`. Final 128-field lookup medians were 875.5 ns compact, 14.09 ns indexed, 6.497 ns current KVO, and 364.1 ns MessagePack. Owned patching measured 11.04 ns, full indexed four-edit serialization 4,626 ns, and change encoding/reading 25.39/45.1 ns. All 25 final cases still reported zero allocations. Raw ranges show normal run-to-run variation; the standalone README uses this final run.

The initial run is useful evidence that the current compact cursor and multi-pass editor need optimization before production adoption. Smaller encoded sizes do not imply faster local lookup. The current KVO hot-object lookup remains faster than the draft indexed lookup. The earlier large-working-set results concern an earlier prototype, so they are not transferred to this grammar. This cleanup does not claim new end-to-end database performance results.

On this build, `View` is 24 bytes, `Value` 40, pointer-free `Edit` 40, `Editor` 88, and pointer-free `PathPart` 32. The source uses no unsafe operations; `unsafe.Sizeof` appears only in the layout test. Caller lifetime and non-aliasing rules are part of the API contract.

### Validation and current reproduction

- Full EDB suite and `-short` suite passed, retaining production Bolt and in-memory coverage. Production sources are unchanged.
- Race checks passed across the research packages; `go vet ./...` passed. The focused output includes allocation budgets and executable examples.
- The raw parser fuzzer includes valid seeds for both layouts and all container kinds, then checks iteration and lookup after successful validation. The final 10-second run completed 892,495 executions. Numeric round-trip fuzzing completed 408,727 executions; change-parser fuzzing completed 356,495 executions. All passed. These runs are useful validation, not proof of production security.
- Randomized editing is checked against a plain Go map across both source and destination layouts, including scratch/arena growth, replacement, insertion, deletion, and byte values. Golden fixtures cover object and change bytes. Unknown-field tests deliberately use a byte payload that cannot be parsed as a nested object and verify cross-layout preservation.
- Current results: `sizes-draft.txt`, `bench-draft.txt`/`.csv`, `test-draft-{full,short,race,focused}.txt`, `vet-draft.txt`, and `fuzz-draft-{open,roundtrip,changes}.txt` under `results/`. The allocation instrumentation failure has its own retained result.

```sh
go test ./...
go test -short ./...
go test -race ./research/kvo/...
go vet ./...
go test -v ./research/kvo/draft
go test -run TestDraftFixtureSizes -v ./research/kvo/experiments
GOMAXPROCS=1 go test -run '^$' -bench '^BenchmarkDraft' \
  -benchmem -benchtime=100ms -count=5 \
  ./research/kvo/draft ./research/kvo/experiments
GOMAXPROCS=2 go test ./research/kvo/draft -run '^$' \
  -fuzz '^FuzzOpen$' -fuzztime=10s
```

Repeat the last command with `FuzzNumericRoundTrip` or `FuzzChanges` for the other fuzz targets. All prior experiment files, results, and user decisions remain available. The standalone README is the current proposal; this file is the durable work history.

Final staged review: all old experiment Go files match the checkpoint byte-for-byte after relocation; existing CSV values match after CRLF normalization. README links resolve, public examples contain no application-specific names or private paths, `gofmt` reports no changes, and `git diff --cached --check` passes. Only `research/kvo/` is included in the cleanup commit, `WIP: consolidate schema-skippable KVO draft and specification`. Both commits are local drafts; no push is authorized or performed.


## 2026-10-16 — main branch and whitespace cleanup

User instructions:

> Move these to master, please.

> # Preserve trailing spaces in verbatim user requests.
> STATE.md whitespace=-blank-at-eol
>
> Kill this, please. When I say verbatim, I don't mean that verbatim, especially given that I dictated the whole thing and what you get is the output of the recognition.

Decision: preserving the user's wording does not require preserving incidental trailing whitespace. Removed the STATE.md whitespace exception and trimmed trailing spaces throughout this file. The repository's primary branch is named `main`; move the local draft commits there without pushing.


## 2026-10-16 — error policy, zero defaults, and row-level format identification

### User instruction

So there are changes I want to make here. The first one is that the API you designed is hard to use because of all the pervasive error handling. So let's create an error policy.

The idea of the policy is that we follow what we do elsewhere in edb and Bubblehouse. We return errors for expected errors, and we panic for invalid usage and unexpected errors.

So, for example, setting a value, from what I understand, should never fail, because again, I'm not sure how you're imagining the implementation or how you did the implementation, but ideally, it would automatically allocate memory as necessary, so it might go out of the pre-allocated storage area, but what it cannot do is fail.

Usage errors like setting an unknown key or something like that should panic. It should still panic with a defined error, but it should panic so that there is no pervasive error checking.

And the final thing that really complicates APIs is presence testing. I propose that we stop distinguishing zero values from missing values. This will help us in multiple ways.

First of all, it's the Go way. Right? So second, it maps nicely to structs. We wouldn't have to find a way to represent separate missing state. And in particular, we don't have separate null value. Null is just zero, same as missing.

And I think we never, I mean, for edits, we have to encode whether we're setting a zero value. But when storing, just like right now, storing zero value should not store anything, which helps when designing these data types.

In Go, we already are conscious of what zero value is, and this just extends it further. Similarly, zero length byte array, zero length string, those are all zero values and should just not be encoded.

Or at the very least, I mean, we don't have to devise an encoding for them, but even if we do in the normal usage, it should not be distinguishable.

I could imagine that we could mark a certain type as optional, and then we would explicitly distinguish zero value from missing value. But this is a bonus feature. I'm not sure if we need it.

But if we do use optional types in a few places, if we do use them, we could simplify. I mean, instead of encoding like a literal, like optional something type, we could just make mark the value, mark a particular key as being optional, and then define an extended API for this.

Which either the API you did, which returns the value and boolean presence flag, or separate like has, like if it's if the key is something, you would do has something which returns a bool, something which returns a value, and maybe like maybe something that returns the value and bool.

I don't know, something like that so that although in that case, just calling something, I guess, should be like must something or something like that. I don't know, like, I don't like these conventions, honestly.

But if it's missing, just something would return like an optional type with a missing flag, and must something would return the value or panic if it's not available, has something would return a bool, and maybe something would return a value and a boolean. Where if it's missing, it's zero value. That is a better convention.

A few more comments on the encodings you have. I think you added a header byte to every object, and by object you mean every map that we have. I'm not really liking it. I could see one bit used for like encoding type, compact or non-compact, but otherwise I don't think we need a version in there.

The reason is that the version of the encoding will be like a global thing, which will be once per row, and not just version, but like the specific kind of encoding, right? We currently specify msgpack, and I think we reserved some bits for picking among encodings, and even if we did it, we'll find a way. We do not need to version every object for sure.

### Accepted direction

- Expected external-input failures return errors. Invalid usage and unexpected failures panic with defined errors. Ordinary setters allocate beyond scratch capacity rather than returning failure.
- Ordinary reads return the Go zero value for absent fields. Null has no separate semantics. Map/record writers omit zero scalars and empty strings/bytes. Lists retain their positions, and edits/change streams retain an explicit zero assignment.
- Optional presence is a possible future key-level feature, not the default API and not required in this revision.
- Encoding kind/version belongs in the enclosing row (or change envelope), never in each nested object. Pack structural layout/kind/class metadata with the existing count/descriptor instead of a separate version byte.
- Keep unknown-field structural skipping and opaque rewrite guarantees. Retain prior measurements as history and rerun sizes/benchmarks for the new grammar.
- The only preexisting working-tree edit was trailing whitespace in a README example. Updating that example will normalize it under the previous user instruction. Production EDB remains outside this experiment.

### Implementation and policy decisions

The reference and standalone README now implement the instruction together:

- `Get`, `GetString`, `At`, scalar/byte accessors, and `Object` return their value directly. There is no `NullValue`, `IsNull`, or ordinary `(value, present)` API. A missing object can be traversed as an empty child; indexing outside a list is still invalid usage and panics.
- `NewEditor`, `Set`, all append builders, and `AppendChanges` no longer return errors. The editor grows edit storage and its byte arena. Removed the old 64 MiB cumulative arena cap, which could reject repeated replacements even when their final result was small. Serialized objects retain the documented 64 MiB reference limit, enforced with `ErrLimit`; this is not a fixed scratch-capacity limit.
- Explicit external-input decoding (`Open`, `ReadValue`, `OpenChanges`, `NextChange`) returns `ErrFormat`. Normal child access panics on corrupt data. Parsers now check bounds explicitly and recover only their defined format error; unrelated programming panics propagate rather than being disguised as malformed input.
- Defined misuse errors are `ErrOrder`, `ErrKind`, `ErrValue`, `ErrType`, `ErrUnknownKey`, `ErrIndex`, and `ErrLimit`. Existing constructors validate the data they can know; semantic interpretation still belongs to the model.
- Added a minimal shared `RecordModel` containing declared keys. Its Get/Set/Delete check membership and panic with `ErrUnknownKey`. It supplies an executable example of the record policy without pretending to implement the complete schema system. Raw dictionaries still allow arbitrary new keys; absence from stored data is not an unknown-property error.
- Numeric and string-map builders omit zero scalars and empty bytes. The zero setter records a clear operation so an old nonzero field is removed. Lists retain positions with a zero class that has no payload. Signed float zero collapses to ordinary zero; NaN payload bits remain intact. `ObjectValue` handles empty modeled containers without guessing whether arbitrary bytes contain a child object.
- `TryPatchOwned` is explicitly an optional optimization. Clearing a map field falls back to the normal editor/rebuild, preserving the default zero-elision rule. A provisional implementation retained a zero cell for fast patching; this was removed because ordinary writes should not introduce a second stored-zero behavior. The API still returns false for other unsupported fast-path cases, without changing the data.
- Changes keep explicit Set-zero operations. For maps these clear values; for lists they replace a position rather than deleting it. Set-zero, Delete, and no operation remain distinct in the stream. Path scratch now grows rather than treating insufficient capacity as malformed input.
- Optional presence remains deliberately unimplemented. Its possible key-level declaration and specialized accessor convention are documented as future choices. This version does not claim lossless round trips for a future optional-zero encoding; that extension will need an explicit preservation rule as well as its API.

### Revised framing

The compact object no longer starts with a separate header/version byte. Its count varint is `(count << 6) | (shared_class << 3) | (kind << 1)`, with low bit zero for compact. Two kind bits retain numeric/string/list traversal without semantic schema; three class bits describe physical boundaries. Those are structural metadata, not type names or version numbers.

The indexed descriptor packs layout bit 1, two kind bits, three class bits, and two width bits into one byte, followed by the existing fixed count. Its fixed prefix drops from six bytes to five. The separate object version is gone. The standalone change payload also drops its `KC`/version prefix and relies on its enclosing format identity.

A single numeric field `{1: Uint(7)}` now takes three bytes (`50 01 07`). The two-field golden object still takes eight compact bytes because its combined count/metadata tag occupies two varint bytes; its indexed form drops from 27 to 26. Header removal is not claimed to save one byte for every possible compact count.

Inspected EDB's current `encvalue.go`: `valueFlags.encoding()` still returns MessagePack. The research defines where format selection belongs but does not claim existing production flags already dispatch this codec. Assigning the new row-envelope encoding identifier remains future integration work. Existing draft bytes are not auto-detected or silently reinterpreted.

### Fresh measurements

All eight fixture trees are checked to contain no zero-valued map entries before comparing sizes, so this comparison does not give KVO alone the benefit of zero elision. Both candidates retain the same fields and any synthetic root ID. The existing generic fixtures and MessagePack baselines remain otherwise unchanged.

| Fixture | Compact | All indexed | Indexed root / compact children | Integer MP | Short-key MP |
| --- | ---: | ---: | ---: | ---: | ---: |
| Profile | 285 | 376 | 332 | 279 | 289 |
| Nested state | 1,076 | 3,193 | 1,444 | 2,202 | 2,270 |
| Settings | 5,130 | 7,873 | 6,273 | 4,972 | 5,293 |
| Document with child records | 1,010 | 1,919 | 1,038 | 1,008 | 1,141 |
| Sparse event | 47 | 81 | 81 | 48 | 54 |
| String dictionary, 128 | 2,562 | 2,823 | 2,823 | 2,563 | 2,563 |
| Long-key dictionary, 128 | 8,066 | 8,327 | 8,327 | 8,195 | 8,195 |
| Blob, 32 KiB | 32,783 | 32,792 | 32,792 | 32,783 | 32,785 |

Five 100 ms repetitions during this revision, on Apple M5 Max / Go 1.27.1 with `GOMAXPROCS=1`, report medians of 881.7 ns compact and 13.97 ns indexed for 128-field lookup, compared with 6.838 ns current KVO and 359.6 ns MessagePack. String-map lookup measured 1,215 ns compact and 95.67 ns indexed. Owned nonzero-cell replacement measured 10.56 ns. Four indexed edits plus full serialization into reused output measured 5,309 ns, versus 4,626 ns in the prior draft run: zero filtering and the revised cursor add work, so no blanket speed improvement is claimed. The fixed-word mutation fixture now uses nonzero initial values to keep all 128 fields under zero elision.

Two logical changes encoded in 27.41 ns and decoded in 45.76 ns. All 25 benchmark cases report zero allocations with sufficient reused buffers. Ordinary-build allocation assertions also cover stack-backed scalar and byte edits. Growing an editor or path buffer does allocate, by design. Struct sizes remain View 24 bytes, Value 40, Edit 40, Editor 88, PathPart 32 on this build. The minimal record declaration is shared and adds no pointer to those representations.

Results are retained separately as `results/sizes-zero.txt`, `bench-zero.txt`/`.csv`, `test-zero-{full,short,race,focused}.txt`, `vet-zero.txt`, and `fuzz-zero-{open,roundtrip,changes}.txt`. Previous draft results remain historical and have not been overwritten. The README cites the new measurements.

### Validation

- Full EDB tests and in-memory (`-short`) tests pass. Research race checks and `go vet ./...` pass.
- New tests exercise zero-default getters, scalar/string/byte/container omission, positional zero list elements, zero edits, explicit Set-zero change payloads, growing edit/byte/path scratch, and the declared error panics. Unknown fields with deliberately uninterpretable payloads survive cross-layout rewrites through a model that does not declare those fields.
- Three ten-second fuzz runs pass: 1,517,672 executions for structural open/traversal, 419,749 numeric round trips, and 233,625 change parses. A parser programming panic is no longer converted to `ErrFormat`, so these runs also exercise the explicit-boundary checks.
- Golden bytes cover compact and indexed objects plus change payloads under the new framing. Executable examples use the simplified APIs. Ordering remains checked even for zero entries that will be omitted.
- Production sources and historical experiments are unchanged except the current-draft comparison adapter. The README's incidental preexisting trailing spaces were normalized while replacing the affected example, consistent with the preceding user instruction.

Current work is left uncommitted on `main` for review; no push was performed. Reproduction commands in the previous section still apply to the same current package paths.


## 2026-10-16 — interactive iteration: tags, incremental encoding, alignment, and two-word values

### User instruction

I'm going to propose certain changes and you measure whether they actually are an improvement. I am totally willing to discuss if you disagree that proposed changes are an improvement right from get go, or if you see a better way. I'm just pointing out potential optimizations, but I do want you to disagree if I'm wrong. In fact, that's true across this entire research project, so maybe put it in a prominent place in the state file. I also want you to update both the state file and the readme file as you make changes. We are now in the interactive iterations phase, so you are allowed to talk to me and ask me questions.&#x20;



## Wire classes



Now I think your wire classes could use some tweaking, especially based on what I said about missing == zero.  Now I'm wondering why we use wire classes, because it seems like we're losing quite a lot of bits to encode these wire classes, while what we really need is to know the length of the value, right? Especially to be able to skip or move it quickly, we need to know the length. And the wire class doesn't really give you length, I mean, it does give you a length for scalars, but if it's a container, you don't know the size of it. If instead we went with the byte size encoded using uvarint, we would be able to skip it.



We should also explore a mixed layout with tag bytes, for example:



1. Small positive integers (0 .. 127) and small negative integers (-1 .. -63) are encoded as themselves as single byte. Thats 128+63 different values.
2. Values 0 to 63 bytes in size specify byte count in first byte, then encode the bytes. We never use uvarint encoding for actual values (because we already know the size).
3. Longer values use a sentinel value as first byte, then uvarint-encoded (length-64), then the value itself. Rationale: for a large value, wasting an extra byte is very low %, and this maximizes the number of values that have a single-byte header.



That's 128+63+64+1 different values, covering the 256 possibilities we have.  We can certainly do a different balance here. For example, I'm not sure we need to encode negative integers up to -63, because likely negatives are usually either small (up to -15 say) or rather large, so maybe allowing a compact encoding up to -63 is a waste of bits that could be put to a better use for something else.



We could also cut this differently, using 1 bit for scalar vs composite, and for scalar do small positive and negative integers, and lengths otherwise; zero value is just 0 here (for when it needs encoding, ie keys that distinguish missing and zero). For non-scalar, integers make no sense, so it's just the number of bytes 0..63, sentinel value for 128+. 0 bytes = zero value for when it needs to be encoded (keys that distinguish missing and zero). Theoretically we could even do:



0xxx xxxx - scalar small positive integer (0 .. 127)

1111 xxxx - scalar small negative integer (-1 .. -16)

10xx xxxx - composite value with given byte size (0 .. 62 bytes, or sentinel with separate uvarint(length-63) for 63+)

110x xxxx - variable-length scalar value with given byte size (0 .. 30 bytes, or sentinel with separate uvarint(length-31) for 31+)

...we could introduce a separate bit pattern here specifically for 8-byte, 4-byte and 2-byte integers perhaps...



I believe this would drop your shared wire classes idea. The reason I don't like shared wire classes is that while it minimizes overhead on large lists of similar items, it cannot be encoded incrementally. Like if you're adding stuff to a list, at some point you will have to jump to a larger size class or something like that. For the same reason, if we know the length of every value before decoding it, it might make sense to put metadata at the end and not at the start, to improve encoding efficiency. Right?



Does dropping shared classes add a lot of overhead? Note that most one-byte values encode as themselves, so only larger values like uint16 .. uint64 gain an extra byte.



I could see, however, re-introducing wire classes to combine it with key delta in numeric dictionaries. We could allocate 2 bits maybe, where 00 = normal tag byte follows, 01 = 2-byte value, 10 = 4-byte value, 11 = 8-byte value... but maybe no need and tag bytes are fine for everyone...



&#x20;but this is not elegant. Would be very nice to somehow combine tag with key deltas in numeric dictionaries, but I don't know. Or maybe just leaving them separate is just fine and we don't need to overthink it&#x20;



## Indexed



Is alignment a concern? Because my original layout used 8-byte alignment for everything, assuming that it's much faster to access aligned values. If it isn't much faster, we need to maybe look into alignment in our structures.



## In-memory



For Value, I definitely want lower level optimizations. Look at how slog.Value is built. We want to do something like that where we unpack the slices and use raw pointers. After all, if the point is that we're only reading this value, we don't need the cap of the slice, for example. We just need the length. So maybe we can pack anything into one pointer and one uint64, as long as its type is known externally.



Same for View, I suppose -- it does not need cap, right.



## Overall



&#x20;I confess I'm getting confused now because there are all of these options. So maybe you can untangle this for me, look into this some more and find again, spend a lot of time.\
\
I'm actually going away for dinner, so spend time untangling all of this and coming up with the best you can and then comparing it against the previous encoding scheme. What's better? What's worse?\
\
The idea here is to allow incremental encoding so that encoding uses less memory allocations or fewer buffers or something, less copying, right? Because you would incrementally encode each value separately. You would need to encode values and then see like one, say A key crosses certain threshold, you need to go and upgrade all previous keys or something like that.\
\
So ideally we want to avoid that so that we can go and encode stuff without going through everything. But maybe it's not a concern. I just know that encoding values is a big part of what we do as well. So we need to benchmark the time encoding takes, not just decoding, right.

### Investigation underway

Compare the current draft unchanged against length-only values, the proposed unified immediate/length tags, and split scalar/span tags. Measure complete encoding, buffer growth, nested construction, and reads separately. Test alignment on this host and inspect Go code generation. Prototype two-word borrowed values/views with GC-visible pointers and verify GC/checkptr behavior. Preserve the previous grammar as a baseline until results justify a recommendation.

### User clarification

For unknown child lengths, yes, you would be buffering the children, right? For like offset catalog or something. But that's reusable buffers that usually are bounded by the largest container size you can encounter. So those buffers can be reused during encoding.


### User clarification

Like, think of it this way. We have a single large buffer that we're encoding into, and we'd like to be able to put large stuff into it without separately encoding it into an intermediary buffer and then copying that buffer, right? Because if we copy a value at each layer of the tree, that's actually a lot of copying.


Implementation focus after clarification: one growing destination contains every payload exactly once. Only per-container metadata is buffered. Test a trailing length catalog against inline tags with reused child buffers and inline tags with backpatching, and account for payload bytes copied at every nesting level. Reused payload buffers alone do not satisfy the requested construction property.

### Interim findings and controls retained

- Implemented length-only, the exact 128-positive/63-negative/64-length tag proposal, the proposed split partition, and a denser split (128 positive, 16 negative, eight scalar widths, 103 direct byte lengths). The proposed split leaves codes 0xe0..0xef unused. No candidate is treated as an approved replacement format.
- Implemented forward inline encoding with reusable child payload buffers, forward inline encoding with prefix reservation/backpatching, and a trailing length catalog that buffers only metadata. Added a two-bit numeric-key/width variant to measure the suggestion rather than dismiss its complexity. It recovers most of the extra numeric-map bytes, but not homogeneous-list overhead.
- First-pass results show ordinary tag encoders about twice as fast as the current reference's complete tree encoder, with all warmed writers at zero allocations. These are implementation comparisons, not proof that wire classes intrinsically cost twice as much. Retained raw initial and later runs separately.
- Exact payload-copy accounting for a 32 KiB blob under 16 records: buffered inline copies 492,480 additional child bytes, backpatching moves the same amount, and the catalog copies no child payload. The tag catalog copies 112 metadata bytes and retains 128 bytes of metadata-buffer capacity (excluding its frame array and destination). Destination growth can still copy already-written bytes; warmed capacity eliminates that in these runs.
- Important correction to the initial concern: shared classes do not inherently prevent forward incremental construction. A trailing catalog can choose the shared class when it closes, after payloads are already in place. Added a class-catalog control that keeps the previous scalar grammar and uses pointer-free 24-byte metadata entries plus a reusable dictionary-key arena. This avoids retaining input strings in pooled scratch. Its metadata flush is a pass over scratch, not a second traversal of the source tree or a payload rewrite.
- Added an indexed numeric variant with payload first, then keys/classes/cells and a five-byte count/descriptor trailer. Key width is chosen at close; widening only affects the metadata written then. Scalar cells remain directly patchable. Only a numeric indexed root is implemented in this experiment; indexed string/list directories remain a design extension.
- Added controls that keep the old bytes: a generated-style two-pass writer and a lookup that skips unrequested scalar bodies. The latter is materially faster than the reference getter and faster than these tag readers, so the earlier read speedup must not be attributed solely to a new encoding.
- Added backward construction, following the construction direction described by FlatBuffers, producing byte-for-byte identical current draft objects. It writes into one destination without per-level payload copies after warm-up. It requires a replayable/random-access source and reverse traversal, with a shared-class/count prepass. This is an alternative writer strategy, not another on-disk representation.
- Added an executable Begin/Uint/Bytes/End catalog API sketch that emits fields incrementally, without building a Value tree. Empty children are elided. It demonstrates the requested producer flow; it does not claim a complete schema-bound API or dict/list streaming support.
- `slog.Value` on this Go build is 24 bytes, not 16: it contains a word and an interface. The same idea can be narrower for KVO. Both a type-directed pointer/word value and a physical-class-preserving value with real static marker pointers are 16 bytes. A pointer/length View is also 16. All pointers are GC-visible; no pointer is retained as uintptr and no numeric bits are fabricated as pointers. Tests exercise heap/interior pointers, forced GC, and stack growth.
- Alignment measurements cover within-line unaligned loads, deliberately split cache lines, and full indexed binary searches. Within a cache line, offsets 0..7 behave alike on this host. Split lines are slower in the larger working set. One-byte indexed keys remain faster/smaller than eight-byte keys in this control; universal eight-byte padding is not justified by these measurements. No x86 runtime benchmark has been run.

The original `draft/` remains the baseline in this iteration; the new prototypes live under `experiments/`. Earlier error/zero/framing edits remain intact and uncommitted.

### Footer packing correction and rejected variant

The first tag/catalog comparisons omitted the per-object layout selector because each experiment was selected explicitly. Added the compact/indexed selector bit to every candidate footer before the final comparison. This changes some small-object boundaries: the shared-class catalog's state fixture becomes 1,096 bytes versus the 1,076-byte baseline (about 1.9%, not the earlier approximately 1%). Current tag/catalog size results are in `test-iteration-candidates.txt`; earlier test outputs retain their original, incomplete framing.

Also tried moving kind bits out of the footer into the class/count header. It saved six bytes on the state fixture but added 33 bytes to the document with 32 four-field children (1,046 instead of 1,013), because each child crossed a count-header threshold. Rejected and restored kind bits in the footer. The rejected result remains in `results/test-class-kind-in-count-rejected.txt`. This is a reminder that packing a field into apparently spare bits can shift a more common boundary elsewhere.

### Selected middle ground: lengths in a trailing catalog

The final additional experiment replaces the old scalar varint/fixed-width menu with literal scalar byte lengths 0 through 8, byte spans as code 9, and mixed lengths as code 15. Codes 10–14 are reserved. Scalar words are little-endian with high zero bytes removed. Signed integers use ZigZag; float bits are zero-extended before interpretation. There are no scalar varints. Four bits suffice for the length selector, and a shared selector still removes per-item codes from uniform containers.

Numeric mixed entries combine delta and length selector, using a full-width escape for deltas above `MaxUint64 >> 4`. String keys and span lengths remain ordinary varints in metadata. The footer is reversed `uvar((metadata_size << 3) | (kind << 1))`, reserving bit 0 for compact/indexed selection. The shared/count metadata header is `uvar((count << 4) | selector)`. This is structural boundary information only; it does not restore a historical semantic schema requirement.

This is the assistant's recommendation after measurements, not a user-approved encoding decision. The proposed persistent family remains two layouts:

1. Compact: write payloads once, then a catalog of keys and byte lengths, then the small footer. A running length sum locates values, and metadata scanning does not require loading preceding payloads.
2. Indexed: write byte/child payloads once, then fixed-width keys and eight-byte word/reference cells, then count/descriptor. The prototype retains the old three-bit indexed class encoding, consistently uses Fixed8 for scalars, and supports owned word replacement. It does not force compact's four-bit length codes into indexed slots. A bitmap refinement is possible but unmeasured and unnecessary for the recommendation.

The selected prototype supports indexed numeric roots with compact length-catalog children. It has a tested owned-cell patch path. Indexed dictionary/list trailers, common validation, editor integration, and a public type-bound API remain work for the next revision. Do not describe the experimental family as production-ready or as already replacing `draft/`.

The incremental Begin/Uint/Bytes/End API sketch was updated to emit the selected length-catalog grammar, not the earlier independent-tag grammar. It computes shared length while accepting values, buffers pointer-free entries, and omits an empty child at End. It accepts sorted numeric keys and does not build a Value tree or size child payloads first. The full recursive fixture writer separately covers dictionaries and lists. These are tests/skeletons, not a proliferation of public APIs.

### Final size comparison

All numbers below include the per-object layout bit. Field sets are the same; there is no asymmetric primary-key or zero omission. The source fixtures contain no zero-valued map entries. Negative and homogeneous-list sensitivity fixtures were added separately.

| Fixture | Baseline compact | Proposed length catalog | Original integer MP | Original short-key MP |
| --- | ---: | ---: | ---: | ---: |
| Profile | 285 | 287 | 279 | 289 |
| State | 1,076 | 1,076 | 2,202 | 2,270 |
| Settings | 5,130 | 5,161 | 4,972 | 5,293 |
| Document with 32 child records | 1,010 | 982 | 1,008 | 1,141 |
| Sparse event | 47 | 47 | 48 | 54 |
| Dictionary, 128 | 2,562 | 2,564 | 2,563 | 2,563 |
| Long-key dictionary, 128 | 8,066 | 8,069 | 8,195 | 8,195 |
| 32 KiB blob | 32,783 | 32,783 | 32,783 | 32,785 |

The dictionary is one byte larger than MessagePack; settings and profile remain larger than integer-key MessagePack. No universal size guarantee is asserted. The recommendation prioritizes the measured construction and access trade-offs, not just a total of bytes across unrelated fixtures.

A 128-field numeric record shrinks from 419 to 409 compact bytes. Its indexed form shrinks from 1,205 to 1,157 bytes because consistently using word cells removes a mixed-class area. Indexed root/compact children are 1,447 bytes for state versus the prior 1,444; the document is 1,010 versus 1,038. A 128-element uint16 list is 259 bytes, nearly the baseline 258, rather than the 386 bytes needed by independent tags. The uint32 and uint64 lists are 515/1,027 versus 514/1,026 baseline and 642/1,154 independent tags. The negative −17..−32 list is 131 versus 130 baseline, 130 with 63 negative immediates, and 258 with 16 negative immediates. No production histogram establishes an optimal immediate-negative allocation.

### Final timing and allocation evidence

Host: Apple M5 Max, darwin/arm64, Go 1.27.1, 48 GiB memory, reported 128-byte cache lines. Broad final comparison: `GOMAXPROCS=1`, five 100 ms repetitions, 178 benchmark cases in `bench-iteration.txt`/`.csv`. The selected length implementation was measured separately with five 150 ms repetitions in `bench-length-selected.txt`/`.csv`. Selected streaming API and patch results are in `bench-selected-stream.*` and `bench-length-patch.*`. Intermediates remain separately named and are not the source for final-format claims.

| Complete encode | Baseline reference | Selected length catalog |
| --- | ---: | ---: |
| Profile | 755 ns | 427 ns |
| State | 8,600 ns | 3,557 ns |
| Settings | 19,790 ns | 9,782 ns |
| Document | 7,366 ns | 3,129 ns |
| Dictionary, 128 | 3,183 ns | 2,638 ns |
| 32 KiB blob | 472 ns | 417 ns |
| 32 KiB blob, depth 16 | 10,689 ns | 1,069 ns |

All warmed cases in that table allocate zero. Both baseline and candidates reuse scratch and output and start from the same fixture tree. The baseline adapter borrows immutable strings, reuses field/pair/value slices, and does not pay for reflection or a newly constructed input tree. Cold benchmarks are retained: for example the baseline settings encoder used 44 allocations/46,168 B, independent-tag catalog 25/17,840 B. These cold figures belong to the independent-tag implementation, not the subsequently selected length-catalog implementation.

The generated-style MessagePack writer still wins shallow complete encoding: 1,628 ns for state and 3,659 ns for settings. This remains a target for further implementation work. Its syntax does not provide the same ability to skip an unknown nested container by a byte extent, so its faster encoder alone does not settle the format choice.

For 128-field lookup, the reference compact getter measured 884.9 ns and the selected compact length catalog 318.4 ns. A skip-only control for the unchanged old bytes measured 219.8 ns. Thus some of the apparent improvement is API/materialization overhead, and old bytes can be read faster than this new compact prototype. Indexed results are 14.04 ns reference and 11.06 ns selected prototype; owned nonzero word patch is 10.93 ns. Indexed reads/mutations remain a separate profile for hot state.

A backward writer produced byte-identical old encodings with no warmed payload movement. It took 1,712 ns for the deep blob, but 9,966 ns for state (worse than the baseline's 8,600). It remains a useful compatibility-preserving construction technique, not the preferred universal writer. Its input must be replayable and traversable in reverse.

The final streaming API skeleton measured 423 ns at depth 4 and 652 ns at depth 16 for the 32 KiB blob, with no allocations after warm-up. It emits fields directly instead of traversing a fixture tree, so these are not equal-source replacements for the table above. Its 24-byte entry arrays need about 768 bytes for two entries at each of 16 depths, plus frame storage. The earlier independent-tag scratch count of 128 bytes excludes frames and is not a claim about the selected writer's scratch footprint.

### Alignment and representation results

- Within-line eight-byte loads: approximately 1.2 ns at all offsets 0..7 in the hot control. In a 32 MiB sparse working set, aligned load was 8.279 ns, offset 1 was 8.150 ns, and split-line offsets 121/127 were 10.8/11.81 ns. Stores were 6.206/6.130/6.322 ns for offsets 0/1/121. Deliberately split lines matter; ordinary unaligned accesses are not uniformly slower.
- Indexed binary-search controls used equal row stride when comparing padded/unpadded versions. One-byte-key hot rows measured 9.803 ns unpadded versus 9.848 ns padded. Wider keys sometimes benefited modestly from padding, but increasing all keys to eight bytes increased row size and did not win overall. Cold-run values varied across process runs; do not generalize small differences. Actual byte decoding uses an ordinary MOVD load/store in this Go arm64 build (`alignment-arm64.txt`). No x86 runtime measurement was performed.
- Baseline Value/View are 40/24 bytes; both packed value designs and the packed view are 16. View lookup was 13.71/13.74 ns for slice/pointer forms. Hot value-array/call measurements were 2.530 ns wide, 1.652 ns marked, 1.421 ns type-directed. For one million entries: 18.56/5.897/4.721 ns in the final broad run, with visible cross-run variation. These measure cache/ABI effects, not total application GC time.
- `slog.Value` was inspected in the installed Go source and measured at 24 bytes. Its num-plus-interface design is an inspiration, not proof that slog itself occupies two words. The KVO-specific marker variant uses real static addresses; the type-directed variant uses nil for words and a GC-visible pointer for spans. No integer-to-pointer fabrication or persistent uintptr is used. Forced collection, interior heap pointers, stack growth, race, and checkptr tests cover the lifetime assumptions; retaining a view still retains its backing allocation and does not extend transaction validity.

### Sources, validation, and scope

Primary sources read: [Go slog implementation](https://go.dev/src/log/slog/value.go), [Go unsafe operations](https://pkg.go.dev/unsafe), [Go binary accessors](https://go.dev/src/encoding/binary/binary.go), and [FlatBuffers construction](https://flatbuffers.dev/internals/#construction). The installed Go sources and actual generated code were inspected as well. The Arm alignment reference was located, but conclusions here are limited to measured host behavior and generated code, not a cross-platform promise.

Final full and short EDB suites, research race checks, focused checkptr=2 checks, and go vet pass. Their final logs use `test-selected-final-*` and `vet-selected-final.txt`. Golden tests cover both catalog grammars; streaming output equals recursive output; the reverse writer equals existing draft bytes; edge tests cover full-width keys, signed extrema, subnormals/NaN bits, zero elision, and list positions. An opaque relocation test changes a preceding value's size without interpreting the unknown field. The final length-catalog parser completed a 15-second fuzz run with 50,625 executions; independent-tag and earlier class-catalog fuzz results are also retained. These are bounded fuzz runs, not exhaustive validation or a complete indexed-trailer validator.

The README now includes the selected grammar, measured trade-offs, API sketch, sources, and integration boundary. Existing `draft/` bytes and API remain the baseline; this turn adds experimental implementations and documentation. All work stays uncommitted on `main`; no push or production/backend change was performed.

The next integration should produce one reference with two storage layouts and common views/editors. Carry forward the 16-byte memory representation and forward length-catalog construction, optimize compact lookup independently, complete indexed dict/list validation, and connect the existing error/zero/change semantics. Do not introduce every compared tag partition as a selectable dialect. The user is explicitly invited to challenge this recommendation; no pending user approval or unanswered question is blocking the completed experiment.

## 2026-10-16 — completion audit and common-container prototype

The previous goal turn made progress: it produced measured framing alternatives, pointer/alignment controls, a selected construction strategy, and durable documentation. It did not prove the full next iteration complete. Re-read the working tree and goal before continuing; preserve the previous draft and measurements as baselines.

The active objective is: “Continue until you're sure you got the best possible next iteration that cannot be improved upon, based on my feedback, and measured it comprehensively against the previous iteration.” No finite benchmark can establish global optimality over every possible encoding or future workload. The actionable completion test is stronger than the previous turn's result: resolve the known competing improvements, compare like-for-like read/write/mutation/change operations across audited workload families, and leave no known unmeasured defect in the proposed next iteration. Do not equate a plausible recommendation or a clean test run with completion.

Known gaps to address now:

- The selected compact getter still loses to a skip-only reader for the previous bytes. Test direct metadata scanning and uniform-list addressing before changing the format.
- Indexed prototypes cover numeric roots only; verify the same payload-first design for string maps, lists, nested/mixed layouts, and malformed input.
- The indexed family now needs only word/span distinctions. Measure a one-bit mixed bitmap against the retained three-bit directory, without adding another user-selectable dialect.
- Connect the 16-byte values/views to realistic access and editing tests, not only representation microbenchmarks.
- Compare cold encoding, sequential traversal, whole-row decode, mutation/rebuild, and changes as well as warm encoding and point lookups. Preserve all earlier numbers as historical rather than overwriting them.

The two-layout direction is still a recommendation. This continuation remains research, not production integration or deployment.

### Completion-audit findings so far

- Added common 16-byte view/value and forward builder prototypes (`length_*_test.go`) for numeric maps, dictionaries, and lists, including mixed child layouts. Compact output remains byte-identical to the selected literal-length grammar.
- Direct metadata scanning reduced the 128-field compact point lookup from roughly 318 ns to 135–150 ns in exploratory runs, beating the unchanged-old-format skip-only control (about 220 ns). A one-byte numeric-key search fast path brought the common indexed getter back to about 11 ns. These supersede the earlier limitation only for the measured fixture; final paired results are still pending.
- The first universal indexed word-cell directory made the 128-entry string dictionary 3,973 bytes versus the previous 2,823. Rejected it for string dictionaries and sequences. Their revised indexed layout stores complete entries followed by variable-width offsets; scalar entries reserve eight bytes, span extents come from adjacent offsets. Numeric maps retain fixed-width keys and eight-byte cells. All indexed kinds use only a shared scalar/span flag or a one-bit mixed bitmap. The string dictionary is now 2,693 bytes. This choice follows container kind; it is not another selectable format.
- The initial common editor allocated one heap object for a caller-local edit array. Go escape analysis identified append-to-receiver slices and pointer updates in the cursor. Explicit growth plus integer cursor positions removed that allocation; the stack-scratch assertion now passes. Edit metadata remains pointer-free and 24 bytes. Arena references are 64-bit offsets to length-prefixed owned bytes, so repeated edits do not inherit a 32-bit cumulative-arena limit.
- Added numeric/dictionary/list overlay tests. List replacement/deletion addresses original base positions during an edit session; insertion/splicing is still outside this API skeleton. Setters grow storage. Borrowed bases remain unchanged and opaque values survive rewrite.
- Added a logical-change length-code experiment that fuses the four-bit code with the path-suffix count. Zero assignment and deletion remain distinct; the redundant outer value-length wrapper disappears. A 16-operation mixed batch shrank from 289 to 261 bytes. A payload-first batch catalog costs two further bytes in that fixture and allows an unknown-size replacement object to be built directly into the final change buffer. Both alternatives are measured; only one will be recommended.
- The first common-parser fuzz run passed but completed only 21 executions because a valid long list triggered quadratic lookup verification in the fuzz oracle. This is insufficient fuzz evidence. Changed the oracle to perform a bounded sample of point lookups while still checking every iterator entry; repeat the fuzz run after final layout changes.
- A truncation assertion originally assumed retaining only the last descriptor byte could never form another valid object. It produced a valid empty sequence. Corrected the test to retain the original count and descriptor when removing body bytes. Structural validation is not a checksum; the WAL/row integrity envelope remains responsible for detecting arbitrary bit changes.

## 2026-10-16 — queued distribution study and cross-hardware benchmarks

### User request — verbatim

> The length codes are:
>
> 0-8 - Exactly that many little-endian scalar bytes; zero-extend to a word
>
> 9 - Byte span; its length follows in the catalog as a uvarint
>
> 15 - Mixed lengths; a per-entry code is required

Um, seems really inefficient... like how beneficial is allowing 3-byte and 5-byte words is, vs doing

0 zero bytes

1 one byte

2 two bytes

3 four bytes

4 eight bytes

5 byte span

6 reserved

7 mixed length

or even!

0 normal tag used (say optimal for small integers, zero values etc)

1 four bytes

2 eight bytes

3 mixed length

...and you use normal tag for byte spans (because most will have short enough length to fit into tag byte, so you're not winning anything by encoding 'byte span' separately and then length 1-byte uvarint), but which only uses two bits.

I think this is where it is appropriate to use backend's debugging tools to get a sample of data from production, get distribution of typical string lengths (which I think is almost the only type of byte span we use), typical integer value ranges, etc. There's a LOT of data, so you might wanna take say one Shopify tenant (American Girl sounds representative) and two non-Shopify tenant (I would go for Life Extension and Milwaukee), and download a SAMPLE of their data via Scan2, and analyze it, and assume real-world data has roughly that kind of distribution.

After you get the sample distributions, consider other optimizations! Run until you exhaust ability to optimize.

But enqueue this and finish everything that you're doing first. Add to task list. Remember in STATE.md.

### User request — verbatim

I think that I want you to keep benchmarks for all approaches and variations considered, which also means keeping the code (okay to make copies of relevant code). I just wanna be able to rerun benchmarks on our actual server hardware too, to avoid over-fitting particular Apple Silicon.

### User clarification — verbatim

Obviously we will kill all extras closer to production, so no maintainability concerns.

### User request — verbatim

I guess I should ask you to prepare all benchmarks, and then ssh into bh311.bubblehouse.com (port knocking required! see backend repo!) to run the tests there too, you'll need to upload a Linux x64 binary.

### User reminder — verbatim

Yeah use task list to not forget anything

### Recorded decisions

All variants must remain runnable during research, including rejected layouts and implementation controls. Copies are acceptable; production cleanup is deferred. Prepare and run Linux amd64 test binaries on the named server, retaining environment details and raw results independently of local measurements. Do not treat the current four-bit selector as selected before the queued alternatives are measured. The task list at the top of this file is the execution checklist.

### User request — verbatim

Also interested in Current indexed root vs existing KVO Immutable format, which was supposedly optimized for reading speed. Note: all reading is usually from mmap'ed data (Bolt database!)

Recorded: add a direct existing-Immutable control to the current indexed comparison. Use equivalent scalar records where current KVO supports the same values, and explicit subset/coverage notes for unsupported string/list workloads. Add real Bolt mmap reads, with hot and larger working sets, navigation-only and transaction/bucket lookup measurements, and file/row sizes. Do not describe Go-heap byte arrays as mmap measurements.

### User clarification — verbatim

Same seq for sure. I've enabled VPN, and also ensure ssh follows knock immediately, knock is only good for like a second.

Recorded: use the documented TCP 222 → 2222 sequence, connect immediately, and reuse the SSH connection for uploads/runs. Earlier attempts failed before VPN was enabled; no different sequence is needed.

### Common-container checkpoint completed before queued distribution work

The current reference now covers compact/indexed numeric maps, string dictionaries, and sequences with one 16-byte view/value layer. The editor uses current logical list indices (superseding the interim base-position API noted above). Capacity hints reduce cold allocations without a source-size pass. Numeric change paths normalize field/dictionary aliases and retain full uint64 keys. The README now specifies this actual reference, leaving the previous grammar in `results/README-before-common.md`.

Final supplements: `bench-common-reserve.*` measures complete warm/cold construction; `bench-common-distributions-selected.*` covers local, high, random32, random64 keys and row working sets; `bench-common-list-control.*` includes direct indexing on unchanged old list bytes; `bench-common-changes-selected.*` covers inline and trailing changes after path simplification. Main paired results are `bench-common-paired.*`.

Key findings:

- Warm state encoding: previous 8.701 µs, current 2.708 µs; settings 19.771 / 7.823 µs; deep16 blob 10.709 / 0.875 µs, all zero allocations. Cold state drops from 17,848 B / 52 allocations to 6,216 B / 23; settings 46,168 / 44 to 21,504 / 20; deep16 661,152 / 69 to 45,112 / 24. Dictionary cold allocations increase by one (20 → 21) while bytes/time improve.
- Paired compact 128-field lookup 890.5 → 146.8 ns, versus approximately 220 ns for an improved old-byte skip-only control. Dictionary lookup 1231 → 242.2 ns. Indexed dictionary 100.3 → 65.25 ns. Edits and rebuilt output with caller-local scratch stay allocation-free.
- The old public list getter had avoidable traversal: direct indexing on old bytes is 4.524 ns, current 6.192 ns. Do not attribute the old API's roughly 1 µs cost to its format.
- Width-dispatched indexed search improves the earlier common implementation but does not consistently beat the old format for wide keys. Latest random64 lookup 15.59 / 15.64 ns. Indexed row-working-set runs changed direction across processes (latest previous 55.95 / current 60.83 ns); no universal cache win is established.
- Typed configuration decode: previous 5.247 µs, current 3.000 µs, generated MessagePack 3.920 µs. All construct equivalent owned data using 5,104 B / 102 allocations. A cached struct costs 168.4 ns to read. Generated MessagePack encoding still beats the current shallow whole-tree encoder in earlier runs.
- A 16-change batch is previous 289 B / inline 261 B / catalog 263 B. Write 285.1 / 141.2 / 184.7 ns; checked single-pass read 245 / 162 / 172.3 ns. Full catalog Open then reading costs 334.9 ns. Payload-first changes support direct unknown-size replacements; the inline alternative remains a control.

Verification: full and short EDB suites, research race checks, focused checkptr=2 checks, and vet pass (`test-common-checkpoint-*`, `vet-common-checkpoint.txt`). Final common-parser fuzz run completed 202,268 executions; change-catalog fuzz completed 1,801,496, each with a 30-second budget and no failure. Earlier common fuzz completed 1,984,000. Periods with no reported executions are retained in raw logs; they do not prove a parser stall or exhaustive coverage.

The implementation checkpoint is complete; overall research is not. New benchmark preservation, real mmap/Immutable comparison, server execution, production sampling, and smaller-selector experiments remain on the task list.

### User formatting request — verbatim

Also lets use ns and space grouped integer formatting, because in your report, comparing 469 ns vs 19.77 µs is a nightmare, 19 770 ns would be so much better.

Recorded: all new human-readable timing comparisons use ns and space-grouped large integers; the README has been reformatted. Preserve numeric precision in raw benchmark logs and CSVs.

### Benchmark preservation work

Restored the rejected kind-in-count-header format as `retained_count_test.go`. Its previous 1 013 → 1 046 B document regression and 1 096 → 1 090 B state improvement reproduce exactly for the three-bit class grammar. Also restored universal indexed word directories as `retained_words_test.go`; the 128-entry dictionary reproduces 3 973 B versus the selected offset layout's 2 693 B. These are grammar reconstructions because the earlier code was overwritten before the preservation request. New timings are explicitly separate from historical implementation timings.

Added `mmap_compare_test.go`: real temporary Bolt databases, reopened read-only, borrowed row slices kept within a read transaction. It compares existing Immutable, previous draft indexed, current indexed, and compact, including local/global keys and nested numeric maps. Timed scopes separate navigation, bucket lookup, and full read transaction. File/row bytes and absolute pointer misalignment are reported. The fixtures fit the current KVO numeric-map feature set; this does not claim old KVO supports arbitrary strings/lists.

The first mmap test's expected-value oracle indexed the root before selecting the nested fixture and panicked. Corrected the oracle; all mmap and restored-layout tests pass. The codec was not implicated.

SSH access is verified after the user enabled VPN and clarified the one-second knock window. The server is Linux amd64, AMD EPYC 7502P, 32 cores/64 threads, approximately 256 GiB RAM. A persistent SSH connection is available for the benchmark upload. No application process or database has been changed.

### Full cross-hardware run started

Prepared `bench.sh` with five standalone test binaries, exact-source archive, SHA-256 hashes, environment/load capture, raw repetitions, and summaries including custom metrics. All prior major variants remain selectable. A native one-iteration harness run passed 677 experimental cases plus 11 draft cases before the final capacity/search controls were added; the final full run also includes those additional controls. The Linux binaries were uploaded and all hashes verified.

Native run: `/tmp/kvo-results-arm64-20261016`, using `/tmp/kvo-native-final`. Server run: `/tmp/kvo-research-20261016/results`, using the uploaded bundle. Both use Go 1.27.1, GOMAXPROCS=1, five 150 ms repetitions. Linux target is baseline amd64/v1, CGO disabled, pinned to logical CPU 24 (SMT sibling 56), nice level 10, schedutil governor. The server had low load before launch. Its `/tmp` is tmpfs; temporary Bolt databases therefore use `/var/tmp/kvo-research-20261016` on `zp0/root` ZFS. mmap reads are page-cache warm; durable-storage numbers refer to that filesystem. No cache dropping or system-setting changes.

Native and server benchmarks run independently on separate CPUs/machines. Avoid other CPU-heavy work locally while the native suite runs. Production sampling remains queued until these existing comparisons finish, as requested.

### Sampling design notes before downloading

Scan2 supports bounded tenant-index scans and exact primary-ID time ranges, with read transactions closed after batches. It emits JSON using each row's ordinary marshaler plus `_mod`. Parse samples back into the registered Go row type before classifying values: JSON strings can represent numeric IDs, time values, byte arrays, and semantic text. Explicitly exclude hidden JSON-only omissions from observed sparsity, and report dynamic JSON number ambiguity rather than pretending to recover original runtime scalar types.

The static client mapping identifies the first two requested production slugs but lists the third only under staging names. Verify live tenant identity before choosing that sample; do not silently substitute a different tenant or call a staging-shaped sample representative production traffic. Raw responses and any private mapping stay outside the generic EDB repository.

For selector comparisons, separate effects: scalar rounding (3/5/6/7 → 4/8), count-header thresholds, key-delta thresholds, homogeneous-list sharing, span length metadata, and forward construction. The two-bit proposal should preserve a normal-tag control; ZigZag-normalized scalar words may allow small signed values to use ordinary small-word immediates without a separate negative range. This is a hypothesis to test, not a selected design. Four-bit and three-bit controls must use comparable builders/readers so implementation dispatch does not masquerade as a format result.

Further indexed-access control to consider after the complete mmap results: compare generated/type-known scalar access with generic Value construction. Existing Immutable returns a raw word and can avoid checks a generic scalar/span API performs. Keep that difference explicit before attributing a hot lookup gap entirely to serialized layout.

The native and Linux source archives have different archive metadata hashes, but their 164 file contents are byte-identical. Their common per-file manifest hashes to `7342248d952099da4c0c04e108ff36045f43f4278f6d0b8dbadbd54f3c8dd829`. Both archives and the content manifest are retained under `results/source-common-*` so uncommitted source history does not depend on `/tmp` surviving.

### User schema and terminology request — verbatim

Also add...

> Each child chooses its own layout

Note: I think schema-wise, we only ever go from indexed to compact; putting indexed inside compact makes no sense really. Most of the time this will be decided on per row basis; very rarely we'll use indexed for entire row and compact for some key or subkey, because it's rarely accessed and we wanna save space.

I imagine edbschema.Compact flag that can be passed somewhere and triggers compact representation, and by default rows use indexed representation for fast access. Or we use compact by default, and edbschema.FastAccess triggers indexed format.

> Container... Numeric

I propose the following terminology:

Container can be: Object (numeric unique keys), Dictionary (string keys), List (renamed Sequence).

Byte span can be: string/bindata or container.

Indexed representation => Fast representation. So we have fast and compact ones.

Top-level row is always an Object.

> Optional presence is deferred. If needed, declare it on particular properties and provide a separate presence-aware API. The ordinary API does not return a presence flag on every read.

Well no, presence I think is part of what we're designing. But! It's just a flag on the schema plus binary repr needs a way to encode zero values. With this flag on (edbschema.ZeroValues), we will encode zero values, and API will distinguish zero from missing. With flag off, API won't distinguish, and encoding skips zero values.

\---

Also Q - we never explored fixed slots on an Object. Would it benefit from having fixed slots encoding? This would get it closer to how relational databases store data, which they probably have a good reason to do... This would of course only make sense for fast repr.

### User fixed-slot clarification — verbatim

well not ALL fields would use fixed slots, only the ones marked edbschema.FixedSlot or smt like that, so we'll only mark those fields that need fastest access or are always present.

### Recorded interpretation and queued measurements

Adopt the requested terminology in the specification. Historical source names and benchmark labels remain usable. Compact is inherited downward as a schema policy; the physical reader must still tolerate preserved unknown children in either representation. The choice of default row layout is not decided by the examples of possible flag names.

Presence is now in scope. `ZeroValues` preserves explicit zero without adding a null state. Generic rewrites must preserve unknown present zeros; Set zero and Delete are distinct for marked fields. Struct bindings need an explicit presence projection when the caller requests that distinction, rather than inferring absence from an ordinary Go zero.

Fixed slots are selected per property. The experiment concerns a small selected slot area plus the ordinary keyed area, including direct versus bitmap-packed selected slots, stable field/slot identity, and interactions with presence. A dense all-field layout is at most a comparison. Do not allocate slots indexed by the largest existing global key. No fixed-slot format has yet been selected.

### Full cross-hardware run completed

Both complete suites passed: 703 experimental benchmark cases and 11 draft benchmark cases, with five repetitions per case, plus correctness tests in all five packages. Native results are retained in `results/common-arm64-20261016`; Linux results in `results/common-amd64-20261016`. These are the frozen common checkpoint, before the new schema presence/fixed-slot work. Summaries and custom metrics accompany raw repetitions. Benchmark success does not imply a particular format won; the cross-hardware analysis follows.

### Cross-hardware comparison (retained intermediate result)

Five 150 ms repetitions, Go 1.27.1, one Go processor. Mac: M5 Max. Server: EPYC 7502P, pinned logical CPU 24, nice 10, baseline amd64/v1. Both complete runs passed correctness checks and measured 703 experimental plus 11 draft cases. Raw results, ranges, allocations and custom metrics are in the adjacent `common-*-20261016` directories. The source content manifest is identical on both machines.

All human-readable times below are median nanoseconds, rounded for comparison. A full read transaction is measured separately from navigation. These are page-cache-warm mmap reads, not disk-cold I/O; views borrow actual read-only Bolt mappings. File allocation includes spare Bolt pages. There is no production workload trace.

#### Direct mmap comparison

| Fixture | Rows | Codec | Row bytes | Mac ns | Server ns |
| --- | ---: | --- | ---: | ---: | ---: |
| local128 | 1 | immutable | 2 064 | 32 | 59 |
| local128 | 1 | draft-indexed | 1 205 | 35 | 71 |
| local128 | 1 | Fast | 1 157 | 35 | 72 |
| local128 | 1 | compact | 409 | 146 | 412 |
| local128 | 32 768 | immutable | 2 064 | 105 | 254 |
| local128 | 32 768 | draft-indexed | 1 205 | 93 | 211 |
| local128 | 32 768 | Fast | 1 157 | 97 | 302 |
| local128 | 32 768 | compact | 409 | 233 | 503 |
| global128 | 1 | immutable | 2 064 | 33 | 61 |
| global128 | 1 | draft-indexed | 2 101 | 37 | 74 |
| global128 | 1 | Fast | 2 053 | 43 | 76 |
| global128 | 1 | compact | 416 | 151 | 422 |
| global128 | 32 768 | immutable | 2 064 | 102 | 262 |
| global128 | 32 768 | draft-indexed | 2 101 | 128 | 334 |
| global128 | 32 768 | Fast | 2 053 | 132 | 386 |
| global128 | 32 768 | compact | 416 | 252 | 518 |
| nested | 1 | immutable | 2 256 | 37 | 64 |
| nested | 1 | draft-indexed | 1 317 | 48 | 117 |
| nested | 1 | Fast | 1 269 | 50 | 102 |
| nested | 1 | compact | 452 | 55 | 158 |
| nested | 1 | Fast root, Compact children | 509 | 50 | 136 |
| nested | 32 768 | immutable | 2 256 | 137 | 210 |
| nested | 32 768 | draft-indexed | 1 317 | 155 | 311 |
| nested | 32 768 | Fast | 1 269 | 171 | 320 |
| nested | 32 768 | compact | 452 | 172 | 252 |
| nested | 32 768 | Fast root, Compact children | 509 | 172 | 289 |

Every navigation case allocates zero. Current Fast is not a universal improvement over existing Immutable: Immutable wins the single-row and nested cases on both machines. For 32 768 local-key rows, the Mac favors current Fast slightly while the server favors Immutable. The earlier draft Fast is faster than both on that server case. Global keys remove the narrow-key space advantage. The generic scalar/span API and directory parsing are still potential implementation costs; a type-known accessor control is required before attributing the entire gap to layout.

#### Paired whole-object encoding

| Fixture | Mac previous ns | Mac current ns | Server previous ns | Server current ns |
| --- | ---: | ---: | ---: | ---: |
| profile | 761 | 347 | 2 661 | 865 |
| state | 8 713 | 2 725 | 31 747 | 6 627 |
| settings | 19 848 | 7 828 | 70 227 | 18 891 |
| transaction | 7 386 | 2 489 | 27 989 | 5 679 |
| dictionary-128 | 3 188 | 2 082 | 11 192 | 5 234 |
| blob-32k | 465 | 417 | 1 064 | 770 |
| deep16 | 10 763 | 895 | 18 376 | 2 072 |

These reused-buffer encoders allocate zero. Current encoding improves every listed fixture on both machines. Cold allocation counts and bytes are in the CSVs; they are identical across architectures. This comparison is against the earlier draft, not a claim that current encoding beats generated MessagePack everywhere.

#### Selected access and edit controls

| Operation | Mac previous ns | Mac current ns | Server previous ns | Server current ns |
| --- | ---: | ---: | ---: | ---: |
| Compact Object get | 885 | 138 | 3 231 | 408 |
| Fast Object get | 16 | 13 | 46 | 51 |
| Compact Object scan | 2 060 | 763 | 8 616 | 1 800 |
| Fast Object scan | 2 065 | 379 | 7 643 | 896 |
| Compact Dictionary get | 1 236 | 249 | 4 883 | 755 |
| Fast Dictionary get | 95 | 63 | 357 | 176 |
| Compact edit 1 and rebuild | 11 981 | 1 608 | 33 610 | 4 633 |
| Fast edit 1 and rebuild | 5 952 | 1 070 | 20 864 | 2 913 |

The Fast numeric getter reverses direction across architectures (Mac improves, server regresses). Most traversal, validation, Dictionary and editor gains survive. List access on unchanged old bytes with a direct accessor remains faster than the new generic getter: Mac 4 / 6 ns, server 13 / 18 ns. The earlier public list traversal cost was an implementation issue.

Typed configuration decode plus one read: previous / current / generated MessagePack is Mac 5 460 / 3 147 / 4 029 ns and server 17 182 / 8 584 / 10 899 ns; all construct equivalent owned data using 5 104 B and 102 allocations. A cached struct read is 176 / 404 ns (Mac/server), zero allocations.

#### Alignment

Unaligned scalar loads within a cache line are essentially equal to aligned loads in these controls on both CPUs. Crossing a line is measurable: larger-working-set Mac loads 8 / 10–11 ns and server 26 / 27 ns. Stores are more sensitive on the server: aligned / one-byte shifted / line-crossing is 14 / 18 / 19 ns, versus Mac 6 / 6 / 7 ns. This does not justify padding every stored value: Bolt row placement is not guaranteed aligned and density also changes cache behavior. Keep aligned and unaligned controls, and measure selected hot slots in real mappings.

#### Interpretation

Retain the current forward writer, borrowed two-word representations, and iterator/editor improvements. Do not adopt the current Fast reader as the performance endpoint. Continue with smaller compact selectors, explicit-zero schema behavior, selected fixed slots and type-known readers. The fixed-slot experiment is opt-in for a small subset of fields, not a dense row schema by default. None of these results establish a universal winner.

### Presence and selected-slot prototypes added

`schema_presence_test.go` adds explicit-zero writing without changing the common binary grammar, a small property model, presence-aware lookup/editing, and owned zero patching. Ordinary fields still elide zero. The editor distinguishes wire code 0 (present zero) from code 10 (deletion) and preserves unknown present zeros. Empty modeled children normalize to present zero when requested. Tests cover Compact and Fast, schema misuse, caller scratch growth, opaque unknown spans, present versus absent zero, and downward Compact inheritance. This is a package-local API experiment, not an `edbschema` production package.

`fixed_slots_test.go` tests eight selected fields among 128 Object fields. The direct variant puts their eight-byte cells at the beginning; the packed control stores only present selected cells and uses bitmap rank. All other fields use the existing Fast keyed area. Slot keys remain in the row; a presence bitmap and scalar/span bitmap let an unknown slot be preserved without its old type. Dense scalar rows add only the two-byte slot count and two one-byte bitmaps relative to the ordinary keyed layout. Absent direct slots consume reserved cells; this is the deliberate cost of opting into a stable offset. Packed slots avoid those empty cells but add rank calculation and lose stable physical offsets across insertion/deletion.

The experimental descriptor uses the existing reserved Fast bit to identify slots; one otherwise-unused Object-kind bit distinguishes the packed control. There is no per-object version. Both variants retain normal field numbers. They do not allocate an array up to the largest global field number. Slot positions are append-only and never reused for another field. An evolution-aware generated accessor checks the expected key and falls back to keyed lookup for old rows when a previously ordinary field is promoted. A trusted direct-slot accessor is retained separately to measure the lower bound; it requires the slot assignment to be known.

Initial correctness tests pass. Timing runs are underway; neither slot variant is selected yet. Encoding currently uses a fixture adapter that searches the input field list for selected fields; this is not a fair upper bound on generated encoding performance and is labeled in the benchmark interpretation.

Primary-source context: [PostgreSQL row layout](https://www.postgresql.org/docs/19/storage-page-layout.html) combines a null bitmap, schema widths/alignment, and variable-length data. It does not imply every SQL field has a fixed physical offset. [Cap’n Proto structs](https://capnproto.org/encoding.html) show stable field placement and separate scalar/pointer regions for schema-independent traversal. [FlatBuffers internals](https://github.com/google/flatbuffers/blob/master/docs/source/white_paper.md) use field offsets through a vtable to support omitted fields and evolution. Our selected-slot experiment retains explicit field identities and bounded span references to meet the stronger unknown-field rewrite requirement.

Production sampling has verified all three requested live tenant identities, including the production Milwaukee slug missing from the static client map. Bounded samples are in a private cache outside this repository. One response exceeded a 30-second read timeout; the downloader now resumes completed groups and retries a smaller request. The typed adapter emits only counts, lengths, bit widths, and synthetic shapes. It records hidden JSON fields, custom marshaling, and dynamic-number ambiguity rather than treating those as exact storage observations.

### Selected-slot first native measurements and encoder correction

Five 150 ms repetitions on Apple M5 Max, real borrowed Bolt mappings. Eight selected fields among 128. Across 32 768 dense rows, existing Immutable / ordinary Fast / generic direct-slot lookup / known direct-slot lookup cost 70 / 82 / 74 / 10 ns, all zero allocations. One hot row costs 18 / 24 / 20 / 3 ns. These queries target the eight selected fields, so they are a different access distribution from the earlier whole-key-space mmap comparison. They do not establish a whole-row workload speedup.

Dense row sizes are 1 157 B ordinary Fast versus 1 161 B selected slots. With 32 of the 128 fields present and half of the selected slots absent, ordinary Fast is 293 B, direct slots 333 B, and packed selected slots 301 B. The generic direct lookup loses on this sparse larger working set (58 ns versus Fast 52 ns); known direct slots still cost 9 ns.

The initial source adapter is deliberately retained but is a poor encoder: it repeatedly searches the source field list. Warm dense encode is Fast 1 234 ns versus direct slots 3 030 ns. Added a separate streaming builder with schema-known slot ordinals, patching reserved cells while emitting span payloads directly into the one destination. It produces byte-identical output; normal fields remain ordered and payloads are not copied through an intermediate child buffer. Fresh controls cover encoding, 80% selected / 20% ordinary access, large global keys, and selected string/blob fields.

Stable append-only slot assignments let a typed accessor test only the stored slot count. An older row predating a field's promotion falls back to normal keyed lookup. This is stronger than the initial known-slot lower bound, which merely returned zero for an out-of-range slot. An explicit expected-key check remains a separate measured control. Slot reassignment is never allowed.

Presence prototypes: explicit-zero versus nonzero lookup is 12 / 12 ns for Fast; the initial Compact presence iterator costs 286 / 275 ns. This is an implementation control; the ordinary Compact getter is faster and can share its metadata loop with a presence-aware return. With caller scratch, editing and rebuilding remains allocation-free. Full EDB tests, focused race checks, and checkptr=2 checks pass for the first prototypes. Both initial source and benchmark runs are retained.

### First selected-slot server comparison

Server correctness checks and all 46 initial presence/slot benchmark cases passed, with five repetitions. Raw results are in `results/slots-amd64-initial-20261016`; the exact source archive is `results/source-slots-presence-initial-20261016.tar.gz`.

| Workload / access | Mac ns | Server ns |
| --- | ---: | ---: |
| sparsefalse/immutable | 70 | 176 |
| sparsefalse/fast | 82 | 254 |
| sparsefalse/fast-known-word | 80 | 249 |
| sparsefalse/slots-direct | 74 | 273 |
| sparsefalse/slots-direct-known | 10 | 68 |
| sparsefalse/slots-direct-evolving | 62 | 237 |
| sparsefalse/slots-packed | 74 | 272 |
| sparsefalse/slots-packed-known | 60 | 182 |
| sparsetrue/immutable | 44 | 128 |
| sparsetrue/fast | 52 | 122 |
| sparsetrue/fast-known-word | 46 | 116 |
| sparsetrue/slots-direct | 58 | 160 |
| sparsetrue/slots-direct-known | 9 | 52 |
| sparsetrue/slots-direct-evolving | 48 | 126 |
| sparsetrue/slots-packed | 51 | 130 |
| sparsetrue/slots-packed-known | 40 | 109 |

Known direct slots win this selected-field workload on both machines. Generic lookup of the slot format regresses on the server. The type-known ordinary Fast accessor does not remove most of the gap, so generic Value construction alone is not the explanation. Stable resolved property handles are central to obtaining the slot benefit; arbitrary numeric-key lookup still needs a directory search.

### Bounded production sample collected

Completed 168 tenant/table groups (56 typed tables across the three requested tenants), 2 409 unique sampled rows, and 12 067 520 bytes of private JSON responses. Sources contribute 1 152 / 697 / 560 rows. Queries use exact tenant indexes or tenant-keyed singleton reads, positive limits, and bounded striding over oldest/newest primary-key ranges. Tenant identity is checked per row using its actual JSON field alias. A file-table alias mismatch stopped collection before those rows were persisted; it was corrected from the registered Go field tag and the bounded request was repeated. No application data was changed. The temporary local auth token was removed after collection.

All 100 registrations remain covered by the static audit. The sample is narrower: Scan2 exposes typed tables, not KVO/raw maps, and this collection excludes unscoped/global tables and unsupported composite tenant keys. It is deliberately not a random production trace. The first source has more profile/catalog rows because the initial 96-row limit was reduced to 32 for subsequent hot-family requests after a timed-out response. Empty sample groups do not establish that the tenant never uses that table.

The private cache contains raw responses, exact request manifests, and a typed adapter outside the EDB repository. The adapter converts JSON IDs/numeric wrappers back to their Go types, honors JSON and storage exclusions, recognizes text-marshaled values and map keys, and explicitly accounts for the semantic pair wrapper. Numeric map keys in replay shapes retain only delta bit widths; scalar payloads retain only bit widths; strings and byte arrays retain only lengths. Names, literal strings, original IDs and numeric values never enter replay fixtures. Final distributions and sampling limitations follow.

### Typed sample distributions and replay (retained intermediate result)

Bounded Scan2 reads collected 2 409 rows across 56 typed tables for each of three requested tenants (168 groups). Raw responses and authentication material are excluded from EDB. The checked-in replay contains 189 synthetic root shapes selected as small/median/large examples by a shape-size estimate within each nonempty group. Each literal has been replaced: scalar words retain bit widths, numeric map keys retain delta bit widths, strings/blobs retain lengths, and declared fields use ordinal numbers. The fixture is 131 500 compressed bytes. It contains no original key names, strings, IDs, or scalar payloads.

The full static registration audit covers 100 tables/maps. Scan2 sampling does not cover every registration: KVO/raw maps, unscoped/global rows and unsupported composite tenant keys are excluded. Samples use bounded oldest/newest primary-key ranges and every-eighth-row selection, not random sampling or access tracing. Settings snapshots and large import/file rows have tighter bounds. The first source has a larger profile/catalog sample than the other two. Do not infer request frequency from pooled counts or treat an empty group as proof that a table has no data.

#### Scalar widths

| Stored word bytes before zero elision | Observations | Share of nonzero words |
| --- | ---: | ---: |
| 0 | 119 658 | omitted for ordinary fields |
| 1 | 14 341 | 13.8% |
| 2 | 2 407 | 2.3% |
| 3 | 2 434 | 2.3% |
| 4 | 4 215 | 4.1% |
| 5 | 334 | 0.3% |
| 6 | 5 | 0.0% |
| 7 | 0 | 0.0% |
| 8 | 80 204 | 77.2% |

Rounding scalar widths to 0/1/2/4/8 adds 3 446 payload bytes across 103 940 nonzero words before considering header/key savings. Width 8 dominates. Negative integers are uncommon: 690 observations; 662 have magnitude width 63. This distribution does not support reserving a large dedicated small-negative tag range. It also includes domain sentinel values; it is not a distribution of arithmetic results.

#### Spans and keys

| Quantity (nonempty only) | Count | Median bytes | 95th percentile bytes | Maximum bytes |
| --- | ---: | ---: | ---: | ---: |
| Text | 78 017 | 16 | 70 | 27 714 |
| Binary | 3 572 | 7 | 452 | 4 478 |
| Dictionary key | 62 087 | 13 | 48 | 82 |

Of nonempty text values, 78.9% fit 30 bytes, 93.9% fit 62 bytes, and 98.6% fit 127 bytes. A short-span tag is therefore relevant. Binary payloads are fewer, but are present and must stay in the model. Container spans are additional; these text/binary histograms are not a histogram of every encoded child length.

#### Container structure

| Kind | All observed containers | Nonempty containers | Median nonempty entries | 95th percentile |
| --- | ---: | ---: | ---: | ---: |
| Object | 60 404 | 31 720 | 2 | 8 |
| Dictionary | 7 014 | 3 206 | 7 | 34 |
| List | 52 793 | 12 886 | 2 | 31 |

Empty modeled children collapse to zero before ordinary property storage. Declared Go fields and JSON defaults create many empty containers; counting them as stored objects would be misleading. The sample also has numeric maps with large key gaps, so the dense local-field distribution cannot stand in for every Object.

#### Interpretation limits

Typed JSON decoding succeeded for all 2 409 rows. The adapter excluded 2 962 explicitly hidden JSON fields, 7 314 field observations unavailable under the JSON shape, and 183 fields excluded by storage tags. It encountered 336 dynamic JSON numbers whose original integer/float runtime type cannot be recovered exactly; replay uses float-word widths for these and records the ambiguity. Known IDs and integer wrappers are decoded into their registered Go types. Time values are modeled as signed Unix nanoseconds; this is an explicit modeling choice, not a byte-for-byte model of the current MessagePack timestamp extension.

Text-marshaled values and compound string map keys remain spans. A known semantic amount/code pair is projected to two modeled fields. Numeric map gaps are quantized downward to the same bit width; the original numbers are absent. Synthetic Dictionary keys preserve length and uniqueness, not the original prefix distribution. Prefix-length histograms are retained separately, so key-comparison timings need dedicated prefix controls. Replays approximate shape and size distributions; they are not a substitute for application traces.

Next compare four-bit literal lengths, three-bit rounded widths, and two-bit normal-tag/4/8 selectors on identical synthetic roots and equivalent forward builders. Measure total bytes and construction, not only the byte saved by one selector. Keep schema-known slots separate from the Compact comparison.

Replay content SHA-256: `db09fd5ebf8702768e63f7d36d678d4f261695253389b51ce59ef735fdcc0bc4`. The portable source snapshot includes the exact sanitized embedded fixture. All 189 replay roots pass Compact round-trip checks.

### Comparable compact-selector prototypes

Added `selector_catalog_test.go` with one forward builder, reused metadata frames and one payload destination for the four-bit literal-length, three-bit rounded-width, and two-bit normal-tag/4/8 candidates. The four-bit control produces byte-identical output to the common checkpoint. All three initial grammars round-trip the synthetic fixtures and 189 replay roots, including zero list positions, full uint64 key deltas, span thresholds, explicit present-zero rewrites, and deletion. Unknown spans remain opaque. Readers, scans, validation, rewrites, warm/cold construction, and direct homogeneous-list indexing are retained for comparison.

Initial replay totals: literal4 2 035 755 B; rounded3 2 034 675 B; tagged2 with 63-byte span threshold 2 039 606 B. These are sums across the 189 selected replay roots, not database compression estimates. Size differences are small enough that timing and API behavior can decide the trade-off. The three-bit result includes padding costs and actual metadata savings; it should not be judged only by scalar-byte rounding.

The initial two-bit normal tag deliberately mirrors the earlier small-span partition: immediate words 0–127, short spans 0–62, a longer-span sentinel, and literal scalar byte lengths. It leaves tag space unused. Added a retained wide-span variant: 128 immediate words, 121 span lengths (0–120), one long-span sentinel, and six scalar widths (1/2/3/5/6/7). Four- and eight-byte words already have selector codes, so repeating them inside the normal tag is unnecessary. Signed values still use normalized ZigZag words, avoiding a separate negative tag range. This variant is not selected until measured.

A two-bit tagged List can still index small words directly when the payload is empty and the catalog has one byte per item. No extra wire flag is needed: the existing lengths establish that each item is an immediate (or a zero-length alias). General tagged Lists still scan, while shared four/eight-byte Lists index directly. Direct-index controls include values beginning at 1, 128, 2^16, 2^24, 2^32 and 2^56 to expose the cases that lose the earlier format's shared-width shortcut.

The initial source is preserved as `source-selector-initial-addition-20261016.tar.gz`, applied over `source-slots-final-20261016.tar.gz`; the latter already contains the sanitized replay fixture and all other source files. Native initial measurements are running from that frozen three-variant source. The expanded four-variant source remains in the working tree.

### Row-version consequence of opaque children

A row supplies its codec version once. Editing a partially known row must keep that codec version: an unknown span might contain a child encoded with that version, and changing only the outer version would silently reinterpret the preserved bytes. Fast/Compact layout changes within one codec version are safe because each container carries its layout bit. Moving between codec versions requires knowing which spans are containers, or retaining the old row codec until an explicit migration can do so. No per-child version is added. Borrowed child views must inherit the row codec context; a bounded span's length word has spare high bits if carrying that context in the two-word view becomes necessary. This is a versioning constraint, not a third storage representation.


### Final selector controls and selective-slot evolution

Completed the four-dialect comparison on both hosts (176 benchmark cases, five repetitions per case). The wide two-bit replay total is 2 034 538 B, just 137 B smaller than rounded three-bit lengths. All four preserve the same values. Results favor literal lengths on server encoding; most size differences are well below one percent. Full comparison and the decision will follow after the final controls.

Added retained controls for all-zero/boolean/small/16-bit/mixed-word/span Lists; trusted Object lookup with and without an inner helper call; and the same path-compressed trailing-catalog change stream with all four value selectors. Go refused to inline the original selector-number helper, so the manually inlined variant isolates that implementation cost. Literal-four change bytes match the earlier change writer exactly. Delete uses code 10 in literal-four, code 6 in rounded-three, and code 3 in the two-bit variants; Set-zero remains distinct. Warm/cold container construction, warm changes, validation, iteration, rebuilds, and scratch metrics remain reproducible. No nested payload buffer was added.

The latest deterministic tests, full/short suite, race checks, pointer checks, and vet passed before these final timing runs. The new change parser passed a 30-second fuzz campaign with 2 898 826 executions. Timing uses uninstrumented frozen native/Linux bundles, one process on each host, with checksum verification on upload. `source-final-controls-20261016.tar.gz` preserves this exact control checkpoint.

A later slot-evolution control distinguishes the fastest slot-only accessor from a resolved property handle that also accepts pre-slot ordinary Fast or Compact rows. The handle retains both key and slot ordinal, checks the layout, uses the direct cell for current rows, and falls back for old rows. An older handle without a slot ordinal can still find a field that a newer schema promoted. No slot is reused. These checks are necessary for a usable API and will be timed separately; the earlier lower-bound measurements are retained.


## Completed next iteration — 2026-10-16


Measurements: 2026-10-16, Go 1.27.1, Apple M5 Max and AMD EPYC 7502P. Five 150 ms repetitions, GOMAXPROCS=1; server pinned to logical CPU 24 at nice 10. Tables show medians, rounded to integer nanoseconds. Raw ranges and every repetition remain linked below. These are bounded experiments, not a production latency model or proof of global optimality.

## Recommendation

Keep two persistent layouts, Compact and Fast. Retain the literal four-bit lengths for Compact and logical changes. Add optional selected direct slots within Fast Objects, with stable resolved property handles. Keep ZeroValues in the model and use the existing structural ability to store zero. A slot is an extension of Fast, not a third independently chosen row representation. Keep all alternatives runnable.

The four-bit choice is not based on the number of selector bits in isolation. Numeric key deltas share a varint with the selector, so removing one bit often saves no byte. Three-bit rounding is competitive, but its measured size gain is tiny and server encoding generally slows. Two-bit tags have larger list-access regressions. Do not automatically choose a different codec for each row from these marginal size differences.

## Replay sizes

The same 189 generic root shapes, with the same values and zero policy, encode to:

| Compact dialect | Total bytes | Difference from literal4 |
| --- | ---: | ---: |
| Literal byte lengths, four bits | 2 035 755 B | — |
| Rounded 0/1/2/4/8 widths, three bits | 2 034 675 B | −1 080 B (−0.053%) |
| Tag / 4 / 8, short spans through 62 | 2 039 606 B | +3 851 B (+0.189%) |
| Tag / 4 / 8, short spans through 120 | 2 034 538 B | −1 217 B (−0.060%) |

The widest two-bit tag saves just 137 B over three-bit lengths. The totals are sums across selected shapes, not database-wide compression estimates. The replay does not preserve original strings, IDs, scalar values, or dictionary prefixes, and is not access-frequency weighted. See the [sampling study](results/workload-distributions-20261016.md).

## Construction and rebuilding

All these warm cases reuse the output and metadata. Each variant writes child payloads directly into one destination, with no intermediate payload copy at each ancestor. They use the same metadata structures and per-depth storage; no candidate wins by omitting an input traversal from its timing. Cold cases and allocation measurements are retained in the raw results. The selector experiment is parameterized; its literal4 writer emits the exact common-checkpoint bytes but is not the fastest specialized common writer.

| Operation | Mac literal4 | Mac rounded3 | Mac tag2-wide | Server literal4 | Server rounded3 | Server tag2-wide |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| State encode | 3 014 ns | 3 051 ns | 3 377 ns | 7 743 ns | 8 140 ns | 8 592 ns |
| Settings encode | 8 779 ns | 8 846 ns | 8 997 ns | 22 884 ns | 24 396 ns | 25 187 ns |
| Document encode | 2 690 ns | 2 697 ns | 2 897 ns | 7 456 ns | 7 553 ns | 7 718 ns |
| String Dictionary 128 encode | 2 439 ns | 2 443 ns | 2 472 ns | 6 025 ns | 6 593 ns | 6 675 ns |
| Largest replay root encode | 180 028 ns | 181 656 ns | 183 434 ns | 487 009 ns | 500 598 ns | 511 515 ns |
| Object scalar edit + rebuild | 1 669 ns | 1 716 ns | 2 106 ns | 5 216 ns | 5 488 ns | 6 811 ns |

All warmed cases in this table allocate zero. For the settings fixture, fresh writer/output allocation costs are:

| Dialect | Mac time | Server time | Allocated bytes | Allocations |
| --- | ---: | ---: | ---: | ---: |
| literal4 | 16 392 ns | 38 936 ns | 21 504 B | 20 |
| rounded3 | 16 415 ns | 40 418 ns | 21 504 B | 20 |
| tagged2-wide | 16 529 ns | 41 385 ns | 21 496 B | 19 |

## Matched trusted Object readers

The first optimized three-bit reader beat the existing generic four-bit lookup loop. That was not sufficient evidence about the formats: Go did not inline a helper in the first experiment, and the common View reader also supports layout dispatch. Retained controls remove those differences. The matched loops both assume a trusted Compact Object and differ in selector width and width decoding; the generic Value/View API still needs appropriate dispatch.

| Lookup | Mac common literal | Mac matched literal | Mac matched rounded | Server common literal | Server matched literal | Server matched rounded |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Local keys | 144 ns | 117 ns | 123 ns | 362 ns | 284 ns | 284 ns |
| Global keys | 149 ns | 122 ns | 127 ns | 373 ns | 296 ns | 296 ns |
| Mixed scalars/spans | 168 ns | 136 ns | 145 ns | 405 ns | 330 ns | 329 ns |

Literal lengths are faster on this Mac and effectively tied on the server when the loops match. The worthwhile next implementation improvement is the specialized reader, which changes no bytes. All lookup variants allocate zero. This result removes the apparent three-bit read advantage; it does not turn small timing differences into statistically established wins.

## Lists expose the two-bit trade-off

| 128-position List | Literal4 | Rounded3 | Tag2-wide |
| --- | ---: | ---: | ---: |
| All zero | 3 B | 3 B | 132 B |
| Alternating 0/1 | 196 B | 196 B | 132 B |
| Unsigned 16-bit words | 259 B | 259 B | 388 B |
| Mostly eight-byte words | 1 149 B | 1 149 B | 1 149 B |
| Alternating small/eight-byte words | 707 B | 707 B | 708 B |

Two-bit immediate tags win for mixed zero/one lists, but lose shared zero and short-word compression. Direct indexing remains possible for immediate-only and shared four/eight-byte lists; other tagged lists scan. Literal and rounded shared-width lists retain direct indexing.

| Positional read | Mac literal4 | Mac rounded3 | Mac tag2-wide | Server literal4 | Server rounded3 | Server tag2-wide |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Zero | 12 ns | 12 ns | 12 ns | 27 ns | 28 ns | 27 ns |
| Boolean | 247 ns | 236 ns | 12 ns | 594 ns | 614 ns | 27 ns |
| 16-bit words | 12 ns | 12 ns | 251 ns | 27 ns | 28 ns | 714 ns |
| Mixed small/eight-byte | 234 ns | 229 ns | 255 ns | 598 ns | 618 ns | 661 ns |

A dedicated packed Boolean-vector model could improve the Boolean case further, but adding a general small-width bit-packing codec is not justified by this sample or required for this iteration. The narrow Boolean loss is recorded rather than hidden by an extra format special case.

## Change batches

The path grammar, revisions, operation order, and prefix compression are identical. Only the value selector varies. Literal4 uses code 10 for Delete, rounded3 uses 6, and tag2 uses 3. Set-zero is explicit in every dialect. Values still go straight into the output; the reusable scratch holds path/length metadata. These are encoding/reading measurements, not a WAL durability test.

| Sixteen changes | Literal4 | Rounded3 | Tag2-wide |
| --- | ---: | ---: | ---: |
| Encoded size | 263 B | 263 B | 268 B |
| Metadata capacity | 256 B | 256 B | 256 B |
| Mac encode | 181 ns | 190 ns | 198 ns |
| Server encode | 758 ns | 779 ns | 810 ns |
| Mac read | 186 ns | 192 ns | 183 ns |
| Server read | 607 ns | 607 ns | 596 ns |

Every measured change encode/read reuses storage and allocates zero. Long paths, full-width keys, empty keys, empty spans, zero assignments, deletion, and malformed inputs are covered. A 30-second change-parser fuzz campaign completed 2 898 826 executions. The container parser also passed a separate 30-second campaign. These checks are evidence, not exhaustive validation.

## Presence

ZeroValues changes schema policy, not the binary grammar. Ordinary zero assignments omit a map entry; flagged assignments retain a present zero, and Delete removes it. Empty strings/blobs/containers normalize to zero while the schema supplies the type. Unknown present zeros survive a rewrite.

The optimized presence-returning Compact loop matches the work of the ordinary lookup loop. A retained iterator control is much slower; this was an implementation cost, not a necessary presence cost. Benchmarks of this low-level lookup exclude semantic model membership checks. Do not infer that the current linear objectSpec lookup is free. Resolved handles and generated wrappers avoid repeating that model search.

## Selected fixed slots

Only the eight marked fields in the 128-field fixture receive slots. Ordinary fields keep the keyed directory. The dense row is 1 161 B versus 1 157 B for ordinary Fast, a 4 B increase. With 32 of 128 fields present and four selected slots absent, direct slots cost 333 B versus 293 B for ordinary Fast. The bitmap-packed control uses 301 B but loses stable direct offsets.

Both direct and packed variants retain slot keys, presence, and scalar/span boundaries. Direct slots are the proposed form: selecting a sparse field is an explicit space trade-off. The schema catalog must persist append-only slot assignments. An older writer carries the stored slot directory forward, including unknown slots; it cannot rebuild the directory from only the fields it currently recognizes.

Generic lookup does not become faster merely because a row has slots. Resolve a property to its key/slot once, or generate a typed accessor. The low-level selected-only server test measured Immutable 176 ns, ordinary Fast 254 ns, known direct slots 68 ns, and generic slot search 273 ns over 32 768 mmap rows. These historical lower-bound measurements omit accepting pre-slot row layouts; the complete handle is measured separately below.

Owned scalar updates can set an absent reserved slot, clear it, or retain an explicit zero without reallocating. Here copying means copying the whole row into an already allocated owned buffer, not modifying Bolt's mapping:

| Operation | Mac ordinary Fast | Mac selected slots | Server ordinary Fast | Server selected slots |
| --- | ---: | ---: | ---: | ---: |
| Patch owned scalar | 12 ns | 13 ns | 39 ns | 31 ns |
| Copy row, then patch | 26 ns | 22 ns | 55 ns | 46 ns |

These patch cases allocate zero. Standalone slot patching is slightly slower on the Mac and faster on the server; it is not a universal mutation win. Ordinary setters still rebuild when a property is a span or an old row has no reserved slot. Transaction ownership, Bolt Put, full-value writes, WAL durability, and concurrency are separate costs.

## Complete slot handles and construction costs

A resolved handle accepts empty, ordinary Compact, ordinary Fast, direct-slot and packed-control rows. Its key enables fallback when an old row lacks the slot, while an old key-only handle can search a newly slotted row. Direct slots retain their ordinal even after their property is dropped. Schema-independent rebuild tests preserve unknown slot assignments, scalar values, opaque spans and explicit zeros, including ordinary keyed zeros.

The mixed-access fixture selects 32 768 warm mmap-backed rows and sends 80% of reads to the eight selected fields. The remaining reads use the ordinary directory:

| Reader | Mac | Server |
| --- | ---: | ---: |
| Existing Immutable | 73 ns | 193 ns |
| Ordinary Fast | 89 ns | 258 ns |
| Slot-only lower-bound handle | 32 ns | 119 ns |
| Complete resolved handle | 34 ns | 118 ns |

The first three rows are the earlier policy run; the complete handle is the later run with the same fixture. Its own paired control measured Mac 33 ns / server 114 ns without layout fallback versus Mac 34 ns / server 119 ns with fallback. The added compatibility checks preserve the large benefit; do not compare small cross-run differences as if they were paired. Every reader in this table allocates zero.

Full-row construction from already prepared values is slower for selected slots:

| Prepared-value writer | Mac ordinary Fast | Mac direct slots | Server ordinary Fast | Server direct slots |
| --- | ---: | ---: | ---: | ---: |
| Dense 128 fields | 648 ns | 1 184 ns | 1 910 ns | 2 109 ns |
| Sparse 32 fields | 178 ns | 317 ns | 505 ns | 555 ns |

All four writer cases allocate zero with reused buffers. The initial fixture-searching adapter and a streaming source adapter are retained separately; their extra lookup/conversion work must not be attributed to the slot grammar. The prepared-value comparison still shows a real construction cost. No universal speed win is claimed.

## Evidence

- [Four-dialect native measurements](results/bench-selectors-arm64-wide.csv) and [server measurements](results/selectors-amd64-wide-20261016/bench-experiments.csv): 176 cases each.
- [Final native controls](results/final-controls-arm64-20261016/bench-experiments.csv) and [server controls](results/final-controls-amd64-20261016/bench-experiments.csv): 172 cases each, including trusted-loop variants, list cases, changes, and patches.
- [Complete handle and matched-reader native controls](results/next-iteration-arm64-20261016/bench-experiments.csv) and [server controls](results/next-iteration-amd64-20261016/bench-experiments.csv): 17 cases each.
- [Common checkpoint versus previous draft and Immutable](results/common-cross-hardware-20261016.md): the earlier complete 703-experiment/11-draft-case runs on both machines.
- [Portable runner and inventory](BENCHMARKS.md), including exact source archives and all rejected alternatives.

Raw `.txt` files contain every repetition. `.metrics.csv` includes encoded bytes and scratch metrics. The source snapshots identify dirty experimental code independently of the Git commit. Full/short tests, race tests, pointer checks, and vet passed after the added controls. All production package source and application data remain unchanged.

## Stopping point for this iteration

The experiments cover the proposed partitions, a denser tag partition, direct versus packed selected slots, generic versus resolved access, old-row fallback, present-zero policy, one-buffer construction, copies, allocations, alignment, actual mmap access, changes, and both CPU families. Further variations now trade one measured case for another rather than exposing a clear broadly useful winner. Boolean-vector packing, string-prefix coding, an access trace, and production-scale cache/I/O behavior could justify later work, but this sample does not support adding those mechanisms to the specification now.

The recommendation is the simplest supported next iteration, not an assertion that no future optimization exists. The whole-row Fast lookup regression against Immutable remains explicit. Selected slots address the marked hot fields; they do not repair every arbitrary-key workload. The integrated public editor, model compiler, migration, WAL applier and plugin ABI remain separate implementation work. No production migration or deployment has occurred; current research changes are uncommitted and unpushed.


### Final execution and decision record

- All four compact dialects are kept. Selected literal-four for the next proposal: the best alternative saves 0.060% across replay roots, construction generally favors literal lengths, and matched server lookup loops tie. Boolean Lists remain a concrete case where tagged encoding wins; shared-zero and fixed-width Lists are concrete losses for it.
- Selected direct fixed slots for marked properties only. Kept packed slots as a control. The complete property handle preserves compatibility with pre-slot and pre-promotion rows. Added `slotRewrite` to retain unknown slot assignments, spans, and explicit zeros without the old semantic schema. This is still separate from the common editor.
- ZeroValues is implemented as model policy over existing zero framing. Added presence-aware lookup, editing, Set-zero versus Delete, and owned zero patching; tested ordinary zero elision and unknown-zero preservation. Full public model binding remains future work.
- `next-iteration-{arm64,amd64}-20261016` completed all requested correctness tests and 17 new benchmark cases per host. `final-controls-{arm64,amd64}-20261016` completed 172 cases per host; the prior wide-selector runs completed 176. Every timing case has five repetitions and raw allocation/size metrics. Server bundle SHA-256 verification passed before each run. Native-only initial and evolution controls remain labeled separately.
- Final code validation: full and short EDB suites, research race tests, `checkptr=2`, and vet passed (`test-next-iteration-*`, `vet-next-iteration.txt`). Focused slot-evolution checks also passed. No timing used instrumented binaries. Container and change fuzz results are retained; fuzzing is bounded, not a proof.
- Exact current executable source snapshot: `source-next-iteration-20261016.tar.gz`. Prior checkpoints are preserved under `source-final-controls-20261016.tar.gz`, `source-slot-evolution-20261016.tar.gz`, `source-selectors-wide-20261016.tar.gz`, and the earlier named archives. README/STATE were updated after timing, so documentation may be newer than a benchmark source snapshot.
- No questions or new decisions were inferred from elapsed time. Layout-default spelling remains an explicitly open API choice. There are no outstanding required measurements in this iteration. Production integration, schema catalog allocation, full binding/generation, recursive path editing, WAL application, sealing and plugin ABI are future implementation scope, not completed features.


## User decision — commit after substantial steps (2026-10-16)

> Everything committed?

> Remember - commit after each substantial step, it's all WIP anyway.

Record this as the ongoing commit policy. The earlier three commits were local, but the interactive iteration was still dirty. Checkpoint the current experiments, source snapshots, anonymized fixtures, measurements, and documentation now; commit subsequent substantial steps as they complete. No push is authorized. Generated benchmark scratch databases are ignored under `results/*/tmp/`; their generators and results are retained. No research source or measured result is discarded.

Commit preparation removed cosmetic trailing whitespace from result logs and normalized CSV line endings, without changing measurements or source-snapshot archives. The last full/short suites, race/pointer checks, fuzz runs and server benchmark runs are recorded above; no executable code changed during this checkpoint preparation.


## User decision — presence flag spelling (2026-10-16)

> Nitpick after: rename edbschema.ZeroValues to edbschema.PresenceIsSignificant

Checkpoint `3ae2df7` commits the preceding research. Rename the proposed public flag to `edbschema.PresenceIsSignificant`, the experimental model flag to `propertyPresenceIsSignificant`, and the slot-plan field to `presenceIsSignificant`. Update the current specification, examples, comparison report, and this file's active decisions. Historical requests and frozen source snapshots retain the earlier spelling. This is a naming change; presence semantics and stored bytes are unchanged.

Validation after the rename: `go test ./...` and `git diff --check` passed. Commit this naming step separately; no push.


## Interactive follow-up — selector bits and attribution scope (2026-10-16)

User requests, preserved as received apart from cosmetic whitespace:

> Why is ordinary Fast still slower than the existing Immutable format? Is it because it is a lot more compact? Investigate whether we can close that gap without relying on fixed slots and keeping most of the upsides of the new format. Work very hard and long.

> Note: as a first step, I want you to understand what are the most significant factors that cause the difference, and what's the contribution of each. Then we can decide what to try to change if anything.

> Well, actually, before you go there, I figured, can you give me, you ran for a while while I was snorkeling, so could you give me a short rundown of what you learned from exploring the database, from exploring the distribution of data, and basically what you learned from the last run, okay? And then we'll continue with what you are doing.

The rundown was delivered. The next question selected the earlier sentence about 77% of nonzero scalar words needing eight bytes:

> Explain why simpler bits classes do not help much and what consequences, what design decisions we could derive from this finding.

The Fast work remains attribution-first: reader overhead, key widths, metadata placement, cache behavior and interactions are hypotheses, not measured contributions yet. No format or production reader changes have been chosen.

### Selector explanation and new deterministic evidence

Added `experiments/selector_sizes_test.go`, which accounts for actual encoded replay bytes recursively and asserts conservation for every root. The complete output is [selector-byte-accounting-20261016.txt](results/selector-byte-accounting-20261016.txt); the explanatory table and reasoning are in [the comparison report](results/next-iteration-20261016.md#why-fewer-selector-bits-save-few-bytes). All four measured totals exactly reproduce the prior replay totals.

- Across the same 189 roots, string/binary payload plus Dictionary key bytes are 1 729 026 B, or 84.93% of the 2 035 755 B literal encoding; these bytes are unchanged by selector choices.
- Rounded3 saves 503 B in entry metadata, 845 B in count/shared-code headers and 84 B in footers, totaling 1 432 B. It adds 352 scalar payload bytes, leaving 1 080 B net saving. The full observation sample's 3 446 B rounding penalty is a different population and cannot be subtracted from the selected replay totals.
- Four/three/two class bits change mixed Object one-byte key-delta thresholds from 7 to 15 to 31; most smaller deltas still occupy a whole byte. Shared classes already amortize width metadata. Mixed Dictionary/List class codes remain whole bytes in these implementations, not a continuously packed bitstream.
- Two-bit immediate tags partly move scalar payload into the catalog; raw component totals must acknowledge that relocation. They also trade Boolean List wins against shared-zero and two-byte List losses.
- Correct the conversational shorthand: the 77.2% statistic counts encoded scalar words, including IDs, float bits and the chosen nanosecond timestamp representation. It does not establish an arithmetic integer distribution or incompressibility.
- Interpretation, not a new user decision: rare unusual widths mean rounding is inexpensive, so rounded3 remains credible. Small measured size differences do not prove literal4 uniquely optimal. Its direct length interpretation and preserved shared-width access, plus the measured construction/read trade-offs, support retaining it for now. Do not add an automatic dialect selector for the tiny aggregate gain. Dictionary key redundancy and semantic wide-word representations are possible future measurement subjects, not approved new complexity.

Updated README, comparison report and benchmark instructions. No encoder, reader, stored format, sampling data or production code changed. The accounting test, the full `research/kvo/experiments` test suite and `git diff --check` passed; prior runtime measurements remain the runtime evidence. Commit this explanatory step locally under the ongoing WIP policy.


## Interactive follow-up — broader lessons from distributions (2026-10-16)

The user selected the earlier rundown's paragraphs on wide words, short strings and near-equal selector sizes, then asked:

> I guess I was really asking a bigger picture. From the numbers that we have produced, what are the learning points that we should derive?
>
> Are you saying that basically all of our encoding schemes produce roughly the same encoded size and we should focus on picking what produces best performance versus trying to micro-optimize for compactness? Would that be correct?
>
> And also from your learnings of size distribution of strings and integers, what are the cases that we want to special case to optimize and which cases should we note?

Added [workload-design-lessons-20261016.md](results/workload-design-lessons-20261016.md) and linked it from README. The interpretation is: prioritize speed, allocation/copy behavior and simplicity among the closely related Compact selector variants, whose aggregate sizes nearly tie. Do not generalize that near-tie to Fast versus Compact versus Immutable, or assume universal equality with MessagePack. The state encoder's unchanged 1 076 B size and server improvement from 31 747 to 6 627 ns illustrate the larger opportunity in removing repeated work.

Additional arithmetic from the already retained anonymized histograms, with no new production sampling:

- Scalars: 119 658 / 223 598 observations are zero (53.5%). Text: 48 302 / 126 319 observations are empty (38.2%). These are pre-elision observations, not byte-savings estimates.
- Eight-byte scalar words: 80 204 total, of which 75 218 are unsigned (93.8%); 75 191 of those have bit width 62. This clarifies the prior general warning that words include floats and timestamps: the observed wide population is overwhelmingly unsigned integers. The histogram does not itself identify the fields or establish a compression scheme.
- Text payload totals 1 945 645 B in 78 017 nonempty observations. Above 62 B: 4 728 observations (6.1%), containing 664 724 B (34.2%). Above 127 B: 1 077 observations (1.4%), containing 376 188 B (19.3%).
- Binary payload totals 277 890 B in 3 572 observations. Above 127 B: 216 observations (6.0%), containing 242 519 B (87.3%).
- These byte-weighted totals are computed as the sum of length times count in `workload-distributions-20261016.json`. They are a different population from the 189 selected replay roots.

Design priorities inferred for discussion: ordinary zero omission, cheap common short lengths, straightforward short-word and full-width paths, cheap tiny containers, first-class Dictionary costs, and minimal copying for large payloads. Keep generic handling for unusual widths and negative magnitudes. Note Boolean List and shared-word trade-offs, and possible dictionary-key redundancy, without adding automatic codecs or special cases from counts alone. Shape data cannot determine access frequency, hot-field placement, or the final layout default.

This is a documentation and interpretation step only. No production code, codec implementation, benchmark variant or user-approved format choice changed. The Fast-versus-Immutable attribution task remains pending and unchanged. Validate Markdown links and `git diff --check`, then commit locally under the ongoing WIP policy.

## User request — performance iteration (2026-10-16)

Say, if we just want fast 8-byte cells, maybe that's what we do. Like, we do one bit instead of two or three or four bits we proposed for size classes, where if it's one, we do an 8-byte cell. If it's zero, we use a tag byte which will encode small integers directly, and the remaining weird cases will require a significant, more significant space to encode, something like that.

It feels weird to have to include a tag byte separately from one bit (which is within the object record), but if it covers a substantial number of cases, why not? Ofc, this one bit would not be present outside of objects and dictionaries, so isn't always an option...

And tiny containers is another good point. Is there any change we want to do based on that?

> **Small negative integers:** the sample gives little reason to reserve a large tag range for them.

Good point, maybe we drop them from special case encoding!

> So my proposed priority is: **preserve good compactness, optimize the common operations, and require a meaningful workload benefit before adding another encoding special case.**

I absolutely agree. In fact, what we want to do is drop encoding special cases that are not supported by actual data to simplify the format as much as we can and to use those bits that free up for something better.

Like, say, if we only special-case small positive integers, that means we can just use a high bit to select whether it's a small integer or just some other representation, which basically means a byte sequence, right? And then, with the high bit set, it's just like the number of bytes or something like that. So that allows us to do simpler encoding and decoding.

So now I want you to go and first of all apply these updates, right? And like see basically remeasure a tweaked version and then go back to the task you were doing before that I asked you to do, which is to figure out how to make the fast fast format faster. Maybe again, if we reduce special cases, that could make it faster. Also look at optimizing the actual implementation, not just the format, because maybe there is a way to implement decoding and encoding of it faster and accessing. Really, there's no encoding and decoding, right? It's more of accessing. So faster accessing. So let's go ahead and do all this. And I'm very interested in the performance of mutation. Like if we only change a few keys, right? Because if we if we change a lot, it makes more sense to like fully decode and update stuff and re-encode. But say we only change a few keys and we want if the original is in a compact format obviously we have to rewrite it right no way around it so nothing really to do there. But if the original is using our immutable format, the idea, the hope is that we somehow, somehow apply these changes with minimum, like the fastest way possible, although I guess now applying changes in both encodings is roughly the same. I mean, previously, right? Previously I had this object map. I guess it wasn't really necessary. My idea was that to allow to allow to tweak particular keys without going off and rewriting the entire byte array. But I suppose that's not really, I mean, well, I guess the point was that we had a very compact format that could represent the changes in each object, right? Within each object. Because you're having a sorted list of updates now, I'm not actually sure how performant the updates would be because, first of all, it's a sorted list, so you have to make space. Like, what you have to move stuff around each time we add a new change so that seems inefficient. And I mean, because these don't have to be, these don't have to be threat safe, what we can do is like at least a flag so that we could append a whole bunch of changes, but the first time you read, we do a resorting, right? Like like basically a dirty flag. So when we append, we set dirty or like need sorting and then when we read we sort and clear that flag, something like that. So mutation performance is also important to me.

so I guess what I'm saying is that your next frontier is performance of the fast path, because compact encoding and decoding — I mean, really, your next frontier is performance, I guess. While I'm out having dinner, you can look into all of these aspects and optimize all of them.

So, but they're sort of separate, right? Compact encoding and decoding performance is one thing and is important for sure, but less important than performance of accessing and mutating of fast representations. We want accessing fast representations to be as fast as possible, and we want mutating, like mutating a small subset of a fast representation, to also be very fast and memory efficient. And we also want separately mutating pretty much the entire fast representation to also be fast, which is probably not possible with our current representation.

Basically, the idea is that so we have fast representation. At some point, we will be building that row from ground up from scratch, right? Which will set pretty much every key on that row that is supported, that is non-zero. So you will have like, if you have a big object, you will have a case where the mutation covers every field in that big object, and that needs to be performant.

So we should be measuring all of these things, like mutating a few shallow keys, a few deep mutations, a really wide mutation that, like, I guess you can even measure two wide mutations separately. One is like we are right 70% of the object. And the other one is like we build up an object from scratch from zero, right? And the read performance, of course.

So maybe you expand your benchmarks a bit. Maybe you, in fact, use what you learned from Bubblehouse database study. You create some shapes, some mock shapes that are similar to what Bubblehouse uses. Ideally, you don't. I mean, you name them generically so that again, Bubblehouse terms, use case, loyalty stuff does not show up. So you improve these benchmarks, you copy over shapes of the schema, right, the tables, and also shapes of the data from what you studied, and then you do performance tests on these data that matches the real shapes and not on just some theoretical distribution. Definitely not a uniform one, right? You know how the data is distributed.

So do all this, work for a long time, and produce the best performing version.

### Execution plan and initial access check

The latest request authorizes proceeding beyond the earlier attribution-only stopping point: diagnose Fast costs first, then implement and compare improvements. Priorities are Fast access and mutation, followed by Compact runtime. Keep the one-bit and tiny-container experiments first as requested. Candidate simplifications must still preserve schema-independent value boundaries and full-width keys. A high-bit length tag can deliberately leave short word versus span interpretation to the schema; tests must check that distinction rather than silently treating every span as an integer.

The existing working tree was clean at `4372788`. Go is 1.27.1. Access to the server succeeded after the documented knock immediately followed by SSH; the stale control socket alone did not connect. Server uptime/load was read, and no application data or settings changed. No perf executable was found, so use retained benchmark controls, compiler output and process-local profiling unless another existing profiler is available.

### User addition — preserve rejected fast options

> If you cut a certain solution as unreasonable/unmaintainable, but it would give a significant performance boost, please absolutely record it in the file, and let me review a list later!

Accepted as an ongoing research rule. The final performance report will include a review list, including complexity-heavy or assumption-limited winners, with their runnable evidence and reasons for not selecting them.

### Step 1 — one-bit candidate and tiny-container counts

Implemented `experiments/one_bit_test.go`. Object deltas and Dictionary key lengths carry one low bit selecting eight raw bytes versus a normal tag. A 65-bit uvarint preserves all 64 numeric-key bits plus the flag without reserving key zero. Lists have no selector bit. Normal tags use 0–127 for immediate words, 128–254 for byte lengths 0–126, and 255 plus `uvar(length-127)` for longer spans. No small-negative tag range exists; signed semantic words still use the existing ZigZag mapping. Short non-immediate words are structurally opaque spans, interpreted through a schema-known word accessor. Eight-byte spans may also use the raw-cell flag. Unknown-field rewrites preserve bytes without that schema.

The count-free variant omits Object/Dictionary counts and retains List counts. This removes a byte from most tiny containers but makes an Object/Dictionary count require scanning. The encoded count policy belongs to the row codec, not an extra per-child version. Every one of the 189 replay roots passes recursive value checks and exact unknown-value rewrites, including full-width numeric keys, key zero, empty containers, byte-length thresholds and explicit present zeros.

Replay totals: literal4 2 035 755 B; one-bit counted 2 042 013 B (+0.307%); one-bit without Object/Dictionary counts 2 030 977 B (−0.235%). The count omission saves 11 036 B relative to the counted one-bit form. These are selected-shape totals, not database-wide estimates.

Both `onebit-arm64-20261016` and `onebit-amd64-20261016` completed 132 benchmark cases with five 120 ms repetitions. Source snapshot `source-onebit-20261016.tar.gz` retains the initial implementation exactly. The initial getter used the checked iterator and was slower than the baseline trusted getter; this is not a fair format-only attribution. The next run therefore retains that getter and adds a trusted lookup with matching validation assumptions. It also removes unused shared-class tracking from the candidate writer. Do not attribute that implementation work to a format change. The List regression remains structural: dropping shared widths loses direct access to a two-byte-word List.

Initial state encode, baseline / counted one-bit / countless one-bit: Mac 2 768 / 3 336 / 3 294 ns; server 6 666 / 7 247 / 7 301 ns. Initial two-byte List read: Mac 7 / 170 / 173 ns; server 20 / 408 / 408 ns, with the iterator/validation caveat above. These are controls, not a final rejection of the simpler tag proposal. All warmed measured operations allocate zero.

The following run combines trusted Compact access with Fast attribution controls. Fast controls hold bytes constant while varying layout dispatch, scalar knowledge, typed key loads, equality-exit search and prepared headers. Separate scalar-only controls move the header or force eight-byte keys, including padding narrow-key rows to the same physical Bolt footprint. Prepared-header caches and front-header layouts are diagnostic alternatives, not silently adopted APIs. The compiler confirms the common header, search, value and getter functions do not inline; the generic typed search kernels do inline. Timing must establish the significance.


### Step 2 — Fast attribution and mmap correction

The paired initial and corrected attribution runs are retained, with source archives. See [performance attribution and review list](results/performance-iteration-20261016.md) for the controls and numbers. On the corrected 128-key / 32 768-row server run, common Fast is 303 ns versus Immutable 228 ns. Skipping the outer dispatch is 297 ns, native key loads 295 ns, scalar-known 281 ns, equality search 277 ns, and a cached 80-byte header 198 ns. These deltas interact and must not be summed as independent percentages.

Found and corrected two attribution hazards: Bolt Bucket.Get does not fault every payload page, and the first front-header control also simplified parsing. New warmup touches every payload page; process fault counters confirm the eliminated fault cost. Matched general/simple front/back controls now separate parsing from location. Historical results and code remain intact. No production layout has changed.

Expanded mutation controls now include sorted insertion, append plus reverse reads, sort-on-read, unsorted upsert, and flat hash indexing, with cold and reused storage. Fixtures are actual selected containers from the sanitized replay, including string Dictionaries; the largest sampled Dictionary has 71 stored entries, so its label is `dictionary-largest`, not a fictional 256-entry workload. Full-row path and owned-patch experiments are being prepared next.


### Step 3 — unchanged-byte reader and overlay matrix

`read-batch-{arm64,amd64}-20261016` completed 594 cases per host; exact source is `source-read-batch-20261016.tar.gz`. The fused Fast getter avoids the temporary generic header and uses typed key search. Gapped 128-key hits over 32 768 rows improve from 105 to 76 ns on Mac and 316 to 234 ns on the server with equality exit. Miss scenarios also improve versus baseline, though lower-bound search sometimes beats equality on Mac. Matched header controls confirm a separate placement effect on the server; see the performance report, including the retained front-header option.

Sorted insertion beats append/sort on many final-output cases despite slower staging. Lazy sort-on-read loses badly with interleaved reads/writes. The append reverse-read scenario initially favors the most recent key; a new refinement reads other keys. Hash and linear-upsert variants remain retained; no universal policy is selected from staging-only results.

Full-row fixtures now select 31 / 503 / 1 521 / 10 leaves, depths 4 / 2 / 7 / 1, and 673 / 39 806 / 35 906 / 4 145 B, respectively, from generic replay IDs. These are shape probes, not claims about production hot paths. The tree editor preserves untouched children opaquely and writes edited children into one destination. An owned patch planner requires existing slots, matching scalar/span category, unchanged span lengths and valid zero semantics; the source mapping is never modified. New sampled full-row correctness checks pass.

The first tree benchmark exposed signed conversion of a random uint64 before modulo in the read harness. Corrected to unsigned modulo; this was a benchmark query-index bug, not codec corruption. Preserve the incomplete `tree-arm64-20261016` run and exact `source-tree-20261016.tar.gz`; use the corrected `refined-*` runs for comparisons. Repeated benchmark smoke runs now cover every newly added operation.


### Step 4 — sampled full-row edits and preparation experiments

`refined-{arm64,amd64}-20261016` completed 383 cases per host, with three 150 ms repetitions and exact source snapshot `source-refined-20261016.tar.gz`. Tests cover four sampled whole-row shapes, shallow/deep edits, 70% updates and construction, owned patch preflight, unchanged borrowed inputs, Lists, Dictionaries, and comparison with the existing Mutable/Immutable implementation on compatible numeric fixtures.

Server example: four edits in the 39 806-byte wide row take 1 030 ns including planning and a full owned copy, versus 23 955 ns for the general tree merge. Four deepest edits in the 35 906-byte, depth-seven row take 1 885 ns with copy/patch versus 6 040 ns with the append-overlay tree merge. A 70% deep rewrite still costs 315 818 ns with repeated path lookup and copying, versus 649 400 ns with the generic tree; directly emitting an already prepared model takes 91 821 ns. This motivates a separate prepared-shape value-slot control, with setup cost and retained memory measured explicitly.

The optional prepared Fast handle was reduced from an 80-byte header to a 24-byte cursor and then a 16-byte packed view. The existing 64 MiB / 1 Mi-entry limits allow count, key-directory offset, key width, scalar/span mode and container kind to fit in a word beside the data pointer. No wire bits change. The next paired run measures opening cost, repeated reads, cache cost and exact Fast/Immutable query matches. The packed prototype currently accelerates Object getters; it validates/reconstructs extents for all three kinds, but Dictionary/List prepared getters are not implemented.

A large local-array scratch experiment does not stay on the stack: compiler escape output shows its arrays flow through the recursive editor into heap storage. A smaller flat patch-plan control is being measured separately. Preserve rejected scratch/code options and actual allocation counts rather than assuming local arrays imply stack allocation.

### Step 5 — a unified front discriminator without added framing

The all-Fast front-header control now has a complete forward writer and Object/Dictionary/List readers. It reserves five header bytes, writes children directly into the destination, then fills the header. Across 189 replay roots, moving offsets forward crosses a directory-width boundary in three roots, adding only six bytes total. Its mixed-layout detection limitation motivated a further simplification rather than rejecting the measured speedup.

New candidate: both layouts begin with the byte that already existed in their framing. Fast stores descriptor then uint32 count at the front. Compact moves only the low byte of its reversed footer to the front; any remaining high varint bytes remain at the end. Thus bit zero identifies Fast/Compact without adding bytes, extra child flags, or object versions. Compact payloads/catalogs are otherwise unchanged. The writer reserves one Compact prefix byte or five Fast header bytes and fills them at End; it never buffers/copies child payloads separately.

`unified_front_test.go` implements this framing, mixed children, structural validation, trusted views/iteration, a prepared 16-byte Fast view, unknown-value rewrite tests and fuzz entry points. `unified_edit_test.go` mechanically adapts the existing editor algorithms, retaining the old implementation as a control. It supports sparse/deep/dense edits, List construction/replacement, explicit present zeros and owned patch planning. Compact stays Compact when edited, including newly created descendants. Both copies are experimental; production would share algorithms rather than retaining duplicate implementations.

Recursive tests pass on all replay roots and generic fixtures with Compact, all-Fast, and Fast-root/Compact-child policies. Unknown entries round-trip byte-for-byte. Compact byte counts are unchanged. The new prepared-view flag is in memory only; the row codec still versions the whole format. The final comparison will include explicit Bolt mmap source rows for mutations, because earlier mutation ordering tests used hot heap source rows.

### Step 6 — front-placement tradeoff and adjacent-key control

The complete all-Fast front-header experiment is not a universal improvement. In the five-repetition server comparison over 32 768 mmap rows, fused back / front / Immutable are 232 / 178 / 249 ns for local keys, 291 / 252 / 262 ns for global keys, and 260 / 337 / 209 ns for the nested numeric control. The sampled Fast rows also favor the back header. Moving the header away from a trailing directory can add a separate memory dependency for span-bearing parents; this explanation is a hypothesis supported by placement controls, not a hardware-counter attribution. The mixed-layout unified version is being measured separately and is not selected merely because its framing is elegant.

A simpler follow-up, `keys_last_test.go`, keeps the old trailing discriminator and all Compact bytes. It emits Object cells, then the bitmap, then keys immediately before the five-byte footer. Dictionary/List encoding is unchanged. This adds no bytes, prepass, payload copy or additional representation choice inside a row. It specifically tests whether locating the search keys beside the count/descriptor improves flat reads without the front-header nested regression. All selected replay leaves and mixed-layout policies pass semantic checks; paired runtime measurement is next. The front-header alternatives remain on the review list even if this control wins.

The initial Mac adjacent-key run is retained separately: fused back / adjacent keys are 67 / 64 ns for local keys, 90 / 93 ns for global keys, and 106 / 102 ns for the nested control. These modest differences do not select a format by themselves. Added bounded validation, opaque iteration/rewrites, owned patching, recursive editing and mixed-layout/presence tests for this candidate. Its source copies are research controls, not a proposed duplicate production stack.

The unified-front prototype initially dispatched through two non-inlined getters. A retained `unified_direct_test.go` control fuses that dispatch, matching the back reader's structure, so the added wrapper cost is not attributed to the front format. The final adjacent-key suite compares both implementations and includes complete mmap mutations. Earlier source archives and raw results are preserved.

The adjacent-key validation parser initially called the old 80-byte-header constructor and then returned a second header. Retained it as `keysLastIndexPrevious`, and added a direct constructor for the next mutation comparison. This is an implementation correction, not a format advantage. A bounded 10-second fuzz run of the new format passed 420 695 executions before that equivalent constructor cleanup.

`byte_search_test.go` tests standard-library `bytes.IndexByte` for one-byte key directories, retaining binary search for wider keys. This requires no new wire case or dependency. Both old and adjacent-key layouts are measured so any search gain is not credited to directory placement. Kernel controls cover 0–255 entries, successful and absent keys; exhaustive getter tests reject targets above 255 rather than narrowing them accidentally. Paired mmap and sampled-path controls follow.

### Step 7 — the read gap closes in two controls; tiny-path refinement

The complete adjacent-key comparison passes on both hosts. Server fused back / adjacent keys are 219 / 182 ns for local128, 278 / 258 ns for global128, and 260 / 253 ns for nested numeric reads. Sampled path reads improve on all four server shapes in that run. Front framing remains substantially slower for nested server reads even after fusing its extra getter call (335 ns versus 253 ns for adjacent keys). No encoded-size growth is needed.

The 183-case byte-search run completes on both hosts. With identical mmap queries, adjacent keys plus byte search / Immutable are 152 / 229 ns for local128, 267 / 249 ns for global128, and 180 / 210 ns for nested reads on the server. Mac figures are 51 / 100, 94 / 97, and 87 / 137 ns. The remaining global-key server gap is about 7%; do not claim universal parity. These are ordinary keyed lookups, with no fixed slots or prepared cache.

The sampled-shape controls expose another tradeoff: byte search improves the span-heavy path substantially, but can regress paths whose numeric lookup is almost always one tiny Object. Server adjacent-key binary / byte reads are 39 / 45 ns for the wide sampled row and 257 / 271 ns for the deep sampled row; Mac also shows losses. `tiny_get_test.go` therefore retains direct linear cutoffs of one, two and four entries, with both binary and byte-search controls. This changes implementation only, not the encoding. The current API examples, portable little-endian fallback and validation are in `current_usage_test.go`; further timing uses frozen source archives rather than relabeling prior runs.


### Performance alternatives for user review — 2026-10-16 checkpoint

This freezes the current review ledger in the state file as requested. The linked report also keeps a navigable copy. Benefits belong to the stated workload and source snapshot; prepared-cache figures are from the older reader and must be remeasured against the new getter before adopting a cache.

| Option | Measured server benefit | Cost and disposition | Runnable evidence |
| --- | --- | --- | --- |
| Cache an 80-byte parsed header | 303 → 198 ns in the attribution workload | Real gain, but 80 bytes per row and borrowed-data lifetime management. Superseded as the leading cache proposal by the 16-byte experiment; retained for review. | `FastAttribution/.../parsed-header` |
| Retain a 16-byte prepared view | Eight reads: common 707 → cached 385 ns | Optional. Preparing inside the operation costs 482 ns; a cache adds 16 bytes per row. It can lose on nested one-shot reads. No additional on-disk layout. | `FastCursor`, `FinalFast` |
| Put the complete Fast header first | Fused back 229 → front 180 ns for local128 | Not selected universally: nested reads regress 261 → 344 ns. The mixed-layout discriminator is solved without added bytes; locality is the remaining tradeoff. Keep both direct and wrapped getters. | `FrontShape`, `UnifiedRows`, `KeysLast` |
| Prebind dense value slots | Deep 70% rewrite: generic tree 649 400 → bound 74 337 ns | Optional decoded/generated path. Shape preparation costs 350 341 ns and 329 744 B; the naive path binding utility adds 12 601 613 ns and is excluded from the hot figure. Dynamic Dictionary keys/List lengths invalidate the plan. This prototype requires a better binder before arbitrary path input benefits. | `ShapeBuilder`, `ShapeBind`, `TreeMutation` |
| Retain physical patch offsets and already-owned bytes | Four deep stores: 1 885 → 20 ns | The 20 ns figure excludes lookup, allocation, copying, and persistence. Optional only while ownership, shape and revision remain valid. Never apply it to a Bolt mapping or treat offsets as durable WAL changes. | `OwnedMutation/.../owned-patch` versus `copy-patch` |
| Append changes without maintaining order | Ordered 64-key construction: sorted 3 497 → append 2 906 ns | Useful as an explicit bulk/ordered path. Other final-write and interleaved-read cases regress. Sorting on every read is especially poor. Keep policies rather than silently adding a guessed adaptive threshold. | `BatchOverlay`, `BatchRefinements` |
| Shortcut insertion after the last key | Ordered construction 3 413 → 2 401 ns | Retained, not universal: shuffled 70% update regresses 3 918 → 4 401 ns. | `BatchRefinements/.../tail` |
| Native-width unaligned key access | Helps fused lookup and several isolated search kernels | Prototype fast path uses unsafe slices on amd64/arm64. Structural bounds, empty containers and lifetime correctness still matter. The selected getter and patch path now have an explicit little-endian fallback for other architectures. | `FastSearchKernel`, `fast_reader_test.go` |
| Count-free one-bit Compact tags | Saves 0.235% across selected roots; improves some tiny reads | Still a viable simplification. It loses constant-time Object/Dictionary counts and shared-width scalar List indexing; larger encodes are mixed. No additional persistent dialect selected just for this small size win. | `OneBit`, `CompactMatched` |


No faster option was deleted for maintainability. All code, incomplete runs, corrections, raw repetitions, and exact source snapshots remain available. Current research selection is adjacent Object keys with trailing framing. The final tiny-search refinement selects direct comparison up to four entries, then byte search for one-byte keys; this is an implementation choice, not another encoding dialect.


### Step 8 — final tiny-directory decision

`tiny-{arm64,amd64}-20261016` completes 38 cases per host, five 150 ms repetitions, from `source-tiny-20261016.tar.gz`. The simple up-to-four-entry loop recovers the broad-path losses of always calling byte search: server wide row 46 → 38 ns and deep row 271 → 262 ns; Mac 17 → 13 and 208 → 201 ns. There are tradeoffs: server small row 78 → 83 ns and span-heavy 34 → 36 ns, with Mac small roughly unchanged at 43 ns. Binary-only still wins that Mac small row at 39 ns. All three cutoffs remain runnable. Four is the measured compromise for this research default, not a format feature or a universal optimum.

The final same-run Fast / Immutable controls are 50 / 98, 93 / 99 and 86 / 134 ns on Mac for local128 / global128 / nested, and 150 / 257, 266 / 262 and 174 / 208 ns on the server. All are zero-allocation reads over 32 768 borrowed mmap rows, with no prepared view or fixed slots. Across the last two cohorts the server wide-key gap is about 2–7%; Immutable local128 also varies from 229 to 257 ns. Retain paired results and avoid claiming fine percentage accuracy across runs.

Selected `keysLastView.Get` now calls the four-entry refinement. `GetByte` and `GetBinary` retain the previous implementations. No encoded bytes change in this step. Added explicit tiny-directory empty/present-zero/span/missing/oversized-key boundary checks, a portable little-endian comparison, and an executable golden check for the README's 26-byte Fast example. The final validation/source checkpoint follows; performance sources remain frozen separately.


### Step 9 — validation and local closeout

Selected implementation checkpoint: `449f8d3`. The final code is frozen in `results/source-performance-close-20261016.tar.gz` (SHA-256 `b9a4218aa11f9d16471b5e8521ef61f2f9cf8409fc139843e24a2e974ae9efde`). Earlier benchmark archives remain separate and matched to each run's SHA manifest. The code checked here includes the four-entry getter, boundary tests and executable README example; it does not retroactively change earlier benchmark sources.

Validation passed:

- `go test -count=1 ./...` and `go test -short -count=1 ./...`.
- `go vet ./...`.
- `go test -race -count=1 ./research/kvo/...`.
- `go test -gcflags=all=-d=checkptr=2 -count=1 ./research/kvo/...`.
- All five freshly built Linux amd64 binaries (`edb`, `kvo`, `mmap`, `draft`, `experiments`) on the server, single process at a time, CPU 24, GOMAXPROCS=1, nice 10. Uploaded binaries and source archive matched their SHA-256 manifest.
- Compiler output confirms the selected Get wrapper inlines at its call sites. The fused reader remains a normal function; timing directly calls that same kernel.
- The five current/archive Markdown documents have no missing local file links. Whitespace checks pass. Trailing spaces in captured tool output are normalized; benchmark numbers, repetitions and source snapshots are unchanged.

Logs use the `performance-close-*` prefix. The earlier bounded fuzz runs and their exact source checkpoints remain preserved; all final fuzz seeds also pass in the correctness suites. This final step changes no production EDB or backend code, never writes through a borrowed Bolt mapping, and performs no deployment. Substantial WIP steps are committed on `main`; no commits have been pushed.

The requested exploration is complete for this iteration. The current recommendation, limitations, tradeoffs, and the faster-option review list are in README and the performance report. Future integration and optional designs remain explicit review decisions, not unfinished benchmarks hidden behind a completion claim.


## Broader goal continuation — 2026-10-16

Objective: Produce a reasonably best performing version across benchmarks; prefer server results, retain multiple approaches where a universal balance is poor, and exhaust reasonable optimization avenues without excessive complexity. The prior checkpoint completed its selected comparison and validation, but the active goal requires an additional completion audit.

Current audit confirms two concrete unfinished opportunities: `keysLastTreeEditor.child` linearly scans siblings both while navigating edits and while emitting every field; `boundShapeEdits` does an all-leaves scan for every edit. The latter is explicitly measured at 12 601 613 ns for the deep shape, so leaving it as the only dynamic binder cannot be called an exhausted simple optimization. Patch planning also still builds the large header and uses older numeric search. All original implementations remain controls. No new persistent encoding is proposed by these observations.


### Continuation step 1 — measured remaining mutation work

A fresh five-second server CPU profile of `KeysLastMmap/row-deep/keys-last/rows1024/wide70/tree` attributes 28.57% flat / 29.04% cumulative samples to the repeated child scan. Profiles include benchmark setup, but the timed work dominates the retained 6.43-second sample. This supports testing an in-memory child index without changing stored bytes. `tree_child_index_test.go` keeps sorted child-number arrays and a global `(parent, key, text)` map beside the unchanged sibling-list baseline. All variants keep the same append-overlay policy to isolate child navigation.

`shape_bind_index_test.go` compares canonical-path binary search with a reusable string-keyed path map against the quadratic reference. Canonical DFS leaf order is verified across every replay root; typed/length-prefixed path keys do not rely on collision-prone fingerprints. New tests include repeated reuse, absent/present-zero behavior and ancestor replacement. End-to-end benchmarks include shape preparation and binding separately from already-prepared cases. Cold tree cases reuse only the writer/output, and are labeled accordingly.


### Continuation step 2 — index measurements and simpler refinements

Both `index-*` runs complete 152 cases. Sorted child indexes help reused deep 70% edits (server 676 959 → 647 558 ns) and construction (716 870 → 675 570 ns), but lose cold and sparse cases; the hash child index is generally worse. The report preserves those options and their costs. Inspection identifies avoidable calls for containers with no edited children; the refinement keeps that case inlinable and handles a sole child directly. Do not select the index universally from its best case.

Binary path binding cuts the server deep reference from 11 389 277 to 291 046 ns without an auxiliary index. A prepared path map binds in 69 610 ns but its first implementation allocates 1 531 times during preparation. The follow-up groups keys in one immutable string buffer using strings.Builder. All original variants remain runnable or frozen in exact source archives. `performance-continuation-20261016.md` records complete binding/setup/write numbers, not just the hot lookup.

`patch_fused_test.go` transfers the selected getter's parsing/search approach into physical patch lookup, and separately tests within-call parent-prefix reuse with local eight-level scratch. No physical offsets survive the call; input bytes remain borrowed/read-only. Correctness covers all replay leaves, shuffled edits, rejection, and depth beyond the local scratch capacity. Paired runtime measurement follows.


### Continuation step 3 — writer and batch avenues

The next writer control reserves the final Fast directory extent once at End, passes the frame by pointer, and dispatches key width outside the entry loop. It keeps all stored bytes identical and still streams child payloads directly. Compact encoding remains the old body. Dirty-buffer byte comparisons cover every replay root and generic fixtures. Cold and reused whole-row encoding are measured separately.

A further reasonable mutation avenue is a one-shot leaf-change merge: sort a compact index of incoming resolved leaf paths, group by common parent, and merge directly with the base while writing one output. This could avoid per-container editor allocation and repeated sibling navigation for broad batches. It has a narrower contract than interactive ordered ancestor replacements, so it must remain explicit and be compared with preparation and final output included. Add it to the task list rather than declaring the general mutation frontier exhausted.


### Continuation step 4 — select fused physical lookup

Both refinement suites complete 204 cases. Server deep 70% copy/patch improves from 319 792 to 172 499 ns with fused lookup and 152 221 ns with within-call parent reuse. Wide 70% improves 27 233 → 14 913 / 12 665 ns. Four shallow small-row edits improve 213 → 137 ns with plain fused lookup; prefix reuse is slower at 165 ns. All reused variants allocate zero in these workloads. Select plain fused lookup as the default planner and retain prefix reuse as an explicit broad/deep option. Keep original functions as `keysLastPatchLocatePrevious` and `keysLastPatchPlanPrevious`; the earlier layout comparison benchmark continues to call the previous planner explicitly.

The empty-child refinement improves the indexed tree controls: server deep 70% linear/sorted/hash is 657 638 / 609 667 / 591 414 ns. Sparse small rows still lose with an index. The direct batch merge remains worth exploring before choosing a broad-edit API.

The hash key arena cuts preparation allocations but increases total allocated bytes and is a little slower. Do not select it from allocation count alone. An exact-capacity sizing pass is retained as the next simple control. Fixed-source results, including the slower arena, remain available. A separate fixture-isolation correction clones shared replay maps before tests add extra controls; benchmark processes were already isolated from those test mutations.


The one-shot `leafMerger` now sorts only integer indexes of borrowed input changes, combines duplicates with last-write-wins, rejects ancestor overlaps, and emits grouped changes directly alongside untouched opaque values. It retains no child-editor tree. Existing container layouts are preserved; the Fast hint applies only to newly built rows, because converting opaque old subtrees needs their model. Tests compare exact output with the recursive editor across all replay roots plus mixed-layout, empty-construction, explicit-zero and duplicate cases. A patch-first version uses the selected fused planner before falling back to this merge. The earlier tree remains required for interactive ordered ancestor operations.


### Correctness correction before broad batch measurements

The new all-replay differential test failed against the recursive editor for additional large shapes (including `group115-1` and `group114-0`). Diagnosis: a Dictionary edit uses its numeric metadata word as an offset into the key arena. `batchEditor.visit` forwarded that private offset as the callback's numeric key. The tree's child lookup expects the Dictionary path identity `(0, string)`, so an edited child after the first arena entry could be missed and its internal placeholder emitted instead. This is a reference-editor correctness bug, not a limitation of the one-shot merge.

Normalize the emitted numeric key to zero for Dictionary changes in all three retained back/front/adjacent adapters. Added `TestTreeDictionaryChildren` for three edited Dictionary child objects and untouched sibling leaves across all adapters and both new child indexes. `TestLeafMergeReplay` now checks exact output and every expected leaf against all replay roots. The four previously timed whole-row shapes had already passed their complete leaf checks, so the broader bug does not by itself invalidate those measured shape cases; historical source archives still contain the defect on affected additional shapes. Preserve the failed log and flag this scope explicitly. New timing uses corrected sources.


### Continuation step 5 — broad batches and selected simple writer/binder improvements

Both merge suites completed 236 cases; the continuation report retains times, bytes, allocations, source hashes and the native/Linux archive metadata difference (all member contents match). A direct leaf batch saves substantial cold editor allocation: server deep 70% tree / merge is 843 270 / 485 972 ns and 1 416 / 1 allocations. It is slower for some wide rows and lacks interactive ancestor-operation semantics, so retain an explicit `leafMerger.Append`, not an invisible editor replacement. Patch-first remains fastest when eligible; callers who already know rebuilding is required can skip the probe.

Select bulk directory reservation for `keysLastWriter.End`, preserving `appendDirectoryWriter` and the old append loop. Stored bytes remain identical. Typical reused gains are modest (~5% small/deep); the large simple-object cold gain comes from eliminating repeated output growth. Select binary search for the ordinary canonical path-binding helper and explicitly name the quadratic control `boundShapeEditsPrevious`. Keep sized-arena hash binding as an optional prepared-shape design: roughly equal complete preparation time but 38 versus 1 557 allocations in the deep case, and much faster repeated binding. It costs retained indexing memory and is not automatically appropriate for dynamic Dictionary/List shapes.

The last remaining read avenues are now implemented as separate controls: fused Dictionary search, direct canonical scalar-List addressing, and optional stored numeric-key base. All replay leaf values and key boundary controls pass before measurement. The key-base extension is a format experiment; it must not be read with current validators/editors. It remains a review candidate until complete integration is warranted by measurements. Final selection and validation are still pending.


### Continuation step 6 — remaining read gains and key-base review

Both read-followup suites completed 88 cases. Select fused Dictionary lookup and canonical scalar-List direct addressing in the public experimental view: server complete deep replay paths improve 260 → 254 ns, and direct dictionary controls improve about 5–9%. The latter already has validated contiguous word payloads, so neither change adds stored metadata. Transfer the redundant-decode removal to the patch locator too, preserving the prior fused version as an explicit benchmark control.

The stored-key-base pilot saves 38% read time (258 → 160 ns) and 888 B per row on the server clustered-global128 control; gapped keys improve 258 → 193 ns. Widely spread keys do not shrink and regress 258 → 270 ns. All 189 replay roots save only 652 B across 2 308 196 B, with 62 of 9 710 Objects choosing a base. Anonymized schema IDs were local, so this does not establish the prevalence of clustered global codes in production. The continuation report records both CPUs, complete sizes, the archive hashes and the extra writer-reservation confound on encoding time.

**Retain for user review:** the base is promising when preserving large global field identities matters. It remains a separate format pilot with writer/getter tests, not integrated into current validation/editors. Its temporary descriptor bit also overlaps the separate FixedSlot experiment. Do not hide the win or describe it as intrinsically unreasonable; integration and field-identity policy would be the next deliberate design choice. The selected format keeps small local codes and full-width fallback.

A final cold-allocation audit found repeated growth and duplicated values/pointers in patch scratch. Add controls for one reservation after the first eligible leaf and pointer-free eight-byte offset/size entries, using unchanged input edits for values. Test all replay plans and changed values, then measure complete copying/applying plus early/late fallback, cold and reused. Keep the earlier 32-byte-reference planner callable. No persisted change-set bytes change. Final checks and the consolidated review list follow this measurement.


### Continuation step 7 — pointer-free patch scratch and review ledger

Both scratch suites complete 96 cases. With the earlier fused locator held fixed, server deep cold 70% append / reserved / offset plans take 225 188 / 204 173 / 168 867 ns and allocate 119 392 / 40 960 / 9 472 B in 12 / 1 / 1 calls. Reused deep append / offsets is effectively tied at 160 689 / 161 571 ns. Small reused four-shallow costs 137 / 149 ns, while cold improves 408 / 210 ns. The eight-byte offset/size plan obtains values from the unchanged input batch by ordinal and contains no GC pointers. Prefer it for general resolved batches, keeping pointer-bearing references as the small hot-path comparison. Caller-local `[8]patchOffset` plus reused owned output is verified allocation-free.

`TestCurrentLeafBatch` demonstrates owned patching and a successful resize fallback to direct merge, with the borrowed base unchanged. The all-replay offset comparison uses changed values and shuffled 70% selections, with an explicit empty-leaf guard in the test helper. Neither plan is durable or reusable across row revisions. The patch planner remains a narrow resolved-leaf optimization, not an ordered ancestor replacement engine or a substitute for semantic presence handling.

**User review list:** the consolidated table in `results/performance-continuation-20261016.md#consolidated-review-list` records every faster-but-costlier option from this continuation and links the earlier ledger. Major entries: stored key base (38% clustered-global read gain), prepared path map, within-call parent cache, indexed child lookup, cold batch merge, pointer-bearing hot plans, prepared 16-/80-byte views, front headers, retained physical offsets, append staging and count-free tags. Every entry names the cost, workload/hardware, disposition and runnable evidence. None is silently discarded as “unmaintainable.”

The final combined checkpoint reruns selected reads, patching, batch construction, preparation and directory emission on both CPUs, then closes the audit with correctness, race, pointer and fuzz checks. The wider ordinary Fast/Immutable gap varies across paired cohorts (roughly 2–13% in the available global-key controls); preserve this uncertainty instead of reporting the best cohort as universal. Production integration and choosing the optional global-key/slot descriptor are future design decisions, not unrun measurements concealed by the closeout.


### Continuation step 8 — final paired validation and closeout

Implementation checkpoint `d6c6826`; both final source archives match every Go file in the working implementation and each other by member contents. Native archive SHA-256 `1c3792cb634efad7b9de4231bd033db62bd0196bbdfbf35dc2afc0a595eab80a`; Linux `f78c425bdb2cfa41b27d604abee694c082772ef9c91932789537585083997d1d`. Both `continuation-final-*` suites complete 426 cases with five 100 ms repetitions. All selected read controls allocate zero. All five correctness binaries pass on both hosts; uploaded Linux binary/source hashes are verified. The final report contains the full selected comparison and a completion audit.

Final server Fast / Immutable reads are 151 / 258 ns (local128), 266 / 263 ns (global128), and 173 / 209 ns (nested). Deep 70% copy/patch improves 319 097 → 158 114 ns versus the old planner; the previous fused locator is 175 191 ns, isolating the later Dictionary/List gain. Pointer-free offsets take 160 758 ns reused / 173 941 ns with fresh plan scratch. Cold deep 70% tree / merge is 854 488 / 488 792 ns. Cold construction is 1 070 485 / 585 297 ns, with 1 833 / 1 allocations. Writer/output storage is reused in the cold-editor comparisons. Do not mistake these for full database commit timings.

The combined run retains small losses too: bulk directory emission is 15 122 → 15 319 ns on the wide reused shape and 423 → 437 ns on span-heavy; deep improves 92 847 → 88 909 ns and simple cold objects allocate much less. The final small reused offset-plan case ties/slightly improves on references (140 / 142 ns), so the prior 12 ns loss is a cohort-specific cost, not a stable law. Prepared hash binding still needs its setup and memory; the final complete prepared hot operation is 145 206 ns versus binary 425 378 ns, while including shape/sized-arena preparation is 724 413 ns.

Validation: full and short tests, vet, race, checkptr, fresh bounded fuzz (325 241 executions), independent all-replay leaf verification, local stack scratch, both platform correctness suites, source/binary hash equality, documentation file links and whitespace checks. Detailed logs are retained. Every significant faster alternative has measured gains, costs, scope and runnable evidence in the consolidated ledger; no rejection is hidden behind a maintenance judgment.

This closes the active reasonable optimization goal for the experimental design. The evidence supports the selected balance, not a universal optimality claim. Public schema integration, WAL/change application, selected-slot/global-key descriptor composition, migrations and plugin/security integration remain deliberately outside this performance experiment. The retained pilots and API contracts make those future choices reviewable. No production data or production implementation was changed; commits remain local and unpushed.


### Performance options for user review — consolidated list

This table is also kept directly in STATE.md, as requested. It combines the retained comparison cohorts; the final combined checkpoint above reports the latest selected figures. These options are not silently removed because of complexity, memory, ownership or narrower API contracts. Source code and raw results remain rerunnable. The full evidence and qualifications are in [the continuation report](results/performance-continuation-20261016.md).

| Faster option | Measured server gain | Cost / current decision | Evidence |
| --- | --- | --- | --- |
| Stored numeric-key base | Clustered-global mmap read 258 → 160 ns; 2 053 → 1 165 B | Promising separate format pilot. Scattered keys regress; descriptor/FixedSlot allocation and validator/editor integration remain a design review choice. | `BaseKey`, `BaseKeyShape` |
| Prepared path map and dense values | Repeated deep bind + write 368 889 → about 145 650 ns versus binary binding | Explicit prepared option. Shape/key catalog must remain valid; sized arena preparation + bind + write is 690 373 ns and 437 768 B. | `ShapeBindIndex` |
| Within-call parent-prefix cache | Deep copy/patch 172 499 → 152 221 ns; wide 14 913 → 12 665 ns | Optional deep/broad planner. Small shallow case regresses 137 → 165 ns. No offsets retained across calls. | `PatchLookup` |
| Indexed changed-child lookup | Deep 70% interactive tree 657 638 → 591 414 ns with hash | Optional broad interactive control. Extra index state and worse small/cold cases; batch merge removes more cold allocation when its narrower contract applies. | `TreeChildIndex` |
| Direct leaf-batch merge | Cold deep 70% 843 270 → 485 972 ns, 1 416 → 1 allocations | Selected explicit batch API; sorted leaf changes only, no ordered ancestor operations. Wide shallow server updates can lose. | `LeafMerge` |
| Pointer-bearing patch references | Small reused batch 149 → 137 ns versus offset plan | Retained latency control. Four times the live plan width, GC pointers, more cold allocation; pointer-free offsets preferred for general batches. | `PatchReserve` |
| Prepared 16-byte view | Earlier eight-read workload 707 → 385 ns; 482 ns with preparation included | Optional repeated-read view; retains transaction-bound data and per-row state. Historical code uses the earlier directory layout. | `FastCursor`, `FinalFast` |
| Cached 80-byte header | Earlier attribution read 303 → 198 ns | More memory than the 16-byte option; retained, not selected. | `FastAttribution` |
| Front header | Earlier local read 229 → 180 ns | Loses nested server read 261 → 344 ns. Adjacent trailing keys selected as the balance. | `FrontShape`, `UnifiedRows` |
| Retained physical offsets and already-owned bytes | Earlier four-deep stores 1 885 → 20 ns | Excludes planning, allocation, copy and persistence; only valid for unchanged shape/revision and exclusive ownership. Never write a Bolt mapping. | `OwnedMutation` |
| Append-only staging / last-key shortcut | Earlier ordered construction 3 497 → 2 906 ns / 3 413 → 2 401 ns | Optional explicit policy. Interleaved reads or shuffled updates can regress; no guessed automatic threshold. | `BatchOverlay`, `BatchRefinements` |
| Count-free one-bit tags | 0.235% size reduction and some tiny-read wins | Simpler candidate preserved, but loses count/index operations and has mixed encode results. Literal lengths remain selected. | `OneBit`, `CompactMatched` |


## 2026-10-17 — direct tags and signed two’s complement

User requests and clarification (dictation whitespace normalized):

> For scalars with fixed known byte width, is there any reason to use zigzag and not simply two's complement of that byte size? I don't think we want zigzag signed-integer mapping...

> Okay, implement that in the main candidate: direct positive tags, two’s-complement signed payloads, no ZigZag.

> Basically, I want each special case and weird decision in our spec to earn its keep, because remember, simplicity is the goal too, not only performance.

> And keep code committed as you make changes.

The earlier explanation conflated negative immediate tags, ZigZag semantic mapping, shared List widths and omitting container counts. These are independent choices. The new main candidate uses immediate tags 0–127 as themselves; signed byte payloads use their minimum sign-preserving two’s-complement width and signed reads sign-extend. Fast cells retain all eight bytes. Unsigned payloads zero-extend. Removing ZigZag improves the positive signed immediate range from 0–63 to 0–127; minimum signed byte-width thresholds otherwise match ZigZag. Counts remain, to avoid bundling in another decision.

The interrupted patch made every negative a full-width word and left comparison tests inconsistent. It was not a completed implementation. Its exact diff is retained in `results/interrupted-direct-word-20261017.patch`; those edits have been removed from the live baseline. Existing `keysLast`, `length`, `oneBit`, and draft variants retain their prior semantics and benchmarks. The selected implementation will be a separate research adapter, so historical code remains directly runnable.

Core checkpoint: `currentValue` and `currentView` remain 16 bytes. Known signed construction carries an in-memory hint so deferred encoding can choose a sign-preserving width without allocating a temporary byte buffer. Opaque Fast words carry an eight-byte hint, needed to prevent truncating unknown signed +128 to byte 80. Compact payloads borrow their exact bytes and width. These hints are real static GC-visible pointers, not persisted type tags. Positive tags need no hint. Fast directory code and fused searches are reused unchanged.

Validation: signed boundary tests cover every byte width and both int64 extremes in Objects, Dictionaries and Lists, Fast and Compact; exact bytes check −1/−128/−129/+128, zero and immediate 64/127. Schema-free Fast-to-Compact copying preserves values. All 189 replay shapes plus generic fixtures validate with Compact, Fast and mixed layout policies. Reused signed construction allocates zero on the local host. Core check: `go test ./research/kvo/experiments -run 'TestCurrent(Signed|Scalar|Codec)' -count=1`. Editor integration follows before the final recommendation is declared complete.

Editor checkpoint: selected adapters retain the existing sorted/append/lazy/hash overlays, recursive editor, one-shot leaf merge and pointer-free physical patch algorithm. Signed/raw-width hints survive edit scratch without per-scalar allocation. Fast signed changes patch the ordinary eight-byte cell after copying to owned bytes; Compact edits rebuild. All 189 replay roots pass byte-identical no-op rewrites in Fast and Compact. Sparse tests cover two edited Dictionary children, List positions, sign/width transitions, presence zeros and unknown non-minimal signed widths. Small copy/patch uses local eight-entry scratch and zero allocations.

The new adapters also close two inherited gaps exposed by these tests: the simple editor now retains unknown present-zero entries on rewrite; sorted/tail overlays accept explicit zero/List positions rather than rejecting the presence flag. The older implementations remain unchanged controls. `go test ./research/kvo/... -count=1` passed after integration. No production code or backend data changed.

Logical change checkpoint: the selected change stream now reuses the same direct positive/byte-length tags and signed payloads. A single low bit alongside the path-suffix count distinguishes Delete from Set (including explicit Set-zero). Existing path encoding and prefix reuse remain. Payloads stream into one destination; `RecordSpan` records a directly built unknown-size replacement. The writer copies prior path steps into reusable scratch, so a producer can reuse its path array. Tests cover all signed boundaries, full-width numeric paths, string/index steps, root replacements, repeated paths, explicit zeros/deletes and opaque signed-width rewrites. Reused writer/output and reader scratch allocate zero; per-call local reader path scratch currently escapes (192 B for eight slots) and is measured explicitly, not described as stack-free.

Benchmark preparation: `BenchmarkCurrentScalars` isolates signed mapping against the old counted tag writer; `BenchmarkCurrentMmap` compares Fast against the preceding candidate and Immutable using 32 768 real Bolt mmap rows; `BenchmarkCurrentRows` covers encoding and complete few-shallow/few-deep/70%/empty construction operations on four representative replay rows, Fast and Compact. Scratch/output are reused and durable commit is excluded. All old benchmark implementations are unchanged.

Deterministic sizes: all 189 replay roots total 2 042 013 B with selected tags, matching the previous counted tag grammar, versus 2 035 755 B for literal lengths (+0.307%). Replay scalar words are synthetic unsigned widths, so this is not evidence about actual signed prevalence. For controlled Lists, signed 0–127 is 132 B versus ZigZag’s 196 B; −1 through −128 is 260 B versus 196 B; the signed boundary fixture is 267 B versus 269 B. These symmetric small-number trade-offs are explicit; no claim of a universal size reduction. Server access succeeded after an IPv4 knock/SSH retry; no application state was touched.

Access follow-up: the first paired run at `9f48ba3` completes 97 cases × five 100 ms repetitions on each host. The generic current value wrapper adds a non-inlined call around the existing Fast kernel; retained arm64 disassembly confirms that call. On the server the initial current/preceding Fast reads are 163/153 ns local, 294/258 ns global and 220/174 ns nested. This is an implementation regression, not a reason to add a new wire rule. Native row timings overlapped the subsequent local access-validation work and are retained as exploratory results only; the final cohort will be isolated.

The selected reader now fuses the same Fast kernels and Compact dispatch into one call. `GetWrapped`, `GetStringWrapped` and `AtWrapped` preserve the first implementation as controls. No stored bytes or scalar rules change. A dedicated equivalence test checks exact values and borrowed boundaries through every path in all 189 replay shapes with Fast, Compact and mixed policies. Full follow-up timing and verification are pending.

Scalar-access checkpoint: compiler diagnostics identified a second avoidable call in `Uint` (cost 148 versus Go's inline budget 80). Merely outlining payload decoding left costs 88/89 for Uint/Int, still above budget. The selected 16-byte value now uses nil for ordinary opaque Fast words and the two real marker pointers only for typed signed/unsigned construction. This supersedes the earlier marker assignment, not the wire format: unknown words still retain eight bytes, and Compact payloads retain their exact widths. Uint/Int now inline at costs 69/70. `UintBeforeInlining` and the `value-call` mmap mode retain the larger accessor for paired comparison; `wrapped` independently retains the extra lookup call. The previous exact implementations remain in source archives.

All selected tests passed after the new hint assignment, including signed boundaries, unknown Fast-to-Compact copies, opaque replay rewrites, deferred edits and zero-allocation checks. The benchmark's synthetic unsigned edit inputs now explicitly use `currentWord`, rather than an opaque-read conversion, so construction measures typed writes. A new complete paired timing run is needed because constructor work also changed; the prior 7176723 fused run is retained as an intermediate result.

Intermediate evidence checkpoint: the initial 9f48ba3 run (97 cases × five repetitions) and fused 7176723 run (100 × five) are preserved for both arm64 and amd64 with all five passing correctness logs, raw timings, machine environments and source/binary manifests. They live in `results/signed-initial-*` and `results/signed-fused-*`, with exact source archives beside them. Separate full/short/vet/race/checkptr and bounded fuzz logs are retained too. The initial native row timings remain explicitly exploratory due to overlapping local verification. These intermediate measurements are not overwritten by the final inline-accessor run.

The complete 09ee9a8 inline-accessor run passes 103 cases × five on both hosts, with zero allocations except deliberately fresh change-reader scratch. Server selected/preceding/Immutable reads are 154/152/234 ns local, 269/264/260 ns global and 175/175/208 ns nested. However, small Fast-row sparse/broad mutations remain 28–33% slower, and some larger writes also regress. Those losses are retained in `results/signed-inline-*`, not concealed by the read improvement.

A final implementation-only probe removes the kind check from `IsZero`: empty spans already normalize to a nil/zero value, so `num == 0` is sufficient for every constructible value. Byte payloads containing zero still have nonzero length and remain opaque. This removes pointer-marker comparisons from every zero test without changing any encoding rule. Test and paired timing results will determine its effect; the earlier complete implementation is archived.

The zero-check probe passes all selected tests. Five 200 ms native repetitions show only small changes: small Fast encode 1 265 → 1 245 ns, four shallow edits 55 → 54 ns, deeper edits 619 → 608 ns, 70% edits 1 986 → 1 980 ns. It simplifies a redundant predicate but does not explain the material mutation regression. The raw before/after probe is retained. Next inspect the extra iterator wrapper used by rebuilds, analogous to the already-measured read-wrapper cost; preserve its callable implementation too.

Iterator attribution: fusing only the per-entry Fast dispatch barely helps the native small-row probe (deep 608 → 604 ns, 70% 1 980 → 1 950 ns). Removing unused old-codec state reduces the returned iterator from 280 to 208 bytes and lowers those operations to 526 and 1 674 ns; the preceding candidate is about 519 and 1 687 ns in the paired control. Construction improves 1 689 → 1 630 ns. This identifies returned/initialized state as the consequential part, rather than another wire-format case. The simpler zero predicate remains, without claiming it caused this gain.

The selected iterator directly stores the Fast header and index plus the Compact cursor. `IterWrapped().Next()` and `NextFused()` retain both larger controls, and `CurrentIteration` benchmarks each with no per-entry mode branch. Semantic replay checks compare all three through every nested container, including exact borrowed boundaries. Both hosts will remeasure affected writers/mutations/scalar encoding and these new iteration controls. The final 09ee9a8 mmap read and change-reader results remain the evidence for those unchanged paths; no need to repeat their expensive dataset setup for iterator-only work.

The 659b8fe paired writer/iterator suite passes 110 cases × five repetitions with zero allocations in every case, and all five correctness binaries pass on each host. Server small Fast deep edits are now 1 788 ns versus the previous 1 742 ns, and 70% updates 5 545 versus 5 292 ns; deep 70% rows tie at about 162 000 ns. The earlier 28–33% small-row rebuild regression is largely removed. Some encoding/construction cases still lose. Root iteration on the small Fast row measures 129 ns with the wrapper, 118 ns fused-large, and 93 ns selected.

The final writer probe delays `scalarWidth()` until a non-immediate Compact scalar actually needs it. Previously it also ran for Fast cells, spans and immediate tags, then its answer was discarded. This is a control-flow cleanup, not a new format rule. The complete preceding results/source snapshots are retained under `signed-slim-*`; the final measurement will rerun affected row writes/scalar encoding. Unchanged reads and iteration retain their complete paired checkpoints.

The delayed-width probe passes all selected tests. The initial benchmark filter omitted the codec path component and matched no cases; it was corrected before interpreting results, and the retained files contain six cases × five repetitions. On arm64, Fast encoding is effectively unchanged: small 1 244 → 1 252 ns, deep 34 863 → 34 990 ns, span-heavy 190 → 192 ns. Keep the simpler computation placement without claiming a speed win; both original and changed code remain reproducible. The last paired write run checks the server and the full affected mutation matrix. No further format or accessor changes are planned in this step.

### 2026-10-17 completion

The selected main candidate now implements direct positive tags, minimal sign-preserving two’s-complement signed payloads and no ZigZag, including construction, generic reads, edits, physical patches and logical changes. The README specifies its formats, in-memory state, actual API sketches and reasons for the retained exceptions. All preceding formats and intermediate adapters remain reproducible. The simplicity criterion is prominent at the start of this file.

Final writer checkpoint `ebd5da7` passes 86 cases × five repetitions on each host, all with zero allocations under reuse. Both hosts pass all five correctness binaries. The unchanged mmap-read/change-reader paths use the complete `09ee9a8` 103-case run; iterator controls use the complete `659b8fe` 110-case run. These are explicitly different checkpoints, not a fabricated single final timing cohort. Both final source archives match all 213 Go files in the working implementation and one another. The report preserves exact hashes, timings, byte counts, environments and source archives. An expired SSH control connection was reopened with the documented immediate IPv4 knock/SSH sequence; no application state was modified.

Selected/preceding server Fast reads are 154/152 ns local, 269/264 ns global and 175/175 ns nested. The extra read calls were the main new regression; inlining removed them. During mutation, returned iterator state mattered more than the extra per-entry call: reducing 280 → 208 bytes recovered most of the small-row rebuild regression. Final server small Fast deep edits are 1 757 versus 1 711 ns, 70% edits 5 487 versus 5 263 ns, and deep 70% rows 160 362 versus 159 393 ns. Some losses remain: small construction 4 991 versus 4 629 ns; wide 70% edits 16 806 versus 15 723 ns in this cohort; tiny span-heavy Fast updates pay 31–42 ns extra. These are recorded, not declared inevitable or hidden behind a complexity judgment. No additional wire special case was introduced to chase them.

The width-computation cleanup remains chiefly a clarity change; the native focused probe did not show a meaningful speed win. The final complete matrix, including slower cells, is in `results/signed-tags-20261017.md`. Signed positive List 0–127 uses 132 B versus the old counted ZigZag form’s 196 B; −1 through −128 uses 260 versus 196 B. All 189 unsigned replay roots total 2 042 013 B versus literal lengths’ 2 035 755 B (+0.307%). No signed-prevalence inference is made from that unsigned replay.

Validation includes full/short tests, vet, selected-candidate race/checkptr, signed/malformed/change fuzzing at the relevant checkpoints, all-replay iterator/value equivalence, unknown-width and present-zero preservation, local scratch/allocation checks, both platform correctness suites, exact source equality and documentation checks. The final writer cleanup reruns signed boundaries, all replay encodes and allocation checks under race/checkptr, plus fresh signed fuzzing. Production schema integration, migrations, WAL application, selected-slot composition and plugin/security integration remain the previously declared next phase. This requested scalar/tag change is complete; all work remains local WIP commits and nothing is pushed.

Final packaging normalizes trailing padding in the new slim/final text logs and removes a blank line at the report’s end. No whitespace exception is added. Numeric repetitions and source/binary manifests are unchanged.

## 2026-10-17 native struct mapping request

> Let's also implement struct mapping via edb:"prop_name" tag. The goal of struct mapping is that, of course, native both structs provide the fastest access from native code for properties that it knows about; if we need to access literally every property, I assume it is fastest to decode into a struct first, and then use native Go code to access it. If that assumption is incorrect, LET ME KNOW, but we still want to implement native struct encoding because it might be preferred for API convenience.
>
> So there's gonna be basically two API layers:
>
> 1. Key-based access. With possibly a generated wrapper sublayer that provides statically typed interface to specific schema and delegates to key-based accessors inside.
> 2. Native types encoding and decoding -- structs, maps, slices, values, something for representing optionals.
>
> edb:"-" is a native way to exclude struct props from en/decoding, e.g. see msgpack:"-" props on PointSource rn.
>
> edb:",compact" uses compact representation, etc.
>
> There is no default key; lack of edb annotation on a field panics, so that it's impossible to forget to add one. For now struct keys must be a subset of keys defined via normal KVO schema define prop calls; if no prop matches edb key, that's a panic too.
>
> Now, when you declare new kvo type, you will also be passing which structs, if any, can be used with it. Multiple structs are allowed, because you might want to decode different views of the same row. We want edb-like methods, like Get, Put; we want allocating Get for one-offs and zero-allocation Get as the main path (can call it GetFast). I also want Patch which does Put but only overwrites the props that struct has declared, leaving other ones intact.
>
> Keep building the bulk of this on the side, but any changes to KVO schemas that you need to introduce should go here. Commit as you go ofc.

Initial inspection: clean `main` at `cf30bd7`; the current model already provides property-name lookup and per-entity membership. Existing entity declarations have no native registration, Lists or general byte type; scalar constructors currently discard their Go type parameter. The backend example explicitly excludes derived/cache fields. Its performance model identifies row decoding and garbage collection as routine costs. No backend files or data are being changed.

### Native schema checkpoint

Added optional `reflect.Type` registrations to `NewEntityType`; existing calls remain valid. `NewModel` validates explicit tags against model membership and publishes immutable-copy `NativeStruct` metadata. Missing/blank tags, duplicate mappings, unknown names/options, unexported mapped fields and unregistered projections panic with `ErrNativeMapping`. Added `ListOf` (the name `List` already denotes the legacy view), `TBytes`, retained scalar native types and model flags `Compact` / `PresenceIsSignificant`. `MachineStringType` remains a source-compatible alias of the more accurate `ScalarDataType`. These are descriptions, not new legacy codec implementations.

Patch contract: replace each declared property as a complete value, leaving unrepresented siblings untouched. There is no implicit recursive merge of declared maps or child structs. Zero clears an ordinary property; an absent optional clears a presence-significant property. Key-based paths remain the way to make a narrower deep edit. The binder will operate on trusted row views; external framing validation remains a separate returned-error boundary.

Schema-focused tests pass, including invalid declarations and immutable metadata copies. No production storage path is changed.

### Native codec checkpoint

Implemented `bindNative[T]` and Get/GetFast/Put/Patch in the experimental package. Binding checks registered nested projections and scalar families once. Put streams payloads into the selected writer; Patch merges the sorted declared property list with the base iterator and preserves opaque unrepresented siblings, including present zeros. Added selected-codec EndPresent for empty significant children. Optional[T] supplies explicit presence without another wire tag. Unknown child fields are dropped only when that entire declared child is replaced, as specified.

Get returns owned data; GetFast borrows immutable strings (including map keys), copies writable binary data, and reuses caller collections. Excluded fields remain untouched. The time round-trip test exposed the existing KVO helper's local-time location; the native layer normalizes to UTC while retaining the existing microsecond word semantics. Nil/empty collection identity, original timezone and monotonic metadata are not persisted.

Asked whether native maps should preserve zero-valued entries. With no answer yet, proceeding with the stated recommended assumption: preserve map membership, continue zero elision for ordinary struct properties. This is needed for faithful native Go map semantics, not an extra wire case. Revisit if the user chooses ordinary zero elision instead.

Fixed a scratch design issue before measuring: a single per-depth map frame would recreate typed scratch whenever sibling maps had different types. The workspace now reuses frames by (depth, map type), avoiding recurring allocations without mutating compiled plans. Nested decoding cannot overwrite parent map scratch. Temporary map keys/values are cleared after use to avoid retaining borrowed transaction memory.

Focused tests pass for Fast/Compact round trips, signed boundaries, nested Lists/maps, significant empty children, tag layout inheritance, stale-field reset, ignored fields, projection Patch, unknown preservation, ownership and type errors. Reused GetFast, Put and Patch each pass zero-allocation assertions on the mixed fixture (nested map aggregate values are measured separately). Mutable map values with nested storage still require independent decode allocations; do not claim universal zero allocation. Fuzz and cross-platform timing follow.

### Native projection and correctness follow-up

Added repeatable mmap benchmarks for all-field access once/four/sixteen times, owned/reused native decode, a manual generated-decoder control, late-field projections, mixed collections, fresh/reused construction and two-property Patch versus key edits. Scalar widths and string lengths are drawn from the retained anonymous small/wide replay shapes; the 24-field native schema and repetition counts are explicit scenarios, not an observed access trace.

Initial arm64 probe (three repetitions, 100 ms) contradicts the broad decode-first assumption: small Fast all-field iteration is about 92 ns versus reflection GetFast+one native pass at 225 ns; four iterations cost 378 ns versus one decode+four native passes at 233 ns. The manual generated decoder is about 120 ns for one pass. Decoding earns its cost for repeated access/convenience, not necessarily one scan. Raw exploratory probes are retained; final paired results follow.

The initial two-late-field projection scanned 24 fields in about 106 ns while direct key reads cost 10 ns. Added a read policy using key lookups for sparse Fast projections (<= stored count/4), retaining explicit scan and lookup controls. The follow-up native projection is about 26 ns versus 103 ns for scan, including reflection stores and policy dispatch. Full/Compact reads still scan once. The threshold is a conservative replaceable heuristic, not a new encoding rule or proof of a universal optimum.

Expanded tests to named scalars/text/bytes, numeric map keys, maps of slices/pointers, arrays, optional strings/Lists, narrowing failures and recursive models. Found and fixed two concrete issues: EndPresent must retain a Fast Dictionary's key prefix when rolling an empty child back to a zero cell; the old length-header search assumes the old key/catalog ordering, so the selected layout must use keysLastSafeFind for presence lookups. Nil pointer map entries now retain membership. Pointer-to-zero and nil still share the ordinary zero meaning. Optional-pointer nesting is either explicitly represented or rejected during plan compilation rather than causing a reflect.Field panic. Recursive data cycles are bounded by the existing depth limit.

### Native first paired checkpoint

Both arm64 and Linux amd64 completed 110 cases with five 100 ms repetitions and all five correctness binaries passing, from code checkpoint `5c96236`. Exact source archives, hashes, environments, raw runs and summaries are retained under `native-initial-*`. Full/short tests and vet pass. Native tests pass under race. checkptr instrumentation adds reflection allocations (15 in the allocation assertion), so semantic cases were rerun under checkptr separately and passed; ordinary builds retain the strict zero-allocation checks. Bounded fuzzing passed 229 912 inputs.

The mixed scalar/collection reuse paths allocate zero. The aggregate map workload (32 slices plus 32 pointers) still allocates 64 child objects, 1 792 B, per read on arm64. This motivates one further measured implementation change: snapshot reusable map values by key before clearing/rebuilding membership. Keep the initial rebuild decoder runnable as a control. Do not claim that fresh or growing native data can avoid allocation. Existing borrowed keys must remain valid while a reusable destination is read; transaction ownership is part of this API contract.

### Native nested-map reuse follow-up

Implemented independent child-storage reuse for matching map keys. The decoder snapshots old addressable values before clearing membership, then decodes into each matching value's slices/maps/pointers. Scratch is cleared after decoding and never donates a previous destination's mutable storage to a different destination. Insertions/deletions, distinct destinations, pointer/slice independence and zero-allocation steady-state reuse are tested.

The first sorted snapshot removes all 64 allocations in the aggregate map case but costs about 8 623 ns versus rebuild's 6 420 ns on arm64. A reusable key index reduces this to about 7 262 ns (still 0 B / 0 allocs), versus sorted's 8 720 and rebuild's 6 395 ns in the paired probe. Keep all three runnable (`GetFast`, `GetFastSortedMaps`, `GetFastRebuild`). The default follows the requested allocation-conscious GetFast contract; the CPU/GC trade-off is explicit and will be checked on the server. This is per-codec scratch, not per-row pointers or a wire-format extension.

Ownership constraint: old borrowed string keys must remain valid through reuse because matching reads them. Reuse within one read transaction, or clear borrowed-key maps before ending it. Growing or newly inserted nested values can allocate; a fresh destination never reuses another destination's storage. The zero-allocation claim is conditional on stable keys/capacity, not universal. No unsafe map-runtime hooks or generated production bindings were introduced.

### Native mapping completion

Completed the requested two-layer native mapping experiment. Schema registration/tag metadata lives in `kvo`; native Get/GetFast/Put/Patch, Optional and reusable binding/encoding remain alongside the candidate codec. The README and `results/native-mapping-20261017.md` specify actual APIs, ownership, whole-property Patch semantics, explicit primary-key exclusion and limitations. No binary-format change, production table integration or dependency on MessagePack annotations was introduced.

Final code checkpoint `73d3a73` passes 112 cases × five repetitions on each host, all five correctness binaries, full/short tests, vet, native race and semantic checkptr tests. Final bounded fuzzing completes 101 737 value cases and 29 816 map-reuse cases. Both source archives match all 220 Go files in the working implementation and one another. The initial audit encountered macOS AppleDouble `._` metadata entries; the corrected audit excludes those non-source entries and records that rule explicitly. Source hashes and raw results are preserved under `native-final-*`.

On Linux, one flat all-field traversal is 252 ns versus 612 ns for GetFast + one native pass. Four traversals are 995 ns versus 628 ns after one decode. A sparse two-field native projection uses 76 ns versus 288 ns scanning. Wide reused native Put is 13 918 ns; replacing two native properties via Patch is 623 ns and does not decode unrepresented children. Warm reusable paths allocate zero; fresh/owned results retain explicit allocation measurements.

Nested-map key-index reuse takes 21 293 ns / 0 allocations on Linux, versus 19 328 ns / 64 allocations / 1 792 B for rebuild; sorted snapshots cost 25 694 ns. This is an explicit CPU/GC trade-off, not a universal speed win. A generated flat decoder control uses 293 ns versus reflection's 612 ns for decode plus one read. Both faster options remain on the review list and runnable; no production generator or unsafe map-runtime hook was added. The first/final server wide decode cohorts also differ (9 453 → 10 181 ns, arm64 3 275 → 3 252 ns); no isolated cause is claimed, and both full cohorts remain visible.

The unresolved preference remains visible: native maps preserve zero-valued membership under the stated assumption while ordinary object properties elide zeros. GetFast strings/keys borrow their input lifetime; matching old map keys requires them to remain valid through reuse. Stable keys/capacity can avoid allocations, while new/growing storage can allocate. Get owns data for one-offs. Source/data identity, durable WAL/transactions, migrations, catalogs and generated native bindings remain subsequent production work. All requested work for this native mapping step is complete, committed locally, and not pushed.


## Follow-up: schema-owned dictionary zero policy (2026-10-17)

User decision, preserved with normalized whitespace:

> Well this is not about Go maps. The key will be declared KVO style, and we should have consistent handling whether native struct is used or key accessors are used. So the question is how our system handles maps. I believe conservative choice for dictionaries is to omit zero entries. But because key identity could have a meaning of its own, provide a prop option like KeepDictionaryZeroValues that can be applied where necessary. That option would also change generated wrapper API to return (value, isPresent) on get.

This supersedes the unanswered-question assumption recorded in the native mapping completion above. Add the flag to real model declarations and native metadata. Validate it for Map properties, independent of whether a native struct is registered. Both string and numeric data-key dictionaries follow the same rule. An entity Object uses its individual property declarations instead. No new stored bits are needed: the existing explicit zero representation suffices.

The option controls entries immediately inside that property's dictionary. It does not make the property itself optional and does not propagate into deeper dictionary values. A nested modeled property can declare its own policy. A native plan shared by differently flagged fields must not leak one property's policy into another.

Schema checkpoint: declaration tests cover default, keep-entry, property-presence and both flags; invalid scalar, List and entity Object declarations panic with `kvo.ErrPropOption`. `go test ./kvo` passes. Codec/accessor adaptation and its verification follow in the next checkpoint.

Codec/accessor checkpoint: native map plans default to zero elision. A retaining property receives a small immutable binding copy, including its pointer/optional wrapper if present; shared type plans remain unchanged. Both native decode paths filter stored zero entries under an ordinary policy, including reused child-map storage. Lists still preserve every position. Keeping dictionary zeros is independent of retaining the property itself, and does not propagate into nested dictionary values.

The selected schema-aware dictionary adapter now shares the actual flag for writer emission, Set/Delete, reads, presence-aware lookup and enumeration. Handwritten generated-wrapper equivalents demonstrate `Get(key) value` versus `Get(key) (value, isPresent)`. No generator is claimed. Presence lookup uses the existing Fast directory or Compact traversal; the ordinary Fast Get implementation is untouched. Native optional-property lookup reuses the same structural presence helper. A generic editor previously emitted all edits through ordinary Put; it now emits live edits with PutPresent, because ordinary zero assignments have already become tombstones during staging. This preserves explicitly retained zero edits without changing ordinary assignments. Raw rewrites still preserve unknown zeros; a schema-bound ordinary dictionary rewrite omits its known zero entries.

Existing historical map-membership fixtures explicitly opt in to retention, preserving their data/benchmark contract. New tests cover both policies on shared map types, string/numeric keys, empty keys, all-zero dictionaries, zero/empty scalar and child kinds, negative-zero floats, Lists containing zero positions, independent property presence, pointer/optional binding wrappers, non-inheritance, native/key byte agreement, edits, projection Patch and reads of explicitly stored zeros. Focused tests and the full suite pass. Paired timing, allocation checks and bounded fuzz validation remain queued.

Verification checkpoint: full and short suites, vet, focused native/dictionary race tests, all new zero-kind checks and warmed allocation assertions pass. A bounded DictionaryPolicy fuzz campaign completed 368 411 executions (10-second budget, two workers), with no failures. Added 32 mmap-backed dictionary-policy benchmark cases covering native reused reads/writes, key lookup and zero/delete materialization, retaining the original 112 native controls. The injected zero fraction is a sensitivity fixture, not a measured application frequency. Source and paired runs are queued next.

Preparation review caught one missing creation path: an absent dictionary has no physical string/numeric kind. Added schema-bound Edit initialization, so the first Set can grow an absent dictionary without guessing its kind or panicking. Tests cover zero/nonzero insertion from absence under both policies and layouts. Pointer-safety validation passed separately from allocation assertions; instrumentation-induced allocations are not codec regressions. Both portable bundles built successfully; final measurements will rebuild after this creation-path fix.

The first paired policy cohort (24ab3b8) completed 144 cases × five repetitions on each CPU, with all five correctness binaries passing. It exposed a reader implementation gap: on arm64 the no-zero Fast dictionary's ordinary key getter took 20 ns, while the presence getter took 61 ns. The latter used generic header/offset helpers rather than the already selected typed-offset dictionary search. This is not an inherent cost of returning presence or preserving zero entries.

Reuse that existing search for presence-aware Fast Dictionaries; keep the general-directory implementation as an explicitly named benchmark control. Ordinary Get remains unchanged. Added parity tests for absent/present-zero/nonzero values, scalar/span/mixed catalogs, short/long offsets and arbitrary string bytes. Focused tests and vet pass. Retain the complete first paired cohort, and measure the same suite plus two directory controls after this implementation-only change.

Final dictionary-policy completion: checkpoint 6743de6 passes all five correctness binaries on each CPU and completes 146 benchmark cases × five repetitions per host. Short/race/checkptr validation and allocation assertions pass; the preceding native policy fuzz campaign completed 368 411 executions. Both final archives and the working Go sources match (223 files). The initial 24ab3b8 cohort (144 cases × five repetitions) remains intact with its own exact sources and validation.

The new presence-aware Fast dictionary lookup now takes 22 ns on arm64 / 75 ns on amd64, versus ordinary lookup's 22 ns / 73 ns. The retained general-directory helper takes 57 ns / 165 ns. These are one warm 32-entry fixture, not universal latency claims. All 34 policy cases allocate zero after warming. On the no-zero fixture both policies use 787 B Fast / 713 B Compact; with eight explicit zeros, omission uses 595 B / 537 B while retention uses 787 B / 657 B. The difference buys retained key identity; the flag itself adds no stored bits.

The 112 common native controls have essentially unchanged median final/initial timing ratios; individual fluctuations are not all attributed, and no blanket improvement claim is made. The [dictionary policy report](results/dictionary-policy-20261017.md) records scope, tables, source hashes, complete raw comparisons and reproduction. README specifies the shared policy and the generated getter contract; executable handwritten wrappers demonstrate it, while generation itself remains future work. This user decision is implemented and validated, committed locally without pushing.


## Source-only WIP preparation (2026-10-28)

User request, with normalized whitespace:

> Have squashed all your work so far into a single commit. Now, here is what I want us to do.
>
> I want you to gitignore the results because all the data we have collected, while it is definitely valuable and we'll continue using it, I do not want it in a git repository. But I do want you to keep the experiments we did so far, the code. And I want to commit and push this out as a work in progress. And I might actually do it on the branch now that I think about it. But anyway, let's prepare a commit without artifacts so that I can at least safely do it. For this, you need to basically unpack the commit that exists and adjust gitignores and make a proper commit here.

The user asked which fixture was involved. Explained that `workload-shapes.json.gz` is about 128 KiB and contains 189 anonymized row shapes in 75 groups, with lengths, widths, key spacing and nesting from which tests reconstruct dummy values. It is benchmark input rather than output. Asked whether it should stay tracked with the code or be local too. User decision:

> Keep the fixture only locally too

Starting state: clean `main`, one squashed commit `08ca87a` above `origin/main` (`7f9faed`). Inventory found 1 561 result files totaling 64 165 871 B and the 131 500 B replay fixture. A SHA256 preservation manifest was saved outside the committed tree, under `.git/`, for all 1 562 artifact files and the 122 research code/tool files. There are no direct Go/shell/Python source files under results; historical source bundles remain available locally.

The optional fixture loader embeds a directory containing a tracked README, so compilation succeeds with or without the ignored gzip input. Missing input skips dependent tests/benchmark families with an explicit message; corrupt input still fails. Test context is threaded through fixture helpers only, preserving comparison implementations and workload generation. The two derived fixture caches check input availability before their once-only initialization, so one skipped call cannot leave a partially initialized cache for later tests. Local portable builds explicitly include the ignored replay in their source snapshot and record whether it was embedded. This changes artifact handling, not the codec or measured algorithms.

Validation with the existing local replay: the experiment suite passes. Final source-only checkout, bundle verification and preservation audit are recorded below when complete.

Final source-only validation: full and short suites pass both in the working checkout with the local replay and in an exported index tree containing no results or replay data. Vet passes on that source-only tree. Missing-input checks explicitly skip the replay/derived-row/derived-container tests and sample-dependent benchmark cases, while native round-trip tests still execute and pass. Portable bundles build in both modes and their experiment binaries work outside the source directory. Each source archive contains the tracked fixture README and includes the exact ignored replay only when that binary embedded it. A deliberately corrupt replay in the disposable validation tree fails with a gzip error instead of being silently skipped.

Preservation audit confirms all 1 562 original artifact files (64 297 371 B) remain byte-identical on disk, all 122 original research code/tool paths remain tracked, and none of the collected artifact paths remain in the index. Validation logs and the audit are themselves local under ignored `results/source-only-20261028/`. The replacement commit is based directly on `origin/main` at `7f9faed`; this is a rewritten single WIP commit, not a follow-up deletion commit that would still publish artifacts in its parent. No push or branch move was performed.
