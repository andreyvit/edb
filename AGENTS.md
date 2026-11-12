# Repository guide

EDB is an in-process Go database: bbolt, concurrent reads, serialized writes.
Keep business logic in ordinary loops; keep storage contracts explicit.

## Layout

- Root `edb`: database, tables, schemas, indexes, transactions and row envelopes.
- `kvo`: models, typed properties, records, collections, editors and Go bindings.
- `values` / `nativebind`: shared runtime values and native field operations.
- `kvo/codec/row2`: Fast2/Compact2. `fast1` and `msgpack`: supported legacy codecs.
- `evm`: compiler/runtime. `gox`: Go/markup templates.
- `research`: experiments and benchmark controls, not application APIs.
- `mmap`: platform memory-map helpers.

Keep tests beside code. Keep package entry docs in `README.md`; other Markdown
belongs in root `_docs/` or `<component>/_docs/`. Use descriptive filenames.
Docs state current contracts; dates belong on measurements and audit evidence.

## Tests

```sh
go test ./...                    # bbolt
go test -short ./...             # in-memory test backend
go test -run TestName ./...
go test -coverprofile=cover.out ./...
go tool cover -html=cover.out -o cover.html
```

Run both full and short suites after scan or storage changes. `setup()` in
`db_test.go` selects the backend. If cache permissions block coverage, prefix
Go commands with `env GOCACHE="$PWD/.gocache"`.

`modd` reruns tests on edits (`go install github.com/cortesi/modd/cmd/modd@latest`).
Rebuild book examples with `go run ./evm/EVMBook-rebuild.go`.

## Storage

- Every operation needs a transaction: `Tx`, `Read` or `Write`.
- `storage.go` defines storage/transaction/bucket/cursor interfaces;
  `storage_bolt.go` and `storage_mem.go` implement them.
- `schema*.go` defines registration; `op*.go` implements operations.
- Keys use `enctuple.go`; `encvalue.go` and `encoding.go` handle row envelopes.
  Ordinary rows default to MessagePack; explicit storage selects Fast2/Compact2.
- Table buckets: `tablename/data`, `tablename/i_<indexname>`.
- KV buckets: `kvtname/`, `kvtname_i_<indexname>/`.
- `DB.Bolt()` and `Tx.BoltTx()` expose bbolt; both return nil for memory storage.
- KV scans use `RawRange`: prefix, lower/upper bounds, inclusion and direction.

## Code

Run `gofmt`. Use idiomatic Go names and existing `schema*`, `op*`, `enc*` groupings.
Use named enum types/constants for modes, states and kinds, preferably with
`String()`. Preserve numeric wire values when replacing discriminators.
