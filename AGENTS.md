# Repository Guidelines

EDB is an in-process document database for Go using Bolt (bbolt) as the backend. It enables single-server deployments by providing fast in-memory data access with serialized writes. The philosophy is that looping over data in code is simpler than optimizing complex SQL queries.

## Project Structure

- Root package `edb`: core database, schema, encoding (`*.go` in repo root).
- `kvo/`: key-value-objects layer used by KV tables.
- `mmap/`: cross-platform memory-mapped file helpers.
- Tests live alongside code as `*_test.go`.

## Build, Test, and Development Commands

- `go test ./...`: run the full suite (uses Bolt/bbolt-backed storage by default).
- `go test -short ./...`: run faster tests using the in-memory backend (see `setup()` in `db_test.go`).
- `go test -run TestName ./...`: run a single test.
- `go test -coverprofile=cover.out ./...`: write coverage profile (`go tool cover -html=cover.out -o cover.html`).
- `modd`: auto-run tests on file changes (install: `go install github.com/cortesi/modd/cmd/modd@latest`).

## Storage & Testing Notes

- Storage is abstracted behind `storage`/`storageTx`/`storageBucket`/`storageCursor` in `storage.go`.
- `storage_bolt.go` is the production Bolt adapter; `storage_mem.go` is a simple in-memory backend intended for tests.
- Test helper `setup()` runs Bolt by default; pass `-short` to switch to the in-memory backend.
- When changing `storage_mem.go` or scan semantics, run both:
  - `go test -short ./...` (exercise in-memory backend)
  - `go test ./...` (exercise Bolt backend)

## Architecture Notes

- All operations require a transaction: `db.Tx(writable, func(*Tx) error)`, `db.Read(func(*Tx))`, or `db.Write(func(*Tx))`.
- Schema layer: `schema.go`, `schematable.go`, `schemaindex.go` (`Schema.Include` composes schemas; `Table` is a collection; `Index` is a secondary index).
- Operations live in `op*.go`:
  - CRUD: `Put/Get/Delete`
  - Scans: `TableScan/IndexScan` + helpers `FullScan/ExactScan/RangeScan/LowerBoundScan/UpperBoundScan`
  - KV scans use `RawRange` in `scan.go` (`Prefix`/`Lower`/`Upper`, inclusive/exclusive, `Reverse`)
- Encoding: keys are tuple-encoded (`enctuple.go`); values are `value header + msgpack row data + index keys` (`encvalue.go` + `encoding.go`).

## Bolt Bucket Layout (Implementation Detail)

- Table row data: `tablename/data`
- Table index data: `tablename/i_<indexname>`
- KV table data: `kvtname/` (root bucket)
- KV index data: `kvtname_i_<indexname>/` (root bucket)

## Escape Hatches

- `db.Bolt()` returns the underlying `*bbolt.DB` (nil for in-memory backend).
- `tx.BoltTx()` returns the underlying `*bbolt.Tx` (nil for in-memory backend).

## Coverage in Sandbox

- If coverage or `go tool cover` fails due to Go build cache permissions, run with a repo-local cache:
  - `env GOCACHE="$PWD/.gocache" go test -coverprofile=cover.out ./...`
  - `env GOCACHE="$PWD/.gocache" go tool cover -func=cover.out`

## Coding Style

- Run `gofmt`; follow idiomatic Go naming (`CamelCase` exported, `camelCase` unexported).
- Prefer extending existing file groupings (`schema*.go`, `op*.go`, `enc*.go`) over introducing new patterns.
