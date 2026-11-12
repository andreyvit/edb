package edb

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

type IndexDef struct {
	table       *TableDef
	pos         int // index in table.indices, unstable across code changes
	name        string
	buck        string
	recType     reflect.Type
	keyEnc      *flatEncoding
	isUnique    bool
	makeIndexer func(*indexerRun) func(*Tx, values.Value, kvo.Record)

	initialFillMode IndexInitialFillMode
	debugScans      bool
}

func indexBucketName(name string) string { return "i_" + name }

// registerIndexName reserves an index name before either table family appends it
// to its ordered index list. A rejected registration leaves both unchanged.
func registerIndexName[I any](table, name string, byName map[string]*I, idx *I) {
	if _, exists := byName[name]; exists {
		panic(fmt.Errorf("table %s already has index named %q", table, name))
	}
	byName[name] = idx
}

type IndexInitialFillMode int

const (
	IndexInitialFillModeSkip IndexInitialFillMode = iota
	IndexInitialFillModeBlocking
)

type IndexOpt int

const (
	// Deprecated: use IndexInitialFillModeSkip.
	IndexOptSkipInitialFill IndexOpt = iota
	IndexOptDebugScans
	// IndexOptDeprecatedUnique stores one primary key per index key. Conflicting
	// writes retain the historical last-write-wins behavior, not a constraint.
	IndexOptDeprecatedUnique
)

// AddIndex registers an index on this table and returns it. The optional
// callback yields keys of T.
// A nil callback leaves population to TableBuilder.SetIndexer. Both forms may
// coexist. Callbacks share the write transaction; records, primary-key cells,
// and yield are borrowed and must not be mutated or retained. The primary key
// uses the shared native-value ABI; use AsUint/AsString or IndexPrimaryKey[T].
func (b *TableBuilder[Key]) AddIndex[T any](name string, indexer func(tx *Tx, pk values.Value, row kvo.Record, yield func(T)), opts ...any) *Index[T] {
	recType := reflect.TypeOf((*T)(nil)).Elem()

	idx := &IndexDef{
		name:    name,
		buck:    indexBucketName(name),
		recType: recType,
		keyEnc:  flatEncodingOf(recType),
	}

	if indexer != nil {
		idx.makeIndexer = func(run *indexerRun) func(*Tx, values.Value, kvo.Record) {
			yield := func(key T) { run.builder.addTyped(idx, key) }
			return func(tx *Tx, pk values.Value, row kvo.Record) {
				indexer(tx, pk, row, yield)
			}
		}
	}

	for _, opt := range opts {
		switch opt := opt.(type) {
		case IndexInitialFillMode:
			switch opt {
			case IndexInitialFillModeSkip, IndexInitialFillModeBlocking:
				idx.initialFillMode = opt
			default:
				panic(fmt.Errorf("invalid option %T %v", opt, opt))
			}
		case IndexOpt:
			switch opt {
			case IndexOptSkipInitialFill:
				idx.initialFillMode = IndexInitialFillModeSkip
			case IndexOptDeprecatedUnique:
				idx.isUnique = true
			case IndexOptDebugScans:
				idx.debugScans = true
			default:
				panic(fmt.Errorf("invalid option %T %v", opt, opt))
			}
		default:
			panic(fmt.Errorf("invalid option %T %v", opt, opt))
		}
	}

	b.tbl.addIndex(idx)
	return &Index[T]{idx}
}

func (idx *IndexDef) requireTable() {
	if idx.table == nil {
		panic(fmt.Errorf("index %q was not added to a table", idx.name))
	}
}

func (idx *IndexDef) Table() *TableDef {
	return idx.table
}

func (idx *IndexDef) ShortName() string {
	return idx.name
}
func (idx *IndexDef) FullName() string {
	idx.requireTable()
	return idx.table.name + "." + idx.name
}

func (idx *IndexDef) IsUnique() bool {
	return idx.isUnique
}

func (idx *IndexDef) bucketIn(tx *Tx) storageBucket {
	idx.requireTable()
	return nonNil(tx.stx.Bucket(idx.table.name, idx.buck))
}

func (idx *IndexDef) keyTupleToString(indexKeyTup tuple) string {
	return strings.Join(idx.keyEnc.tupleToStrings(indexKeyTup), "|")
}

func (idx *IndexDef) keyType() reflect.Type {
	return idx.keyEnc.typ
}

func (idx *IndexDef) DecodeIndexKeyVal(tup tuple) reflect.Value {
	keyVal := reflect.New(idx.recType).Elem()
	idx.DecodeIndexKeyValInto(keyVal, tup)
	return keyVal
}

func (idx *IndexDef) DecodeIndexKeyValInto(keyVal reflect.Value, tup tuple) {
	err := idx.keyEnc.decodeTup(tup, keyVal)
	if err != nil {
		panic(fmt.Errorf("failed to decode %s key: %w", idx.FullName(), err))
	}
}

func (idx *IndexDef) DecodeKeyVal(rawKey []byte) reflect.Value {
	keyVal := reflect.New(idx.recType).Elem()
	idx.DecodeKeyValInto(keyVal, rawKey)
	return keyVal
}

func (idx *IndexDef) DecodeKeyValInto(keyVal reflect.Value, rawKey []byte) {
	err := idx.TryDecodeKeyValInto(keyVal, rawKey)
	if err != nil {
		panic(err)
	}
}

func (idx *IndexDef) TryDecodeKeyValInto(keyVal reflect.Value, rawKey []byte) error {
	err := idx.keyEnc.decodeVal(rawKey, keyVal)
	if err != nil {
		return fmt.Errorf("failed to decode %s key %q: %w", idx.FullName(), rawKey, err)
	}
	return nil
}

func (idx *IndexDef) parseRawKeyFrom(buf []byte, s string) ([]byte, error) {
	return idx.keyEnc.stringsToRawKey(buf, strings.Split(s, idx.table.keyStringSep))
}

func (idx *IndexDef) ParseNakedIndexKeyVal(s string) (reflect.Value, error) {
	buf := acquireKeyBuffer()
	defer releaseKeyBuffer(buf)
	raw, err := idx.parseRawKeyFrom(buf.data, s)
	if err != nil {
		return reflect.Value{}, err
	}
	buf.data = raw[:0]
	keyVal := reflect.New(idx.recType).Elem()
	err = idx.keyEnc.decodeVal(raw, keyVal)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("failed to decode %s key %q: %w", idx.FullName(), raw, err)
	}
	return keyVal, nil
}

func (idx *IndexDef) ParseNakedIndexKey(s string) (any, error) {
	val, err := idx.ParseNakedIndexKeyVal(s)
	if err != nil {
		return nil, err
	}
	return val.Interface(), nil
}

func (idx *IndexDef) RawKeyString(keyRaw []byte) string {
	tup, err := decodeTuple(keyRaw)
	if err != nil {
		panic(fmt.Errorf("%s key: %w", idx.FullName(), err))
	}
	return strings.Join(idx.keyEnc.tupleToStrings(tup), "|")
}

func (idx *IndexDef) KeyString(key any) string {
	return idx.RawKeyString(idx.EncodeKey(key))
}

func (idx *IndexDef) EncodeKey(key any) []byte {
	return idx.EncodeKeyVal(reflect.ValueOf(key))
}
func (idx *IndexDef) EncodeKeyVal(keyVal reflect.Value) []byte {
	return idx.keyEnc.encode(nil, keyVal)
}

// Each active table invocation owns a callback set. Nested calls acquire a
// separate set, and release clears the builder even when a callback panics.
type indexerRun struct {
	builder   *IndexBuilder
	callbacks []func(*Tx, values.Value, kvo.Record)
}

func (o IndexOpt) String() string {
	switch o {
	case IndexOptSkipInitialFill:
		return "skip-initial-fill"
	case IndexOptDebugScans:
		return "debug-scans"
	case IndexOptDeprecatedUnique:
		return "deprecated-unique"
	default:
		return fmt.Sprintf("IndexOpt(%d)", int(o))
	}
}
