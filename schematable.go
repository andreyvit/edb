package edb

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"go.etcd.io/bbolt"
)

type Table struct {
	schema          *Schema
	name            string
	latestSchemaVer uint64
	pos             int // index in schema.tables, unstable across code changes
	buck            bucketName
	rowType         reflect.Type
	rowTypePtr      reflect.Type
	rowInfo         *structInfo
	indices         []*Index
	indicesByName   map[string]*Index
	indexer         func(row any, ib *IndexBuilder)
	keyEnc          *flatEncoding
	keyType         reflect.Type
	valueEnc        encodingMethod
	keyStringSep    string
	zeroKey         []byte
	migrator        func(tx *Tx, row any, oldVer uint64)
	suppressContent bool

	TaggableImpl
}

func (tbl *Table) Name() string {
	return tbl.name
}

type tableOpt int

const (
	SuppressContentWhenLogging = tableOpt(1)
)

func AddTable[Row any](scm *Schema, name string, latestSchemaVer uint64, indexer func(row *Row, ib *IndexBuilder), migrator func(tx *Tx, row *Row, oldVer uint64), indices []*Index, opts ...any) *Table {
	return DefineTable[Row, any](scm, name, func(b *TableBuilder[Row, any]) {
		b.SetSchemaVersion(latestSchemaVer)
		if indexer != nil {
			b.Indexer(indexer)
		}
		if migrator != nil {
			b.Migrate(migrator)
		}
		for _, idx := range indices {
			b.AddIndex(idx)
		}
		for _, opt := range opts {
			switch opt := opt.(type) {
			case tableOpt:
				if opt == SuppressContentWhenLogging {
					b.SuppressContentWhenLogging()
				}
			case *Tag:
				b.Tag(opt)
			default:
				panic(fmt.Errorf("invalid option %T %v", opt, opt))
			}
		}
	})
}

func (tbl *Table) AddIndex(idx *Index) *Table {
	if tbl.indicesByName[idx.name] != nil {
		panic(fmt.Errorf("table %s already has index named %q", tbl.name, idx.name))
	}
	idx.pos = len(tbl.indices)
	tbl.indices = append(tbl.indices, idx)
	tbl.indicesByName[idx.name] = idx
	idx.table = tbl
	return tbl
}

func (tbl *Table) Indices() []*Index {
	return slices.Clone(tbl.indices)
}

func (tbl *Table) IndexNamed(name string) *Index {
	return tbl.indicesByName[name]
}

func (tbl *Table) KeyType() reflect.Type {
	return tbl.keyType
}

func (tbl *Table) newRow(schemaVer uint64) reflect.Value {
	_ = schemaVer // TODO: create legacy version if needed
	return reflect.New(tbl.rowType)
}

func (tbl *Table) rootBucketIn(btx *bbolt.Tx) *bbolt.Bucket {
	return nonNil(btx.Bucket(tbl.buck.Raw()))
}

func (tbl *Table) dataBucketIn(tableRootB *bbolt.Bucket) *bbolt.Bucket {
	return nonNil(tableRootB.Bucket(dataBucket.Raw()))
}

func (tbl *Table) ensureCorrectKeyType(keyVal reflect.Value) reflect.Value {
	if keyVal.Type() != tbl.keyType {
		if keyVal.CanConvert(tbl.keyType) {
			return keyVal.Convert(tbl.keyType)
		}
		panic(fmt.Errorf("%s: key must be %v, got %v %v", tbl.name, tbl.keyType, keyVal.Type(), keyVal.Interface()))
	}
	return keyVal
}

func (tbl *Table) RowKeyVal(rowVal reflect.Value) reflect.Value {
	return tbl.rowInfo.keyValue(rowVal)
}
func (tbl *Table) RowKey(row any) any {
	return tbl.RowKeyVal(reflect.ValueOf(row)).Interface()
}
func (tbl *Table) SetRowKey(row, key any) {
	tbl.SetRowKeyVal(reflect.ValueOf(row), reflect.ValueOf(key))
}
func (tbl *Table) SetRowKeyVal(rowVal, keyVal reflect.Value) {
	tbl.RowKeyVal(rowVal).Set(keyVal)
}

func (tbl *Table) EncodeKey(key any) []byte {
	return tbl.EncodeKeyVal(reflect.ValueOf(key))
}
func (tbl *Table) EncodeKeyVal(keyVal reflect.Value) []byte {
	return tbl.keyEnc.encode(nil, keyVal)
}

func (tbl *Table) encodeKeyVal(buf []byte, key reflect.Value, zeroOK bool) []byte {
	buf = tbl.keyEnc.encode(buf, key)
	if !zeroOK && bytes.Equal(buf, tbl.zeroKey) {
		v := key.Interface()
		panic(fmt.Errorf("attempt to encode zero key for table %s: %T %v", tbl.Name(), v, v))
	}
	return buf
}

func (tbl *Table) RowHasZeroKey(row any) bool {
	return tbl.RowValHasZeroKey(reflect.ValueOf(row))
}

func (tbl *Table) RowValHasZeroKey(rowVal reflect.Value) bool {
	// TODO: better logic here!
	return tbl.RowKeyVal(rowVal).IsZero()
}

func (tbl *Table) encodeRowVal(buf []byte, rowVal reflect.Value) []byte {
	return tbl.valueEnc.EncodeValue(buf, rowVal)
}

func (tbl *Table) DecodeKeyVal(rawKey []byte) reflect.Value {
	keyVal := reflect.New(tbl.keyType).Elem()
	tbl.DecodeKeyValInto(keyVal, rawKey)
	return keyVal
}

func (tbl *Table) DecodeKeyValInto(keyVal reflect.Value, rawKey []byte) {
	err := tbl.keyEnc.decodeVal(rawKey, keyVal)
	if err != nil {
		panic(fmt.Errorf("failed to decode %s key %q: %w", tbl.name, rawKey, err))
	}
}

func (tbl *Table) RawKeyString(keyRaw []byte) string {
	tup, err := decodeTuple(keyRaw)
	if err != nil {
		panic(fmt.Errorf("%s key: %w", tbl.name, err))
	}
	return strings.Join(tbl.keyEnc.tupleToStrings(tup), tbl.keyStringSep)
	// keyVal := tbl.decodeKey(keyRaw)
	// return fmt.Sprint(keyVal.Interface())
}

func (tbl *Table) KeyString(key any) string {
	return tbl.RawKeyString(tbl.EncodeKey(key))
}

func (tbl *Table) parseRawKeyFrom(buf []byte, s string) ([]byte, error) {
	return tbl.keyEnc.stringsToRawKey(buf, strings.Split(s, tbl.keyStringSep))
}

func (tbl *Table) ParseKeyVal(s string) (reflect.Value, error) {
	buf := keyBytesPool.Get().([]byte)
	defer releaseKeyBytes(buf)
	raw, err := tbl.parseRawKeyFrom(buf, s)
	if err != nil {
		return reflect.Value{}, err
	}
	return tbl.DecodeKeyVal(raw), nil
}

func (tbl *Table) ParseKey(s string) (any, error) {
	val, err := tbl.ParseKeyVal(s)
	if err != nil {
		return nil, err
	}
	return val.Interface(), nil
}

func (tbl *Table) NewRowVal() reflect.Value {
	return reflect.New(tbl.rowType)
}
func (tbl *Table) NewRow() any {
	return tbl.NewRowVal().Interface()
}

func (tbl *Table) decodeRow(buf []byte) (reflect.Value, error) {
	rowVal := reflect.New(tbl.rowType)
	err := tbl.valueEnc.DecodeValue(buf, rowVal)
	// log.Printf("decoded row from %q: %#v", buf, rowVal.Elem().Interface())
	return rowVal, err
}

func (tbl *Table) RowType() reflect.Type {
	return tbl.rowType
}
