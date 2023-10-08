package edb

import (
	"bytes"
	"fmt"
	"reflect"
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
}

func (tbl *Table) Name() string {
	return tbl.name
}

type tableOpt int

const (
	SuppressContentWhenLogging = tableOpt(1)
)

func AddTable[Row any](scm *Schema, name string, latestSchemaVer uint64, indexer func(row *Row, ib *IndexBuilder), migrator func(tx *Tx, row *Row, oldVer uint64), indices []*Index, opts ...any) *Table {
	scm.init()
	rowPtrType := reflect.TypeOf((*Row)(nil))
	if rowPtrType.Kind() != reflect.Ptr || rowPtrType.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("%s: second arg to AddTable must be (*YourStruct)(nil)", name))
	}
	tbl := &Table{
		schema:          scm,
		name:            name,
		latestSchemaVer: latestSchemaVer,
		buck:            makeBucketName(name),
		rowTypePtr:      rowPtrType,
		rowType:         rowPtrType.Elem(),
		rowInfo:         reflectTypeWithoutCache(rowPtrType),
		valueEnc:        defaultValueEncoding,
		keyStringSep:    "|",
		indicesByName:   make(map[string]*Index),
		indexer: func(row any, ib *IndexBuilder) {
			indexer(row.(*Row), ib)
		},
	}
	if migrator != nil {
		tbl.migrator = func(tx *Tx, row any, oldVer uint64) {
			migrator(tx, row.(*Row), oldVer)
		}
	}
	tbl.keyType = tbl.rowInfo.keyField.Type
	tbl.keyEnc = flatEncodingOf(tbl.keyType)
	scm.addTable(tbl)
	tbl.zeroKey = tbl.keyEnc.encode(nil, reflect.Zero(tbl.keyType))

	for _, idx := range indices {
		tbl.AddIndex(idx)
	}

	for _, opt := range opts {
		switch opt := opt.(type) {
		case tableOpt:
			if opt == SuppressContentWhenLogging {
				tbl.suppressContent = true
			}
		default:
			panic(fmt.Errorf("invalid option %T %v", opt, opt))
		}
	}

	return tbl
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

func (tbl *Table) IndexNamed(name string) *Index {
	return tbl.indicesByName[name]
}

func (tbl *Table) KeyType() reflect.Type {
	return tbl.keyType
}

func (tbl *Table) newRow(schemaVer uint64) reflect.Value {
	// TODO: create legacy version if needed
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

func (tbl *Table) DecodeKeyVal(buf []byte) reflect.Value {
	keyPtr := reflect.New(tbl.keyType)
	err := tbl.keyEnc.decode(buf, keyPtr)
	if err != nil {
		panic(fmt.Errorf("failed to decode %s key %q: %w", tbl.name, buf, err))
	}
	return keyPtr.Elem()
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

func (tbl *Table) parseKeyComps(buf []byte, comps []string) ([]byte, error) {
	return tbl.keyEnc.stringsToRawKey(buf, comps)
}

func (tbl *Table) parseRawKeyFrom(buf []byte, s string) ([]byte, error) {
	return tbl.parseKeyComps(buf, strings.Split(s, tbl.keyStringSep))
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

// func (tbl *Table) IsGeneratableOnEmptyKey() bool {
// }

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
