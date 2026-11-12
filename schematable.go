package edb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
)

type TableDef struct {
	schema             *Schema
	name               string
	latestSchemaVer    uint64
	pos                int // index in the declaring schema; Include never changes it
	rowType            reflect.Type
	rowTypePtr         reflect.Type
	rowInfo            *structInfo
	indices            []*IndexDef
	indicesByName      map[string]*IndexDef
	recordIndexer      func(kvo.Record, *IndexBuilder)
	indexers           []*IndexDef
	indexerPool        sync.Pool
	keyEnc             *flatEncoding
	keyType            reflect.Type
	messagePackInserts bool
	kvoModel           *kvo.Model
	projection         *kvo.Projection
	binding            *kvo.Binding
	rowLayout          row2.Layout
	valueEnc           encodingMethod
	keyStringSep       string
	zeroKey            []byte
	migrator           func(tx *Tx, key []byte, edit *kvo.Editor, oldVer uint64)
	suppressContent    bool

	TaggableImpl
}

func (tbl *TableDef) Name() string {
	return tbl.name
}

// SchemaVersion is the version written by current saves. Stored rows may have
// older versions until they are loaded, migrated, and saved.
func (tbl *TableDef) SchemaVersion() uint64 {
	return tbl.latestSchemaVer
}

func (tbl *TableDef) addIndex(idx *IndexDef) {
	if idx.table != nil {
		panic(fmt.Errorf("index %s already belongs to table %s", idx.name, idx.table.name))
	}
	registerIndexName(tbl.name, idx.name, tbl.indicesByName, idx)
	idx.pos = len(tbl.indices)
	tbl.indices = append(tbl.indices, idx)
	idx.table = tbl
	if idx.makeIndexer != nil {
		tbl.indexers = append(tbl.indexers, idx)
	}
}

func (tbl *TableDef) Indices() []*IndexDef {
	return slices.Clone(tbl.indices)
}

func (tbl *TableDef) IndexNamed(name string) *IndexDef {
	return tbl.indicesByName[name]
}

func (tbl *TableDef) KeyType() reflect.Type {
	return tbl.keyType
}

func (tbl *TableDef) newRow(schemaVer uint64) reflect.Value {
	_ = schemaVer // TODO: create legacy version if needed
	return reflect.New(tbl.rowType)
}

func (tbl *TableDef) rootBucketIn(tx *Tx) storageBucket {
	return nonNil(tx.stx.Bucket(tbl.name, ""))
}

func (tbl *TableDef) dataBucketIn(tx *Tx) storageBucket {
	return nonNil(tx.stx.Bucket(tbl.name, dataBucketName))
}

func (tbl *TableDef) ensureCorrectKeyType(keyVal reflect.Value) reflect.Value {
	if keyVal.Type() != tbl.keyType {
		if keyVal.CanConvert(tbl.keyType) {
			return keyVal.Convert(tbl.keyType)
		}
		panic(fmt.Errorf("%s: key must be %v, got %v %v", tbl.name, tbl.keyType, keyVal.Type(), keyVal.Interface()))
	}
	return keyVal
}

func (tbl *TableDef) RowKeyVal(rowVal reflect.Value) reflect.Value {
	return tbl.rowInfo.keyValue(rowVal)
}
func (tbl *TableDef) RowKey(row any) any {
	return tbl.RowKeyVal(reflect.ValueOf(row)).Interface()
}
func (tbl *TableDef) SetRowKey(row, key any) {
	tbl.SetRowKeyVal(reflect.ValueOf(row), reflect.ValueOf(key))
}
func (tbl *TableDef) SetRowKeyVal(rowVal, keyVal reflect.Value) {
	tbl.RowKeyVal(rowVal).Set(keyVal)
}

func (tbl *TableDef) EncodeKey(key any) []byte {
	return tbl.EncodeKeyVal(reflect.ValueOf(key))
}
func (tbl *TableDef) EncodeKeyVal(keyVal reflect.Value) []byte {
	return tbl.keyEnc.encode(nil, keyVal)
}

func (tbl *TableDef) encodeKeyVal(buf []byte, key reflect.Value, zeroOK bool) []byte {
	buf = tbl.keyEnc.encode(buf, key)
	if !zeroOK && bytes.Equal(buf, tbl.zeroKey) {
		v := key.Interface()
		panic(fmt.Errorf("attempt to encode zero key for table %s: %T %v", tbl.Name(), v, v))
	}
	return buf
}

func (tbl *TableDef) RowHasZeroKey(row any) bool {
	return tbl.RowValHasZeroKey(reflect.ValueOf(row))
}

func (tbl *TableDef) RowValHasZeroKey(rowVal reflect.Value) bool {
	// TODO: better logic here!
	return tbl.RowKeyVal(rowVal).IsZero()
}

func (tbl *TableDef) encodeRowVal(buf []byte, rowVal reflect.Value) []byte {
	return tbl.valueEnc.EncodeValue(buf, rowVal)
}

func (tbl *TableDef) DecodeKeyVal(rawKey []byte) reflect.Value {
	keyVal := reflect.New(tbl.keyType).Elem()
	tbl.DecodeKeyValInto(keyVal, rawKey)
	return keyVal
}

func (tbl *TableDef) decodeUintKey(rawKey []byte) uint64 {
	if bits := tbl.keyEnc.uintKeyBits; bits != 0 {
		// A single unsigned component is eight big-endian bytes and the tuple count.
		if len(rawKey) != 9 || rawKey[8] != 1 {
			panic(fmt.Errorf("%s: invalid unsigned primary key %x", tbl.name, rawKey))
		}
		v := binary.BigEndian.Uint64(rawKey)
		if bits < 64 && v>>bits != 0 {
			panic(fmt.Errorf("%s: primary key overflows %s", tbl.name, tbl.keyType))
		}
		return v
	}
	if kind := tbl.keyType.Kind(); kind >= reflect.Uint && kind <= reflect.Uintptr {
		// Named unsigned types can override the built-in encoding.
		return tbl.DecodeKeyVal(rawKey).Uint()
	}
	panic(fmt.Errorf("%s: UintKey requires an unsigned integer primary key, got %s", tbl.name, tbl.keyType))
}

func (tbl *TableDef) DecodeKeyValInto(keyVal reflect.Value, rawKey []byte) {
	err := tbl.TryDecodeKeyValInto(keyVal, rawKey)
	if err != nil {
		panic(err)
	}
}

func (tbl *TableDef) TryDecodeKeyValInto(keyVal reflect.Value, rawKey []byte) error {
	err := tbl.keyEnc.decodeVal(rawKey, keyVal)
	if err != nil {
		return fmt.Errorf("failed to decode %s key %q: %w", tbl.name, rawKey, err)
	}
	return nil
}

func (tbl *TableDef) RawKeyString(keyRaw []byte) string {
	str, err := tbl.TryRawKeyString(keyRaw)
	if err != nil {
		panic(err)
	}
	return str
}

func (tbl *TableDef) TryRawKeyString(keyRaw []byte) (string, error) {
	tup, err := decodeTuple(keyRaw)
	if err != nil {
		return "", err
	}
	comps, err := tbl.keyEnc.tryTupleToStrings(tup)
	if err != nil {
		return "", err
	}
	return strings.Join(comps, tbl.keyStringSep), nil
}

func (tbl *TableDef) KeyString(key any) string {
	return tbl.RawKeyString(tbl.EncodeKey(key))
}

func (tbl *TableDef) parseRawKeyFrom(buf []byte, s string) ([]byte, error) {
	return tbl.keyEnc.stringsToRawKey(buf, strings.Split(s, tbl.keyStringSep))
}

func (tbl *TableDef) ParseKeyVal(s string) (reflect.Value, error) {
	buf := acquireKeyBuffer()
	defer releaseKeyBuffer(buf)
	raw, err := tbl.parseRawKeyFrom(buf.data, s)
	if err != nil {
		return reflect.Value{}, err
	}
	buf.data = raw[:0]
	return tbl.DecodeKeyVal(raw), nil
}

func (tbl *TableDef) ParseKey(s string) (any, error) {
	val, err := tbl.ParseKeyVal(s)
	if err != nil {
		return nil, err
	}
	return val.Interface(), nil
}

func (tbl *TableDef) NewRowVal() reflect.Value {
	return reflect.New(tbl.rowType)
}
func (tbl *TableDef) NewRow() any {
	return tbl.NewRowVal().Interface()
}

func (tbl *TableDef) decodeRow(buf []byte) (reflect.Value, error) {
	rowVal := reflect.New(tbl.rowType)
	err := tbl.valueEnc.DecodeValue(buf, rowVal)
	return rowVal, err
}

func (tbl *TableDef) RowType() reflect.Type {
	return tbl.rowType
}
