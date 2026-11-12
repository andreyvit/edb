package edb

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
)

// RecordStorage selects Fast2 or Compact2 for new keys. Format must be explicit.
// Existing keys keep their envelope version until UpgradeRecord converts them in a
// write transaction.
type RecordStorage struct {
	// MessagePackInserts deploys the dual reader before enabling KVO on new keys.
	MessagePackInserts bool
	Format             StorageFormat
	Mode               nativebind.Mode
	Generated          *kvo.Generated
}

var ErrNoRecordBinding = errors.New("table has no record binding")
var errPackedRecordRequired = errors.New("record is not Fast2 or Compact2")
var ErrRecordSchemaVersion = errors.New("row needs a schema migration before record access")

func (b *TableBuilder[Key]) SetRecordStorage(storage RecordStorage) {
	if storage.Format != Fast2 && storage.Format != Compact2 {
		panic("RecordStorage requires Fast2 or Compact2; omit SetRecordStorage for MessagePack rows")
	}
	t := b.tbl
	if t.projection != nil || t.binding != nil {
		panic("choose one record storage configuration")
	}
	if t.kvoModel == nil {
		panic("SetRecordStorage requires a table model")
	}
	t.binding = kvo.Bind(t.rowType, t.kvoModel)
	t.binding = t.binding.WithGenerated(storage.Generated, storage.Mode)
	t.rowLayout = row2.Compact2
	if storage.Format == Fast2 {
		t.rowLayout = row2.Fast2
	}
	t.messagePackInserts = storage.MessagePackInserts
}

// Model returns the table's logical model.
func (t *TableDef) Model() *kvo.Model { return t.kvoModel }

func (t *TableDef) packedView(raw []byte) (row2.View, ValueMeta, error) {
	if t.binding == nil {
		return row2.View{}, ValueMeta{}, ErrNoRecordBinding
	}
	if raw == nil {
		return row2.View{}, ValueMeta{}, nil
	}
	var v value
	if err := v.decode(raw, false); err != nil {
		return row2.View{}, ValueMeta{}, err
	}
	return t.packedPayload(v)
}
func (t *TableDef) packedPayload(v value) (row2.View, ValueMeta, error) {
	if t.binding == nil {
		return row2.View{}, v.ValueMeta(), ErrNoRecordBinding
	}
	if v.Flags != vfKVO {
		return row2.View{}, v.ValueMeta(), errPackedRecordRequired
	}
	if v.SchemaVer != t.latestSchemaVer && !(v.SchemaVer < t.latestSchemaVer && t.migrator != nil) {
		return row2.View{}, v.ValueMeta(), ErrRecordSchemaVersion
	}
	view := row2.Trusted(v.Data)
	if view.Missing() || view.Kind() != row2.Object {
		return row2.View{}, v.ValueMeta(), row2.ErrFormat
	}
	return view, v.ValueMeta(), nil
}

func (tx *Tx) storePackedRecord(t *TableDef, key any, row, current row2.View, meta ValueMeta) (ValueMeta, error) {
	if row.Missing() {
		return meta, row2.ErrFormat
	}
	if t.kvoModel.PrimaryKey() != nil && !bytes.Equal(t.EncodeKey(key), t.EncodeKeyVal(t.recordKey(t.binding.Encoded(row)))) {
		return meta, errors.New("record primary key conflicts with supplied key")
	}
	if bytes.Equal(current.Raw(), row.Raw()) && meta.SchemaVer == t.latestSchemaVer {
		return meta, nil
	}
	keyVal := t.ensureCorrectKeyType(reflect.ValueOf(key))
	var native reflect.Value
	if tx.isVerboseLoggingEnabled() {
		native = t.NewRowVal()
		if err := t.binding.TryDecode(row.Raw(), native.Elem()); err != nil {
			return meta, err
		}
		t.SetRowKeyVal(native, keyVal)
	} else if err := t.binding.TryValidate(row.Raw()); err != nil {
		return meta, err
	}
	_, m := tx.putVal(t, keyVal, native, row.Raw())
	return m, nil
}

// UpgradeRecord panics on error. Use TryUpgradeRecord to handle errors explicitly.
func (tx *Tx) UpgradeRecord(tArg AnyTable, key any) bool {
	t := tArg.TableDef()

	v, err := tx.TryUpgradeRecord(t, key)
	if err != nil {
		panic(err)
	}
	return v
}

// TryUpgradeRecord changes only the envelope version and payload. Index bytes,
// revision and schema version remain intact. Repeating it is a no-op.
func (tx *Tx) TryUpgradeRecord(tArg AnyTable, key any) (bool, error) {
	t := tArg.TableDef()

	if !tx.IsWritable() {
		return false, errors.New("record upgrade requires a write transaction")
	}
	if t.binding == nil {
		return false, ErrNoRecordBinding
	}
	bucket := t.dataBucketIn(tx)
	k := t.EncodeKey(key)
	raw := bucket.Get(k)
	if raw == nil {
		return false, nil
	}
	var old value
	if err := old.decode(raw, false); err != nil {
		return false, err
	}
	if old.Flags == vfKVO {
		return false, nil
	}
	if old.SchemaVer != t.latestSchemaVer {
		return false, ErrRecordSchemaVersion
	}
	row, _, err := decodeTableRowFromValue(&old, t, k, tx)
	if err != nil {
		return false, err
	}
	// Refuse to drop unmodeled legacy fields or normalize a historical value.
	if !sameLegacyPayload(t.encodeRowVal(nil, row), old.Data, 0) {
		return false, fmt.Errorf("legacy data does not round-trip through %v", t.rowType)
	}
	out := reserveValueHeader(make([]byte, 0, maxValueHeaderSize+len(raw)))
	out, err = t.binding.TryEncode(out, row.Elem(), t.rowLayout)
	if err != nil {
		return false, err
	}
	check := t.NewRowVal()
	if err := t.binding.TryDecode(out[maxValueHeaderSize:], check.Elem()); err != nil {
		return false, err
	}
	t.SetRowKeyVal(check, t.RowKeyVal(row))
	if !sameLegacyPayload(t.encodeRowVal(nil, check), old.Data, 0) {
		return false, fmt.Errorf("KVO mapping does not round-trip %v", t.rowType)
	}
	if t.hasIndexers() {
		view := row2.Trusted(out[maxValueHeaderSize:])
		if !tx.sameUpgradeIndexes(t, k, t.nativeRecord(row), t.encodedRecord(view)) {
			return false, fmt.Errorf("KV indexer for %s produces different entries for MessagePack and KVO", t.name)
		}
	}
	end := len(out)
	out = append(out, old.Index...)
	out = putValueHeader(out, vfKVO, old.SchemaVer, old.ModCount, end)
	tx.markWritten()
	if err := bucket.Put(k, out); err != nil {
		return false, err
	}
	return true, nil
}

// Upgrade preserves the existing index buckets, including skipped backfills.
// A callback that inspects physical presence can disagree on an empty object.
func (tx *Tx) sameUpgradeIndexes(t *TableDef, key []byte, before, after kvo.Record) bool {
	ts := tx.db.tableState(t)
	a, b := makeIndexBuilder(ts, key), makeIndexBuilder(ts, key)
	defer a.release()
	defer b.release()
	pk := t.DecodeKeyVal(key)
	t.runIndexers(tx, pk, before, a)
	t.runIndexers(tx, pk, after, b)
	a.finalize()
	b.finalize()
	return slices.EqualFunc(a.rows, b.rows, func(a, b IndexRow) bool {
		return compareIndexRows(a, b) == 0 && bytes.Equal(a.ValueRaw, b.ValueRaw)
	})
}
