package edb

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
)

func (t *TableDef) validateRecordPrimaryKey() {
	p := t.kvoModel.PrimaryKey()
	if p == nil {
		return
	}
	index := 0
	if t.projection != nil {
		index = t.projection.NativeFieldIndex(p.Code())
	} else {
		index = t.binding.NativeFieldIndex(p.Code())
	}
	if index != 0 {
		panic(fmt.Sprintf("%s: declared primary key must bind the table key field", t.name))
	}
	tag := t.rowType.Field(0).Tag.Get("msgpack")
	name, options, _ := strings.Cut(tag, ",")
	if name == "-" || strings.Contains(options, "omitempty") {
		panic(fmt.Sprintf("%s: declared primary key must be stored by MessagePack", t.name))
	}
}

// GetRecord panics on error. Use TryGetRecord to handle errors explicitly.
func (tx *Tx) GetRecord(tArg AnyTable, key any) kvo.Record {
	t := tArg.TableDef()

	v, _, err := tx.TryGetRecord(t, key)
	if err != nil {
		panic(err)
	}
	return v
}

// TryGetRecord borrows the row payload and selects its codec. Fastest can decode
// once for repeated access. Migrations and custom object codecs may decode eagerly.
func (tx *Tx) TryGetRecord(tArg AnyTable, key any) (kvo.Record, ValueMeta, error) {
	t := tArg.TableDef()

	return tx.TryGetRecordByKeyVal(t, reflect.ValueOf(key))
}

// GetRecordByKeyVal accepts an existing reflected key without boxing it into an
// interface. It panics on error; TryGetRecordByKeyVal also returns metadata.
func (tx *Tx) GetRecordByKeyVal(tArg AnyTable, key reflect.Value) kvo.Record {
	t := tArg.TableDef()

	r, _, err := tx.TryGetRecordByKeyVal(t, key)
	if err != nil {
		panic(err)
	}
	return r
}
func (tx *Tx) TryGetRecordByKeyVal(tArg AnyTable, key reflect.Value) (kvo.Record, ValueMeta, error) {
	t := tArg.TableDef()

	buf := acquireKeyBuffer()
	k := t.encodeKeyVal(buf.data, t.ensureCorrectKeyType(key), true)
	buf.data = k[:0]
	defer releaseKeyBuffer(buf)
	return tx.TryGetRecordByKeyRaw(t, k)
}

// GetRecordByKeyRaw panics on error. Use TryGetRecordByKeyRaw to handle errors explicitly.
func (tx *Tx) GetRecordByKeyRaw(t AnyTable, key []byte) kvo.Record {
	return tx.getRecordByKeyRaw(t.TableDef(), key)
}

func (tx *Tx) getRecordByKeyRaw(t *TableDef, key []byte) kvo.Record {

	v, _, err := tx.tryGetRecordByKeyRaw(t, key)
	if err != nil {
		panic(err)
	}
	return v
}

// TryGetRecordByKeyRaw accepts an already encoded key. The result borrows only the
// stored payload, not this key. A transaction reuses one lookup cursor per table.
func (tx *Tx) TryGetRecordByKeyRaw(t AnyTable, key []byte) (kvo.Record, ValueMeta, error) {
	return tx.tryGetRecordByKeyRaw(t.TableDef(), key)
}

func (tx *Tx) tryGetRecordByKeyRaw(t *TableDef, key []byte) (kvo.Record, ValueMeta, error) {
	return tx.record(t, key, tx.recordRaw(t, key))
}

// Reuse the table's cursor for reads and stale-edit checks. Bucket.Get creates
// a fresh cursor stack on each lookup after the first write in a transaction.
func (tx *Tx) recordRaw(t *TableDef, key []byte) []byte {
	if tx.recordLookups == nil {
		tx.recordLookups = make(map[*TableDef]storageCursor)
	}
	cursor := tx.recordLookups[t]
	if cursor == nil {
		cursor = t.dataBucketIn(tx).Cursor()
		tx.recordLookups[t] = cursor
	}
	k, raw := cursor.Seek(key)
	if !bytes.Equal(k, key) {
		raw = nil
	}
	return raw
}

// Record panics on error. Use TryRecord to handle errors explicitly.
func (c Cursor[Row]) Record() kvo.Record {
	v, _, err := c.TryRecord()
	if err != nil {
		panic(err)
	}
	return v
}

func (c Cursor[Row]) TryRecord() (kvo.Record, ValueMeta, error) {
	return c.Tx().record(c.Table(), c.RawKey(), c.RawRow())
}

// Record panics on error. Use TryRecord to handle errors explicitly.
func (c *RawTableCursor) Record() kvo.Record {
	v, _, err := c.TryRecord()
	if err != nil {
		panic(err)
	}
	return v
}

func (c *RawTableCursor) TryRecord() (kvo.Record, ValueMeta, error) {
	return c.tx.record(c.table, c.RawKey(), c.RawRow())
}

// Record panics on error. Use TryRecord to handle errors explicitly.
func (c *RawIndexCursor) Record() kvo.Record {
	v, _, err := c.TryRecord()
	if err != nil {
		panic(err)
	}
	return v
}

func (c *RawIndexCursor) TryRecord() (kvo.Record, ValueMeta, error) {
	return c.tx.record(c.table, c.RawKey(), c.RawRow())
}
func (tx *Tx) record(t *TableDef, k, raw []byte) (kvo.Record, ValueMeta, error) {
	r, _, m, err := tx.recordValue(t, k, raw)
	return r, m, err
}
func (tx *Tx) recordValue(t *TableDef, k, raw []byte) (kvo.Record, reflect.Value, ValueMeta, error) {
	var none reflect.Value
	if t.kvoModel == nil {
		return kvo.Record{}, none, ValueMeta{}, ErrNoRecordBinding
	}
	if raw == nil {
		return kvo.Record{}, none, ValueMeta{}, nil
	}
	var v value
	if err := v.decode(raw, false); err != nil {
		return kvo.Record{}, none, ValueMeta{}, err
	}
	return tx.recordFromValue(t, k, &v)
}

func (tx *Tx) recordFromValue(t *TableDef, k []byte, v *value) (kvo.Record, reflect.Value, ValueMeta, error) {
	var none reflect.Value
	if t.kvoModel == nil {
		return kvo.Record{}, none, ValueMeta{}, ErrNoRecordBinding
	}
	if v.Flags == vfKVO {
		view, meta, err := t.packedPayload(*v)
		if err != nil {
			return kvo.Record{}, none, meta, err
		}
		return t.migrateRecord(tx, k, t.encodedRecord(view), v.SchemaVer), none, meta, nil
	}
	if t.directMessagePack() {
		r := t.messagePackRecord(v.Data)
		if r.MessagePack() == nil {
			// A nested embedding alias can require the Go decoder even when
			// canonical rows support direct access. Preserve that native carrier
			// for edits/reindexing, including its unstored database primary key.
			row := reflect.ValueOf(r.Native())
			t.SetRowKeyVal(row, t.DecodeKeyVal(k))
			return t.migrateRecord(tx, k, r, v.SchemaVer), row, v.ValueMeta(), nil
		}
		return t.migrateRecord(tx, k, r, v.SchemaVer), none, v.ValueMeta(), nil
	}
	row, meta, err := decodeTableRowFromValue(v, t, k, tx)
	if err != nil {
		return kvo.Record{}, none, meta, err
	}
	return t.nativeRecord(row), row, meta, nil
}

func (t *TableDef) migrateRecord(tx *Tx, key []byte, r kvo.Record, version uint64) kvo.Record {
	if version >= t.latestSchemaVer || t.migrator == nil {
		return r
	}
	e := r.Edit()
	t.migrator(tx, key, e, version)
	result := e.Record()
	if t.kvoModel.PrimaryKey() != nil && !bytes.Equal(key, t.EncodeKeyVal(t.recordKey(result))) {
		panic("record migration changed primary key")
	}
	return result
}

func (t *TableDef) directMessagePack() bool {
	if t.projection != nil {
		return t.projection.DirectMessagePack()
	}
	return t.binding != nil && t.binding.DirectMessagePack()
}
func (t *TableDef) encodedRecord(v row2.View) kvo.Record {
	return t.binding.Encoded(v)
}
func (t *TableDef) messagePackRecord(raw []byte) kvo.Record {
	var r kvo.Record
	if t.projection != nil {
		r = t.projection.MessagePack(raw)
	} else {
		r = t.binding.MessagePack(raw)
	}
	return r
}
func (t *TableDef) nativeRecord(row reflect.Value) kvo.Record {
	if t.projection != nil {
		return t.projection.Record(row)
	}
	return t.binding.Record(row)
}

func (t *TableDef) recordKey(r kvo.Record) reflect.Value {
	if t.kvoModel == nil || t.kvoModel.PrimaryKey() == nil {
		panic(fmt.Sprintf("%s: record has no declared primary key; supply a key explicitly", t.name))
	}
	p := t.kvoModel.PrimaryKey()
	if r.Model() == nil {
		r = r.WithModel(t.kvoModel)
	}
	if r.Missing() || r.Model() != nil && r.Model() != t.kvoModel || !r.Has(p.Code()) {
		panic(fmt.Sprintf("%s: record is missing its primary key", t.name))
	}
	v := reflect.New(t.keyType).Elem()
	var err error
	if t.binding != nil {
		err = t.binding.TryDecodeProperty(v, r, p.Code())
	} else {
		err = t.projection.TryDecodeProperty(v, r, p.Code())
	}
	if err != nil {
		panic(err)
	}
	return v
}

// PutRecordChecked panics on error. Use TryPutRecordChecked to handle errors explicitly.
func (tx *Tx) PutRecordChecked(tArg AnyTable, key any, row kvo.Record, expected ValueMeta) {
	t := tArg.TableDef()

	_, err := tx.TryPutRecordChecked(t, key, row, expected)
	if err != nil {
		panic(err)
	}
}

// TryPutRecordChecked inserts or replaces a record using an explicit primary key.
// expected is the revision from GetRecord, or zero metadata for an insert.
// The existing row's storage codec is preserved; new keys follow table policy.
func (tx *Tx) TryPutRecordChecked(tArg AnyTable, key any, row kvo.Record, expected ValueMeta) (ValueMeta, error) {
	t := tArg.TableDef()

	_, next, err := tx.putRecord(t, key, row, &expected)
	return next, err
}

func (tx *Tx) putRecord(t *TableDef, key any, row kvo.Record, expected *ValueMeta) (ValueMeta, ValueMeta, error) {
	var zero ValueMeta
	if tx.closed || !tx.IsWritable() {
		return zero, zero, errors.New("record put requires a write transaction")
	}
	if t.kvoModel == nil {
		return zero, zero, ErrNoRecordBinding
	}
	if row.Missing() {
		return zero, zero, errors.New("cannot put a missing record")
	}
	if m := row.Model(); m != nil && m != t.kvoModel {
		return zero, zero, errors.New("incorrect record model")
	}
	keyVal := t.ensureCorrectKeyType(reflect.ValueOf(key))
	k := t.encodeKeyVal(nil, keyVal, false)
	if t.kvoModel.PrimaryKey() != nil && !bytes.Equal(k, t.EncodeKeyVal(t.recordKey(row))) {
		return zero, zero, errors.New("record primary key conflicts with supplied key")
	}
	stored := t.dataBucketIn(tx).Get(k)
	var old value
	if stored != nil {
		if err := old.decode(stored, false); err != nil {
			return zero, zero, err
		}
	}
	meta := old.ValueMeta()
	if expected != nil && *expected != meta {
		return meta, meta, errors.New("record revision changed")
	}
	if stored != nil && old.SchemaVer > t.latestSchemaVer {
		return meta, meta, ErrRecordSchemaVersion
	}
	fast := old.Flags == vfKVO || stored == nil && t.binding != nil && !t.messagePackInserts
	if fast {
		if t.binding == nil {
			return meta, meta, ErrNoRecordBinding
		}
		var payload row2.View
		if raw := row.MessagePack(); raw != nil {
			// Crossing codecs must not silently discard unmodeled legacy data.
			native := reflect.ValueOf(row.Native())
			if !native.IsValid() || native.Type() != reflect.PointerTo(t.rowType) || !sameLegacyPayload(raw, t.encodeRowVal(nil, native), 0) {
				return meta, meta, errors.New("record cannot convert to KVO without losing MessagePack fields")
			}
			layout := t.rowLayout
			if stored != nil {
				layout = row2.Trusted(old.Data).Layout()
			}
			raw, err := t.binding.TryEncode(nil, native.Elem(), layout)
			if err != nil {
				return meta, meta, err
			}
			payload = row2.Trusted(raw)
		} else {
			payload = row.Snapshot()
		}
		layout := t.rowLayout
		if stored != nil {
			layout = row2.Trusted(old.Data).Layout()
		}
		if !payload.Missing() && payload.Kind() == row2.Object && payload.Layout() != layout {
			w := row2.AcquireWriter(nil)
			defer w.Release()
			w.Begin(0, "", row2.Object, layout)
			it := payload.Iter()
			for {
				key, _, field, ok := it.Next()
				if !ok {
					break
				}
				w.PutRaw(key, "", field)
			}
			w.End()
			payload = row2.Trusted(w.BytesWritten())
		}
		next, err := tx.storePackedRecord(t, keyVal.Interface(), payload, row2.Trusted(old.Data), meta)
		return meta, next, err
	}
	payload := row.MessagePack()
	var native reflect.Value
	if payload == nil {
		native = reflect.ValueOf(row.Native())
		if !native.IsValid() {
			if t.binding == nil {
				return meta, meta, errors.New("MessagePack replacement requires a complete native row or MessagePack record")
			}
			native = t.NewRowVal()
			if err := t.binding.TryDecode(row.Snapshot().Raw(), native.Elem()); err != nil {
				return meta, meta, err
			}
		}
		if native.Type() != reflect.PointerTo(t.rowType) {
			return meta, meta, errors.New("incorrect native record type")
		}
		copy := t.NewRowVal()
		copy.Elem().Set(native.Elem())
		t.SetRowKeyVal(copy, keyVal)
		native = copy
		payload = t.encodeRowVal(nil, native)
	} else {
		if _, err := msgpack.TryOpen(payload); err != nil {
			return meta, meta, err
		}
		if tx.isVerboseLoggingEnabled() {
			v := value{Flags: vfDefault, SchemaVer: t.latestSchemaVer, Data: payload}
			var err error
			native, _, err = decodeTableRowFromValue(&v, t, k, tx)
			if err != nil {
				return meta, meta, err
			}
		}
	}
	before, next := tx.putVal(t, keyVal, native, payload)
	return before, next, nil
}

// RecordEdit holds a private edit of the row as it was opened in this write
// transaction. SaveRecord updates its revision so it can be saved again.
type RecordEdit struct {
	editor       *kvo.Editor
	tx           *Tx
	table        *TableDef
	key          []byte
	original     []byte
	row          reflect.Value
	meta         ValueMeta
	savedChanges uint64
}

func (e *RecordEdit) Editor() *kvo.Editor { return e.editor }
func (e *RecordEdit) Table() *TableDef    { return e.table }
func (e *RecordEdit) Record() kvo.Record  { return e.editor.Record() }
func (e *RecordEdit) Map[K comparable, V any](p *kvo.MapPropRef[K, V]) *kvo.MapEditor[K, V] {
	return e.editor.Map(p)
}
func (e *RecordEdit) List[E any, P kvo.ListProperty[E]](p P) E {
	return e.editor.List(p)
}
func (e *RecordEdit) Edit(p *kvo.PropRef[kvo.Record]) *kvo.Editor { return e.editor.Edit(p) }
func (e *RecordEdit) New(p *kvo.PropRef[kvo.Record]) *kvo.Editor  { return e.editor.New(p) }
func (e *RecordEdit) EditOrCreate(p *kvo.PropRef[kvo.Record]) *kvo.Editor {
	return e.editor.EditOrCreate(p)
}
func (e *RecordEdit) Has(p interface{ Code() uint64 }) bool { return e.editor.Has(p) }
func (e *RecordEdit) Delete(p interface{ Code() uint64 })   { e.editor.Delete(p) }

// Get and Set access an edit's typed properties directly. Editor exposes the
// underlying editor for advanced path and collection operations.
func (e *RecordEdit) Get[T any, P kvo.Property[T]](p P) T { return e.editor.Get(p) }
func (e *RecordEdit) Set[T any, P kvo.Property[T]](p P, value T) {
	e.editor.Set(p, value)
}
func (e *RecordEdit) TrySet[T any, P kvo.Property[T]](p P, value T) error {
	return e.editor.TrySet(p, value)
}

// NewRecordEdit panics on error. Use TryNewRecordEdit to handle errors explicitly.
func (tx *Tx) NewRecordEdit(tArg AnyTable, key any) *RecordEdit {
	t := tArg.TableDef()

	v, err := tx.TryNewRecordEdit(t, key)
	if err != nil {
		panic(err)
	}
	return v
}

// TryNewRecordEdit prepares an insert. The table's insert policy selects the
// backing representation. SaveRecord rechecks that the key is still free.
func (tx *Tx) TryNewRecordEdit(tArg AnyTable, key any) (*RecordEdit, error) {
	t := tArg.TableDef()

	if tx.closed || !tx.IsWritable() {
		return nil, errors.New("record insert requires a write transaction")
	}
	if t.kvoModel == nil {
		return nil, ErrNoRecordBinding
	}
	keyVal := t.ensureCorrectKeyType(reflect.ValueOf(key))
	k := t.encodeKeyVal(nil, keyVal, false)
	if t.dataBucketIn(tx).Get(k) != nil {
		return nil, errors.New("record already exists")
	}
	edit := &RecordEdit{tx: tx, table: t, key: k}
	if t.binding != nil && !t.messagePackInserts {
		edit.editor = kvo.Encoded(row2.Empty(t.rowLayout)).WithModel(t.kvoModel).Edit()
	} else if t.directMessagePack() {
		// New records use the same encoded editor as existing MessagePack
		// rows. Unset fields have their ordinary zero values on native decode.
		edit.editor = t.messagePackRecord(emptyMessagePackRecord).Edit()
	} else {
		edit.row = t.NewRowVal()
		t.SetRowKeyVal(edit.row, keyVal)
		edit.editor = t.nativeRecord(edit.row).Edit()
	}
	if !edit.row.IsValid() {
		if prop := t.kvoModel.PrimaryKey(); prop != nil {
			// Use the native binding so composite and custom keys retain the
			// same representation as keys stored by an ordinary row write.
			seed := t.NewRowVal()
			t.SetRowKeyVal(seed, keyVal)
			if err := edit.editor.TrySetValue(prop.Code(), t.nativeRecord(seed).GetValue(prop.Code())); err != nil {
				return nil, err
			}
		}
	}
	return edit, nil
}

// Shared immutable empty map; editors only publish new byte spans.
var emptyMessagePackRecord = []byte{0x80}

// OpenRecordEdit panics on error. Use TryOpenRecordEdit to handle errors explicitly.
func (tx *Tx) OpenRecordEdit(tArg AnyTable, key any) *RecordEdit {
	t := tArg.TableDef()

	v, err := tx.TryOpenRecordEdit(t, key)
	if err != nil {
		panic(err)
	}
	return v
}

// TryOpenRecordEdit returns nil without error when the key is missing.
func (tx *Tx) TryOpenRecordEdit(tArg AnyTable, key any) (*RecordEdit, error) {
	t := tArg.TableDef()

	if tx.closed || !tx.IsWritable() {
		return nil, errors.New("record edit requires a write transaction")
	}
	k := t.EncodeKey(key)
	return tx.openRecordEdit(t, k, tx.recordRaw(t, k))
}

// EditRecord opens, edits, and saves one record using the table's storage codec.
// A missing key is a no-op. Use NewRecordEdit to prepare an insert.
func (tx *Tx) EditRecord(tArg AnyTable, key any, edit func(*kvo.Editor)) {
	t := tArg.TableDef()

	_, err := tx.TryEditRecord(t, key, func(e *kvo.Editor) error {
		edit(e)
		return nil
	})
	if err != nil {
		panic(err)
	}
}

// TryEditRecord discards the edit if the callback returns an error. It checks
// for intervening writes before saving, just like an explicitly opened edit.
func (tx *Tx) TryEditRecord(tArg AnyTable, key any, edit func(*kvo.Editor) error) (ValueMeta, error) {
	t := tArg.TableDef()

	e, err := tx.TryOpenRecordEdit(t, key)
	if err != nil || e == nil {
		return ValueMeta{}, err
	}
	if err := edit(e.Editor()); err != nil {
		return e.meta, err
	}
	return tx.TrySaveRecord(e)
}

// RecordEdit panics on error. Use TryRecordEdit to handle errors explicitly.
func (c Cursor[Row]) RecordEdit() *RecordEdit {
	v, err := c.TryRecordEdit()
	if err != nil {
		panic(err)
	}
	return v
}

// TryRecordEdit opens the current cursor row for writing without full decoding
// when its codec supports direct access. It retains the stale-save check. Call it after
// a successful Next, while the cursor is positioned on the row.
func (c Cursor[Row]) TryRecordEdit() (*RecordEdit, error) { return cursorRecordEdit(c.RawCursor) }

// RecordEdit panics on error. Use TryRecordEdit to handle errors explicitly.
func (c *RawTableCursor) RecordEdit() *RecordEdit {
	v, err := c.TryRecordEdit()
	if err != nil {
		panic(err)
	}
	return v
}

func (c *RawTableCursor) TryRecordEdit() (*RecordEdit, error) { return cursorRecordEdit(c) }

// RecordEdit panics on error. Use TryRecordEdit to handle errors explicitly.
func (c *RawIndexCursor) RecordEdit() *RecordEdit {
	v, err := c.TryRecordEdit()
	if err != nil {
		panic(err)
	}
	return v
}

func (c *RawIndexCursor) TryRecordEdit() (*RecordEdit, error) { return cursorRecordEdit(c) }

func cursorRecordEdit(c RawCursor) (*RecordEdit, error) {
	tx := c.Tx()
	if tx.closed || !tx.IsWritable() || c.RawKey() == nil {
		return nil, errors.New("record edit requires a positioned write cursor")
	}
	return tx.openRecordEdit(c.Table(), c.RawKey(), c.RawRow())
}

func (tx *Tx) openRecordEdit(t *TableDef, k, raw []byte) (*RecordEdit, error) {
	r, row, meta, err := tx.recordValue(t, k, raw)
	if err != nil || r.Missing() {
		return nil, err
	}
	return &RecordEdit{editor: r.Edit(), tx: tx, table: t, key: k, original: raw, row: row, meta: meta}, nil
}

// SaveRecord panics on error. Use TrySaveRecord to handle errors explicitly.
func (tx *Tx) SaveRecord(e *RecordEdit) {
	_, err := tx.TrySaveRecord(e)
	if err != nil {
		panic(err)
	}
}

func (tx *Tx) TrySaveRecord(e *RecordEdit) (ValueMeta, error) {
	if e == nil || e.tx != tx || tx.closed {
		return ValueMeta{}, errors.New("record edit belongs to another transaction")
	}
	t := e.table
	raw := tx.recordRaw(t, e.key)
	if !bytes.Equal(raw, e.original) {
		return ValueMeta{}, errors.New("record changed since opening edit")
	}
	if payload := e.editor.MessagePack(); payload != nil {
		if t.kvoModel.PrimaryKey() != nil && !bytes.Equal(e.key, t.EncodeKeyVal(t.recordKey(t.messagePackRecord(payload)))) {
			return e.meta, errors.New("record edit changed primary key")
		}
		if e.original != nil && e.savedChanges == e.editor.Changes() && e.meta.SchemaVer == t.latestSchemaVer {
			return e.meta, nil
		}
		var native reflect.Value
		if tx.isVerboseLoggingEnabled() {
			v := value{Flags: vfDefault, SchemaVer: t.latestSchemaVer, Data: payload}
			var err error
			native, _, err = decodeTableRowFromValue(&v, t, e.key, tx)
			if err != nil {
				return e.meta, err
			}
		}
		_, e.meta = tx.putVal(t, t.DecodeKeyVal(e.key), native, payload)
		e.savedChanges = e.editor.Changes()
	} else if e.row.IsValid() {
		if !bytes.Equal(e.key, t.EncodeKeyVal(t.RowKeyVal(e.row))) {
			return e.meta, errors.New("record edit changed primary key")
		}
		if e.original != nil && e.savedChanges == e.editor.Changes() && e.meta.SchemaVer == t.latestSchemaVer {
			return e.meta, nil
		}
		_, e.meta = tx.PutVal(t, e.row)
		e.savedChanges = e.editor.Changes()
	} else {
		current, m, err := t.packedView(raw)
		if err != nil {
			return m, err
		}
		next := e.editor.Snapshot()
		// Compare encoded bytes before decoding the database key. Do not use
		// Changes here: writes through Editor.Packed bypass that counter.
		if t.kvoModel.PrimaryKey() == nil && bytes.Equal(current.Raw(), next.Raw()) && m.SchemaVer == t.latestSchemaVer {
			return m, nil
		}
		m, err = tx.storePackedRecord(t, t.DecodeKeyVal(e.key).Interface(), next, current, m)
		if err != nil {
			return m, err
		}
		e.meta = m
	}
	e.original = tx.recordRaw(t, e.key)
	return e.meta, nil
}
