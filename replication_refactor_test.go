package edb

import (
	"errors"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/fast1"
)

type replicationRefactorRow struct {
	ID   uint64 `edb:"-" msgpack:"-"`
	Name string `edb:"name" msgpack:"name"`
}

type replicationRefactorFixture struct {
	db    *DB
	table *TableDef
	raw   *KVTable
	key   *SKey[string]
	sink  *replicationTestSink
	armed bool
	calls int
}

func newReplicationRefactorFixture(t *testing.T, mode string) *replicationRefactorFixture {
	t.Helper()
	f := &replicationRefactorFixture{sink: &replicationTestSink{}}
	ks := kvo.NewSchema()
	entity := kvo.NewEntityType(ks, "row", reflect.TypeFor[replicationRefactorRow]())
	kvo.NewProp(ks, 1, "name", kvo.TMachineString, nil)
	model := kvo.NewModel(ks, entity, func(b *kvo.ModelBuilder) { b.Prop(ks.MustPropByCode(1)) })
	sc := new(Schema)
	f.table = DefineTable(sc, "rows", model, func(b *TableBuilder[uint64]) {
		b.SetRecordStorage(RecordStorage{Format: Compact2, MessagePackInserts: true})
	}).TableDef()
	f.raw = DefineRawTable(sc, "raw", nil)
	f.key = AddSingletonKey[string](AddKVMap(sc, "settings"), "key")
	opt := replicationCaptureOptions(f.sink)
	opt.Capture.Include = func(ReplicationObject) bool {
		if !f.armed {
			return true
		}
		f.calls++
		if mode == "filter-panic" {
			panic("filter failed")
		}
		return mode != "excluded"
	}
	opt.Capture.Shard = func(ReplicationObject, []byte, ReplicationValue) ([]byte, error) {
		if f.armed && mode == "shard-panic" {
			panic("resolver failed")
		}
		return []byte("tenant"), nil
	}
	f.db = openReplicationTest(t, sc, opt)
	f.db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &replicationRefactorRow{1, "seed"}) })
	return f
}

func TestReplicationPreparation_filter_once_and_panic_rollback(t *testing.T) {
	operations := []struct {
		name string
		run  func(*Tx, *replicationRefactorFixture)
	}{
		{"put", func(tx *Tx, f *replicationRefactorFixture) { Put(tx, &replicationRefactorRow{1, "changed"}) }},
		{"delete", func(tx *Tx, f *replicationRefactorFixture) { tx.DeleteByKey(f.table, uint64(1)) }},
		{"unsafe-delete", func(tx *Tx, f *replicationRefactorFixture) {
			tx.UnsafeDeleteByKeyRawSkippingIndex(f.table, f.table.EncodeKey(uint64(1)))
		}},
		{"upgrade", func(tx *Tx, f *replicationRefactorFixture) { tx.UpgradeRecord(f.table, uint64(1)) }},
		{"kv", func(tx *Tx, f *replicationRefactorFixture) { tx.KVPutRaw(f.raw, []byte("key"), []byte("value")) }},
		{"singleton", func(tx *Tx, f *replicationRefactorFixture) { SPutRaw(tx, f.key, []byte("value")) }},
	}
	for _, mode := range []string{"included", "excluded", "filter-panic", "shard-panic"} {
		for _, op := range operations {
			t.Run(mode+"/"+op.name, func(t *testing.T) {
				f := newReplicationRefactorFixture(t, mode)
				f.armed = true
				panics := mode == "filter-panic" || mode == "shard-panic"
				var swallowed error
				err := f.db.Tx(TxMetadata{}, true, func(tx *Tx) error {
					if panics {
						tx.CommitDespiteError()
						f.armed = false
						Put(tx, &replicationRefactorRow{2, "prior write"})
						f.armed = true
					}
					swallowed = safelyCall(func(tx *Tx) error { op.run(tx, f); return nil }, tx)
					return nil
				})
				if f.calls != 1 {
					t.Fatalf("filter calls=%d", f.calls)
				}
				groups := f.sink.snapshot()
				if panics {
					if swallowed == nil || err == nil {
						t.Fatalf("swallowed=%v result=%v", swallowed, err)
					}
					if len(groups) != 1 {
						t.Fatal("poisoned attempt published")
					}
					requireReplicationID(t, f.db, 1)
					f.db.Read(TxMetadata{}, func(tx *Tx) {
						row := Get[replicationRefactorRow](tx, uint64(1))
						if row == nil || row.Name != "seed" || Get[replicationRefactorRow](tx, uint64(2)) != nil {
							t.Fatal("caught panic committed data")
						}
					})
				} else {
					if err != nil || swallowed != nil {
						t.Fatalf("result=%v operation=%v", err, swallowed)
					}
					want := 2
					if mode == "excluded" {
						want = 1
					}
					if len(groups) != want {
						t.Fatalf("groups=%d want=%d", len(groups), want)
					}
				}
				for _, g := range groups {
					releaseTestGroup(g)
				}
				replicationAssertClean(t, f.db)
			})
		}
	}
}

type replicationReadStorage struct {
	storageTx
	reads map[string]int
}

func (s *replicationReadStorage) Bucket(name, sub string) storageBucket {
	b := s.storageTx.Bucket(name, sub)
	if b == nil {
		return nil
	}
	return &replicationReadBucket{storageBucket: b, reads: s.reads, name: name + "/" + sub}
}

type replicationReadBucket struct {
	storageBucket
	reads map[string]int
	name  string
}

func (b *replicationReadBucket) Get(key []byte) []byte {
	b.reads[b.name]++
	return b.storageBucket.Get(key)
}

func TestReplicationPreparation_raw_reads_are_lazy_and_shared(t *testing.T) {
	for _, mode := range []string{"disabled", "excluded", "included"} {
		t.Run(mode, func(t *testing.T) {
			sc := new(Schema)
			plain := DefineRawTable(sc, "plain", nil)
			indexed := DefineRawTable(sc, "indexed", func(b *KVTableBuilder) {
				b.DefineIndex("value", nil, func(ik []byte) []byte { return ik[1:] }, func(b *KVIndexContentBuilder, key []byte, v fast1.ImmutableRecord) {
					b.Add(append([]byte{byte(v.Root().Get(1))}, key...))
				})
			})
			key := AddSingletonKey[string](AddKVMap(sc, "settings"), "key")
			sink := &replicationTestSink{}
			opt := replicationCaptureOptions(sink)
			if mode == "disabled" {
				opt = nil
			}
			if mode == "excluded" {
				opt.Capture.Include = func(ReplicationObject) bool { return false }
			}
			db := openReplicationTest(t, sc, opt)
			db.Write(TxMetadata{}, func(tx *Tx) {
				tx.KVPutRaw(plain, []byte("key"), []byte("old"))
				tx.KVPutRaw(indexed, []byte("key"), buildKV(1, 1).Bytes())
				SPutRaw(tx, key, []byte("old"))
			})
			reads := make(map[string]int)
			db.Write(TxMetadata{}, func(tx *Tx) {
				tx.stx = &replicationReadStorage{storageTx: tx.stx, reads: reads}
				tx.KVPutRaw(plain, []byte("key"), []byte("new"))
				tx.KVPutRaw(indexed, []byte("key"), buildKV(1, 2).Bytes())
				SPutRaw(tx, key, []byte("new"))
			})
			want := 0
			if mode == "included" {
				want = 1
			}
			if reads["plain/"] != want || reads["settings/"] != want || reads["indexed/"] != 1 {
				t.Fatalf("reads=%v", reads)
			}
			for _, g := range sink.snapshot() {
				releaseTestGroup(g)
			}
			if opt != nil {
				replicationAssertClean(t, db)
			}
		})
	}
}

type replicationFaultStorage struct {
	storageTx
	createErr error
	deleteErr error
	putErr    error
}

func (s *replicationFaultStorage) CreateBucket(name, sub string) (storageBucket, error) {
	if s.createErr != nil {
		return nil, s.createErr
	}
	b, err := s.storageTx.CreateBucket(name, sub)
	if err != nil {
		return nil, err
	}
	return &replicationFaultBucket{storageBucket: b, err: s.putErr}, nil
}
func (s *replicationFaultStorage) DeleteBucket(name, sub string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.storageTx.DeleteBucket(name, sub)
}

type replicationFaultBucket struct {
	storageBucket
	err error
}

func (b *replicationFaultBucket) Put(key, value []byte) error {
	if b.err != nil {
		return b.err
	}
	return b.storageBucket.Put(key, value)
}

func TestReplicationStorage_all_bucket_mutations_poison_real_failures(t *testing.T) {
	for _, mode := range []string{"create", "delete", "created-bucket-put", "missing"} {
		t.Run(mode, func(t *testing.T) {
			sink := &replicationTestSink{}
			db := openReplicationTest(t, basicSchema, replicationCaptureOptions(sink))
			db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1}) })
			failure := errors.New("storage failure")
			var swallowed error
			err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
				tx.CommitDespiteError()
				Put(tx, &User{ID: 2})
				tracked := tx.stx.(*replicationStorageTx)
				fault := &replicationFaultStorage{storageTx: tracked.storageTx}
				tracked.storageTx = fault
				switch mode {
				case "create":
					fault.createErr = failure
					_, swallowed = tx.stx.CreateBucket("scratch", "index")
				case "delete":
					fault.deleteErr = failure
					swallowed = tx.stx.DeleteBucket("scratch", "index")
				case "created-bucket-put":
					fault.putErr = failure
					b, e := tx.stx.CreateBucket("scratch", "index")
					if e != nil {
						return e
					}
					swallowed = b.Put([]byte("key"), []byte("value"))
				case "missing":
					swallowed = tx.stx.DeleteBucket("missing", "index")
				}
				return nil
			})
			wantID := uint64(1)
			if mode == "missing" {
				if err != nil || !errors.Is(swallowed, ErrBucketNotFound) {
					t.Fatalf("result=%v missing=%v", err, swallowed)
				}
				wantID = 2
			} else if !errors.Is(err, failure) || !errors.Is(swallowed, failure) {
				t.Fatalf("result=%v swallowed=%v", err, swallowed)
			}
			requireReplicationID(t, db, wantID)
			db.Read(TxMetadata{}, func(tx *Tx) {
				if (Get[User](tx, ID(2)) != nil) != (mode == "missing") {
					t.Fatal("wrong storage failure commit policy")
				}
			})
			for _, g := range sink.snapshot() {
				releaseTestGroup(g)
			}
			replicationAssertClean(t, db)
		})
	}
}
