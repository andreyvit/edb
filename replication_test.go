package edb

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

type replicationTestSink struct {
	mu               sync.Mutex
	groups           []*ReplicationGroup
	cancelled        int
	fail             error
	stageFail        error
	maxGroupBytes    int64
	maxBufferedBytes int64
	reserved         int64
	onStage          func(ReplicationChange)
	onPublish        func(uint64, uint64)
}
type replicationTestReservation struct {
	sink   *replicationTestSink
	group  *ReplicationGroup
	limit  int64
	size   int64
	closed bool
}

var replicationTestReleases sync.Map

func releaseTestGroup(g *ReplicationGroup) {
	if release, ok := replicationTestReleases.LoadAndDelete(g); ok {
		release.(func())()
	}
	g.Changes = nil
}
func testChangeSize(c ReplicationChange) int64 {
	return 2*int64(unsafe.Sizeof(c)) + int64(len(c.Key)+len(c.Value.Data)+len(c.Shard))
}
func copyTestChange(c ReplicationChange) ReplicationChange {
	c.Key = bytes.Clone(c.Key)
	c.Value.Data = bytes.Clone(c.Value.Data)
	c.Shard = bytes.Clone(c.Shard)
	return c
}
func (s *replicationTestSink) Reserve() (ReplicationReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return nil, s.fail
	}
	limit := s.maxGroupBytes
	if limit == 0 {
		limit = 1 << 20
	}
	buffer := s.maxBufferedBytes
	if buffer == 0 {
		buffer = 32 << 20
	}
	if s.reserved > buffer-limit {
		return nil, ErrReplicationOverload
	}
	s.reserved += limit
	return &replicationTestReservation{sink: s, group: new(ReplicationGroup), limit: limit}, nil
}
func (r *replicationTestReservation) Stage(c ReplicationChange) (ReplicationChange, error) {
	s := r.sink
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.closed {
		panic("reservation already resolved")
	}
	if s.stageFail != nil {
		return ReplicationChange{}, s.stageFail
	}
	if s.onStage != nil {
		s.onStage(c)
	}
	size := testChangeSize(c)
	if r.size > r.limit-size {
		return ReplicationChange{}, ErrReplicationGroupSize
	}
	owned := copyTestChange(c)
	r.group.Changes = append(r.group.Changes, owned)
	r.size += size
	return owned, nil
}
func (r *replicationTestReservation) Publish(id, rev uint64) {
	s := r.sink
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.closed {
		panic("reservation already resolved")
	}
	r.closed = true
	g := r.group
	g.TransactionID, g.SchemaRev = id, rev
	if s.onPublish != nil {
		s.onPublish(id, rev)
	}
	s.groups = append(s.groups, g)
	replicationTestReleases.Store(g, func() {
		s.mu.Lock()
		s.reserved -= r.limit
		s.mu.Unlock()
	})
}
func (r *replicationTestReservation) Cancel() {
	s := r.sink
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.closed {
		panic("reservation already resolved")
	}
	r.closed = true
	r.group.Changes = nil
	s.reserved -= r.limit
	s.cancelled++
}
func (s *replicationTestSink) Drain() error {
	for _, g := range s.snapshot() {
		releaseTestGroup(g)
	}
	return nil
}
func (s *replicationTestSink) snapshot() []*ReplicationGroup {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.groups)
}
func replicationCaptureOptions(s ReplicationSink) *ReplicationOptions {
	return &ReplicationOptions{Capture: &ReplicationCaptureOptions{SchemaRev: 3, Sink: s, Shard: func(_ ReplicationObject, _ []byte, _ ReplicationValue) ([]byte, error) { return []byte("tenant"), nil }}}
}
func replicationApplyOptions() *ReplicationOptions {
	return &ReplicationOptions{Apply: &ReplicationApplyOptions{SchemaRev: 3}}
}
func openReplicationTest(t *testing.T, schema *Schema, o *ReplicationOptions, policies ...Options) *DB {
	t.Helper()
	opt := Options{IsTesting: true, ReuseTxBuffers: true, Replication: o}
	if len(policies) > 0 {
		opt.LocalWrites = policies[0].LocalWrites
		opt.RowMigrations = policies[0].RowMigrations
	}
	db, err := Open(filepath.Join(t.TempDir(), "db"), schema, opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return db
}
func replicationAssertClean(t *testing.T, db *DB) {
	t.Helper()
	r := db.replication
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.attempts) != 0 {
		t.Fatalf("attempts=%d", len(r.attempts))
	}
}
func TestReplicationCommitOwnershipAndApply(t *testing.T) {
	sink := &replicationTestSink{}
	source := openReplicationTest(t, basicSchema, replicationCaptureOptions(sink))
	replica := openReplicationTest(t, basicSchema, replicationApplyOptions(), Options{LocalWrites: LocalWritePolicy{Mode: RejectLocalWrites}, RowMigrations: SkipRowMigrations})
	source.Write(TxMetadata{}, func(tx *Tx) {
		Put(tx, &User{ID: 1, Name: "first", Email: "one"})
		Put(tx, &User{ID: 1, Name: "second", Email: "two"})
	})
	groups := sink.snapshot()
	if len(groups) != 1 || len(groups[0].Changes) != 2 || groups[0].TransactionID != 1 {
		t.Fatalf("groups=%+v", groups)
	}
	g := groups[0]
	before := bytes.Clone(g.Changes[0].Value.Data)
	source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 2, Name: "pool reuse", Email: "three"}) })
	if !bytes.Equal(before, g.Changes[0].Value.Data) {
		t.Fatal("borrowed bytes changed")
	}
	if err := replica.ApplyReplication(g); err != nil {
		t.Fatal(err)
	}
	if err := replica.ApplyReplication(g); err != nil {
		t.Fatal(err)
	}
	replica.Read(TxMetadata{}, func(tx *Tx) {
		u := Get[User](tx, ID(1))
		if u.Name != "second" || Lookup[User](tx, usersByEmail, "two") == nil || Lookup[User](tx, usersByEmail, "one") != nil {
			t.Fatalf("row/index apply: %+v", u)
		}
		v := rowReplicationValue(tx.stx.Bucket(usersTable.name, dataBucketName).Get(usersTable.EncodeKey(ID(1))))
		if v.ModCount != g.Changes[1].Value.ModCount || !bytes.Equal(v.Data, g.Changes[1].Value.Data) {
			t.Fatal("source metadata/payload changed")
		}
	})
	if err := replica.Tx(TxMetadata{}, true, func(tx *Tx) error { Put(tx, &User{ID: 9}); return nil }); err == nil {
		t.Fatal("replica write admitted")
	}
	replica.Write(TxMetadata{}, func(tx *Tx) {
		tx.OnChange(map[*TableDef]ChangeFlags{usersTable: ChangeFlagNotify}, func(*Tx, *Change) { t.Error("replica hook") })
		tx.Reindex(usersTable, nil)
	})
	committed, err := replica.ReplicationStatus()
	if err != nil || committed != 1 {
		t.Fatalf("checkpoint=%d err=%v", committed, err)
	}
	for _, g := range sink.snapshot() {
		releaseTestGroup(g)
		releaseTestGroup(g)
	}
	replicationAssertClean(t, source)
}
func TestReplicationNoopsRollbackAndCommitDespiteError(t *testing.T) {
	sink := &replicationTestSink{}
	db := openReplicationTest(t, basicSchema, replicationCaptureOptions(sink))
	u := &User{ID: 1, Name: "one"}
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, u) })
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, u); tx.DeleteByKey(usersTable, ID(999)); tx.Reindex(usersTable, nil) })
	if len(sink.snapshot()) != 1 {
		t.Fatal("no-op or index-only event")
	}
	appErr := errors.New("business rejection")
	if err := db.Tx(TxMetadata{}, true, func(tx *Tx) error { Put(tx, &User{ID: 2}); return appErr }); !errors.Is(err, appErr) {
		t.Fatal(err)
	}
	if len(sink.snapshot()) != 1 {
		t.Fatal("aborted group published")
	}
	if err := db.Tx(TxMetadata{}, true, func(tx *Tx) error { Put(tx, &User{ID: 3}); tx.CommitDespiteError(); return appErr }); !errors.Is(err, appErr) {
		t.Fatal(err)
	}
	groups := sink.snapshot()
	if len(groups) != 2 || groups[1].TransactionID != 2 {
		t.Fatalf("committed despite error: %+v", groups)
	}
	tx := db.BeginUpdate(TxMetadata{})
	Put(tx, &User{ID: 4})
	tx.Close()
	tx.Close()
	if len(sink.snapshot()) != 2 {
		t.Fatal("manual rollback published")
	}
	for _, g := range groups {
		releaseTestGroup(g)
	}
	replicationAssertClean(t, db)
}
func TestReplicationLimitsPoison(t *testing.T) {
	for _, mode := range []string{"size", "ram", "sink", "shard"} {
		t.Run(mode, func(t *testing.T) {
			sink := &replicationTestSink{}
			o := replicationCaptureOptions(sink)
			switch mode {
			case "size":
				sink.maxGroupBytes = 32
			case "ram":
				sink.maxBufferedBytes = 1 << 20
			case "sink":
				sink.fail = ErrReplicationOverload
			case "shard":
				o.Capture.Shard = func(ReplicationObject, []byte, ReplicationValue) ([]byte, error) {
					return nil, errors.New("unknown shard")
				}
			}
			db := openReplicationTest(t, basicSchema, o)
			if mode == "ram" {
				db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1}) })
			}
			err := db.Tx(TxMetadata{}, true, func(tx *Tx) error { tx.CommitDespiteError(); Put(tx, &User{ID: 2}); return nil })
			if err == nil {
				t.Fatal("capture failure committed")
			}
			db.Read(TxMetadata{}, func(tx *Tx) {
				if Get[User](tx, ID(2)) != nil {
					t.Fatal("failed mutation persisted")
				}
			})
			tx := db.BeginUpdate(TxMetadata{})
			_ = safelyCall(func(tx *Tx) error { Put(tx, &User{ID: 2}); return nil }, tx)
			if err := tx.Commit(); err == nil {
				t.Fatal("poisoned manual commit")
			}
			tx.Close()
			for _, g := range sink.snapshot() {
				releaseTestGroup(g)
			}
			replicationAssertClean(t, db)
		})
	}
	if _, err := Open(InMemory, basicSchema, Options{Replication: replicationCaptureOptions(&replicationTestSink{})}); err == nil {
		t.Fatal("unsupported backend admitted")
	}

}
func TestReplicationBatchSharesPhysicalGroup(t *testing.T) {
	sink := &replicationTestSink{}
	db := openReplicationTest(t, basicSchema, replicationCaptureOptions(sink))
	db.bdb.MaxBatchSize = 8
	db.bdb.MaxBatchDelay = time.Hour
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := db.Tx(TxMetadata{}, true, func(tx *Tx) error { Put(tx, &User{ID: ID(i + 1), Name: fmt.Sprint(i)}); return nil }); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	groups := sink.snapshot()
	if len(groups) != 1 || len(groups[0].Changes) != 8 {
		t.Fatalf("groups=%d", len(groups))
	}
	for i, c := range groups[0].Changes {
		if c.Ordinal != uint64(i) {
			t.Fatalf("ordinal=%d", c.Ordinal)
		}
	}
	releaseTestGroup(groups[0])
	replicationAssertClean(t, db)
}
func TestReplicationSchemaAtomicApplyAndFilteredGaps(t *testing.T) {
	sink := &replicationTestSink{}
	source := openReplicationTest(t, basicSchema, replicationCaptureOptions(sink))
	db := openReplicationTest(t, basicSchema, replicationApplyOptions())
	source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1}); Put(tx, &User{ID: 2}) })
	g := sink.snapshot()[0]
	defer releaseTestGroup(g)
	blocked := &ReplicationGroup{TransactionID: 1, SchemaRev: 4, Changes: g.Changes}
	if err := db.ApplyReplication(blocked); !errors.Is(err, ErrReplicationSchema) {
		t.Fatal(err)
	}
	changes := slices.Clone(g.Changes)
	changes[1].Object = ReplicationObject{}
	bad := &ReplicationGroup{TransactionID: 1, SchemaRev: 3, Changes: changes}
	if err := db.ApplyReplication(bad); err == nil {
		t.Fatal("invalid mapping admitted")
	}
	db.Read(TxMetadata{}, func(tx *Tx) {
		if CountAll(tx, usersTable) != 0 {
			t.Fatal("partial apply")
		}
	})
	if err := db.ApplyReplication(g); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyReplication(&ReplicationGroup{TransactionID: 9, SchemaRev: 3}); err != nil {
		t.Fatal(err)
	}
	committed, err := db.ReplicationStatus()
	if err != nil || committed != 9 {
		t.Fatalf("checkpoint %d: %v", committed, err)
	}
}
func TestReplicationLocalIndexesAndRebuild(t *testing.T) {
	sink := &replicationTestSink{}
	so := replicationCaptureOptions(sink)
	source := openReplicationTest(t, basicSchema, so)
	ro := replicationApplyOptions()
	replica := openReplicationTest(t, basicSchema, ro)
	source.Write(TxMetadata{}, func(tx *Tx) {
		Put(tx, &User{ID: 1, Email: "one", Name: "n"})
		Put(tx, &User{ID: 1, Email: "two", Name: "n"})
	})
	source.Write(TxMetadata{}, func(tx *Tx) { tx.Reindex(usersTable, nil) })
	source.Write(TxMetadata{}, func(tx *Tx) { tx.DeleteByKey(usersTable, ID(1)) })
	groups := sink.snapshot()
	if len(groups) != 2 {
		t.Fatal(len(groups))
	}
	for _, g := range groups {
		if err := replica.ApplyReplication(g); err != nil {
			t.Fatal(err)
		}
		releaseTestGroup(g)
	}
	replica.Read(TxMetadata{}, func(tx *Tx) {
		if Get[User](tx, ID(1)) != nil || Lookup[User](tx, usersByEmail, "two") != nil {
			t.Fatal("local index delete failed")
		}
	})
	replicationAssertClean(t, source)
}

func TestReplicationRawSemanticsAndExclusions(t *testing.T) {
	sc := new(Schema)
	raw := DefineRawTable(sc, "raw", nil)
	mp := AddKVMap(sc, "singletons")
	key := AddSingletonKey[string](mp, "key")
	sink := &replicationTestSink{}
	source := openReplicationTest(t, sc, replicationCaptureOptions(sink))
	replica := openReplicationTest(t, sc, replicationApplyOptions())
	source.Write(TxMetadata{}, func(tx *Tx) {
		tx.KVPutRaw(raw, []byte("key"), []byte("value"))
		SPutRaw(tx, key, nil)
		tx.KVPutRaw(raw, []byte("key"), nil)
	})
	g := sink.snapshot()[0]
	defer releaseTestGroup(g)
	if len(g.Changes) != 3 || g.Changes[1].Op != ReplicationPut || !g.Changes[1].Value.Present || g.Changes[2].Op != ReplicationDelete {
		t.Fatalf("changes=%+v", g.Changes)
	}
	if err := replica.ApplyReplication(g); err != nil {
		t.Fatal(err)
	}
	replica.Read(TxMetadata{}, func(tx *Tx) {
		if tx.KVGetRaw(raw, []byte("key")) != nil || SGetRaw(tx, key) == nil {
			t.Fatal("nil put/delete conflated")
		}
	})
	excluded := replicationCaptureOptions(&replicationTestSink{})
	excluded.Capture.Include = func(ReplicationObject) bool { return false }
	db := openReplicationTest(t, sc, excluded)
	db.Write(TxMetadata{}, func(tx *Tx) { tx.KVPutRaw(raw, []byte("x"), []byte("x")); SPutRaw(tx, key, nil) })
	head, err := db.ReplicationStatus()
	if err != nil || head != 0 {
		t.Fatalf("excluded source head=%d: %v", head, err)
	}
	replicationAssertClean(t, db)
}

func TestReplicationBatchRetryPublishesOnlyCommittedAttempt(t *testing.T) {
	sink := &replicationTestSink{}
	db := openReplicationTest(t, basicSchema, replicationCaptureOptions(sink))
	db.bdb.MaxBatchSize = 2
	db.bdb.MaxBatchDelay = time.Hour
	appErr := errors.New("reject second callback")
	var invocations, rejected atomic.Int32
	var attempts [2]atomic.Int32
	type result struct {
		id  int
		err error
	}
	done := make(chan result, 2)
	for i := range 2 {
		go func() {
			err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
				attempts[i].Add(1)
				Put(tx, &User{ID: ID(i + 1)})
				if invocations.Add(1) == 2 {
					rejected.Store(int32(i + 1))
					return appErr
				}
				return nil
			})
			done <- result{i, err}
		}()
	}
	var results [2]error
	for range 2 {
		r := <-done
		results[r.id] = r.err
	}
	loser := int(rejected.Load()) - 1
	if loser < 0 {
		t.Fatal("batch never reached the failing second callback")
	}
	winner := 1 - loser
	if results[winner] != nil || !errors.Is(results[loser], appErr) {
		t.Fatalf("results=%v", results)
	}
	if attempts[winner].Load() != 2 || attempts[loser].Load() != 1 {
		t.Fatalf("retry attempts=%d/%d", attempts[winner].Load(), attempts[loser].Load())
	}
	groups := sink.snapshot()
	if len(groups) != 1 || groups[0].TransactionID != 1 || len(groups[0].Changes) != 1 || !bytes.Equal(groups[0].Changes[0].Key, usersTable.EncodeKey(ID(winner+1))) {
		t.Fatalf("published=%+v", groups)
	}
	sink.mu.Lock()
	cancelled := sink.cancelled
	sink.mu.Unlock()
	if cancelled != 1 {
		t.Fatalf("aborted attempt cancellations=%d", cancelled)
	}
	releaseTestGroup(groups[0])
	replicationAssertClean(t, db)
}

func TestReplicationReplicaNeverMigrates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	old := new(Schema)
	DefineTable(old, "users", testTableModel[User](), func(b *TableBuilder[ID]) {}).TableDef()
	db, err := Open(path, old, Options{IsTesting: true})
	if err != nil {
		t.Fatal(err)
	}
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "source"}) })
	db.Close()
	sc := new(Schema)
	var idx *IndexDef
	tbl := DefineTable(sc, "users", testIndexAccess[User](indexName), func(b *TableBuilder[ID]) {
		b.SetSchemaVersion(2)
		b.AddMigrator(2, func(*Tx, ID, *kvo.Editor) { t.Error("replica migrator invoked") })
		idx = b.AddIndex[string]("name", nil).IndexDef()

		b.SetIndexer(func(_ ID, r kvo.Record, ib *IndexBuilder) { ib.Add(idx, r.Get(indexName)) })
	}).TableDef()
	db, err = Open(path, sc, Options{IsTesting: true, LocalWrites: LocalWritePolicy{Mode: RejectLocalWrites}, RowMigrations: SkipRowMigrations, Replication: replicationApplyOptions()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.AllowsRowMigrations() {
		t.Fatal("replica allows migration jobs")
	}
	db.Read(TxMetadata{}, func(tx *Tx) {
		if Get[User](tx, ID(1)).Name != "source" || Lookup[User](tx, idx, "source") == nil {
			t.Fatal("startup index build")
		}
	})
	db.Write(TxMetadata{}, func(tx *Tx) { tx.Reindex(tbl, nil) })
	db.Read(TxMetadata{}, func(tx *Tx) {
		v := rowReplicationValue(tbl.dataBucketIn(tx).Get(tbl.EncodeKey(ID(1))))
		if v.SchemaVer != 1 || v.ModCount != 1 {
			t.Fatal("reindex changed source metadata")
		}
	})
}

func TestReplicationNestedDeleteOrderAndStorageFailure(t *testing.T) {
	sink := &replicationTestSink{}
	o := replicationCaptureOptions(sink)
	db := openReplicationTest(t, basicSchema, o)
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Email: "old"}) })
	deletedBeforeNotify := false
	db.Write(TxMetadata{}, func(tx *Tx) {
		tx.OnChange(map[*TableDef]ChangeFlags{usersTable: ChangeFlagNotify}, func(tx *Tx, c *Change) {
			if c.op == OpDelete {
				deletedBeforeNotify = Get[User](tx, ID(1)) == nil
				Put(tx, &User{ID: 2, Email: "nested"})
			}
		})
		tx.DeleteByKey(usersTable, ID(1))
	})
	changes := sink.snapshot()[1].Changes
	if !deletedBeforeNotify || len(changes) != 2 || changes[0].Object.Table != usersTable || changes[0].Op != ReplicationDelete || changes[1].Op != ReplicationPut {
		t.Fatalf("nested data order: %+v", changes)
	}

	err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
		tx.CommitDespiteError()
		ensure(tx.stx.DeleteBucket(usersTable.name, usersByEmail.buck))
		Put(tx, &User{ID: 3, Email: "broken"})
		return nil
	})
	if err == nil {
		t.Fatal("missing required index bucket committed primary")
	}
	db.Read(TxMetadata{}, func(tx *Tx) {
		if Get[User](tx, ID(3)) != nil || Lookup[User](tx, usersByEmail, "nested") == nil {
			t.Fatal("partial primary/index commit")
		}
	})
	for _, g := range sink.snapshot() {
		releaseTestGroup(g)
	}
	replicationAssertClean(t, db)
}

func TestReplicationCodecUpgradeAndPrivateEdits(t *testing.T) {
	modelSchema := kvo.NewSchema()
	entity := kvo.NewEntityType(modelSchema, "row", reflect.TypeFor[keylessRecordRow]())
	modelProp := kvo.NewProp(modelSchema, 1, "name", kvo.TMachineString, nil)
	model := kvo.NewModel(modelSchema, entity, func(b *kvo.ModelBuilder) { b.Prop(modelSchema.MustPropByCode(modelProp)) })
	sc := new(Schema)
	tbl := DefineTable(sc, "rows", model, func(b *TableBuilder[uint64]) {
		b.SetRecordStorage(RecordStorage{Format: Compact2, MessagePackInserts: true})
	}).TableDef()
	sink := &replicationTestSink{}
	source := openReplicationTest(t, sc, replicationCaptureOptions(sink))
	replica := openReplicationTest(t, sc, replicationApplyOptions())
	source.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &keylessRecordRow{ID: 1, Name: "legacy"}) })
	source.Write(TxMetadata{}, func(tx *Tx) {
		if !tx.UpgradeRecord(tbl, uint64(1)) {
			t.Fatal("no codec upgrade")
		}
	})
	groups := sink.snapshot()
	if len(groups) != 2 || groups[0].Changes[0].Value.Flags != uint64(vfVer1) || groups[1].Changes[0].Value.Flags != uint64(vfKVO) {
		t.Fatal("codec capture")
	}
	old, next := groups[0].Changes[0].Value, groups[1].Changes[0].Value
	if old.ModCount != next.ModCount || old.SchemaVer != next.SchemaVer {
		t.Fatal("upgrade changed metadata")
	}
	source.Write(TxMetadata{}, func(tx *Tx) {
		if tx.UpgradeRecord(tbl, uint64(1)) {
			t.Fatal("repeated upgrade wrote")
		}
		e := tx.OpenRecordEdit(tbl, uint64(1))
		ensure(e.Editor().TrySetValue(1, values.String("unsaved")))
		_, err := tx.TryEditRecord(tbl, uint64(1), func(e *kvo.Editor) error {
			ensure(e.TrySetValue(1, values.String("discarded")))
			return errors.New("discard")
		})
		if err == nil {
			t.Fatal("edit accepted")
		}
	})
	if len(sink.snapshot()) != 2 {
		t.Fatal("private edit captured")
	}
	source.Write(TxMetadata{}, func(tx *Tx) {
		e := tx.OpenRecordEdit(tbl, uint64(1))
		stale := tx.OpenRecordEdit(tbl, uint64(1))
		ensure(e.Editor().TrySetValue(1, values.String("saved")))
		tx.SaveRecord(e)
		ensure(stale.Editor().TrySetValue(1, values.String("stale")))
		if _, err := tx.TrySaveRecord(stale); err == nil {
			t.Fatal("stale save accepted")
		}
	})
	if g := sink.snapshot()[2]; len(g.Changes) != 1 {
		t.Fatal("stale save captured")
	}
	source.Write(TxMetadata{}, func(tx *Tx) { tx.UnsafeDeleteByKeyRawSkippingIndex(tbl, tbl.EncodeKey(uint64(1))) })
	for _, g := range sink.snapshot() {
		if err := replica.ApplyReplication(g); err != nil {
			t.Fatal(err)
		}
		releaseTestGroup(g)
	}
	replica.Read(TxMetadata{}, func(tx *Tx) {
		if Get[keylessRecordRow](tx, uint64(1)) != nil {
			t.Fatal("unsafe delete not applied")
		}
	})
	replicationAssertClean(t, source)
}

func TestReplicationStatus_reports_committed_ids_without_WAL_policy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	sink := &replicationTestSink{}
	opt := Options{IsTesting: true, Replication: replicationCaptureOptions(sink)}
	open := func(opt Options) *DB {
		t.Helper()
		db, err := Open(path, basicSchema, opt)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(db.Close)
		return db
	}
	db := open(opt)
	assertStatus := func(want uint64) {
		t.Helper()
		head, err := db.ReplicationStatus()
		if err != nil || head != want {
			t.Fatalf("head=%d; want %d: %v", head, want, err)
		}
	}
	assertStatus(0)
	tx := db.BeginUpdate(TxMetadata{})
	Put(tx, &User{ID: 1, Name: "rolled back"})
	assertStatus(0)
	tx.Close()
	assertStatus(0)
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "committed"}) })
	assertStatus(1)
	db.Close()
	// No WAL exists: the consumer, not Open, decides whether capture can continue.
	db = open(opt)
	assertStatus(1)
	db.Close()
	db = open(Options{IsTesting: true, LocalWrites: LocalWritePolicy{Mode: RejectLocalWrites}, RowMigrations: SkipRowMigrations})
	assertStatus(1)
	db.Close()
	db = open(opt)
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 2, Name: "consumer chose to continue"}) })
	assertStatus(2)
	groups := sink.snapshot()
	if len(groups) != 2 || groups[1].TransactionID != 2 {
		t.Fatalf("groups=%+v", groups)
	}
}

func TestReplicationStatus_without_capture_metadata(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), basicSchema, Options{IsTesting: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	head, err := db.ReplicationStatus()
	if err != nil || head != 0 {
		t.Fatalf("empty status=%d: %v", head, err)
	}
}

func rowReplicationValue(raw []byte) ReplicationValue {
	if raw == nil {
		return ReplicationValue{}
	}
	var v value
	ensure(v.decode(raw, false))
	return v.replicationValue(true)
}
