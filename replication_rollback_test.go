package edb

import (
	"errors"
	"strings"
	"testing"
)

func TestDelete_manual_and_memory_poison_incomplete_mutations(t *testing.T) {
	for _, mode := range []string{"memory", "bolt", "capture"} {
		t.Run(mode, func(t *testing.T) {
			sink := &replicationTestSink{}
			var db *DB
			if mode == "memory" {
				var err error
				db, err = Open(InMemory, basicSchema, Options{IsTesting: true})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(db.Close)
			} else {
				var opt *ReplicationOptions
				if mode == "capture" {
					opt = replicationCaptureOptions(sink)
				}
				db = openReplicationTest(t, basicSchema, opt)
			}
			db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Email: "seed@example.com"}) })
			remove := func(tx *Tx) error {
				tx.CommitDespiteError()
				tx.OnChange(map[*TableDef]ChangeFlags{usersTable: ChangeFlagNotify}, func(*Tx, *Change) { panic("handler failed") })
				tx.DeleteByKey(usersTable, ID(1))
				return nil
			}
			tx := db.BeginUpdate(TxMetadata{})
			if err := safelyCall(remove, tx); err == nil {
				tx.Close()
				t.Fatal("missing handler failure")
			}
			commitErr := tx.Commit()
			tx.Close()
			if commitErr == nil {
				t.Fatal("manual commit accepted incomplete mutation")
			}
			for _, caught := range []bool{false, true} {
				err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
					if caught {
						_ = safelyCall(remove, tx)
						return nil
					}
					return remove(tx)
				})
				if err == nil {
					t.Fatal("batch committed incomplete mutation")
				}
				if !caught && !strings.Contains(err.Error(), "handler failed") {
					t.Fatalf("original failure lost: %v", err)
				}
			}
			db.Read(TxMetadata{}, func(tx *Tx) {
				if Get[User](tx, ID(1)) == nil || Lookup[User](tx, usersByEmail, "seed@example.com") == nil {
					t.Fatal("failed delete changed row or index")
				}
			})
			if mode == "capture" {
				requireReplicationID(t, db, 1)
				if len(sink.snapshot()) != 1 {
					t.Fatal("failed delete published")
				}
			}
			for _, g := range sink.snapshot() {
				releaseTestGroup(g)
			}
			if mode == "capture" {
				replicationAssertClean(t, db)
			}
		})
	}
}

func TestReplicationSize_rolls_back_post_delete_handler_writes(t *testing.T) {
	sink := &replicationTestSink{}
	opt := replicationCaptureOptions(sink)
	sink.maxGroupBytes = 900
	db := openReplicationTest(t, basicSchema, opt)
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1, Name: "seed", Email: "parent@example.com"}) })
	nested := 0
	err := db.Tx(TxMetadata{}, true, func(tx *Tx) error {
		tx.CommitDespiteError()
		tx.OnChange(map[*TableDef]ChangeFlags{usersTable: ChangeFlagNotify}, func(tx *Tx, c *Change) {
			if c.op == OpDelete {
				Put(tx, &User{ID: 2, Name: "child", Email: "2"})
				nested++
				Put(tx, &User{ID: 3, Name: "child", Email: "3"})
				nested++
			}
		})
		tx.DeleteByKey(usersTable, ID(1))
		return nil
	})
	if !errors.Is(err, ErrReplicationGroupSize) || nested != 1 {
		t.Fatalf("nested=%d error=%v", nested, err)
	}
	requireReplicationID(t, db, 1)
	db.Read(TxMetadata{}, func(tx *Tx) {
		if Get[User](tx, ID(1)) == nil || Lookup[User](tx, usersByEmail, "parent@example.com") == nil || Get[User](tx, ID(2)) != nil || Get[User](tx, ID(3)) != nil {
			t.Fatal("late size failure committed partial data")
		}
	})
	if len(sink.snapshot()) != 1 {
		t.Fatal("oversized group published")
	}
	for _, g := range sink.snapshot() {
		releaseTestGroup(g)
	}
	replicationAssertClean(t, db)
}
