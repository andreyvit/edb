package edb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadReplicationStatusDoesNotCreateOrMigrateDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.db")
	if id, err := ReadReplicationStatus(path); err != nil || id != 0 {
		t.Fatalf("missing preflight: %d %v", id, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("preflight created the source database")
	}
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if id, err := ReadReplicationStatus(path); err != nil || id != 0 {
		t.Fatalf("empty preflight: %d %v", id, err)
	}
	sink := &replicationTestSink{}
	db, err := Open(path, basicSchema, Options{IsTesting: true, Replication: replicationCaptureOptions(sink)})
	if err != nil {
		t.Fatal(err)
	}
	db.Write(TxMetadata{}, func(tx *Tx) { Put(tx, &User{ID: 1}) })
	db.Close()
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := ReadReplicationStatus(path); err != nil || id != 1 {
		t.Fatalf("existing checkpoint: %d %v", id, err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("preflight mutated source metadata")
	}
}
