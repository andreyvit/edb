package kvolab

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/kvo"
	"go.etcd.io/bbolt"
)

func BenchmarkStorage(b *testing.B) {
	for _, durable := range []bool{false, true} {
		for _, name := range []string{"kvo-double-pack", "kvo-single-pack", "words-copy-patch"} {
			b.Run(fmt.Sprintf("durable%t/%s", durable, name), func(b *testing.B) {
				s := &edb.Schema{}
				tbl := edb.DefineRawTable(s, "records", nil)
				db, err := edb.Open(filepath.Join(b.TempDir(), "experiment.db"), s, edb.Options{IsTesting: !durable, MmapSize: 16 << 20})
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				v := numeric(128, 0)
				var initial []byte
				if name == "words-copy-patch" {
					initial = EncodeWords(v)
				} else {
					initial = oldKVO(v).Bytes()
				}
				key := []byte("row")
				db.Write(func(tx *edb.Tx) { tx.KVPutRaw(tbl, key, initial) })
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					db.Write(func(tx *edb.Tx) {
						raw := tx.KVGetRaw(tbl, key)
						if name == "words-copy-patch" {
							out := bytes.Clone(raw)
							Words(out).SetExisting(17, uint64(i+1))
							tx.KVPutRaw(tbl, key, out)
						} else {
							m := kvo.UpdateRecord(kvo.LoadRecord(raw, nil))
							m.Set(17, uint64(i+1))
							if name == "kvo-double-pack" {
								tx.KVPut(tbl, key, m.Record())
							} else {
								tx.KVPutRaw(tbl, key, m.Record().Pack().Bytes())
							}
						}
					})
				}
				b.StopTimer()
				db.Read(func(tx *edb.Tx) {
					raw := tx.KVGetRaw(tbl, key)
					var got uint64
					if name == "words-copy-patch" {
						got = Words(raw).Get(17)
					} else {
						got = kvo.LoadRecord(raw, nil).Root().Get(17)
					}
					if got != uint64(b.N) {
						b.Fatalf("persisted %d expected %d", got, b.N)
					}
				})
			})
		}
	}
}

// Shows why reusable output storage must belong to the whole transaction.
func TestBoltRetainsPutBytes(t *testing.T) {
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "lifetime.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("rows"))
		if err != nil {
			return err
		}
		buf := []byte("first")
		if err := bucket.Put([]byte("a"), buf); err != nil {
			return err
		}
		copy(buf, "other")
		if got := bucket.Get([]byte("a")); string(got) != "other" {
			t.Fatalf("expected retained borrowed bytes, got %q", got)
		}
		return bucket.Put([]byte("a"), []byte("saved"))
	})
	if err != nil {
		t.Fatal(err)
	}
}
