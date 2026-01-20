package edb

import (
	"bytes"
	"unsafe"

	"go.etcd.io/bbolt"
)

type boltStorage struct {
	bdb *bbolt.DB
}

func newBoltStorage(bdb *bbolt.DB) storage {
	return &boltStorage{bdb: bdb}
}

func (s *boltStorage) BeginTx(writable bool) (storageTx, error) {
	btx, err := s.bdb.Begin(writable)
	if err != nil {
		return nil, err
	}
	return &boltStorageTx{btx: btx}, nil
}

func (s *boltStorage) Close() error {
	return s.bdb.Close()
}

type boltStorageTx struct {
	btx *bbolt.Tx
}

func (tx *boltStorageTx) BoltTx() *bbolt.Tx { return tx.btx }

func (tx *boltStorageTx) Writable() bool { return tx.btx.Writable() }

func (tx *boltStorageTx) Bucket(name, sub string) storageBucket {
	root := tx.btx.Bucket(unsafeBytesFromString(name))
	if root == nil {
		return nil
	}
	if sub == "" {
		return boltBucket{b: root}
	}
	leaf := root.Bucket(unsafeBytesFromString(sub))
	if leaf == nil {
		return nil
	}
	return boltBucket{b: leaf}
}

func (tx *boltStorageTx) CreateBucket(name, sub string) (storageBucket, error) {
	if sub == "" {
		b, err := tx.btx.CreateBucketIfNotExists(unsafeBytesFromString(name))
		if err != nil {
			return nil, err
		}
		return boltBucket{b: b}, nil
	}
	root, err := tx.btx.CreateBucketIfNotExists(unsafeBytesFromString(name))
	if err != nil {
		return nil, err
	}
	leaf, err := root.CreateBucketIfNotExists(unsafeBytesFromString(sub))
	if err != nil {
		return nil, err
	}
	return boltBucket{b: leaf}, nil
}

func (tx *boltStorageTx) DeleteBucket(name, sub string) error {
	if sub == "" {
		return ErrBucketNotFound
	}
	root := tx.btx.Bucket(unsafeBytesFromString(name))
	if root == nil {
		return ErrBucketNotFound
	}
	err := root.DeleteBucket(unsafeBytesFromString(sub))
	if err == bbolt.ErrBucketNotFound {
		return ErrBucketNotFound
	}
	return err
}

func (tx *boltStorageTx) Commit() error { return tx.btx.Commit() }

func (tx *boltStorageTx) Rollback() error {
	err := tx.btx.Rollback()
	if err == bbolt.ErrTxClosed {
		return nil
	}
	return err
}

func (tx *boltStorageTx) Size() int64 { return tx.btx.Size() }

type boltBucket struct {
	b *bbolt.Bucket
}

func (b boltBucket) Get(key []byte) []byte { return b.b.Get(key) }

func (b boltBucket) Put(key, value []byte) error { return b.b.Put(key, value) }

func (b boltBucket) Delete(key []byte) error { return b.b.Delete(key) }

func (b boltBucket) Cursor() storageCursor { return boltCursor{c: b.b.Cursor()} }

func (b boltBucket) Stats() bucketStats {
	s := b.b.Stats()
	return bucketStats{
		KeyN:        s.KeyN,
		LeafInuse:   int64(s.LeafInuse),
		LeafAlloc:   int64(s.LeafAlloc),
		BranchAlloc: int64(s.BranchAlloc),
	}
}

func (b boltBucket) KeyCount() int { return b.b.Stats().KeyN }

type boltCursor struct {
	c *bbolt.Cursor
}

func (c boltCursor) First() ([]byte, []byte) { return c.c.First() }

func (c boltCursor) Last() ([]byte, []byte) { return c.c.Last() }

func (c boltCursor) Seek(seek []byte) ([]byte, []byte) { return c.c.Seek(seek) }

func (c boltCursor) SeekLast(prefix []byte) ([]byte, []byte) {
	if prefix == nil || len(prefix) == 0 {
		return c.c.Last()
	}

	limit := append([]byte(nil), prefix...)
	if inc(limit) {
		k, _ := c.c.Seek(limit)
		if k == nil {
			return c.c.Last()
		}
		return c.c.Prev()
	}

	// All-0xFF prefix: fall back to linear scan.
	k, _ := c.c.Seek(prefix)
	if k == nil {
		return c.c.Last()
	}
	for k != nil && bytes.HasPrefix(k, prefix) {
		k, _ = c.c.Next()
	}
	if k == nil {
		return c.c.Last()
	}
	return c.c.Prev()
}

func (c boltCursor) Next() ([]byte, []byte) { return c.c.Next() }

func (c boltCursor) Prev() ([]byte, []byte) { return c.c.Prev() }

func (c boltCursor) Delete() error { return c.c.Delete() }

func unsafeBytesFromString(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
