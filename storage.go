package edb

import "errors"

// ErrBucketNotFound is returned by StorageTx.DeleteBucket when the bucket doesn't exist.
var ErrBucketNotFound = errors.New("bucket not found")

// storage represents a key-value storage backend (Bolt, in-memory, Badger, etc.).
type storage interface {
	// BeginTx starts a new transaction.
	BeginTx(writable bool) (storageTx, error)
	// Close closes the storage.
	Close() error
}

// storageTx represents a storage transaction.
type storageTx interface {
	// Writable returns true if this is a writable transaction.
	Writable() bool

	// Bucket returns a bucket. Use sub="" for a root bucket, non-empty for a nested bucket.
	// Returns nil if the bucket doesn't exist.
	Bucket(name, sub string) storageBucket

	// CreateBucket creates a bucket if it doesn't exist.
	// For sub != "", it must also ensure the root bucket exists.
	CreateBucket(name, sub string) (storageBucket, error)

	// DeleteBucket deletes a nested bucket (sub must be non-empty).
	DeleteBucket(name, sub string) error

	// Commit commits the transaction.
	Commit() error

	// Rollback aborts the transaction. It should be safe to call multiple times.
	Rollback() error

	// Size returns the database size in bytes (0 if unknown / not applicable).
	Size() int64
}

// storageBucket represents a bucket (sorted key-value collection).
type storageBucket interface {
	// Get retrieves a value by key. Returns nil if not found.
	Get(key []byte) []byte

	// Put stores a key-value pair.
	Put(key, value []byte) error

	// Delete removes a key.
	Delete(key []byte) error

	// Cursor returns a cursor for iteration.
	Cursor() storageCursor

	// Stats returns storage-specific bucket statistics.
	// Backends that don't track allocation sizes may return zero values except KeyN.
	Stats() bucketStats

	// KeyCount returns the number of keys in the bucket (best effort).
	KeyCount() int
}

type bucketStats struct {
	KeyN       int
	LeafInuse  int64
	LeafAlloc  int64
	BranchAlloc int64
}

func (s bucketStats) TotalAlloc() int64 { return s.BranchAlloc + s.LeafAlloc }

// storageCursor iterates over a sorted bucket.
type storageCursor interface {
	// First moves to the first key-value pair.
	First() (key, value []byte)

	// Last moves to the last key-value pair.
	Last() (key, value []byte)

	// Seek moves to the first key >= seek.
	Seek(seek []byte) (key, value []byte)

	// SeekLast moves to the last key strictly before the successor of the given prefix/boundary.
	// This is commonly implemented as: Seek(inc(prefix)) then Prev().
	SeekLast(prefix []byte) (key, value []byte)

	// Next moves to the next key-value pair.
	Next() (key, value []byte)

	// Prev moves to the previous key-value pair.
	Prev() (key, value []byte)

	// Delete deletes the current key-value pair.
	Delete() error
}
