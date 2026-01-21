package edb

import (
	"bytes"
	"fmt"
	"slices"
	"sort"
	"sync"
)

const memBucketSep = "\x00"

type memStorage struct {
	mu      sync.Mutex
	cond    *sync.Cond
	buckets map[string]*memBucket
	closed  bool
	writer  bool
}

// newMemStorage returns a transient in-memory Storage implementation intended for tests.
func newMemStorage() storage {
	s := &memStorage{buckets: make(map[string]*memBucket)}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *memStorage) BeginTx(writable bool) (storageTx, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, fmt.Errorf("storage closed")
	}
	if writable {
		for s.writer && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return nil, fmt.Errorf("storage closed")
		}
		s.writer = true
	}

	// Snapshot the current root map pointer for MVCC-style isolation:
	// - Read-only txs share the snapshot (immutable).
	// - Writable txs get a shallow map copy + per-bucket copy-on-write.
	baseBuckets := s.buckets
	s.mu.Unlock()

	if !writable {
		return &memTx{
			writable: false,
			base:     s,
			buckets:  baseBuckets,
		}, nil
	}

	buckets := make(map[string]*memBucket, len(baseBuckets))
	for k, b := range baseBuckets {
		buckets[k] = b
	}
	return &memTx{
		writable: true,
		base:     s,
		buckets:  buckets,
		owned:    make(map[string]struct{}),
	}, nil
}

func (s *memStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.buckets = nil
	if s.cond != nil {
		s.cond.Broadcast()
	}
	return nil
}

type memTx struct {
	base     *memStorage
	writable bool
	buckets  map[string]*memBucket
	owned    map[string]struct{}
	closed   bool
}

func (tx *memTx) Writable() bool { return tx.writable }

func (tx *memTx) closeLocked() {
	if tx.closed {
		return
	}
	tx.closed = true
	if tx.writable {
		tx.base.writer = false
		tx.base.cond.Broadcast()
	}
}

func (tx *memTx) Bucket(name, sub string) storageBucket {
	if tx.closed {
		panic("tx is closed")
	}
	key := memBucketKey(name, sub)
	if tx.buckets[key] == nil {
		return nil
	}
	return memBucketHandle{tx: tx, key: key}
}

func (tx *memTx) CreateBucket(name, sub string) (storageBucket, error) {
	if tx.closed {
		panic("tx is closed")
	}
	if !tx.writable {
		return nil, fmt.Errorf("tx not writable")
	}

	// Ensure the root exists for nested buckets (Bolt compatibility).
	rootKey := memBucketKey(name, "")
	if tx.buckets[rootKey] == nil {
		tx.buckets[rootKey] = &memBucket{}
		if tx.owned != nil {
			tx.owned[rootKey] = struct{}{}
		}
	}

	key := memBucketKey(name, sub)
	b := tx.buckets[key]
	if b == nil {
		b = &memBucket{}
		tx.buckets[key] = b
		if tx.owned != nil {
			tx.owned[key] = struct{}{}
		}
	}
	return memBucketHandle{tx: tx, key: key}, nil
}

func (tx *memTx) DeleteBucket(name, sub string) error {
	if tx.closed {
		panic("tx is closed")
	}
	if !tx.writable {
		return fmt.Errorf("tx not writable")
	}
	if sub == "" {
		return ErrBucketNotFound
	}
	key := memBucketKey(name, sub)
	if tx.buckets[key] == nil {
		return ErrBucketNotFound
	}
	delete(tx.buckets, key)
	delete(tx.owned, key)
	return nil
}

func (tx *memTx) Commit() error {
	if tx.closed {
		return nil
	}
	if !tx.writable {
		return fmt.Errorf("tx not writable")
	}
	tx.base.mu.Lock()
	defer tx.base.mu.Unlock()
	if tx.base.closed {
		tx.closeLocked()
		return fmt.Errorf("storage closed")
	}
	tx.base.buckets = tx.buckets
	tx.closeLocked()
	return nil
}

func (tx *memTx) Rollback() error {
	tx.base.mu.Lock()
	defer tx.base.mu.Unlock()
	tx.closeLocked()
	return nil
}

func (tx *memTx) Size() int64 { return 0 }

func memBucketKey(name, sub string) string {
	return name + memBucketSep + sub
}

type memBucket struct {
	items []memKV // sorted by key
}

func (b *memBucket) clone() *memBucket {
	if b == nil {
		return nil
	}
	out := &memBucket{items: make([]memKV, len(b.items))}
	for i, kv := range b.items {
		out.items[i] = memKV{
			key:   slices.Clone(kv.key),
			value: slices.Clone(kv.value),
		}
	}
	return out
}

func (b *memBucket) shallowClone() *memBucket {
	if b == nil {
		return nil
	}
	return &memBucket{items: slices.Clone(b.items)}
}

type memKV struct {
	key   []byte
	value []byte
}

type memBucketHandle struct {
	tx  *memTx
	key string
}

func (b memBucketHandle) Get(key []byte) []byte {
	bucket := b.bucket()
	if bucket == nil {
		return nil
	}
	i, ok := b.find(bucket, key)
	if !ok {
		return nil
	}
	return bucket.items[i].value
}

func (b memBucketHandle) Put(key, value []byte) error {
	if !b.tx.writable {
		return fmt.Errorf("tx not writable")
	}
	bucket := b.tx.ensureBucketOwned(b.key)
	if bucket == nil {
		return ErrBucketNotFound
	}

	i, ok := b.find(bucket, key)
	if ok {
		bucket.items[i].value = slices.Clone(value)
		return nil
	}
	bucket.items = slices.Insert(bucket.items, i, memKV{key: slices.Clone(key), value: slices.Clone(value)})
	return nil
}

func (b memBucketHandle) Delete(key []byte) error {
	if !b.tx.writable {
		return fmt.Errorf("tx not writable")
	}
	bucket := b.tx.ensureBucketOwned(b.key)
	if bucket == nil {
		return ErrBucketNotFound
	}

	i, ok := b.find(bucket, key)
	if !ok {
		return nil
	}
	bucket.items = slices.Delete(bucket.items, i, i+1)
	return nil
}

func (b memBucketHandle) Cursor() storageCursor {
	return &memCursor{tx: b.tx, bucketKey: b.key, pos: -1}
}

func (b memBucketHandle) Stats() bucketStats {
	bucket := b.bucket()
	if bucket == nil {
		return bucketStats{}
	}
	var inuse int64
	for _, kv := range bucket.items {
		inuse += int64(len(kv.key) + len(kv.value))
	}
	return bucketStats{
		KeyN:      len(bucket.items),
		LeafInuse: inuse,
		LeafAlloc: inuse,
	}
}

func (b memBucketHandle) KeyCount() int {
	bucket := b.bucket()
	if bucket == nil {
		return 0
	}
	return len(bucket.items)
}

func (b memBucketHandle) bucket() *memBucket {
	return b.tx.buckets[b.key]
}

func (b memBucketHandle) find(bucket *memBucket, key []byte) (idx int, ok bool) {
	items := bucket.items
	i := sort.Search(len(items), func(i int) bool {
		return bytes.Compare(items[i].key, key) >= 0
	})
	if i < len(items) && bytes.Equal(items[i].key, key) {
		return i, true
	}
	return i, false
}

type memCursor struct {
	tx        *memTx
	bucketKey string
	pos       int
}

func (c *memCursor) First() ([]byte, []byte) {
	items := c.items()
	if len(items) == 0 {
		c.pos = 0
		return nil, nil
	}
	c.pos = 0
	kv := items[c.pos]
	return kv.key, kv.value
}

func (c *memCursor) Last() ([]byte, []byte) {
	items := c.items()
	if len(items) == 0 {
		c.pos = 0
		return nil, nil
	}
	c.pos = len(items) - 1
	kv := items[c.pos]
	return kv.key, kv.value
}

func (c *memCursor) Seek(seek []byte) ([]byte, []byte) {
	items := c.items()
	i := sort.Search(len(items), func(i int) bool {
		return bytes.Compare(items[i].key, seek) >= 0
	})
	c.pos = i
	if i >= len(items) {
		return nil, nil
	}
	kv := items[i]
	return kv.key, kv.value
}

func (c *memCursor) SeekLast(prefix []byte) ([]byte, []byte) {
	if prefix == nil || len(prefix) == 0 {
		return c.Last()
	}

	limit := append([]byte(nil), prefix...)
	if inc(limit) {
		items := c.items()
		i := sort.Search(len(items), func(i int) bool {
			return bytes.Compare(items[i].key, limit) >= 0
		})
		if i == 0 {
			c.pos = 0
			return nil, nil
		}
		c.pos = i - 1
		kv := items[c.pos]
		return kv.key, kv.value
	}

	// All-0xFF prefix.
	return c.Last()
}

func (c *memCursor) Next() ([]byte, []byte) {
	if c.pos < 0 {
		return c.First()
	}
	c.pos++
	items := c.items()
	if c.pos >= len(items) {
		return nil, nil
	}
	kv := items[c.pos]
	return kv.key, kv.value
}

func (c *memCursor) Prev() ([]byte, []byte) {
	if c.pos < 0 {
		return nil, nil
	}
	c.pos--
	items := c.items()
	if c.pos < 0 || c.pos >= len(items) {
		return nil, nil
	}
	kv := items[c.pos]
	return kv.key, kv.value
}

func (c *memCursor) Delete() error {
	if !c.tx.writable {
		return fmt.Errorf("tx not writable")
	}
	bucket := c.tx.ensureBucketOwned(c.bucketKey)
	if bucket == nil {
		return ErrBucketNotFound
	}
	if c.pos < 0 || c.pos >= len(bucket.items) {
		return nil
	}
	bucket.items = slices.Delete(bucket.items, c.pos, c.pos+1)
	c.pos--
	return nil
}

func (c *memCursor) items() []memKV {
	b := c.tx.buckets[c.bucketKey]
	if b == nil {
		return nil
	}
	return b.items
}

func (tx *memTx) ensureBucketOwned(key string) *memBucket {
	if !tx.writable {
		return tx.buckets[key]
	}
	if tx.owned != nil {
		if _, ok := tx.owned[key]; ok {
			return tx.buckets[key]
		}
	}
	b := tx.buckets[key]
	if b == nil {
		return nil
	}
	nb := b.shallowClone()
	tx.buckets[key] = nb
	if tx.owned != nil {
		tx.owned[key] = struct{}{}
	}
	return nb
}
