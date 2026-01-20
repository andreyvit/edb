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
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("storage closed")
	}
	if writable {
		for s.writer && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			return nil, fmt.Errorf("storage closed")
		}
		s.writer = true
	}

	// Snapshot the entire DB for transactional isolation (simplicity over efficiency).
	snap := make(map[string]*memBucket, len(s.buckets))
	for k, b := range s.buckets {
		snap[k] = b.clone()
	}

	return &memTx{
		writable: writable,
		base:     s,
		buckets:  snap,
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
	b := tx.buckets[memBucketKey(name, sub)]
	if b == nil {
		return nil
	}
	return memBucketHandle{tx: tx, b: b}
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
	}

	key := memBucketKey(name, sub)
	b := tx.buckets[key]
	if b == nil {
		b = &memBucket{}
		tx.buckets[key] = b
	}
	return memBucketHandle{tx: tx, b: b}, nil
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

type memKV struct {
	key   []byte
	value []byte
}

type memBucketHandle struct {
	tx *memTx
	b  *memBucket
}

func (b memBucketHandle) Get(key []byte) []byte {
	i, ok := b.find(key)
	if !ok {
		return nil
	}
	return b.b.items[i].value
}

func (b memBucketHandle) Put(key, value []byte) error {
	if !b.tx.writable {
		return fmt.Errorf("tx not writable")
	}
	key = slices.Clone(key)
	value = slices.Clone(value)

	i, ok := b.find(key)
	if ok {
		b.b.items[i].value = value
		return nil
	}
	b.b.items = slices.Insert(b.b.items, i, memKV{key: key, value: value})
	return nil
}

func (b memBucketHandle) Delete(key []byte) error {
	if !b.tx.writable {
		return fmt.Errorf("tx not writable")
	}
	i, ok := b.find(key)
	if !ok {
		return nil
	}
	b.b.items = slices.Delete(b.b.items, i, i+1)
	return nil
}

func (b memBucketHandle) Cursor() storageCursor {
	return &memCursor{tx: b.tx, b: b.b, pos: -1}
}

func (b memBucketHandle) Stats() bucketStats {
	var inuse int64
	for _, kv := range b.b.items {
		inuse += int64(len(kv.key) + len(kv.value))
	}
	return bucketStats{
		KeyN:      len(b.b.items),
		LeafInuse: inuse,
		LeafAlloc: inuse,
	}
}

func (b memBucketHandle) KeyCount() int { return len(b.b.items) }

func (b memBucketHandle) find(key []byte) (idx int, ok bool) {
	items := b.b.items
	i := sort.Search(len(items), func(i int) bool {
		return bytes.Compare(items[i].key, key) >= 0
	})
	if i < len(items) && bytes.Equal(items[i].key, key) {
		return i, true
	}
	return i, false
}

type memCursor struct {
	tx  *memTx
	b   *memBucket
	pos int
}

func (c *memCursor) First() ([]byte, []byte) {
	if len(c.b.items) == 0 {
		c.pos = 0
		return nil, nil
	}
	c.pos = 0
	kv := c.b.items[c.pos]
	return kv.key, kv.value
}

func (c *memCursor) Last() ([]byte, []byte) {
	if len(c.b.items) == 0 {
		c.pos = 0
		return nil, nil
	}
	c.pos = len(c.b.items) - 1
	kv := c.b.items[c.pos]
	return kv.key, kv.value
}

func (c *memCursor) Seek(seek []byte) ([]byte, []byte) {
	items := c.b.items
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
		items := c.b.items
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
	if c.pos >= len(c.b.items) {
		return nil, nil
	}
	kv := c.b.items[c.pos]
	return kv.key, kv.value
}

func (c *memCursor) Prev() ([]byte, []byte) {
	if c.pos < 0 {
		return nil, nil
	}
	c.pos--
	if c.pos < 0 || c.pos >= len(c.b.items) {
		return nil, nil
	}
	kv := c.b.items[c.pos]
	return kv.key, kv.value
}

func (c *memCursor) Delete() error {
	if !c.tx.writable {
		return fmt.Errorf("tx not writable")
	}
	if c.pos < 0 || c.pos >= len(c.b.items) {
		return nil
	}
	c.b.items = slices.Delete(c.b.items, c.pos, c.pos+1)
	c.pos--
	return nil
}
