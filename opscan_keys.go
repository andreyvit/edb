package edb

import (
	"bytes"
	"iter"
)

// KeyCursor is a single-pass primary-key stream. It never fetches row payloads.
// Keys decodes owned Go keys; RawKey borrows the current transaction's storage.
// A cursor and any union/intersection built from it stay in that transaction.
type KeyCursor[Key any] struct {
	source  rawKeySource
	table   *TableDef
	tx      *Tx
	reverse bool
	ordered bool
}

type rawKeySource interface {
	Next() bool
	RawKey() []byte
}

// KeyCursor opens an independent key-only cursor. Table scans and exact index
// scans can be merged with Union or Intersect (subject to the key ordering
// check described on Union).
// A general index scan is ordered by index key first, so it cannot be merged
// directly even when its primary keys happen to look sorted in a small sample.
func (s typedScan[Key]) KeyCursor() *KeyCursor[Key] {
	source := rawKeySource(s.Raw())
	ordered := s.index == nil || s.opt.Method == ScanMethodExact && !s.opt.PrefixValue.IsValid()
	if ordered && s.index != nil && s.table.keyEnc.uintKeyBits == 0 {
		source = &indexKeyOrderCheck{RawCursor: source.(RawCursor), index: s.index, opt: s.opt}
	}
	return &KeyCursor[Key]{source: source, table: s.table, tx: s.tx, reverse: s.opt.Reverse, ordered: ordered}
}

// The tuple footer makes complete variable-length primary keys potentially
// prefix one another. Appending an index footer can then change their ordering.
// Validate these streams BEFORE yielding any set results; detecting inversions
// lazily is insufficient because an intersection can stop before encountering
// one. Built-in unsigned keys have fixed width and need no preflight.
// This validation fetches no rows and allocates nothing per visited key.
// A schema-declared order-preserving key encoding could remove this extra pass.
type indexKeyOrderCheck struct {
	RawCursor
	index *IndexDef
	opt   ScanOptions
}

func preparedKeySource(source rawKeySource) rawKeySource {
	c, check := source.(*indexKeyOrderCheck)
	if !check {
		return source
	}
	opt := c.opt
	if mark := c.Bookmark(); !mark.IsZero() {
		opt = opt.Resume(mark)
	}
	var previous []byte
	for c.Next() {
		next := c.RawKey()
		if previous != nil {
			cmp := bytes.Compare(previous, next)
			if (!opt.Reverse && cmp > 0) || (opt.Reverse && cmp < 0) {
				panic("edb: index keys are not in primary-key order; use a materialized key set for this query")
			}
		}
		previous = next
	}
	return c.Tx().IndexScan(c.index, opt)
}

func (c *KeyCursor[Key]) Next() bool     { return c.source.Next() }
func (c *KeyCursor[Key]) Key() Key       { return decodeTypedKey[Key](c.table, c.source.RawKey()) }
func (c *KeyCursor[Key]) RawKey() []byte { return c.source.RawKey() }
func (c *KeyCursor[Key]) Keys() iter.Seq[Key] {
	return func(yield func(Key) bool) {
		for c.Next() {
			if !yield(c.Key()) {
				return
			}
		}
	}
}

// Union consumes c and the other cursors, returning each primary key once.
// Inputs must scan the same table, transaction, and direction and be ordered by
// primary key. Do not reuse an input cursor after composing it. Encoded keys
// are compared directly, including strings and composite keys; only emitted
// keys are decoded. Setup costs O(inputs) space, with no per-key allocations
// from merging. Native key decoding may allocate for strings/custom key types.
//
// Exact index scans with non-unsigned keys receive a key-only preflight pass:
// tuple-footer bytes embedded in variable-length keys can violate primary-key
// order. An inversion panics before any results are emitted; use materialized
// sets for such key domains. Built-in unsigned keys need no extra pass.
func (c *KeyCursor[Key]) Union(others ...*KeyCursor[Key]) *KeyCursor[Key] {
	return c.merge(keyUnion, others)
}

// Intersect consumes its inputs with the same rules as Union, emitting only
// keys present in every input. It does not read or decode any rows.
func (c *KeyCursor[Key]) Intersect(others ...*KeyCursor[Key]) *KeyCursor[Key] {
	return c.merge(keyIntersection, others)
}

func (c *KeyCursor[Key]) merge(kind keyMergeKind, others []*KeyCursor[Key]) *KeyCursor[Key] {
	inputs := make([]keyMergeInput, 1, len(others)+1)
	if !c.ordered {
		panic("edb: key sets require table scans or exact index scans")
	}
	inputs[0] = keyMergeInput{source: preparedKeySource(c.source)}
	for i, other := range others {
		if other.table != c.table || other.tx != c.tx || other.reverse != c.reverse || !other.ordered {
			panic("edb: key sets require the same table, transaction, direction, and primary-key order")
		}
		if other == c {
			panic("edb: cannot merge a key cursor with itself")
		}
		for _, prior := range others[:i] {
			if prior == other {
				panic("edb: repeated key cursor")
			}
		}
		inputs = append(inputs, keyMergeInput{source: preparedKeySource(other.source)})
	}
	return &KeyCursor[Key]{source: &keyMergeCursor{inputs: inputs, kind: kind, reverse: c.reverse}, table: c.table, tx: c.tx, reverse: c.reverse, ordered: true}
}

type keyMergeKind uint8

const (
	keyUnion keyMergeKind = iota
	keyIntersection
)

func (k keyMergeKind) String() string {
	switch k {
	case keyUnion:
		return "union"
	case keyIntersection:
		return "intersection"
	default:
		return "unknown"
	}
}

type keyMergeInput struct {
	source rawKeySource
	key    []byte
	ok     bool
}

func (s *keyMergeInput) advance() {
	s.ok = s.source.Next()
	if s.ok {
		s.key = s.source.RawKey()
	}
}
func (s *keyMergeInput) advancePast(key []byte) {
	for s.ok && bytes.Equal(s.key, key) {
		s.advance()
	}
}

type keyMergeCursor struct {
	inputs  []keyMergeInput
	kind    keyMergeKind
	reverse bool
	started bool
	done    bool
	key     []byte
}

func (c *keyMergeCursor) RawKey() []byte { return c.key }
func (c *keyMergeCursor) before(a, b []byte) bool {
	cmp := bytes.Compare(a, b)
	if c.reverse {
		return cmp > 0
	}
	return cmp < 0
}
func (c *keyMergeCursor) Next() bool {
	if c.done {
		return false
	}
	if !c.started {
		for i := range c.inputs {
			c.inputs[i].advance()
		}
		c.started = true
	} else {
		for i := range c.inputs {
			c.inputs[i].advancePast(c.key)
		}
	}
	if c.kind == keyUnion {
		var next []byte
		for i := range c.inputs {
			child := &c.inputs[i]
			if child.ok && (next == nil || c.before(child.key, next)) {
				next = child.key
			}
		}
		if next == nil {
			c.done = true
			return false
		}
		c.key = next
		return true
	}
	for {
		var target []byte
		for i := range c.inputs {
			child := &c.inputs[i]
			if !child.ok {
				c.done = true
				return false
			}
			if target == nil || c.before(target, child.key) {
				target = child.key
			}
		}
		matched := true
		for i := range c.inputs {
			child := &c.inputs[i]
			for child.ok && c.before(child.key, target) {
				child.advance()
			}
			if !child.ok {
				c.done = true
				return false
			}
			if !bytes.Equal(child.key, target) {
				matched = false
			}
		}
		if matched {
			c.key = target
			return true
		}
	}
}
