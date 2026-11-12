package values

import "context"

// Map stores immutable key/value snapshots. Mutable values inside them (pointers,
// maps and slices) retain Go aliasing. Deleted entries remain only in iterators.
type Map struct {
	Key     *Shape
	Types   *TypeTable
	buckets map[uint64][]*MapEntry
	order   []*MapEntry
	count   int
}
type MapEntry struct {
	Key, Value Value
	Live       bool
}

func NewMap(key *Shape, ts *TypeTable) *Map {
	return &Map{Key: key, Types: ts, buckets: make(map[uint64][]*MapEntry)}
}
func (m *Map) Len() int {
	if m == nil {
		return 0
	}
	return m.count
}
func (m *Map) Find(k Value, h uint64) (*MapEntry, bool, error) {
	return m.FindContext(context.Background(), k, h)
}

func (m *Map) FindContext(ctx context.Context, k Value, h uint64) (*MapEntry, bool, error) {
	if m == nil {
		return nil, false, nil
	}
	for i, e := range m.buckets[h] {
		if i&255 == 255 {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
		}
		eq, err := m.Key.EqualContext(ctx, e.Key, k, m.Types)
		if err != nil {
			return nil, false, err
		}
		if eq {
			return e, true, nil
		}
	}
	return nil, false, nil
}
func (m *Map) Put(k, v Value, h uint64) error {
	return m.PutContext(context.Background(), k, v, h)
}

func (m *Map) PutContext(ctx context.Context, k, v Value, h uint64) error {
	e, ok, err := m.FindContext(ctx, k, h)
	if err != nil {
		return err
	}
	if ok {
		e.Value = v
		return nil
	}
	e = &MapEntry{Key: k, Value: v, Live: true}
	m.buckets[h] = append(m.buckets[h], e)
	m.order = append(m.order, e)
	m.count++
	return nil
}
func (m *Map) Delete(k Value, h uint64) error {
	return m.DeleteContext(context.Background(), k, h)
}

func (m *Map) DeleteContext(ctx context.Context, k Value, h uint64) error {
	if m == nil {
		return nil
	}
	bucket := m.buckets[h]
	for i, e := range bucket {
		if i&255 == 255 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		eq, err := m.Key.EqualContext(ctx, e.Key, k, m.Types)
		if err != nil {
			return err
		}
		if eq {
			e.Live = false
			e.Key, e.Value = Value{}, Value{}
			m.count--
			copy(bucket[i:], bucket[i+1:])
			bucket[len(bucket)-1] = nil
			if len(bucket) == 1 {
				delete(m.buckets, h)
			} else {
				m.buckets[h] = bucket[:len(bucket)-1]
			}
			return nil
		}
	}
	return nil
}
func (m *Map) Clear() {
	if m == nil {
		return
	}
	for _, e := range m.order {
		e.Live = false
		e.Key, e.Value = Value{}, Value{}
	}
	clear(m.buckets)
	clear(m.order)
	m.order = nil
	m.count = 0
}

// Entries snapshots the existing iteration range; insertions may be skipped.
// Updates are observed and deleted entries are skipped by the iterator.
func (m *Map) Entries() []*MapEntry {
	if m == nil {
		return nil
	}
	return m.order[:len(m.order):len(m.order)]
}

func (m *Map) ProbeCount(hash uint64) uint64 {
	if m == nil {
		return 0
	}
	return uint64(len(m.buckets[hash]))
}
func (m *Map) EntryCount() uint64 {
	if m == nil {
		return 0
	}
	return uint64(len(m.order))
}
