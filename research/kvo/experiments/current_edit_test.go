package kvolab

// Selected format adapter of the retained interactive editors. The algorithms
// are unchanged; scalar edit records retain signed/raw-width constructor hints.
import (
	"bytes"
	"encoding/binary"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"github.com/cespare/xxhash/v2"
	"math"
	"math/bits"
	"slices"
	"unsafe"
)

type currentLengthEditor struct {
	base    currentView
	edits   []classMeta
	arena   []byte
	kind    int
	deleted int
}

func newCurrentLengthEditor(base currentView, scratch []classMeta, arena []byte) currentLengthEditor {
	return currentLengthEditor{base: base, edits: scratch[:0], arena: arena[:0], kind: base.Kind()}
}
func (e *currentLengthEditor) span(at uint64) []byte {
	n, b := lengthU(e.arena[int(at):])
	return b[:int(n)]
}
func (e *currentLengthEditor) text(m classMeta) []byte { return e.span(m.key) }
func (e *currentLengthEditor) copySpan(src []byte) uint64 {
	at := uint64(len(e.arena))
	var header [10]byte
	n := binary.PutUvarint(header[:], uint64(len(src)))
	e.copyArena(header[:n])
	e.copyArena(src)
	return at
}
func (e *currentLengthEditor) compare(m classMeta, key uint64, text string) int {
	if e.kind == 1 {
		return bytes.Compare(e.text(m), tagStringBytes(text))
	}
	if m.key < key {
		return -1
	}
	if m.key > key {
		return 1
	}
	return 0
}
func (e *currentLengthEditor) locate(key uint64, text string) int {
	lo, hi := 0, len(e.edits)
	for lo < hi {
		m := lo + (hi-lo)/2
		if e.compare(e.edits[m], key, text) < 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo
}
func (e *currentLengthEditor) store(key uint64, text string, value currentValue, deleted, keepZero bool) {
	if e.kind == 2 && key >= uint64(e.base.Len()) {
		panic(draft.ErrIndex)
	}
	i := e.locate(key, text)
	existing := i < len(e.edits) && e.compare(e.edits[i], key, text) == 0
	m := classMeta{key: key, word: value.num, wire: currentEditCode(value)}
	if e.kind == 1 {
		if existing {
			m.key = e.edits[i].key
		} else {
			m.key = e.copySpan(tagStringBytes(text))
		}
	}
	if deleted || !keepZero && e.kind != 2 && value.IsZero() {
		m.wire = 10
		m.word = 0
	} else if value.isSpan() {
		m.wire = 9
		m.word = e.copySpan(value.Bytes())
	}
	if existing {
		e.edits[i] = m
	} else {
		if len(e.edits) == cap(e.edits) {
			grown := make([]classMeta, len(e.edits), max(8, len(e.edits)*2))
			copy(grown, e.edits)
			e.edits = grown
		}
		e.edits = e.edits[:len(e.edits)+1]
		copy(e.edits[i+1:], e.edits[i:])
		e.edits[i] = m
	}
}
func (e *currentLengthEditor) copyArena(src []byte) {
	if len(src) > int(^uint(0)>>1)-len(e.arena) {
		panic(draft.ErrLimit)
	}
	n := len(e.arena) + len(src)
	if n > cap(e.arena) {
		grown := make([]byte, len(e.arena), max(n, max(64, 2*cap(e.arena))))
		copy(grown, e.arena)
		e.arena = grown
	}
	start := len(e.arena)
	e.arena = e.arena[:n]
	copy(e.arena[start:], src)
}
func (e *currentLengthEditor) Len() int {
	if e.kind != 2 {
		panic(draft.ErrKind)
	}
	return e.base.Len() - e.deleted
}
func (e *currentLengthEditor) baseIndex(index uint64) uint64 {
	if index >= uint64(e.Len()) {
		panic(draft.ErrIndex)
	}
	for _, m := range e.edits {
		if m.key > index {
			break
		}
		if m.wire == 10 {
			index++
		}
	}
	return index
}
func (e *currentLengthEditor) Set(key uint64, value currentValue) {
	if e.kind == 1 {
		panic(draft.ErrKind)
	}
	if e.kind == 2 {
		key = e.baseIndex(key)
	}
	e.store(key, "", value, false, false)
}
func (e *currentLengthEditor) SetString(key string, value currentValue) {
	if e.kind != 1 {
		panic(draft.ErrKind)
	}
	e.store(0, key, value, false, false)
}
func (e *currentLengthEditor) Delete(key uint64) {
	if e.kind == 1 {
		panic(draft.ErrKind)
	}
	if e.kind == 2 {
		key = e.baseIndex(key)
		e.deleted++
	}
	e.store(key, "", currentValue{}, true, false)
}
func (e *currentLengthEditor) DeleteString(key string) {
	if e.kind != 1 {
		panic(draft.ErrKind)
	}
	e.store(0, key, currentValue{}, true, false)
}
func (e *currentLengthEditor) value(m classMeta) currentValue {
	if m.wire == 10 {
		return currentValue{}
	}
	if m.wire == 9 {
		return currentBytes(e.span(m.word))
	}
	return currentEditWord(m.word, m.wire)
}
func (e *currentLengthEditor) Get(key uint64, text string) currentValue {
	if e.kind == 2 {
		key = e.baseIndex(key)
	}
	i := e.locate(key, text)
	if i < len(e.edits) && e.compare(e.edits[i], key, text) == 0 {
		return e.value(e.edits[i])
	}
	switch e.kind {
	case 0:
		return e.base.Get(key)
	case 1:
		return e.base.GetString(text)
	default:
		return e.base.At(int(key))
	}
}
func (e currentLengthEditor) Append(w *currentWriter, dst []byte, indexed bool) []byte {
	return e.appendWithBaseZeros(w, dst, indexed, true)
}

// A schema-free rewrite preserves unknown presence. A bound Dictionary may
// instead omit existing zero entries under its declared entry policy.
func (e currentLengthEditor) appendWithBaseZeros(w *currentWriter, dst []byte, indexed, keepBaseZeros bool) []byte {
	w.Reset(dst)
	w.Begin(0, "", e.kind, indexed)
	it := e.base.Iter()
	key, text, value, ok := it.Next()
	i := 0
	emitEdit := func(m classMeta) {
		if m.wire == 10 {
			return
		}
		s := ""
		if e.kind == 1 {
			s = unsafe.String(unsafe.SliceData(e.text(m)), len(e.text(m)))
		}
		// Ordinary zero assignments were already recorded as deletions in store.
		w.PutPresent(m.key, s, e.value(m))
	}
	for ok || i < len(e.edits) {
		cmp := 1
		if i < len(e.edits) {
			if !ok {
				cmp = -1
			} else {
				cmp = e.compare(e.edits[i], key, unsafe.String(unsafe.SliceData(text), len(text)))
			}
		}
		if cmp <= 0 {
			emitEdit(e.edits[i])
			i++
		}
		if cmp >= 0 {
			if cmp > 0 {
				w.put(key, unsafe.String(unsafe.SliceData(text), len(text)), value, keepBaseZeros)
			}
			key, text, value, ok = it.Next()
		}
	}
	w.End()
	return w.BytesWritten()
}

type currentBatchEditor struct {
	storage  currentLengthEditor
	changes  []batchChange
	table    []uint32
	mode     string
	sequence uint32
	dirty    bool
}

func (e *currentBatchEditor) Reset(base currentView, kind int, mode string) {
	e.storage = newCurrentLengthEditor(base, e.storage.edits[:0], e.storage.arena[:0])
	e.storage.kind = kind
	e.changes = e.changes[:0]
	clear(e.table)
	e.sequence = 0
	e.dirty = false
	e.mode = mode
}
func (e *currentBatchEditor) text(m batchChange) []byte { return e.storage.span(m.key) }
func (e *currentBatchEditor) compare(m batchChange, key uint64, text string) int {
	return e.storage.compare(m.meta(), key, text)
}
func (e *currentBatchEditor) hash(key uint64, text string) uint64 {
	if e.storage.kind == 1 {
		return xxhash.Sum64String(text)
	}
	key ^= key >> 30
	key *= 0xbf58476d1ce4e5b9
	key ^= key >> 27
	key *= 0x94d049bb133111eb
	return key ^ (key >> 31)
}
func (e *currentBatchEditor) index(key uint64, text string) (at, slot int) {
	mask := len(e.table) - 1
	slot = int(e.hash(key, text)) & mask
	for e.table[slot] != 0 {
		at = int(e.table[slot]) - 1
		if e.compare(e.changes[at], key, text) == 0 {
			return
		}
		slot = (slot + 1) & mask
	}
	return -1, slot
}
func (e *currentBatchEditor) reindex() {
	n := max(16, 1<<bits.Len(uint(len(e.changes)*2)))
	if len(e.table) < n {
		e.table = make([]uint32, n)
	} else {
		clear(e.table)
	}
	for i, m := range e.changes {
		text := ""
		if e.storage.kind == 1 {
			b := e.text(m)
			text = unsafe.String(unsafe.SliceData(b), len(b))
		}
		_, slot := e.index(m.key, text)
		e.table[slot] = uint32(i + 1)
	}
}
func (e *currentBatchEditor) Set(key uint64, text string, v currentValue, keepZero bool) {
	e.store(key, text, v, false, keepZero)
}
func (e *currentBatchEditor) Delete(key uint64, text string) {
	if e.storage.kind == 2 {
		panic(draft.ErrKind)
	}
	e.store(key, text, currentValue{}, true, false)
}
func (e *currentBatchEditor) store(key uint64, text string, v currentValue, deleted, keepZero bool) {
	if e.mode == "tail" && !keepZero {
		currentSortedTailStore(&e.storage, key, text, v, deleted)
		return
	}
	if e.mode == "sorted" || e.mode == "tail" {
		e.storage.store(key, text, v, deleted, keepZero)
		return
	}
	if e.storage.kind == 2 && key >= uint64(e.storage.base.Len()) {
		panic(draft.ErrIndex)
	}
	if e.sequence == math.MaxUint32 {
		e.normalize()
		for i := range e.changes {
			e.changes[i].sequence = uint32(i)
		}
		e.sequence = uint32(len(e.changes))
	}
	at, slot := -1, 0
	if e.mode == "hash" {
		if len(e.table) == 0 || (len(e.changes)+1)*2 >= len(e.table) {
			e.reindex()
		}
		at, slot = e.index(key, text)
	} else if e.mode == "upsert" {
		for i := len(e.changes) - 1; i >= 0; i-- {
			if e.compare(e.changes[i], key, text) == 0 {
				at = i
				break
			}
		}
	}
	m := batchChange{key: key, word: v.num, wire: currentEditCode(v), sequence: e.sequence}
	e.sequence++
	if e.storage.kind == 1 {
		if at >= 0 {
			m.key = e.changes[at].key
		} else {
			m.key = e.storage.copySpan(tagStringBytes(text))
		}
	}
	if deleted || !keepZero && e.storage.kind != 2 && v.IsZero() {
		m.wire = 10
		m.word = 0
	} else if v.isSpan() {
		m.wire = 9
		m.word = e.storage.copySpan(v.Bytes())
	}
	if at >= 0 {
		e.changes[at] = m
	} else {
		e.changes = append(e.changes, m)
		if e.mode == "hash" {
			e.table[slot] = uint32(len(e.changes))
		}
	}
	e.dirty = true
}
func (e *currentBatchEditor) normalize() {
	if !e.dirty {
		return
	}
	slices.SortFunc(e.changes, func(a, b batchChange) int {
		if e.storage.kind == 1 {
			return bytes.Compare(e.text(a), e.text(b))
		}
		if a.key < b.key {
			return -1
		}
		if a.key > b.key {
			return 1
		}
		return 0
	})
	n := 0
	for i := 0; i < len(e.changes); {
		best := e.changes[i]
		j := i + 1
		for j < len(e.changes) {
			other := e.changes[j]
			same := other.key == best.key
			if e.storage.kind == 1 {
				same = bytes.Equal(e.text(other), e.text(best))
			}
			if !same {
				break
			}
			if other.sequence > best.sequence {
				best = other
			}
			j++
		}
		e.changes[n] = best
		n++
		i = j
	}
	e.changes = e.changes[:n]
	e.dirty = false
	if e.mode == "hash" {
		e.reindex()
	}
}
func (e *currentBatchEditor) Get(key uint64, text string) currentValue {
	if e.mode == "sorted" || e.mode == "tail" {
		return e.storage.Get(key, text)
	}
	if e.mode == "lazy" {
		e.normalize()
	}
	at := -1
	if e.mode == "hash" && len(e.table) > 0 {
		at, _ = e.index(key, text)
	} else if !e.dirty {
		lo, hi := 0, len(e.changes)
		for lo < hi {
			m := lo + (hi-lo)/2
			if e.compare(e.changes[m], key, text) < 0 {
				lo = m + 1
			} else {
				hi = m
			}
		}
		if lo < len(e.changes) && e.compare(e.changes[lo], key, text) == 0 {
			at = lo
		}
	} else {
		for i := len(e.changes) - 1; i >= 0; i-- {
			if e.compare(e.changes[i], key, text) == 0 {
				at = i
				break
			}
		}
	}
	if at >= 0 {
		return e.storage.value(e.changes[at].meta())
	}
	switch e.storage.kind {
	case 0:
		return e.storage.base.Get(key)
	case 1:
		return e.storage.base.GetString(text)
	default:
		return e.storage.base.At(int(key))
	}
}
func currentPut(w *currentWriter, key uint64, text string, v currentValue) {
	w.PutPresent(key, text, v)
}

func (e *currentBatchEditor) visit(emit func(uint64, string, currentValue)) {
	if e.mode == "sorted" || e.mode == "tail" {
		e.changes = e.changes[:0]
		for _, m := range e.storage.edits {
			e.changes = append(e.changes, batchChange{key: m.key, word: m.word, wire: m.wire})
		}
		e.dirty = false
	} else {
		e.normalize()
	}
	it := e.storage.base.Iter()
	key, text, value, ok := it.Next()
	i := 0
	for ok || i < len(e.changes) {
		cmp := 1
		s := unsafe.String(unsafe.SliceData(text), len(text))
		if i < len(e.changes) {
			if !ok {
				cmp = -1
			} else {
				cmp = e.compare(e.changes[i], key, s)
			}
		}
		if cmp <= 0 {
			m := e.changes[i]
			if m.wire != 10 {
				k := m.key
				ms := ""
				if e.storage.kind == 1 {
					b := e.text(m)
					ms = unsafe.String(unsafe.SliceData(b), len(b))
					k = 0 // Dictionary identity is the string, not its arena offset.
				}
				emit(k, ms, e.storage.value(m.meta()))
			}
			i++
		}
		if cmp >= 0 {
			if cmp > 0 {
				emit(key, s, value)
			}
			key, text, value, ok = it.Next()
		}
	}
}
func (e *currentBatchEditor) Append(w *currentWriter, dst []byte, indexed bool) []byte {
	if e.mode == "sorted" || e.mode == "tail" {
		return e.storage.Append(w, dst, indexed)
	}
	w.Reset(dst)
	w.Begin(0, "", e.storage.kind, indexed)
	e.visit(func(key uint64, text string, v currentValue) { currentPut(w, key, text, v) })
	w.End()
	return w.BytesWritten()
}
func (e *currentBatchEditor) ScratchBytes() int {
	return cap(e.storage.arena) + 24*cap(e.storage.edits) + 24*cap(e.changes) + 4*cap(e.table)
}

type currentTreeNode struct {
	edit                currentBatchEditor
	parent, first, next int
	step                perfStep
	active, indexed     bool
}
type currentTreeEditor struct {
	nodes   []currentTreeNode
	used    int
	mode    string
	indexed bool
}

func (e *currentTreeEditor) Reset(base currentView, mode string, indexed bool) {
	e.used = 0
	e.mode = mode
	e.indexed = indexed
	e.newNode(-1, perfStep{}, base, 0)
}
func (e *currentTreeEditor) newNode(parent int, step perfStep, base currentView, kind int) int {
	i := e.used
	e.used++
	if i == len(e.nodes) {
		e.nodes = append(e.nodes, currentTreeNode{})
	}
	n := &e.nodes[i]
	n.parent = parent
	n.step = step
	n.active = true
	n.first = -1
	n.next = -1
	n.indexed = e.indexed && (base.n == 0 || base.Indexed())
	if parent >= 0 {
		n.indexed = n.indexed && e.nodes[parent].indexed
	}
	n.edit.Reset(base, kind, e.mode)
	if parent >= 0 {
		n.next = e.nodes[parent].first
		e.nodes[parent].first = i
	}
	return i
}
func (e *currentTreeEditor) child(parent int, s perfStep) int {
	for i := e.nodes[parent].first; i >= 0; i = e.nodes[i].next {
		n := &e.nodes[i]
		if n.active && n.step.key == s.key && n.step.text == s.text {
			return i
		}
	}
	return -1
}
func (e *currentTreeEditor) Set(path []perfStep, v currentValue) {
	e.setPath(path, v, false)
}

// A schema binding with PresenceIsSignificant calls SetPresent for its leaf.
func (e *currentTreeEditor) SetPresent(path []perfStep, v currentValue) {
	e.setPath(path, v, true)
}
func (e *currentTreeEditor) setPath(path []perfStep, v currentValue, keepZero bool) {
	if len(path) == 0 {
		panic(draft.ErrKind)
	}
	parent := 0
	for i, s := range path {
		if i == len(path)-1 {
			if child := e.child(parent, s); child >= 0 {
				e.nodes[child].active = false
			}
			e.set(parent, s, v, keepZero)
			return
		}
		child := e.child(parent, s)
		if child < 0 {
			base := e.lookup(parent, s)
			child = e.newNode(parent, s, currentTrusted(base.Bytes()), path[i+1].kind)
			// The placeholder keeps a newly inserted child in the parent merge.
			// Its actual value is emitted recursively and never stored on disk.
			e.set(parent, s, currentWord(1), false)
		}
		parent = child
	}
}
func (e *currentTreeEditor) set(node int, s perfStep, v currentValue, keepZero bool) {
	n := &e.nodes[node]
	if n.edit.storage.kind == 2 && n.edit.storage.base.n == 0 {
		// Construction can add List slots. A temporary kind change bypasses the
		// replacement-only baseline guard; the writer fills skipped positions.
		n.edit.storage.kind = 0
		n.edit.Set(s.key, s.text, v, true)
		n.edit.storage.kind = 2
	} else {
		n.edit.Set(s.key, s.text, v, keepZero || s.kind == 2)
	}
}
func (e *currentTreeEditor) lookup(node int, s perfStep) currentValue {
	n := &e.nodes[node]
	if n.edit.storage.kind == 2 && n.edit.storage.base.n == 0 {
		n.edit.storage.kind = 0
		v := n.edit.Get(s.key, s.text)
		n.edit.storage.kind = 2
		return v
	}
	return n.edit.Get(s.key, s.text)
}

// GetLeaf reads a leaf through the overlay. An edited container needs a mutable
// container handle, outside this path-only research skeleton; never expose its
// internal parent placeholder as a scalar value.
func (e *currentTreeEditor) GetLeaf(path []perfStep) currentValue {
	if len(path) == 0 {
		panic(draft.ErrKind)
	}
	parent := 0
	for i, s := range path {
		if child := e.child(parent, s); child >= 0 {
			if i+1 == len(path) {
				panic(draft.ErrKind)
			}
			parent = child
			continue
		}
		v := e.lookup(parent, s)
		if i+1 < len(path) {
			return currentPath(v.Bytes(), path[i+1:])
		}
		return v
	}
	return currentValue{}
}
func (e *currentTreeEditor) Append(w *currentWriter, dst []byte) []byte {
	w.Reset(dst)
	e.emit(w, 0, 0, "")
	return w.BytesWritten()
}
func (e *currentTreeEditor) emit(w *currentWriter, node int, key uint64, text string) {
	n := &e.nodes[node]
	w.Begin(key, text, n.edit.storage.kind, n.indexed)
	listNext := uint64(0)
	n.edit.visit(func(k uint64, s string, v currentValue) {
		if n.edit.storage.kind == 2 {
			for listNext < k {
				currentPut(w, listNext, "", currentValue{})
				listNext++
			}
			listNext = k + 1
		}
		if child := e.child(node, perfStep{key: k, text: s}); child >= 0 {
			e.emit(w, child, k, s)
		} else {
			currentPut(w, k, s, v)
		}
	})
	w.End()
}
func (e *currentTreeEditor) ScratchBytes() int {
	n := cap(e.nodes) * int(unsafe.Sizeof(currentTreeNode{}))
	for i := range e.nodes {
		n += e.nodes[i].edit.ScratchBytes()
	}
	return n
}

func currentSortedTailStore(e *currentLengthEditor, key uint64, text string, value currentValue, deleted bool) {
	if e.kind == 2 && key >= uint64(e.base.Len()) {
		panic(draft.ErrIndex)
	}
	i := len(e.edits)
	if i > 0 && e.compare(e.edits[i-1], key, text) >= 0 {
		if e.compare(e.edits[i-1], key, text) == 0 {
			i--
		} else {
			i = e.locate(key, text)
		}
	}
	existing := i < len(e.edits) && e.compare(e.edits[i], key, text) == 0
	m := classMeta{key: key, word: value.num, wire: currentEditCode(value)}
	if e.kind == 1 {
		if existing {
			m.key = e.edits[i].key
		} else {
			m.key = e.copySpan(tagStringBytes(text))
		}
	}
	if deleted || e.kind != 2 && value.IsZero() {
		m.wire = 10
		m.word = 0
	} else if value.isSpan() {
		m.wire = 9
		m.word = e.copySpan(value.Bytes())
	}
	if existing {
		e.edits[i] = m
	} else {
		if len(e.edits) == cap(e.edits) {
			grown := make([]classMeta, len(e.edits), max(8, len(e.edits)*2))
			copy(grown, e.edits)
			e.edits = grown
		}
		e.edits = e.edits[:len(e.edits)+1]
		copy(e.edits[i+1:], e.edits[i:])
		e.edits[i] = m
	}
}
