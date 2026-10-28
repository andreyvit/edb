package kvodraft

// Edit contains no Go pointers. Byte values are offsets into the editor arena.
// Its fields are private; provide an array of Edit as scratch to NewEditor.
type Edit struct {
	key, word  uint64
	start, end int
	wire       Wire
	deleted    bool
}
type Editor struct {
	base        View
	edits       []Edit
	arena       []byte
	count, used int
}

func NewEditor(base View, scratch []Edit, arena []byte) Editor {
	if len(base.data) != 0 && base.Kind() != Numeric {
		panic(ErrKind)
	}
	return Editor{base: base, edits: scratch[:cap(scratch)], arena: arena[:cap(arena)]}
}
func (e *Editor) locate(key uint64) int {
	lo, hi := 0, e.count
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if e.edits[mid].key < key {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}
func (e *Editor) store(edit Edit) {
	i := e.locate(edit.key)
	if i < e.count && e.edits[i].key == edit.key {
		e.edits[i] = edit
		return
	}
	if e.count == len(e.edits) {
		grown := make([]Edit, max(8, e.count*2))
		copy(grown, e.edits)
		e.edits = grown
	}
	copy(e.edits[i+1:e.count+1], e.edits[i:e.count])
	e.edits[i] = edit
	e.count++
}

// Set copies bytes into the arena. Supplied scratch stays on the stack when the
// caller's usage permits it. Growing capacity allocates; replacing bytes may
// leave unused arena ranges until the editor is discarded.
func (e *Editor) Set(key uint64, v Value) {
	if !v.valid() {
		panic(ErrValue)
	}
	if v.IsZero() {
		e.Delete(key)
		return
	}
	edit := Edit{key: key, wire: v.wire, word: v.word}
	if v.wire == Bytes {
		if len(v.data) > int(^uint(0)>>1)-e.used {
			panic(ErrLimit)
		}
		edit.start = e.used
		edit.end = e.used + len(v.data)
		if edit.end > len(e.arena) {
			grown := make([]byte, max(edit.end, max(64, len(e.arena)*2)))
			copy(grown, e.arena[:e.used])
			e.arena = grown
		}
		copy(e.arena[edit.start:edit.end], v.data)
		e.used = edit.end
	}
	e.store(edit)
}
func (e *Editor) Delete(key uint64) { e.store(Edit{key: key, deleted: true}) }
func (e *Editor) value(x Edit) Value {
	v := Value{wire: x.wire, word: x.word}
	if x.wire == Bytes {
		v.data = e.arena[x.start:x.end]
	}
	return v
}
func (e *Editor) Get(key uint64) Value {
	i := e.locate(key)
	if i < e.count && e.edits[i].key == key {
		x := e.edits[i]
		if x.deleted {
			return Value{}
		}
		return e.value(x)
	}
	return e.base.Get(key)
}
func (e *Editor) Append(dst []byte, layout Layout) []byte {
	_, _, _, body := e.base.header()
	return appendNumeric(dst, numberCursor{view: e.base, basePos: len(e.base.data) - len(body), edits: e.edits[:e.count], arena: e.arena}, layout)
}

// The cursor mutates only integers. It returns borrowed values rather than
// storing their slice pointers through a receiver, avoiding a heap escape.
type numberCursor struct {
	fields             []Field
	direct             bool
	index              int
	view               View
	baseIndex, basePos int
	baseKey            uint64
	edits              []Edit
	arena              []byte
	editIndex          int
}

func (c *numberCursor) next() (Field, bool) {
	if c.direct {
		if c.index == len(c.fields) {
			return Field{}, false
		}
		f := c.fields[c.index]
		c.index++
		return f, true
	}
	for {
		hasBase := c.baseIndex < c.view.Len()
		var entry Entry
		nextPos := c.basePos
		nextKey := c.baseKey
		if hasBase {
			entry, nextPos, nextKey = c.view.readEntry(c.baseIndex, c.basePos, c.baseKey)
		}
		if c.editIndex < len(c.edits) && (!hasBase || c.edits[c.editIndex].key <= entry.Key) {
			x := c.edits[c.editIndex]
			c.editIndex++
			if hasBase && x.key == entry.Key {
				c.baseIndex++
				c.basePos, c.baseKey = nextPos, nextKey
			}
			if x.deleted {
				continue
			}
			v := Value{wire: x.wire, word: x.word}
			if x.wire == Bytes {
				v.data = c.arena[x.start:x.end]
			}
			return Field{x.key, v}, true
		}
		if hasBase {
			c.baseIndex++
			c.basePos, c.baseKey = nextPos, nextKey
			return Field{entry.Key, entry.Value}, true
		}
		return Field{}, false
	}
}
