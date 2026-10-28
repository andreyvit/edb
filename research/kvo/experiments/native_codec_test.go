package kvolab

import (
	"bytes"
	"math"
	"reflect"
	"slices"
	"time"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type nativeMapItem struct {
	number uint64
	text   string
	value  reflect.Value
}
type nativeMapFrame struct {
	items      []nativeMapItem
	key, value reflect.Value
	byText     map[string]int
	byNumber   map[uint64]int
}

// A depth/type pair avoids reallocating scratch for differently typed sibling
// maps. Recursive maps use a different depth and cannot overwrite parent state.
type nativeMapKey struct {
	depth int
	typ   reflect.Type
}
type nativeMapWorkspace map[nativeMapKey]*nativeMapFrame

func nativeMapScratch(frames *nativeMapWorkspace, depth int, t reflect.Type) *nativeMapFrame {
	if *frames == nil {
		*frames = make(nativeMapWorkspace)
	}
	k := nativeMapKey{depth, t}
	if f := (*frames)[k]; f != nil {
		return f
	}
	f := &nativeMapFrame{key: reflect.New(t.Key()).Elem(), value: reflect.New(t.Elem()).Elem()}
	(*frames)[k] = f
	return f
}

// Get returns an owned, allocating projection. Strings, byte slices, maps and
// slices are independent of the row and remain valid after its transaction.
func (c *nativeCodec[T]) Get(view currentView) *T {
	out := new(T)
	c.decodeObject(c.plan, reflect.ValueOf(out).Elem(), view, false, 0)
	return out
}

// GetFast reuses caller storage and borrows immutable strings from view. Native
// byte slices are copied, never writable aliases of Bolt mmap. Growth allocates;
// new map keys may allocate. Old borrowed map keys must also remain live while
// reusing a destination; normally reuse it within one read transaction.
func (c *nativeCodec[T]) GetFast(view currentView, out *T) {
	if out == nil {
		panic(draft.ErrValue)
	}
	c.decodeObject(c.plan, reflect.ValueOf(out).Elem(), view, true, 0)
}

// Retained pre-reuse control; production-facing examples use GetFast.
func (c *nativeCodec[T]) GetFastRebuild(view currentView, out *T) {
	prior := c.rebuildMapValues
	c.rebuildMapValues = true
	defer func() { c.rebuildMapValues = prior }()
	c.GetFast(view, out)
}

// Retained sorted-snapshot control for the map-reuse experiment.
func (c *nativeCodec[T]) GetFastSortedMaps(view currentView, out *T) {
	prior := c.sortedMapSnapshots
	c.sortedMapSnapshots = true
	defer func() { c.sortedMapSnapshots = prior }()
	c.GetFast(view, out)
}

func (c *nativeCodec[T]) Put(dst []byte, value *T, fast bool) []byte {
	if value == nil {
		panic(draft.ErrValue)
	}
	c.writer.Reset(dst)
	c.emit(c.plan, reflect.ValueOf(value).Elem(), 0, "", fast, false, 0)
	return c.writer.BytesWritten()
}

// Patch replaces exactly the properties declared by T. Child values are whole
// replacements. Opaque sibling spans and present zeros pass through unchanged.
func (c *nativeCodec[T]) Patch(dst []byte, base currentView, value *T) []byte {
	if value == nil || base.Kind() != 0 {
		panic(draft.ErrKind)
	}
	fast := base.Indexed()
	c.writer.Reset(dst)
	c.writer.Begin(0, "", 0, fast)
	c.writer.ReserveEntries(base.Len() + len(c.plan.fields))
	it := base.Iter()
	key, _, v, ok := it.Next()
	rv := reflect.ValueOf(value).Elem()
	for _, f := range c.plan.fields {
		for ok && key < f.key {
			c.writer.PutPresent(key, "", v)
			key, _, v, ok = it.Next()
		}
		if ok && key == f.key {
			key, _, v, ok = it.Next()
		}
		c.emit(f.plan, rv.Field(f.index), f.key, "", fast && !f.compact, f.significant, 1)
	}
	for ok {
		c.writer.PutPresent(key, "", v)
		key, _, v, ok = it.Next()
	}
	c.writer.End()
	return c.writer.BytesWritten()
}

// EndPresent is needed only when a schema explicitly retains an empty child.
// It uses the selected codec's End; the old lengthWriter method uses old bytes.
func (w *currentWriter) EndPresent() {
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	f := &w.frames[w.depth-1]
	if len(f.entries) != 0 || w.depth == 1 {
		w.End()
		return
	}
	// Keep the Fast Dictionary key prefix, written before the child start.
	key, start := f.parentKey, f.start
	w.out = w.out[:start]
	w.depth--
	parent := &w.frames[w.depth-1]
	if parent.indexed && parent.kind != 0 {
		w.out = append(w.out, 0, 0, 0, 0, 0, 0, 0, 0)
	}
	w.add(key, 0, 0)
}

func (c *nativeCodec[T]) emit(p *nativePlan, v reflect.Value, key uint64, text string, fast, keep bool, depth int) {
	if depth > 256 {
		panic(draft.ErrLimit)
	}
	if p.kind == nativeOption {
		if !v.Field(1).Bool() {
			c.writer.Put(key, text, currentValue{})
			return
		}
		c.emit(p.elem, v.Field(0), key, text, fast, true, depth+1)
		return
	}
	if p.kind == nativePointer {
		if v.IsNil() {
			if keep {
				c.writer.PutPresent(key, text, currentValue{})
			} else {
				c.writer.Put(key, text, currentValue{})
			}
			return
		}
		c.emit(p.elem, v.Elem(), key, text, fast, keep, depth+1)
		return
	}
	var scalar currentValue
	switch p.kind {
	case nativeSigned:
		scalar = currentInt(v.Int())
	case nativeUnsigned:
		scalar = currentWord(v.Uint())
	case nativeBool:
		if v.Bool() {
			scalar = currentWord(1)
		}
	case nativeFloat:
		if x := v.Float(); x != 0 {
			scalar = currentWord(math.Float64bits(x))
		}
	case nativeTime:
		scalar = currentWord(kvo.TimeToUint64(*v.Addr().Interface().(*time.Time)))
	case nativeString:
		scalar = currentBytes(tagStringBytes(v.String()))
	case nativeBytes:
		scalar = currentBytes(v.Bytes())
	case nativeObject:
		c.writer.Begin(key, text, 0, fast)
		c.writer.ReserveEntries(len(p.fields))
		for _, f := range p.fields {
			c.emit(f.plan, v.Field(f.index), f.key, "", fast && !f.compact, f.significant, depth+1)
		}
	case nativeList:
		c.writer.Begin(key, text, 2, fast)
		c.writer.ReserveEntries(v.Len())
		for i := 0; i < v.Len(); i++ {
			c.emit(p.elem, v.Index(i), 0, "", fast, true, depth+1)
		}
	case nativeDictionary, nativeNumbers:
		kind := 0
		if p.kind == nativeDictionary {
			kind = 1
		}
		c.writer.Begin(key, text, kind, fast)
		c.writer.ReserveEntries(v.Len())
		f := nativeMapScratch(&c.maps, depth, p.typ)
		// Each retained item has its own addressable value. SetIterValue avoids
		// the per-entry allocation of MapIndex or MapIter.Value.
		n := v.Len()
		if cap(f.items) < n {
			f.items = slices.Grow(f.items, n-len(f.items))
		}
		f.items = f.items[:max(len(f.items), n)]
		it, i := v.MapRange(), 0
		for it.Next() {
			item := &f.items[i]
			f.key.SetIterKey(it)
			if kind == 1 {
				item.text = f.key.String()
			} else {
				item.number = f.key.Uint()
			}
			if !item.value.IsValid() || item.value.Type() != p.typ.Elem() {
				item.value = reflect.New(p.typ.Elem()).Elem()
			}
			item.value.SetIterValue(it)
			i++
		}
		items := f.items[:n] // remains stable if recursive use grows c.maps
		slices.SortFunc(items, func(a, b nativeMapItem) int {
			if kind == 1 {
				if a.text < b.text {
					return -1
				}
				if a.text > b.text {
					return 1
				}
				return 0
			}
			if a.number < b.number {
				return -1
			}
			if a.number > b.number {
				return 1
			}
			return 0
		})
		for _, item := range items {
			c.emit(p.elem, item.value, item.number, item.text, fast, p.keepDictionaryZeros, depth+1)
		}
		for i := range items {
			items[i].text = ""
			items[i].value.SetZero()
		}
		f.key.SetZero()
	default:
		panic(draft.ErrType)
	}
	if p.kind >= nativeObject && p.kind <= nativeList {
		if keep {
			c.writer.EndPresent()
		} else {
			c.writer.End()
		}
	} else if keep {
		c.writer.PutPresent(key, text, scalar)
	} else {
		c.writer.Put(key, text, scalar)
	}
}

// Fast projections with at most one quarter of the stored fields use key
// lookups. Full objects and Compact projections use one ordered traversal.
// Both explicit controls remain benchmarkable; the threshold is a policy,
// not another format rule or a claimed optimum for every row.
func (c *nativeCodec[T]) decodeObject(p *nativePlan, dst reflect.Value, view currentView, borrow bool, depth int) {
	if view.Indexed() && len(p.fields) <= view.Len()/4 {
		c.decodeObjectKeys(p, dst, view, borrow, depth)
	} else {
		c.decodeObjectScan(p, dst, view, borrow, depth)
	}
}

func (c *nativeCodec[T]) GetFastScan(view currentView, out *T) {
	if out == nil {
		panic(draft.ErrValue)
	}
	c.decodeObjectScan(c.plan, reflect.ValueOf(out).Elem(), view, true, 0)
}
func (c *nativeCodec[T]) GetFastKeys(view currentView, out *T) {
	if out == nil {
		panic(draft.ErrValue)
	}
	c.decodeObjectKeys(c.plan, reflect.ValueOf(out).Elem(), view, true, 0)
}
func (c *nativeCodec[T]) decodeObjectKeys(p *nativePlan, dst reflect.Value, view currentView, borrow bool, depth int) {
	if depth > 256 {
		panic(draft.ErrLimit)
	}
	if view.n != 0 && view.Kind() != 0 {
		panic(draft.ErrKind)
	}
	for _, f := range p.fields {
		x, present := currentValue{}, false
		if !f.significant {
			x = view.Get(f.key)
			present = !x.IsZero()
		} else {
			x, present = currentLookup(view, f.key, "")
		}
		c.decode(f.plan, dst.Field(f.index), x, present, borrow, depth+1)
	}
}

func (c *nativeCodec[T]) decodeObjectScan(p *nativePlan, dst reflect.Value, view currentView, borrow bool, depth int) {
	if depth > 256 {
		panic(draft.ErrLimit)
	}
	if view.n != 0 && view.Kind() != 0 {
		panic(draft.ErrKind)
	}
	it := view.Iter()
	key, _, val, ok := it.Next()
	for _, f := range p.fields {
		for ok && key < f.key {
			key, _, val, ok = it.Next()
		}
		present := ok && key == f.key
		x := currentValue{}
		if present {
			x = val
		}
		c.decode(f.plan, dst.Field(f.index), x, present, borrow, depth+1)
	}
}

func (c *nativeCodec[T]) decode(p *nativePlan, dst reflect.Value, v currentValue, present, borrow bool, depth int) {
	if depth > 256 {
		panic(draft.ErrLimit)
	}
	switch p.kind {
	case nativeOption:
		dst.Field(1).SetBool(present)
		c.decode(p.elem, dst.Field(0), v, present, borrow, depth+1)
	case nativePointer:
		if !present || v.IsZero() {
			dst.SetZero()
			return
		}
		if dst.IsNil() {
			dst.Set(reflect.New(p.typ.Elem()))
		}
		c.decode(p.elem, dst.Elem(), v, true, borrow, depth+1)
	case nativeSigned:
		n := v.Int()
		if dst.OverflowInt(n) {
			panic(draft.ErrValue)
		}
		dst.SetInt(n)
	case nativeUnsigned:
		n := v.Uint()
		if dst.OverflowUint(n) {
			panic(draft.ErrValue)
		}
		dst.SetUint(n)
	case nativeBool:
		dst.SetBool(v.Bool())
	case nativeFloat:
		n := v.Float()
		if dst.OverflowFloat(n) {
			panic(draft.ErrValue)
		}
		dst.SetFloat(n)
	case nativeTime:
		*dst.Addr().Interface().(*time.Time) = kvo.Uint64ToTime(v.Uint()).UTC()
	case nativeString:
		b := v.Bytes()
		if borrow {
			dst.SetString(unsafe.String(unsafe.SliceData(b), len(b)))
		} else {
			dst.SetString(string(b))
		}
	case nativeBytes:
		src := v.Bytes()
		if len(src) > dst.Cap() {
			dst.SetBytes(bytes.Clone(src))
		} else {
			dst.SetLen(len(src))
			copy(dst.Bytes(), src)
		}
	case nativeObject:
		c.decodeObject(p, dst, currentTrusted(v.Bytes()), borrow, depth)
	case nativeList:
		view := currentTrusted(v.Bytes())
		if view.n != 0 && view.Kind() != 2 {
			panic(draft.ErrKind)
		}
		n := view.Len()
		if dst.Kind() == reflect.Array {
			if n != dst.Len() && n != 0 {
				panic(draft.ErrValue)
			}
			if n == 0 {
				dst.SetZero()
				return
			}
		} else {
			if n < dst.Len() {
				dst.Slice(n, dst.Len()).Clear()
			}
			if n > dst.Cap() {
				dst.Grow(n - dst.Len())
			}
			dst.SetLen(n)
		}
		it := view.Iter()
		for i := 0; i < n; i++ {
			_, _, value, _ := it.Next()
			c.decode(p.elem, dst.Index(i), value, true, borrow, depth+1)
		}
	case nativeDictionary, nativeNumbers:
		view := currentTrusted(v.Bytes())
		kind := 0
		if p.kind == nativeDictionary {
			kind = 1
		}
		if view.n != 0 && view.Kind() != kind {
			panic(draft.ErrKind)
		}
		n := view.Len()
		if n > 0 && dst.Len() > 0 && p.reuseMapValues && !c.rebuildMapValues {
			c.decodeMapReuse(p, dst, view, borrow, depth)
			return
		}
		if dst.IsNil() && n > 0 {
			dst.Set(reflect.MakeMapWithSize(p.typ, n))
		} else {
			dst.Clear()
		}
		f := nativeMapScratch(&c.maps, depth, p.typ)
		key, value := f.key, f.value // recursive use can grow c.maps
		it := view.Iter()
		for {
			number, text, x, ok := it.Next()
			if !ok {
				break
			}
			if !p.keepDictionaryZeros && x.IsZero() {
				continue
			}
			if kind == 1 {
				if borrow {
					key.SetString(unsafe.String(unsafe.SliceData(text), len(text)))
				} else {
					key.SetString(string(text))
				}
			} else {
				if key.OverflowUint(number) {
					panic(draft.ErrValue)
				}
				key.SetUint(number)
			}
			// Scalar temporaries are safe to reuse. Aggregate values must have
			// independent storage per map entry; reflection cannot address an
			// existing map element for in-place decode.
			value.SetZero()
			c.decode(p.elem, value, x, true, borrow, depth+1)
			dst.SetMapIndex(key, value)
		}
		key.SetZero()
		value.SetZero()
	default:
		panic(draft.ErrType)
	}
}

// Reflection cannot address map elements. Snapshot their addressable values
// once, clear membership, then match old keys to the wire keys using a reusable
// index. The retained sorted control merges the two ordered key streams.
// Matching entries keep their own mutable storage; unrelated destinations never
// acquire storage retained by the codec. Old borrowed keys must still be live.
func (c *nativeCodec[T]) decodeMapReuse(p *nativePlan, dst reflect.Value, view currentView, borrow bool, depth int) {
	f := nativeMapScratch(&c.maps, depth, p.typ)
	n := dst.Len()
	f.items = slices.Grow(f.items, max(0, n-len(f.items)))
	f.items = f.items[:max(len(f.items), n)]
	items := f.items[:n]
	it := dst.MapRange()
	index := 0
	for it.Next() {
		item := &items[index]
		f.key.SetIterKey(it)
		if p.kind == nativeDictionary {
			item.text = f.key.String()
		} else {
			item.number = f.key.Uint()
		}
		if !item.value.IsValid() || item.value.Type() != p.typ.Elem() {
			item.value = reflect.New(p.typ.Elem()).Elem()
		}
		item.value.SetIterValue(it)
		index++
	}
	if c.sortedMapSnapshots {
		slices.SortFunc(items, func(a, b nativeMapItem) int {
			if p.kind == nativeDictionary {
				if a.text < b.text {
					return -1
				}
				if a.text > b.text {
					return 1
				}
				return 0
			}
			if a.number < b.number {
				return -1
			}
			if a.number > b.number {
				return 1
			}
			return 0
		})
	} else if p.kind == nativeDictionary {
		if f.byText == nil {
			f.byText = make(map[string]int, n)
		}
		for i, item := range items {
			f.byText[item.text] = i
		}
	} else {
		if f.byNumber == nil {
			f.byNumber = make(map[uint64]int, n)
		}
		for i, item := range items {
			f.byNumber[item.number] = i
		}
	}
	dst.Clear()
	iter := view.Iter()
	index = 0
	for {
		number, text, x, ok := iter.Next()
		if !ok {
			break
		}
		if !p.keepDictionaryZeros && x.IsZero() {
			continue
		}
		keyText := ""
		if p.kind == nativeDictionary {
			if borrow {
				keyText = unsafe.String(unsafe.SliceData(text), len(text))
			} else {
				keyText = string(text)
			}
			f.key.SetString(keyText)
		} else {
			if f.key.OverflowUint(number) {
				panic(draft.ErrValue)
			}
			f.key.SetUint(number)
		}
		match := false
		if c.sortedMapSnapshots {
			for index < len(items) && (p.kind == nativeDictionary && items[index].text < keyText || p.kind == nativeNumbers && items[index].number < number) {
				index++
			}
			match = index < len(items) && (p.kind == nativeDictionary && items[index].text == keyText || p.kind == nativeNumbers && items[index].number == number)
		} else if p.kind == nativeDictionary {
			index, match = f.byText[keyText]
		} else {
			index, match = f.byNumber[number]
		}
		value := f.value
		if match {
			value = items[index].value
		} else {
			value.SetZero()
		}
		c.decode(p.elem, value, x, true, borrow, depth+1)
		dst.SetMapIndex(f.key, value)
	}
	clear(f.byText)
	clear(f.byNumber)
	f.key.SetZero()
	f.value.SetZero()
	for i := range items {
		items[i].text = ""
		items[i].value.SetZero()
	}
}
