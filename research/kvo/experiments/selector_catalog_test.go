package kvolab

// Comparable forward writers/readers for literal-4, rounded-3 and tag/4/8-2.
// The row's codec selects this dialect; no child carries another version.
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type selectorMode uint8

const (
	selectorLiteral selectorMode = iota
	selectorRounded
	selectorTagged
	selectorTaggedWide
)

func (m selectorMode) bits() uint {
	if m >= selectorTagged {
		return 2
	}
	return 4 - uint(m)
}
func (m selectorMode) mixed() byte { return byte(1<<m.bits() - 1) }
func (m selectorMode) code(w byte) byte {
	switch m {
	case selectorLiteral:
		return w
	case selectorRounded:
		switch w {
		case 0, 1, 2:
			return w
		case 4:
			return 3
		case 8:
			return 4
		case 9:
			return 5
		}
	case selectorTagged, selectorTaggedWide:
		switch w {
		case 4:
			return 1
		case 8:
			return 2
		default:
			return 0
		}
	}
	panic(draft.ErrType)
}
func (m selectorMode) String() string {
	return []string{"literal4", "rounded3", "tagged2", "tagged2-wide"}[m]
}

type selectorWriter struct {
	lengthWriter
	mode selectorMode
}

func (w *selectorWriter) Begin(key uint64, text string, kind int) {
	w.lengthWriter.Begin(key, text, kind, false)
	w.frames[w.depth-1].shared = w.mode.mixed()
}
func (w *selectorWriter) add(key uint64, wire byte, word uint64) {
	f := &w.frames[w.depth-1]
	code := w.mode.code(wire)
	if len(f.entries) == 0 {
		f.shared = code
	} else if f.shared != code {
		f.shared = w.mode.mixed()
	}
	f.entries = append(f.entries, classMeta{key: key, wire: draft.Wire(wire), word: word})
	if len(f.entries) > tagMaxEntries || len(w.out)-f.start > tagMaxBytes {
		panic(draft.ErrLimit)
	}
}
func (w *selectorWriter) Put(key uint64, text string, v lengthValue, keepZero bool) {
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	key, rollback := w.prepare(key, text)
	f := &w.frames[w.depth-1]
	if v.IsZero() && !keepZero && f.kind != 2 {
		w.out = w.out[:rollback]
		return
	}
	word := v.num
	wire := byte((bits.Len64(word) + 7) / 8)
	if v.ptr != nil {
		if v.num > tagMaxBytes {
			panic(draft.ErrLimit)
		}
		wire = 9
		word = uint64(len(w.out)-f.start) | v.num<<32
		w.out = append(w.out, v.Bytes()...)
	} else {
		if w.mode == selectorRounded {
			if wire == 3 {
				wire = 4
			} else if wire > 4 {
				wire = 8
			}
		}
		if w.mode < selectorTagged || word > 127 {
			w.out = tagAppendWord(w.out, word, int(wire))
		}
	}
	w.add(key, wire, word)
}
func selectorAppendTag(dst []byte, m classMeta, mode selectorMode) []byte {
	if mode == selectorTaggedWide {
		if m.wire == 9 {
			n := m.word >> 32
			if n < 121 {
				return append(dst, 128+byte(n))
			}
			dst = append(dst, 249)
			return binary.AppendUvarint(dst, n-121)
		}
		if m.word <= 127 {
			return append(dst, byte(m.word))
		}
		n := byte(m.wire)
		if n >= 5 {
			n--
		}
		return append(dst, 249+n)
	}

	if m.wire == 9 {
		n := m.word >> 32
		if n < 63 {
			return append(dst, 128+byte(n))
		}
		dst = append(dst, 191)
		return binary.AppendUvarint(dst, n-63)
	}
	if m.word <= 127 {
		return append(dst, byte(m.word))
	}
	return append(dst, 192+byte(m.wire))
}
func (w *selectorWriter) End() {
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	f := &w.frames[w.depth-1]
	if len(f.entries) == 0 && w.depth > 1 {
		key, rollback := f.parentKey, f.rollback
		w.out = w.out[:rollback]
		w.depth--
		if w.frames[w.depth-1].kind == 2 {
			w.add(key, 0, 0)
		}
		return
	}
	cat := len(w.out)
	shift := w.mode.bits()
	mixed := w.mode.mixed()
	w.out = binary.AppendUvarint(w.out, uint64(len(f.entries))<<shift|uint64(f.shared))
	var prev uint64
	for _, m := range f.entries {
		code := w.mode.code(byte(m.wire))
		if f.kind == 0 {
			delta := m.key - prev
			prev = m.key
			if f.shared != mixed {
				w.out = binary.AppendUvarint(w.out, delta)
			} else if delta <= math.MaxUint64>>shift {
				w.out = binary.AppendUvarint(w.out, delta<<shift|uint64(code))
			} else {
				w.out = append(w.out, mixed)
				w.out = binary.AppendUvarint(w.out, delta)
				w.out = append(w.out, code)
			}
		} else {
			if f.kind == 1 {
				at, n := int(uint32(m.key)), int(m.key>>32)
				w.out = binary.AppendUvarint(w.out, uint64(n))
				w.out = append(w.out, f.keys[at:at+n]...)
			}
			if f.shared == mixed {
				w.out = append(w.out, code)
			}
		}
		if w.mode >= selectorTagged {
			if code == 0 {
				w.out = selectorAppendTag(w.out, m, w.mode)
			}
		} else if m.wire == 9 {
			w.out = binary.AppendUvarint(w.out, m.word>>32)
		}
	}
	w.out = tagAppendTail(w.out, uint64(len(w.out)-cat)<<3|uint64(f.kind)<<1)
	n := len(w.out) - f.start
	if n > tagMaxBytes {
		panic(draft.ErrLimit)
	}
	key, start := f.parentKey, f.start
	w.depth--
	if w.depth > 0 {
		w.add(key, 9, uint64(start-w.frames[w.depth-1].start)|uint64(n)<<32)
	}
}
func selectorEncode(w *selectorWriter, dst []byte, v Value) []byte {
	w.Reset(dst)
	var walk func(uint64, string, Value)
	walk = func(key uint64, text string, x Value) {
		switch x.Kind {
		case Record, Dict, List:
			kind := 0
			if x.Kind == Dict {
				kind = 1
			} else if x.Kind == List {
				kind = 2
			}
			w.Begin(key, text, kind)
			switch x.Kind {
			case Record:
				w.ReserveEntries(len(x.Fields))
				for _, f := range x.Fields {
					walk(f.Key, "", f.Value)
				}
			case Dict:
				w.ReserveEntries(len(x.Entries))
				for _, e := range x.Entries {
					walk(0, e.Key, e.Value)
				}
			case List:
				w.ReserveEntries(len(x.Items))
				for _, v := range x.Items {
					walk(0, "", v)
				}
			}
			w.End()
		default:
			word := x.U
			switch x.Kind {
			case Null, False:
				word = 0
			case True:
				word = 1
			case Int:
				i := int64(x.U)
				word = uint64(i<<1) ^ uint64(i>>63)
			case Float:
				if math.Float64frombits(word) == 0 {
					word = 0
				}
			case Text:
				w.Put(key, text, lengthBytes(tagStringBytes(x.S)), false)
				return
			case Blob:
				w.Put(key, text, lengthBytes(x.B), false)
				return
			}
			w.Put(key, text, lengthWord(word), false)
		}
	}
	walk(0, "", v)
	return w.BytesWritten()
}

type selectorIterator struct {
	mode                    selectorMode
	kind, n, index, pos     int
	key                     uint64
	shared                  byte
	payload, meta, prevText []byte
}

func selectorIter(raw []byte, mode selectorMode) selectorIterator {
	if mode > selectorTaggedWide {
		panic(errTag)
	}
	if len(raw) == 0 {
		return selectorIterator{mode: mode}
	}
	if len(raw) > tagMaxBytes {
		panic(errTag)
	}
	footer, b := tagTail(raw)
	kind := int(footer >> 1 & 3)
	n := footer >> 3
	if footer&1 != 0 || kind > 2 || n > uint64(len(b)) {
		panic(errTag)
	}
	at := len(b) - int(n)
	meta := b[at:]
	header, meta := lengthU(meta)
	count := header >> mode.bits()
	if count > tagMaxEntries {
		panic(errTag)
	}
	return selectorIterator{mode: mode, kind: kind, n: int(count), shared: byte(header) & mode.mixed(), payload: b[:at], meta: meta}
}
func (it *selectorIterator) Next() (key uint64, text []byte, value lengthValue, ok bool) {
	if it.index == it.n {
		if it.pos != len(it.payload) || len(it.meta) != 0 {
			panic(errTag)
		}
		return
	}
	code := it.shared
	mixed := it.mode.mixed()
	var delta uint64
	switch it.kind {
	case 0:
		var u uint64
		u, it.meta = lengthU(it.meta)
		if code == mixed {
			code = byte(u) & mixed
			if code == mixed {
				delta, it.meta = lengthU(it.meta)
				code, it.meta = it.meta[0], it.meta[1:]
			} else {
				delta = u >> it.mode.bits()
			}
		} else {
			delta = u
		}
		if delta > math.MaxUint64-it.key || it.index > 0 && delta == 0 {
			panic(errTag)
		}
		it.key += delta
		key = it.key
	case 1:
		n, rest := lengthU(it.meta)
		if n > uint64(len(rest)) {
			panic(errTag)
		}
		text, it.meta = rest[:int(n)], rest[int(n):]
		if it.index > 0 && bytes.Compare(it.prevText, text) >= 0 {
			panic(errTag)
		}
		it.prevText = text
		if code == mixed {
			code, it.meta = it.meta[0], it.meta[1:]
		}
	case 2:
		key = uint64(it.index)
		if code == mixed {
			code, it.meta = it.meta[0], it.meta[1:]
		}
	}
	n := 0
	span := false
	immediate := false
	var word uint64
	switch it.mode {
	case selectorLiteral:
		if code > 9 {
			panic(errTag)
		}
		n = int(code)
		span = code == 9
	case selectorRounded:
		if code > 5 {
			panic(errTag)
		}
		n = int([6]byte{0, 1, 2, 4, 8, 0}[code])
		span = code == 5
	case selectorTagged, selectorTaggedWide:
		switch code {
		case 1:
			n = 4
		case 2:
			n = 8
		case 0:
			tag := it.meta[0]
			it.meta = it.meta[1:]
			if it.mode == selectorTaggedWide {
				switch {
				case tag < 128:
					word = uint64(tag)
					immediate = true
				case tag < 249:
					span = true
					n = int(tag - 128)
				case tag == 249:
					u, rest := lengthU(it.meta)
					if u > uint64(len(it.payload)) {
						panic(errTag)
					}
					it.meta = rest
					span = true
					n = int(u) + 121
				default:
					n = int(tag) - 249
					if n >= 4 {
						n++
					}
				}
				break
			}

			switch {
			case tag < 128:
				word = uint64(tag)
				immediate = true
			case tag < 191:
				span = true
				n = int(tag - 128)
			case tag == 191:
				u, rest := lengthU(it.meta)
				if u > uint64(len(it.payload)) {
					panic(errTag)
				}
				it.meta = rest
				span = true
				n = int(u) + 63
			case tag <= 200:
				n = int(tag - 192)
			default:
				panic(errTag)
			}
		default:
			panic(errTag)
		}
	}
	if span && it.mode < selectorTagged {
		u, rest := lengthU(it.meta)
		if u > uint64(len(it.payload)) {
			panic(errTag)
		}
		n = int(u)
		it.meta = rest
	}
	if n > len(it.payload)-it.pos {
		panic(errTag)
	}
	if immediate {
		value = lengthWord(word)
	} else if span {
		value = lengthBytes(it.payload[it.pos : it.pos+n])
	} else {
		value = lengthWord(tagWord(it.payload[it.pos : it.pos+n]))
	}
	it.pos += n
	it.index++
	return key, text, value, true
}
func selectorOpen(raw []byte, mode selectorMode) (err error) {
	defer func() {
		if recover() != nil {
			err = draft.ErrFormat
		}
	}()
	it := selectorIter(raw, mode)
	for {
		_, _, _, ok := it.Next()
		if !ok {
			break
		}
	}
	return nil
}
func selectorGet(raw []byte, mode selectorMode, key uint64, text string) (lengthValue, bool) {
	it := selectorIter(raw, mode)
	for {
		k, s, v, ok := it.Next()
		if !ok {
			return lengthValue{}, false
		}
		if it.kind == 1 {
			cmp := bytes.Compare(s, tagStringBytes(text))
			if cmp == 0 {
				return v, true
			}
			if cmp > 0 {
				return lengthValue{}, false
			}
		} else {
			if k == key {
				return v, true
			}
			if k > key {
				return lengthValue{}, false
			}
		}
	}
}
func selectorRewrite(w *selectorWriter, dst, raw []byte, key uint64, v lengthValue, keepZero bool) []byte {
	it := selectorIter(raw, w.mode)
	if it.kind != 0 {
		panic(draft.ErrKind)
	}
	w.Reset(dst)
	w.Begin(0, "", 0)
	w.ReserveEntries(it.n + 1)
	emitted := false
	for {
		k, _, x, ok := it.Next()
		if !ok {
			break
		}
		if !emitted && k >= key {
			w.Put(key, "", v, keepZero)
			emitted = true
		}
		if k != key {
			w.Put(k, "", x, true)
		}
	}
	if !emitted {
		w.Put(key, "", v, keepZero)
	}
	w.End()
	return w.BytesWritten()
}
func checkSelector(t testing.TB, want Value, raw []byte, mode selectorMode) {
	t.Helper()
	if err := selectorOpen(raw, mode); err != nil {
		t.Fatal("open", mode, err)
	}
	it := selectorIter(raw, mode)
	count := 0
	check := func(key uint64, text string, w Value) {
		if want.Kind != List && tagZero(w) {
			return
		}
		k, s, v, ok := it.Next()
		if !ok || want.Kind == Record && k != key || want.Kind == Dict && string(s) != text {
			t.Fatal("entry")
		}
		count++
		if tagZero(w) {
			if !v.IsZero() {
				t.Fatal("zero")
			}
			return
		}
		switch w.Kind {
		case Record, Dict, List:
			checkSelector(t, w, v.Bytes(), mode)
		case Text:
			if string(v.Bytes()) != w.S {
				t.Fatal("text")
			}
		case Blob:
			if !bytes.Equal(v.Bytes(), w.B) {
				t.Fatal("blob")
			}
		default:
			if v.Uint() != draftValue(w, draft.Compact).Uint() {
				t.Fatal("word")
			}
		}
	}
	switch want.Kind {
	case Record:
		for _, f := range want.Fields {
			check(f.Key, "", f.Value)
		}
	case Dict:
		for _, e := range want.Entries {
			check(0, e.Key, e.Value)
		}
	case List:
		for i, v := range want.Items {
			check(uint64(i), "", v)
		}
	}
	if _, _, _, ok := it.Next(); ok || count != it.n {
		t.Fatal("count")
	}
}
func TestSelectorCatalog(t *testing.T) {
	all := fixtures()
	for name, v := range workloadFixtures(t) {
		all[name] = v
	}
	all["boundaries"] = Obj(Field{0, U(1)}, Field{8, U(128)}, Field{16, U(1 << 16)}, Field{32, U(1 << 32)}, Field{64, S(string(make([]byte, 62)))}, Field{128, S(string(make([]byte, 63)))}, Field{math.MaxUint64, Value{Kind: List, Items: []Value{U(0), I(-1), I(math.MinInt64), U(math.MaxUint64), S("")}}})
	for name, v := range all {
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			t.Run(name+"/"+mode.String(), func(t *testing.T) {
				w := selectorWriter{mode: mode}
				raw := selectorEncode(&w, nil, v)
				checkSelector(t, v, raw, mode)
				if mode == selectorLiteral {
					var common lengthWriter
					if !bytes.Equal(raw, lengthEncode(&common, nil, v, nil)) {
						t.Fatal("literal control bytes differ")
					}
				}
				raw2 := selectorEncode(&w, w.out[:0], v)
				checkSelector(t, v, raw2, mode)
			})
		}
	}
	for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
		w := selectorWriter{mode: mode}
		raw := selectorEncode(&w, nil, Obj(Field{2, U(7)}, Field{99, Value{Kind: Blob, B: []byte{255, 254}}}))
		out := selectorRewrite(&w, nil, raw, 2, lengthWord(0), true)
		v, ok := selectorGet(out, mode, 2, "")
		if !ok || !v.IsZero() {
			t.Fatal("present zero")
		}
		out = selectorRewrite(&w, nil, out, 2, lengthWord(0), false)
		if _, ok = selectorGet(out, mode, 2, ""); ok {
			t.Fatal("ordinary clear")
		}
	}
}
func FuzzSelectorCatalog(f *testing.F) {
	for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
		for _, v := range fixtures() {
			w := selectorWriter{mode: mode}
			f.Add(uint8(mode), selectorEncode(&w, nil, v))
		}
	}
	f.Fuzz(func(t *testing.T, m uint8, b []byte) {
		if len(b) > 65536 {
			return
		}
		mode := selectorMode(m % 4)
		if selectorOpen(b, mode) == nil {
			it := selectorIter(b, mode)
			for {
				_, _, _, ok := it.Next()
				if !ok {
					break
				}
			}
		}
	})
}
func selectorBenchFixtures(tb testing.TB) map[string]Value {
	out := fixtures()
	keys := make([]string, 0, len(workloadFixtures(tb)))
	for k := range workloadFixtures(tb) {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	// Deterministic quantiles of root encoded size avoid cherry-picking formats.
	var w lengthWriter
	sort.SliceStable(keys, func(i, j int) bool {
		return len(lengthEncode(&w, nil, workloadFixtures(tb)[keys[i]], nil)) < len(lengthEncode(&w, nil, workloadFixtures(tb)[keys[j]], nil))
	})
	for _, q := range []int{0, 25, 50, 75, 90, 100} {
		i := q * (len(keys) - 1) / 100
		out[fmt.Sprintf("sample-q%d", q)] = workloadFixtures(tb)[keys[i]]
	}
	return out
}
func BenchmarkSelectorCatalog(b *testing.B) {
	all := selectorBenchFixtures(b)
	keys := make([]string, 0, len(all))
	for k := range all {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, name := range keys {
		source := all[name]
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			for _, cold := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/%s/cold%v", name, mode, cold), func(b *testing.B) {
					w := selectorWriter{mode: mode}
					dst := selectorEncode(&w, nil, source)
					size := len(dst)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if cold {
							c := selectorWriter{mode: mode}
							dst = selectorEncode(&c, nil, source)
						} else {
							dst = selectorEncode(&w, dst[:0], source)
						}
					}
					sinkBytes = dst
					b.ReportMetric(float64(size), "encoded-B")
				})
			}
		}
	}
}
func BenchmarkSelectorAccess(b *testing.B) {
	for _, kind := range []string{"object", "dictionary", "list"} {
		source := numeric(128, 0)
		key, text := uint64(64), ""
		if kind == "dictionary" {
			source = fixtures()["dictionary-128"]
			text = source.Entries[64].Key
		} else if kind == "list" {
			source = Value{Kind: List}
			for i := 0; i < 128; i++ {
				source.Items = append(source.Items, U(uint64(i*3)))
			}
		}
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			w := selectorWriter{mode: mode}
			raw := selectorEncode(&w, nil, source)
			for _, op := range []string{"get", "scan", "open", "edit"} {
				if op == "edit" && kind != "object" {
					continue
				}
				b.Run(fmt.Sprintf("%s/%s/%s", kind, mode, op), func(b *testing.B) {
					var dst []byte
					if op == "edit" {
						dst = selectorRewrite(&w, nil, raw, key, lengthWord(9), false)
					}
					b.ReportAllocs()
					b.ResetTimer()
					var sum uint64
					for i := 0; i < b.N; i++ {
						switch op {
						case "get":
							v, ok := selectorGet(raw, mode, key, text)
							if ok {
								sum += v.num
							}
						case "scan":
							it := selectorIter(raw, mode)
							for {
								_, _, v, ok := it.Next()
								if !ok {
									break
								}
								sum += v.num
							}
						case "open":
							if selectorOpen(raw, mode) != nil {
								b.Fatal("open")
							}
						case "edit":
							dst = selectorRewrite(&w, dst[:0], raw, key, lengthWord(uint64(i)), false)
						}
					}
					sinkU = sum
					sinkBytes = dst
					b.ReportMetric(float64(len(raw)), "encoded-B")
				})
			}
		}
	}
}
func TestSelectorReplaySizes(t *testing.T) {
	names := make([]string, 0, len(workloadFixtures(t)))
	for k := range workloadFixtures(t) {
		names = append(names, k)
	}
	sort.Strings(names)
	var totals [4]int
	for _, name := range names {
		var sizes [4]int
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			w := selectorWriter{mode: mode}
			sizes[mode] = len(selectorEncode(&w, nil, workloadFixtures(t)[name]))
			totals[mode] += sizes[mode]
		}
		t.Logf("%s %d %d %d %d", name, sizes[0], sizes[1], sizes[2], sizes[3])
	}
	t.Logf("TOTAL %d %d %d %d", totals[0], totals[1], totals[2], totals[3])
}

// Homogeneous scalar lists keep direct indexing. Tagged lists whose payload is
// empty and whose catalog has exactly one byte per item also admit direct access
// without another wire code: every item is an immediate or a zero-length alias.
func selectorAt(raw []byte, mode selectorMode, index int) lengthValue {
	it := selectorIter(raw, mode)
	if it.kind != 2 {
		panic(draft.ErrKind)
	}
	if index < 0 || index >= it.n {
		panic(draft.ErrIndex)
	}
	width := -1
	switch mode {
	case selectorLiteral:
		if it.shared <= 8 {
			width = int(it.shared)
		}
	case selectorRounded:
		if it.shared <= 4 {
			width = int([5]byte{0, 1, 2, 4, 8}[it.shared])
		}
	case selectorTagged, selectorTaggedWide:
		if it.shared == 1 {
			width = 4
		} else if it.shared == 2 {
			width = 8
		} else if it.shared == 0 && len(it.payload) == 0 && len(it.meta) == it.n {
			tag := it.meta[index]
			if tag < 128 {
				return lengthWord(uint64(tag))
			}
			if tag == 128 || mode == selectorTagged && tag == 192 {
				return lengthWord(0)
			}
			panic(draft.ErrFormat)
		}
	}
	if width >= 0 {
		return lengthWord(tagWord(it.payload[index*width : (index+1)*width]))
	}
	for i := 0; ; i++ {
		_, _, v, _ := it.Next()
		if i == index {
			return v
		}
	}
}
func TestSelectorListIndex(t *testing.T) {
	for _, base := range []uint64{0, 1, 128, 1 << 16, 1 << 24, 1 << 32, 1 << 56} {
		v := Value{Kind: List}
		for i := uint64(0); i < 64; i++ {
			v.Items = append(v.Items, U(base+i))
		}
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			w := selectorWriter{mode: mode}
			raw := selectorEncode(&w, nil, v)
			for i, x := range v.Items {
				if selectorAt(raw, mode, i).Uint() != x.U {
					t.Fatal("index", base, mode, i)
				}
			}
		}
	}
}
func BenchmarkSelectorListIndex(b *testing.B) {
	for _, base := range []uint64{1, 128, 1 << 16, 1 << 24, 1 << 32, 1 << 56} {
		v := Value{Kind: List}
		for i := uint64(0); i < 64; i++ {
			v.Items = append(v.Items, U(base+i))
		}
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			w := selectorWriter{mode: mode}
			raw := selectorEncode(&w, nil, v)
			b.Run(fmt.Sprintf("base%d/%s", base, mode), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					sum += selectorAt(raw, mode, i&63).Uint()
				}
				sinkU = sum
				b.ReportMetric(float64(len(raw)), "encoded-B")
			})
		}
	}
}
