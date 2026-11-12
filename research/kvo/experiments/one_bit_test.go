package kvolab

// One-bit Object/Dictionary entries: raw eight bytes, or a positive-immediate /
// byte-length tag. A short non-immediate word is an opaque byte span; its schema
// supplies interpretation. Lists use tags only. Both count policies are retained.
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

type oneBitWriter struct {
	lengthWriter
	omitCount bool
}

func (w *oneBitWriter) add(key uint64, wire byte, word uint64) {
	f := &w.frames[w.depth-1]
	f.entries = append(f.entries, classMeta{key: key, wire: draft.Wire(wire), word: word})
	if len(f.entries) > tagMaxEntries || len(w.out)-f.start > tagMaxBytes {
		panic(draft.ErrLimit)
	}
}

func (w *oneBitWriter) Begin(key uint64, text string, kind int) {
	w.lengthWriter.Begin(key, text, kind, false)
}
func (w *oneBitWriter) Put(key uint64, text string, v lengthValue, keepZero bool) {
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	key, rollback := w.prepare(key, text)
	f := &w.frames[w.depth-1]
	if v.IsZero() && !keepZero && f.kind != 2 {
		w.out = w.out[:rollback]
		return
	}
	wire := byte((bits.Len64(v.num) + 7) / 8)
	word := v.num
	if v.ptr != nil {
		if v.num > tagMaxBytes {
			panic(draft.ErrLimit)
		}
		wire = 9
		word = uint64(len(w.out)-f.start) | v.num<<32
		w.out = append(w.out, v.Bytes()...)
	} else if word > 127 {
		w.out = tagAppendWord(w.out, word, int(wire))
	}
	w.add(key, wire, word)
}

// The flag uses the low bit; delta has all 64 bits. This is a 65-bit uvarint,
// with at most ten bytes. Unlike a reserved-code escape it also covers key zero.
func oneBitAppendKey(dst []byte, key uint64, wide bool) []byte {
	first := byte(key&63) << 1
	if wide {
		first |= 1
	}
	key >>= 6
	if key == 0 {
		return append(dst, first)
	}
	dst = append(dst, first|128)
	return binary.AppendUvarint(dst, key)
}
func oneBitKey(src []byte) (key uint64, wide bool, rest []byte) {
	if len(src) == 0 {
		panic(errTag)
	}
	first := src[0]
	src = src[1:]
	key = uint64(first >> 1 & 63)
	wide = first&1 != 0
	if first < 128 {
		return key, wide, src
	}
	hi, r := lengthU(src)
	if hi > math.MaxUint64>>6 {
		panic(errTag)
	}
	return key | hi<<6, wide, r
}
func oneBitAppendTag(dst []byte, m classMeta) []byte {
	if m.wire != 9 && m.word <= 127 {
		return append(dst, byte(m.word))
	}
	n := uint64(m.wire)
	if m.wire == 9 {
		n = m.word >> 32
	}
	if n < 127 {
		return append(dst, 128+byte(n))
	}
	dst = append(dst, 255)
	return binary.AppendUvarint(dst, n-127)
}
func (w *oneBitWriter) End() {
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
	if !w.omitCount || f.kind == 2 {
		w.out = binary.AppendUvarint(w.out, uint64(len(f.entries)))
	}
	var prev uint64
	for _, m := range f.entries {
		wide := m.wire == 8 || m.wire == 9 && m.word>>32 == 8
		switch f.kind {
		case 0:
			w.out = oneBitAppendKey(w.out, m.key-prev, wide)
			prev = m.key
		case 1:
			at, n := int(uint32(m.key)), int(m.key>>32)
			w.out = oneBitAppendKey(w.out, uint64(n), wide)
			w.out = append(w.out, f.keys[at:at+n]...)
		case 2:
			wide = false
		}
		if !wide {
			w.out = oneBitAppendTag(w.out, m)
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
func oneBitEncode(w *oneBitWriter, dst []byte, v Value) []byte {
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
			switch x.Kind {
			case Text:
				w.Put(key, text, lengthBytes(tagStringBytes(x.S)), false)
			case Blob:
				w.Put(key, text, lengthBytes(x.B), false)
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
				}
				w.Put(key, text, lengthWord(word), false)
			}
		}
	}
	walk(0, "", v)
	return w.BytesWritten()
}

type oneBitIterator struct {
	payload, meta, previousText []byte
	kind, count, index, pos     int
	key                         uint64
}

func oneBitIter(raw []byte, omitCount bool) oneBitIterator {
	if len(raw) == 0 {
		return oneBitIterator{}
	}
	if len(raw) > tagMaxBytes {
		panic(errTag)
	}
	footer, body := tagTail(raw)
	kind := int(footer >> 1 & 3)
	cat := footer >> 3
	if footer&1 != 0 || kind > 2 || cat > uint64(len(body)) {
		panic(errTag)
	}
	at := len(body) - int(cat)
	meta := body[at:]
	n := -1
	if !omitCount || kind == 2 {
		count, r := lengthU(meta)
		meta = r
		if count > tagMaxEntries {
			panic(errTag)
		}
		n = int(count)
	}
	return oneBitIterator{payload: body[:at], meta: meta, kind: kind, count: n}
}
func (it *oneBitIterator) Next() (key uint64, text []byte, value lengthValue, ok bool) {
	if it.count >= 0 && it.index == it.count || it.count < 0 && len(it.meta) == 0 {
		if it.pos != len(it.payload) || len(it.meta) != 0 {
			panic(errTag)
		}
		return
	}
	if it.index >= tagMaxEntries {
		panic(errTag)
	}
	var wide bool
	switch it.kind {
	case 0:
		var delta uint64
		delta, wide, it.meta = oneBitKey(it.meta)
		if delta > math.MaxUint64-it.key || it.index > 0 && delta == 0 {
			panic(errTag)
		}
		it.key += delta
		key = it.key
	case 1:
		var n uint64
		n, wide, it.meta = oneBitKey(it.meta)
		if n > uint64(len(it.meta)) {
			panic(errTag)
		}
		text, it.meta = it.meta[:int(n)], it.meta[int(n):]
		if it.index > 0 && bytes.Compare(it.previousText, text) >= 0 {
			panic(errTag)
		}
		it.previousText = text
	case 2:
		key = uint64(it.index)
	}
	n := 8
	if !wide {
		if len(it.meta) == 0 {
			panic(errTag)
		}
		tag := it.meta[0]
		it.meta = it.meta[1:]
		if tag < 128 {
			it.index++
			return key, text, lengthWord(uint64(tag)), true
		}
		n = int(tag & 127)
		if n == 127 {
			u, r := lengthU(it.meta)
			it.meta = r
			if u > tagMaxBytes-127 {
				panic(errTag)
			}
			n = 127 + int(u)
		}
	}
	if n > len(it.payload)-it.pos {
		panic(errTag)
	}
	value = lengthBytes(it.payload[it.pos : it.pos+n])
	it.pos += n
	it.index++
	return key, text, value, true
}
func oneBitWord(v lengthValue) uint64 {
	if v.ptr == nil {
		return v.num
	}
	if v.num > 8 {
		panic(draft.ErrType)
	}
	return tagWord(v.Bytes())
}
func oneBitGet(raw []byte, omitCount bool, key uint64, text string) lengthValue {
	it := oneBitIter(raw, omitCount)
	for {
		k, s, v, ok := it.Next()
		if !ok {
			return lengthValue{}
		}
		if it.kind == 1 {
			c := bytes.Compare(s, tagStringBytes(text))
			if c == 0 {
				return v
			}
			if c > 0 {
				return lengthValue{}
			}
		} else {
			if k == key {
				return v
			}
			if k > key {
				return lengthValue{}
			}
		}
	}
}
func oneBitOpen(raw []byte, omitCount bool) (err error) {
	defer func() {
		if recover() != nil {
			err = draft.ErrFormat
		}
	}()
	it := oneBitIter(raw, omitCount)
	for {
		_, _, _, ok := it.Next()
		if !ok {
			return
		}
	}
}
func checkOneBitTree(t testing.TB, source Value, raw []byte, omitCount bool) {
	t.Helper()
	if err := oneBitOpen(raw, omitCount); err != nil {
		t.Fatal(err)
	}
	it := oneBitIter(raw, omitCount)
	count := 0
	check := func(key uint64, text string, x Value) {
		if source.Kind != List && tagZero(x) {
			return
		}
		k, s, v, ok := it.Next()
		if !ok || source.Kind == Record && k != key || source.Kind == Dict && string(s) != text || source.Kind == List && k != uint64(count) {
			t.Fatal("key/order")
		}
		switch {
		case tagZero(x):
			if !v.IsZero() {
				t.Fatal("zero")
			}
		case x.Kind == Record || x.Kind == Dict || x.Kind == List:
			checkOneBitTree(t, x, v.Bytes(), omitCount)
		case x.Kind == Text:
			if string(v.Bytes()) != x.S {
				t.Fatal("text")
			}
		case x.Kind == Blob:
			if !bytes.Equal(v.Bytes(), x.B) {
				t.Fatal("blob")
			}
		default:
			if oneBitWord(v) != draftValue(x, draft.Compact).Uint() {
				t.Fatal("word")
			}
		}
		got := oneBitTrustedGet(raw, omitCount, k, string(s))
		if got.num != v.num || got.ptr != v.ptr {
			t.Fatal("trusted lookup")
		}
		count++
	}
	switch source.Kind {
	case Record:
		for _, f := range source.Fields {
			check(f.Key, "", f.Value)
		}
	case Dict:
		for _, e := range source.Entries {
			check(0, e.Key, e.Value)
		}
	case List:
		for _, v := range source.Items {
			check(0, "", v)
		}
	}
	if _, _, _, ok := it.Next(); ok {
		t.Fatal("extra entry")
	}
}
func TestOneBit(t *testing.T) {
	all := fixtures()
	for n, v := range selectorListFixtures() {
		all["list-"+n] = v
	}
	for n, v := range workloadFixtures(t) {
		all[n] = v
	}
	all["full-key"] = Obj(Field{0, U(128)}, Field{math.MaxUint64, U(math.MaxUint64)})
	all["empty"] = Obj()
	all["limits"] = Obj(Field{0, U(0)}, Field{1, U(127)}, Field{2, U(128)}, Field{3, U(65535)}, Field{4, I(math.MinInt64)}, Field{5, S("12345678")}, Field{6, S(string(bytes.Repeat([]byte{'x'}, 127)))})
	for _, omit := range []bool{false, true} {
		total := 0
		for name, source := range all {
			t.Run(fmt.Sprintf("%s/omit%v", name, omit), func(t *testing.T) {
				w := oneBitWriter{omitCount: omit}
				raw := oneBitEncode(&w, nil, source)
				checkOneBitTree(t, source, raw, omit)
				it := oneBitIter(raw, omit)
				var rewrite oneBitWriter
				rewrite.omitCount = omit
				rewrite.Begin(0, "", it.kind)
				for {
					k, s, v, ok := it.Next()
					if !ok {
						break
					}
					rewrite.Put(k, string(s), v, true)
				}
				rewrite.End()
				if !bytes.Equal(raw, rewrite.BytesWritten()) {
					t.Fatal("unknown rewrite changed bytes")
				}
				if _, ok := workloadFixtures(t)[name]; ok {
					total += len(raw)
				}
			})
		}
		t.Logf("replay omit_count=%v total=%d", omit, total)
	}
	for _, omit := range []bool{false, true} {
		w := oneBitWriter{omitCount: omit}
		w.Begin(0, "", 0)
		w.Put(1, "", lengthWord(0), true)
		w.End()
		it := oneBitIter(w.BytesWritten(), omit)
		k, _, v, ok := it.Next()
		if !ok || k != 1 || !v.IsZero() {
			t.Fatal("present zero")
		}
	}
}
func FuzzOneBit(f *testing.F) {
	f.Add([]byte{0}, true)
	f.Add([]byte{0, 8}, false)
	for _, omit := range []bool{false, true} {
		w := oneBitWriter{omitCount: omit}
		f.Add(oneBitEncode(&w, nil, fixtures()["state"]), omit)
	}
	f.Fuzz(func(t *testing.T, raw []byte, omit bool) { _ = oneBitOpen(raw, omit) })
}
func BenchmarkOneBit(b *testing.B) {
	all := fixtures()
	for n, v := range selectorListFixtures() {
		all["list-"+n] = v
	}
	for n, v := range workloadFixtures(b) {
		all[n] = v
	}
	names := []string{"state", "settings", "dictionary-128", "list-zero", "list-boolean", "list-uint16", "list-mostly-word8"}
	replay := make([]string, 0, len(workloadFixtures(b)))
	for n := range workloadFixtures(b) {
		replay = append(replay, n)
	}
	sort.Strings(replay)
	// Shape fixtures at encoded size quantiles, rather than uniform word values.
	sort.SliceStable(replay, func(i, j int) bool {
		var a, c lengthWriter
		return len(lengthEncode(&a, nil, all[replay[i]], nil)) < len(lengthEncode(&c, nil, all[replay[j]], nil))
	})
	for _, q := range []int{len(replay) / 2, len(replay) * 9 / 10, len(replay) - 1} {
		names = append(names, replay[q])
	}
	for _, n := range []int{0, 1, 2, 4, 8} {
		name := fmt.Sprintf("tiny%d", n)
		all[name] = numeric(n, 0)
		names = append(names, name)
	}
	for _, name := range names {
		source := all[name]
		for _, mode := range []string{"literal4", "onebit", "onebit-no-count"} {
			b.Run(name+"/"+mode, func(b *testing.B) {
				var lw lengthWriter
				ow := oneBitWriter{omitCount: mode == "onebit-no-count"}
				var out []byte
				encode := func() {
					if mode == "literal4" {
						out = lengthEncode(&lw, out[:0], source, nil)
					} else {
						out = oneBitEncode(&ow, out[:0], source)
					}
				}
				encode()
				b.Run("encode", func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						encode()
					}
					sinkBytes = out
					b.ReportMetric(float64(len(out)), "encoded-B")
				})
				raw := bytes.Clone(out)

				b.Run("read", func(b *testing.B) {
					type queryValue struct {
						key  uint64
						text string
						word bool
					}
					var queries []queryValue
					add := func(key uint64, text string, x Value) {
						queries = append(queries, queryValue{key, text, x.Kind != Text && x.Kind != Blob && x.Kind != Record && x.Kind != Dict && x.Kind != List})
					}
					switch source.Kind {
					case Record:
						for _, f := range source.Fields {
							add(f.Key, "", f.Value)
						}
					case Dict:
						for _, e := range source.Entries {
							add(0, e.Key, e.Value)
						}
					case List:
						for j, v := range source.Items {
							add(uint64(j), "", v)
						}
					}
					if len(queries) == 0 {
						b.Skip("no entries")
					}
					var sum uint64
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						q := queries[i%len(queries)]
						var v lengthValue
						if mode == "literal4" {
							view := lengthTrusted(raw)
							switch source.Kind {
							case Record:
								v = view.Get(q.key)
							case Dict:
								v = view.GetString(q.text)
							case List:
								v = view.At(int(q.key))
							}
							if q.word {
								sum += v.Uint()
							} else {
								sum += uint64(len(v.Bytes()))
							}
						} else {
							v = oneBitTrustedGet(raw, ow.omitCount, q.key, q.text)
							if q.word {
								sum += oneBitWord(v)
							} else {
								sum += uint64(len(v.Bytes()))
							}
						}
					}
					sinkU = sum
				})
				b.Run("scan-framing", func(b *testing.B) {
					b.ReportAllocs()
					var sum uint64
					if mode == "literal4" {
						for i := 0; i < b.N; i++ {
							it := lengthTrusted(raw).Iter()
							for {
								k, s, v, ok := it.Next()
								if !ok {
									break
								}
								sum += k + uint64(len(s)) + v.num
							}
						}
					} else {
						for i := 0; i < b.N; i++ {
							it := oneBitIter(raw, ow.omitCount)
							for {
								k, s, v, ok := it.Next()
								if !ok {
									break
								}
								sum += k + uint64(len(s)) + v.num
							}
						}
					}
					sinkU = sum
				})
			})
		}
	}
}

// Trusted lookup has the same boundary assumption as lengthView.Get. Retain the
// checked iterator lookup separately; its ordering/bounds validation is extra work.
func oneBitTrustedGet(raw []byte, omitCount bool, target uint64, text string) lengthValue {
	if len(raw) == 0 {
		return lengthValue{}
	}
	footer := uint64(raw[len(raw)-1])
	body := raw[:len(raw)-1]
	if footer >= 128 {
		footer, body = tagTail(raw)
	}
	kind := int(footer >> 1 & 3)
	at := len(body) - int(footer>>3)
	payload, meta := body[:at], body[at:]
	if !omitCount || kind == 2 {
		_, meta = lengthU(meta)
	}
	var key uint64
	pos, index := 0, uint64(0)
	for len(meta) != 0 {
		wide := false
		cmp := 0
		switch kind {
		case 0:
			var delta uint64
			delta, wide, meta = oneBitKey(meta)
			key += delta
			if key > target {
				return lengthValue{}
			}
			if key < target {
				cmp = -1
			}
		case 1:
			var n uint64
			n, wide, meta = oneBitKey(meta)
			s := meta[:int(n)]
			meta = meta[int(n):]
			cmp = bytes.Compare(s, tagStringBytes(text))
			if cmp > 0 {
				return lengthValue{}
			}
		case 2:
			if index < target {
				cmp = -1
			}
			index++
		}
		n := 8
		if !wide {
			tag := meta[0]
			meta = meta[1:]
			if tag < 128 {
				if cmp == 0 {
					return lengthWord(uint64(tag))
				}
				continue
			}
			n = int(tag & 127)
			if n == 127 {
				var u uint64
				u, meta = lengthU(meta)
				n += int(u)
			}
		}
		if cmp == 0 {
			return lengthBytes(payload[pos : pos+n])
		}
		pos += n
	}
	return lengthValue{}
}
