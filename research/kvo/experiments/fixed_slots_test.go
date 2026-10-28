package kvolab

// Selected fixed-slot experiment. The normal keyed area remains Fast. Slot keys
// and scalar/span/presence bits make unknown slots independently preservable.
import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"testing"

	"github.com/andreyvit/edb/kvo"
	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type slotField struct {
	key                   uint64
	presenceIsSignificant bool
}
type slotPlan []slotField

func (p slotPlan) index(key uint64) int {
	for i, f := range p {
		if f.key == key {
			return i
		}
	}
	return -1
}
func slotSource(v Value, key uint64) Value {
	for _, f := range v.Fields {
		if f.Key == key {
			return f.Value
		}
	}
	return U(0)
}

// This adapter covers scalar/span fields. Child containers are supplied as opaque
// spans, just as to the common writer; no second payload buffer is required here.
func slotValue(v Value) lengthValue {
	switch v.Kind {
	case Text:
		return lengthBytes([]byte(v.S))
	case Blob:
		return lengthBytes(v.B)
	case Record, Dict, List:
		panic(draft.ErrType)
	default:
		return lengthWord(draftValue(v, draft.Compact).Uint())
	}
}
func slotEncode(w *lengthWriter, dst []byte, source Value, plan slotPlan, packed bool) []byte {
	if source.Kind != Record || len(plan) > 64 {
		panic(draft.ErrKind)
	}
	var values [64]lengthValue
	var present, spans uint64
	var largest uint64
	for i, p := range plan {
		for j := 0; j < i; j++ {
			if plan[j].key == p.key {
				panic(draft.ErrOrder)
			}
		}
		v := slotSource(source, p.key)
		x := slotValue(v)
		values[i] = x
		// A slot is present only when its source field exists. PresenceIsSignificant does not
		// turn an absent source field into an assignment.
		exists := false
		for _, f := range source.Fields {
			if f.Key == p.key {
				exists = true
				break
			}
		}
		if exists && (!x.IsZero() || p.presenceIsSignificant) {
			present |= 1 << uint(i)
		}
		if x.ptr != nil {
			spans |= 1 << uint(i)
		}
		largest = max(largest, p.key)
	}
	w.Reset(dst)
	w.Begin(0, "", 0, true)
	start := len(dst)
	cells := len(plan)
	if packed {
		cells = bits.OnesCount64(present)
	}
	w.out = append(w.out, make([]byte, cells*8)...)
	pos := 0
	for i, x := range values[:len(plan)] {
		ok := present&(1<<uint(i)) != 0
		if packed && !ok {
			continue
		}
		word := uint64(0)
		if ok {
			word = x.num
			if x.ptr != nil {
				word = uint64(len(w.out)-start) | x.num<<32
				w.out = append(w.out, x.Bytes()...)
			}
		}
		binary.LittleEndian.PutUint64(w.out[start+pos*8:], word)
		pos++
	}
	w.ReserveEntries(len(source.Fields))
	for _, f := range source.Fields {
		if plan.index(f.Key) < 0 {
			w.Put(f.Key, "", slotValue(f.Value))
			if !tagZero(f.Value) {
				largest = max(largest, f.Key)
			}
		}
	}
	f := w.frames[0]
	width := tagKeyWidth(largest)
	bitmapBytes := (len(plan) + 7) / 8
	for _, p := range plan {
		w.out = tagAppendWord(w.out, p.key, width)
	}
	w.out = tagAppendWord(w.out, present, bitmapBytes)
	w.out = tagAppendWord(w.out, spans, bitmapBytes)
	for _, m := range f.entries {
		w.out = tagAppendWord(w.out, m.key, width)
	}
	mode := byte(0)
	if f.shared == 9 {
		mode = 1
	} else if f.shared == 15 && len(f.entries) > 0 {
		mode = 2
		at := len(w.out)
		w.out = append(w.out, make([]byte, (len(f.entries)+7)/8)...)
		for i, m := range f.entries {
			if m.wire == 9 {
				w.out[at+i/8] |= 1 << uint(i%8)
			}
		}
	}
	for _, m := range f.entries {
		w.out = binary.LittleEndian.AppendUint64(w.out, m.word)
	}
	w.out = binary.LittleEndian.AppendUint16(w.out, uint16(len(plan)))
	w.out = binary.LittleEndian.AppendUint32(w.out, uint32(len(f.entries)))
	desc := byte(1|32) | mode<<3 | byte(bits.Len(uint(width))-1)<<6
	if packed {
		desc |= 2
	}
	w.out = append(w.out, desc)
	w.depth = 0
	return w.out
}

type slotHeader struct {
	normal                       lengthIndexHeader
	count, keys, presence, spans int
	packed                       bool
}

func slotHeaderOf(raw []byte) slotHeader {
	d := raw[len(raw)-1]
	n := int(binary.LittleEndian.Uint32(raw[len(raw)-5:]))
	s := int(binary.LittleEndian.Uint16(raw[len(raw)-7:]))
	width := 1 << uint(d>>6)
	h := lengthIndexHeader{raw: raw, n: n, width: width, mode: d >> 3 & 3, cells: len(raw) - 7 - n*8}
	h.codes = h.cells
	if h.mode == 2 {
		h.codes -= (n + 7) / 8
	}
	h.keys = h.codes - n*width
	span := h.keys - (s+7)/8
	present := span - (s+7)/8
	return slotHeader{h, s, present - s*width, present, span, d&2 != 0}
}
func (h slotHeader) mask(at int) uint64 { return tagWord(h.normal.raw[at : at+(h.count+7)/8]) }
func (h slotHeader) slot(i int) (lengthValue, bool) {
	if i < 0 || i >= h.count {
		return lengthValue{}, false
	}
	mask := h.mask(h.presence)
	if mask&(1<<uint(i)) == 0 {
		return lengthValue{}, false
	}
	pos := i
	if h.packed {
		pos = bits.OnesCount64(mask & ((uint64(1) << uint(i)) - 1))
	}
	word := binary.LittleEndian.Uint64(h.normal.raw[pos*8:])
	if h.normal.raw[h.spans+i/8]&(1<<uint(i%8)) == 0 {
		return lengthWord(word), true
	}
	start, n := int(uint32(word)), int(word>>32)
	return lengthBytes(h.normal.raw[start : start+n]), true
}
func (h slotHeader) key(i int) uint64 {
	return getWidth(h.normal.raw[h.keys+i*h.normal.width:], h.normal.width)
}
func (h slotHeader) get(key uint64) lengthValue {
	for i := 0; i < h.count; i++ {
		if h.key(i) == key {
			v, _ := h.slot(i)
			return v
		}
	}
	return h.normal.value(h.normal.findNumber(key))
}
func slotGet(raw []byte, key uint64) lengthValue { return slotHeaderOf(raw).get(key) }
func slotKnown(raw []byte, i int) uint64 {
	// Trusted, schema-known scalar. A checked Open and a stable slot assignment
	// precede this call. Missing slots in old rows read zero.
	if i < 0 || i >= int(binary.LittleEndian.Uint16(raw[len(raw)-7:])) {
		return 0
	}
	if raw[len(raw)-1]&2 == 0 {
		return binary.LittleEndian.Uint64(raw[i*8:])
	}
	h := slotHeaderOf(raw)
	v, _ := h.slot(i)
	return v.Uint()
}
func slotEvolving(raw []byte, i int, key uint64) uint64 {
	h := slotHeaderOf(raw)
	if i >= 0 && i < h.count && h.key(i) == key {
		v, _ := h.slot(i)
		return v.Uint()
	}
	return h.get(key).Uint()
}
func fastKnownWord(raw []byte, key uint64) uint64 {
	h := lengthIndex(raw)
	i := h.findNumber(key)
	if i < 0 {
		return 0
	}
	return binary.LittleEndian.Uint64(raw[h.cells+i*8:])
}
func slotOpen(raw []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = draft.ErrFormat
		}
	}()
	if len(raw) < 7 || raw[len(raw)-1]&37 != 33 {
		return draft.ErrFormat
	}
	h := slotHeaderOf(raw)
	if h.count > 64 || h.normal.n > tagMaxEntries || h.normal.mode == 3 || h.keys < 0 {
		return draft.ErrFormat
	}
	mask := h.mask(h.presence)
	spans := h.mask(h.spans)
	if h.count < 64 && (mask>>uint(h.count) != 0 || spans>>uint(h.count) != 0) {
		return draft.ErrFormat
	}
	if spans&^mask != 0 {
		return draft.ErrFormat
	}
	cells := h.count
	if h.packed {
		cells = bits.OnesCount64(mask)
	}
	if cells*8 > h.keys {
		return draft.ErrFormat
	}
	check := func(word uint64) bool {
		at, n := uint64(uint32(word)), word>>32
		return at >= uint64(cells*8) && at <= uint64(h.keys) && n <= uint64(h.keys)-at
	}
	for i := 0; i < h.count; i++ {
		for j := 0; j < i; j++ {
			if h.key(i) == h.key(j) {
				return draft.ErrFormat
			}
		}
		if h.normal.findNumber(h.key(i)) >= 0 {
			return draft.ErrFormat
		}
		if mask&(1<<uint(i)) != 0 {
			pos := i
			if h.packed {
				pos = bits.OnesCount64(mask & ((uint64(1) << uint(i)) - 1))
			}
			if spans&(1<<uint(i)) != 0 && !check(binary.LittleEndian.Uint64(raw[pos*8:])) {
				return draft.ErrFormat
			}
		} else if !h.packed && binary.LittleEndian.Uint64(raw[i*8:]) != 0 {
			return draft.ErrFormat
		}
	}
	for i := 0; i < h.normal.n; i++ {
		if i > 0 && h.normal.number(i) <= h.normal.number(i-1) {
			return draft.ErrFormat
		}
		if h.normal.span(i) && !check(binary.LittleEndian.Uint64(raw[h.normal.cells+i*8:])) {
			return draft.ErrFormat
		}
	}
	return nil
}
func TestSelectedSlots(t *testing.T) {
	source := Obj(Field{1, U(7)}, Field{4, S("abc")}, Field{8, U(0)}, Field{32, S("opaque")}, Field{math.MaxUint64, U(99)})
	plan := slotPlan{{4, false}, {8, true}, {1, false}, {16, false}}
	for _, packed := range []bool{false, true} {
		var w lengthWriter
		raw := slotEncode(&w, nil, source, plan, packed)
		if err := slotOpen(raw); err != nil {
			t.Fatal(err)
		}
		h := slotHeaderOf(raw)
		if string(h.get(4).Bytes()) != "abc" || h.get(1).Uint() != 7 || h.get(math.MaxUint64).Uint() != 99 {
			t.Fatal("value")
		}
		if v, ok := h.slot(1); !ok || !v.IsZero() {
			t.Fatal("present zero")
		}
		if _, ok := h.slot(3); ok {
			t.Fatal("absent")
		}
		if slotEvolving(raw, 5, math.MaxUint64) != 99 {
			t.Fatal("promotion fallback")
		}
		// No semantic schema: enumerate old slots and keyed entries into the common
		// format, retaining present zeros and opaque span boundaries.
		var fields []Field
		for i := 0; i < h.count; i++ {
			if v, ok := h.slot(i); ok {
				if v.ptr != nil {
					fields = append(fields, Field{h.key(i), Value{Kind: Blob, B: v.Bytes()}})
				} else {
					fields = append(fields, Field{h.key(i), U(v.num)})
				}
			}
		}
		if len(fields) != 3 {
			t.Fatal("unknown slot enumeration")
		}
		broken := append([]byte(nil), raw...)
		binary.LittleEndian.PutUint16(broken[len(broken)-7:], 65)
		if slotOpen(broken) == nil {
			t.Fatal("invalid count")
		}
		for n := 0; n < len(raw); n++ {
			_ = slotOpen(raw[:n])
		}
	}
}
func slotFixture(sparse bool, base uint64) (Value, slotPlan) {
	v := Obj()
	for i := uint64(1); i <= 128; i++ {
		if !sparse || i%4 == 1 {
			v.Fields = append(v.Fields, Field{base + i, U(i * 7)})
		}
	}
	var p slotPlan
	for i := uint64(0); i < 8; i++ {
		p = append(p, slotField{base + 1 + i*2, false})
	}
	return v, p
}
func BenchmarkSelectedSlots(b *testing.B) {
	for _, sparse := range []bool{false, true} {
		for _, rows := range []int{1, 32768} {
			source, plan := slotFixture(sparse, 0)
			for _, codec := range []string{"immutable", "fast", "fast-known-word", "slots-direct", "slots-direct-known", "slots-direct-evolving", "slots-packed", "slots-packed-known"} {
				b.Run(fmt.Sprintf("sparse%v/rows%d/%s", sparse, rows, codec), func(b *testing.B) {
					var w lengthWriter
					var raw []byte
					switch codec {
					case "immutable":
						raw = oldKVO(source).Bytes()
					case "fast", "fast-known-word":
						raw = lengthEncode(&w, nil, source, func(int, Value) bool { return true })
					default:
						raw = slotEncode(&w, nil, source, plan, codec == "slots-packed" || codec == "slots-packed-known")
					}
					d := newMmapDataset(b, raw, rows)
					b.ReportAllocs()
					b.ResetTimer()
					rng := uint64(0xdedabcde12345678)
					var sum uint64
					for i := 0; i < b.N; i++ {
						rng ^= rng << 13
						rng ^= rng >> 7
						rng ^= rng << 17
						r := d.rows[int(rng>>16)&(rows-1)]
						s := int(rng) & 7
						key := plan[s].key
						switch codec {
						case "immutable":
							sum += kvo.LoadRecord(r, nil).Root().Get(key)
						case "fast":
							sum += lengthTrusted(r).Get(key).Uint()
						case "fast-known-word":
							sum += fastKnownWord(r, key)
						case "slots-direct-known", "slots-packed-known":
							sum += slotKnown(r, s)
						case "slots-direct-evolving":
							sum += slotEvolving(r, s, key)
						default:
							sum += slotGet(r, key).Uint()
						}
					}
					sinkU = sum
					b.ReportMetric(float64(len(raw)), "row-B")
					b.ReportMetric(float64(d.fileBytes), "file-B")
				})
			}
		}
	}
}
func BenchmarkSelectedSlotsEncode(b *testing.B) {
	for _, sparse := range []bool{false, true} {
		source, plan := slotFixture(sparse, 0)
		for _, mode := range []string{"fast", "direct", "packed"} {
			b.Run(fmt.Sprintf("sparse%v/%s", sparse, mode), func(b *testing.B) {
				var w lengthWriter
				var dst []byte
				encode := func() {
					if mode == "fast" {
						dst = lengthEncode(&w, dst[:0], source, func(int, Value) bool { return true })
					} else {
						dst = slotEncode(&w, dst[:0], source, plan, mode == "packed")
					}
				}
				encode()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					encode()
				}
				sinkBytes = dst
				b.ReportMetric(float64(len(dst)), "row-B")
			})
		}
	}
}

// Direct streaming writer: slot metadata is known from the schema. A generated
// caller supplies the slot ordinal, so encoding does not search the input fields.
type selectedSlotWriter struct {
	lengthWriter
	plan                 slotPlan
	present, spans, seen uint64
	start                int
}

func (w *selectedSlotWriter) ResetSlots(dst []byte, plan slotPlan) {
	if len(plan) > 64 {
		panic(draft.ErrLimit)
	}
	w.plan, w.present, w.spans, w.seen, w.start = plan, 0, 0, 0, len(dst)
	w.lengthWriter.Reset(dst)
	w.Begin(0, "", 0, true)
	w.out = append(w.out, make([]byte, len(plan)*8)...)
}
func (w *selectedSlotWriter) PutSlot(i int, v lengthValue) {
	if i < 0 || i >= len(w.plan) {
		panic(draft.ErrIndex)
	}
	bit := uint64(1) << uint(i)
	if w.seen&bit != 0 {
		panic(draft.ErrOrder)
	}
	w.seen |= bit
	if v.IsZero() && !w.plan[i].presenceIsSignificant {
		return
	}
	w.present |= bit
	word := v.num
	if v.ptr != nil {
		w.spans |= bit
		word = uint64(len(w.out)-w.start) | v.num<<32
		w.out = append(w.out, v.Bytes()...)
	}
	binary.LittleEndian.PutUint64(w.out[w.start+i*8:], word)
}
func (w *selectedSlotWriter) FinishSlots() []byte {
	if w.depth != 1 {
		panic(draft.ErrKind)
	}
	f := w.frames[0]
	largest := uint64(0)
	for _, p := range w.plan {
		largest = max(largest, p.key)
	}
	if len(f.entries) > 0 {
		largest = max(largest, f.entries[len(f.entries)-1].key)
	}
	width := tagKeyWidth(largest)
	nb := (len(w.plan) + 7) / 8
	for _, p := range w.plan {
		w.out = tagAppendWord(w.out, p.key, width)
	}
	w.out = tagAppendWord(w.out, w.present, nb)
	w.out = tagAppendWord(w.out, w.spans, nb)
	for _, m := range f.entries {
		w.out = tagAppendWord(w.out, m.key, width)
	}
	mode := byte(0)
	if f.shared == 9 {
		mode = 1
	} else if f.shared == 15 && len(f.entries) > 0 {
		mode = 2
		at := len(w.out)
		w.out = append(w.out, make([]byte, (len(f.entries)+7)/8)...)
		for i, m := range f.entries {
			if m.wire == 9 {
				w.out[at+i/8] |= 1 << uint(i%8)
			}
		}
	}
	for _, m := range f.entries {
		w.out = binary.LittleEndian.AppendUint64(w.out, m.word)
	}
	w.out = binary.LittleEndian.AppendUint16(w.out, uint16(len(w.plan)))
	w.out = binary.LittleEndian.AppendUint32(w.out, uint32(len(f.entries)))
	w.out = append(w.out, 33|mode<<3|byte(bits.Len(uint(width))-1)<<6)
	w.depth = 0
	return w.out
}

// Append-only slot assignments need only an old-row count check. If an old row
// predates promotion of this field, look for it in the ordinary keyed area.
func slotStableWord(raw []byte, i int, key uint64) uint64 {
	if i >= 0 && i < int(binary.LittleEndian.Uint16(raw[len(raw)-7:])) {
		return binary.LittleEndian.Uint64(raw[i*8:])
	}
	return slotGet(raw, key).Uint()
}
func TestSelectedSlotStreaming(t *testing.T) {
	for _, sparse := range []bool{false, true} {
		for _, base := range []uint64{0, 1 << 50} {
			source, plan := slotFixture(sparse, base)
			var ordinary lengthWriter
			want := slotEncode(&ordinary, nil, source, plan, false)
			var w selectedSlotWriter
			w.ResetSlots(nil, plan)
			for _, f := range source.Fields {
				v := slotValue(f.Value)
				if i := plan.index(f.Key); i >= 0 {
					w.PutSlot(i, v)
				} else {
					w.Put(f.Key, "", v)
				}
			}
			got := w.FinishSlots()
			if string(got) != string(want) {
				t.Fatal("streaming bytes")
			}
			for q := uint64(0); q < 130; q++ {
				key := base + q
				expected := slotValue(slotSource(source, key)).Uint()
				if slotGet(got, key).Uint() != expected {
					t.Fatal("generic value")
				}
				i := plan.index(key)
				if i >= 0 && slotStableWord(got, i, key) != expected {
					t.Fatal("slot value")
				}
			}
			old := slotEncode(&ordinary, nil, source, plan[:4], false)
			for i := 4; i < len(plan); i++ {
				if slotStableWord(old, i, plan[i].key) != slotValue(slotSource(source, plan[i].key)).Uint() {
					t.Fatal("old-row promotion")
				}
			}
		}
	}
}
func BenchmarkSelectedSlotStreaming(b *testing.B) {
	for _, sparse := range []bool{false, true} {
		source, plan := slotFixture(sparse, 0)
		slots := make([]int, len(source.Fields))
		for i, f := range source.Fields {
			slots[i] = plan.index(f.Key)
		}
		b.Run(fmt.Sprintf("sparse%v", sparse), func(b *testing.B) {
			var w selectedSlotWriter
			var dst []byte
			write := func() {
				w.ResetSlots(dst[:0], plan)
				w.ReserveEntries(len(source.Fields))
				for i, f := range source.Fields {
					v := slotValue(f.Value)
					if slots[i] >= 0 {
						w.PutSlot(slots[i], v)
					} else {
						w.Put(f.Key, "", v)
					}
				}
				dst = w.FinishSlots()
			}
			write()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				write()
			}
			sinkBytes = dst
			b.ReportMetric(float64(len(dst)), "row-B")
		})
	}
}

func BenchmarkSelectedSlotPolicy(b *testing.B) {
	for _, base := range []uint64{0, 1 << 50} {
		for _, rows := range []int{1, 32768} {
			for _, hot := range []int{100, 80} {
				source, plan := slotFixture(false, base)
				for _, codec := range []string{"immutable", "fast", "slots-stable"} {
					b.Run(fmt.Sprintf("base%d/rows%d/hot%d/%s", base, rows, hot, codec), func(b *testing.B) {
						var w lengthWriter
						var raw []byte
						switch codec {
						case "immutable":
							raw = oldKVO(source).Bytes()
						case "fast":
							raw = lengthEncode(&w, nil, source, func(int, Value) bool { return true })
						default:
							raw = slotEncode(&w, nil, source, plan, false)
						}
						d := newMmapDataset(b, raw, rows)
						b.ReportAllocs()
						b.ResetTimer()
						rng := uint64(0xdedabcde12345678)
						var sum uint64
						for i := 0; i < b.N; i++ {
							rng ^= rng << 13
							rng ^= rng >> 7
							rng ^= rng << 17
							r := d.rows[int(rng>>16)&(rows-1)]
							slot := int(rng) & 7
							key := plan[slot].key
							isHot := hot == 100 || rng>>32%5 != 0
							if !isHot {
								key = base + (rng >> 8 & 63) + 33
							}
							switch codec {
							case "immutable":
								sum += kvo.LoadRecord(r, nil).Root().Get(key)
							case "fast":
								sum += lengthTrusted(r).Get(key).Uint()
							default:
								if isHot {
									sum += slotStableWord(r, slot, key)
								} else {
									sum += slotGet(r, key).Uint()
								}
							}
						}
						sinkU = sum
						b.ReportMetric(float64(len(raw)), "row-B")
					})
				}
			}
		}
	}
}
func BenchmarkSelectedSlotSpan(b *testing.B) {
	source := Obj()
	plan := slotPlan{{1, false}, {3, false}, {5, false}, {7, false}, {9, false}, {11, false}, {13, false}, {15, false}}
	for i := uint64(1); i <= 128; i++ {
		source.Fields = append(source.Fields, Field{i, Value{Kind: Blob, B: make([]byte, 16+int(i%4)*16)}})
	}
	for _, rows := range []int{1, 32768} {
		for _, codec := range []string{"fast", "slots-generic", "slots-known"} {
			b.Run(fmt.Sprintf("rows%d/%s", rows, codec), func(b *testing.B) {
				var w lengthWriter
				var raw []byte
				if codec == "fast" {
					raw = lengthEncode(&w, nil, source, func(int, Value) bool { return true })
				} else {
					raw = slotEncode(&w, nil, source, plan, false)
				}
				d := newMmapDataset(b, raw, rows)
				b.ReportAllocs()
				b.ResetTimer()
				rng := uint64(0xdedabcde12345678)
				var sum uint64
				for i := 0; i < b.N; i++ {
					rng ^= rng << 13
					rng ^= rng >> 7
					rng ^= rng << 17
					r := d.rows[int(rng>>16)&(rows-1)]
					slot := int(rng) & 7
					var data []byte
					switch codec {
					case "fast":
						data = lengthTrusted(r).Get(plan[slot].key).Bytes()
					case "slots-generic":
						data = slotGet(r, plan[slot].key).Bytes()
					default:
						word := binary.LittleEndian.Uint64(r[slot*8:])
						at, n := int(uint32(word)), int(word>>32)
						data = r[at : at+n]
					}
					sum += uint64(len(data)) + uint64(data[len(data)-1])
				}
				sinkU = sum
				b.ReportMetric(float64(len(raw)), "row-B")
			})
		}
	}
}

func BenchmarkSelectedSlotWriter(b *testing.B) {
	for _, sparse := range []bool{false, true} {
		source, plan := slotFixture(sparse, 0)
		slots := make([]int, len(source.Fields))
		values := make([]lengthValue, len(source.Fields))
		for i, f := range source.Fields {
			slots[i] = plan.index(f.Key)
			values[i] = slotValue(f.Value)
		}
		for _, direct := range []bool{false, true} {
			b.Run(fmt.Sprintf("sparse%v/direct%v", sparse, direct), func(b *testing.B) {
				var normal lengthWriter
				var selected selectedSlotWriter
				var dst []byte
				write := func() {
					if direct {
						selected.ResetSlots(dst[:0], plan)
						selected.ReserveEntries(len(values))
						for i, v := range values {
							if slots[i] >= 0 {
								selected.PutSlot(slots[i], v)
							} else {
								selected.Put(source.Fields[i].Key, "", v)
							}
						}
						dst = selected.FinishSlots()
					} else {
						normal.Reset(dst[:0])
						normal.Begin(0, "", 0, true)
						normal.ReserveEntries(len(values))
						for i, v := range values {
							normal.Put(source.Fields[i].Key, "", v)
						}
						normal.End()
						dst = normal.BytesWritten()
					}
				}
				write()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					write()
				}
				sinkBytes = dst
				b.ReportMetric(float64(len(dst)), "row-B")
			})
		}
	}
}

// Optional fast path for exclusively owned bytes. The ordinary setter rebuilds
// when an old row lacks this slot or when the property is a span.
func slotPatchOwned(raw []byte, index int, word uint64, keepZero bool) bool {
	h := slotHeaderOf(raw)
	if h.packed || index < 0 || index >= h.count {
		return false
	}
	if h.normal.raw[h.spans+index/8]&(1<<uint(index%8)) != 0 {
		return false
	}
	binary.LittleEndian.PutUint64(raw[index*8:], word)
	if word == 0 && !keepZero {
		raw[h.presence+index/8] &^= 1 << uint(index%8)
	} else {
		raw[h.presence+index/8] |= 1 << uint(index%8)
	}
	return true
}
func TestSelectedSlotPatch(t *testing.T) {
	source, plan := slotFixture(true, 0)
	var w lengthWriter
	raw := slotEncode(&w, nil, source, plan, false)
	if _, ok := slotHeaderOf(raw).slot(1); ok {
		t.Fatal("fixture must be absent")
	}
	if !slotPatchOwned(raw, 1, 42, false) || slotGet(raw, plan[1].key).Uint() != 42 {
		t.Fatal("insert")
	}
	if !slotPatchOwned(raw, 1, 0, false) {
		t.Fatal("clear")
	}
	if _, ok := slotHeaderOf(raw).slot(1); ok {
		t.Fatal("clear presence")
	}
	if !slotPatchOwned(raw, 1, 0, true) {
		t.Fatal("present zero")
	}
	if _, ok := slotHeaderOf(raw).slot(1); !ok {
		t.Fatal("zero missing")
	}
	if err := slotOpen(raw); err != nil {
		t.Fatal(err)
	}
}
func BenchmarkSelectedSlotPatch(b *testing.B) {
	source, plan := slotFixture(false, 0)
	for _, slots := range []bool{false, true} {
		for _, copyFirst := range []bool{false, true} {
			b.Run(fmt.Sprintf("slots%v/copy%v", slots, copyFirst), func(b *testing.B) {
				var w lengthWriter
				var base []byte
				if slots {
					base = slotEncode(&w, nil, source, plan, false)
				} else {
					base = lengthEncode(&w, nil, source, func(int, Value) bool { return true })
				}
				owned := append([]byte(nil), base...)
				view := lengthTrusted(owned)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if copyFirst {
						copy(owned, base)
					}
					ok := false
					if slots {
						ok = slotPatchOwned(owned, i&7, uint64(i)+1, false)
					} else {
						ok = view.Patch(plan[i&7].key, "", uint64(i)+1)
					}
					if !ok {
						b.Fatal("patch")
					}
				}
				sinkBytes = owned
				b.ReportMetric(float64(len(base)), "row-B")
			})
		}
	}
}
