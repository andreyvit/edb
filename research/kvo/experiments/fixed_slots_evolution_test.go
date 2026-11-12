package kvolab

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
)

// Rebuild without the old semantic schema. Carry the persisted slot assignment,
// present zeros, and opaque spans forward. Scratch and output must not alias raw.
func slotRewrite(w *selectedSlotWriter, dst, raw []byte, key uint64, value lengthValue, keepZero bool, scratch slotPlan) []byte {
	h := slotHeaderOf(raw)
	plan := scratch[:0]
	for i := 0; i < h.count; i++ {
		plan = append(plan, slotField{h.key(i), true})
	}
	w.ResetSlots(dst, plan)
	selected := false
	for i, p := range plan {
		v, ok := h.slot(i)
		if p.key == key {
			selected = true
			v, ok = value, keepZero || !value.IsZero()
		}
		if ok {
			w.PutSlot(i, v)
		}
	}
	inserted := selected
	put := func() {
		if keepZero || !value.IsZero() {
			w.PutPresent(key, "", value)
		}
		inserted = true
	}
	for i := 0; i < h.normal.n; i++ {
		k := h.normal.number(i)
		if !inserted && k >= key {
			put()
		}
		if k != key {
			w.PutPresent(k, "", h.normal.value(i))
		}
	}
	if !inserted {
		put()
	}
	return w.FinishSlots()
}

func TestSelectedSlotPreserveUnknown(t *testing.T) {
	var w selectedSlotWriter
	plan := slotPlan{{8, true}, {1, false}, {16, true}}
	w.ResetSlots(nil, plan)
	w.PutSlot(0, lengthWord(0))
	w.PutSlot(1, lengthBytes([]byte{255, 0, 254}))
	w.PutPresent(3, "", lengthWord(0))
	w.Put(4, "", lengthBytes([]byte("opaque")))
	raw := w.FinishSlots()
	var scratch [64]slotField
	for _, key := range []uint64{1, 3, 4, 5, 8, 16} {
		for _, keepZero := range []bool{false, true} {
			out := slotRewrite(&w, nil, raw, key, lengthWord(0), keepZero, scratch[:0])
			if err := slotOpen(out); err != nil {
				t.Fatal(err)
			}
			h := slotHeaderOf(out)
			if h.count != len(plan) {
				t.Fatal("lost slot assignments")
			}
			for i, p := range plan {
				if h.key(i) != p.key {
					t.Fatal("slot reused")
				}
			}
			for _, k := range []uint64{1, 3, 4, 5, 8, 16} {
				p := slotProperty{k, -1}
				a, pa := p.Lookup(raw)
				b, pb := p.Lookup(out)
				if k == key {
					if pb != keepZero || !b.IsZero() {
						t.Fatal("assignment", key)
					}
					continue
				}
				if pa != pb || (a.ptr == nil) != (b.ptr == nil) || a.ptr == nil && a.num != b.num || a.ptr != nil && !bytes.Equal(a.Bytes(), b.Bytes()) {
					t.Fatal("unknown field changed", key, k)
				}
			}
		}
	}
}

// A resolved property retains its key for old rows. The lower-bound slotStableWord
// control assumes a slot-format row; this handle also accepts ordinary Fast and
// Compact rows created before the property was promoted.
type slotProperty struct {
	key   uint64
	index int
}

func (p slotProperty) Word(raw []byte) uint64 {
	if len(raw) == 0 {
		return 0
	}
	d := raw[len(raw)-1]
	if d&33 != 33 {
		return lengthTrusted(raw).Get(p.key).Uint()
	}
	if d&2 == 0 && p.index >= 0 && p.index < int(binary.LittleEndian.Uint16(raw[len(raw)-7:])) {
		return binary.LittleEndian.Uint64(raw[p.index*8:])
	}
	return slotGet(raw, p.key).Uint()
}
func (p slotProperty) Lookup(raw []byte) (lengthValue, bool) {
	if len(raw) == 0 {
		return lengthValue{}, false
	}
	if raw[len(raw)-1]&33 != 33 {
		return presenceLookup(lengthTrusted(raw), p.key)
	}
	h := slotHeaderOf(raw)
	if p.index >= 0 && p.index < h.count {
		return h.slot(p.index)
	}
	// An older schema may know the key but not its later slot assignment.
	for i := 0; i < h.count; i++ {
		if h.key(i) == p.key {
			return h.slot(i)
		}
	}
	i := h.normal.findNumber(p.key)
	return h.normal.value(i), i >= 0
}
func TestSelectedSlotEvolution(t *testing.T) {
	source, plan := slotFixture(false, 0)
	var w lengthWriter
	versions := [][]byte{
		lengthEncode(&w, nil, source, nil),
		lengthEncode(&w, nil, source, func(int, Value) bool { return true }),
		slotEncode(&w, nil, source, plan[:0], false),
		slotEncode(&w, nil, source, plan[:4], false),
		slotEncode(&w, nil, source, plan, false),
		slotEncode(&w, nil, source, plan, true),
	}
	for i, f := range plan {
		p := slotProperty{f.key, i}
		want := slotValue(slotSource(source, f.key)).Uint()
		for j, raw := range versions {
			if p.Word(raw) != want {
				t.Fatal("word", i, j)
			}
			v, ok := p.Lookup(raw)
			if !ok || v.Uint() != want {
				t.Fatal("presence", i, j)
			}
			oldHandle := slotProperty{f.key, -1}
			v, ok = oldHandle.Lookup(raw)
			if !ok || v.Uint() != want || oldHandle.Word(raw) != want {
				t.Fatal("older handle", i, j)
			}
		}
	}
	zero := slotProperty{plan[0].key, 0}
	if zero.Word(nil) != 0 {
		t.Fatal("empty")
	}
	raw := versions[4]
	slotPatchOwned(raw, 0, 0, true)
	if v, ok := zero.Lookup(raw); !ok || !v.IsZero() {
		t.Fatal("present zero")
	}
	slotPatchOwned(raw, 0, 0, false)
	if _, ok := zero.Lookup(raw); ok {
		t.Fatal("absent")
	}
}

func BenchmarkSelectedSlotResolved(b *testing.B) {
	for _, rows := range []int{1, 32768} {
		for _, hot := range []int{100, 80} {
			source, plan := slotFixture(false, 0)
			for _, resolved := range []bool{false, true} {
				b.Run(fmt.Sprintf("rows%d/hot%d/resolved%v", rows, hot, resolved), func(b *testing.B) {
					var w lengthWriter
					raw := slotEncode(&w, nil, source, plan, false)
					d := newMmapDataset(b, raw, rows)
					var properties [8]slotProperty
					for i, p := range plan {
						properties[i] = slotProperty{p.key, i}
					}
					b.ReportAllocs()
					b.ResetTimer()
					rng := uint64(0xdedabcde12345678)
					var sum uint64
					for i := 0; i < b.N; i++ {
						rng ^= rng << 13
						rng ^= rng >> 7
						rng ^= rng << 17
						r := d.rows[int(rng>>16)&(rows-1)]
						p := properties[int(rng)&7]
						if hot == 80 && rng>>32%5 == 0 {
							p = slotProperty{(rng >> 8 & 63) + 33, -1}
						}
						if resolved {
							sum += p.Word(r)
						} else if p.index >= 0 {
							sum += slotStableWord(r, p.index, p.key)
						} else {
							sum += slotGet(r, p.key).Uint()
						}
					}
					sinkU = sum
					b.ReportMetric(float64(len(raw)), "row-B")
				})
			}
		}
	}
}
