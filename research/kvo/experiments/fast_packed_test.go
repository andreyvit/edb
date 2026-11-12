package kvolab

// The existing 64 MiB / 1 Mi-entry limits let a prepared Fast view fit in the
// same 16 bytes as the unprepared pointer/length view. No wire bits change.
import (
	"bytes"
	"encoding/binary"
	"testing"
	"unsafe"
)

type fastPacked struct {
	ptr  *byte
	info uint64
}

func preparePacked(v lengthView) fastPacked {
	raw := v.Raw()
	if !v.Indexed() {
		panic("Fast container required")
	}
	t := raw[len(raw)-1]
	n := int(binary.LittleEndian.Uint32(raw[len(raw)-5:]))
	width := 1 << uint(t>>6)
	mode := int(t >> 3 & 3)
	kind := int(t >> 1 & 3)
	keys := len(raw) - 5 - n*width
	if kind == 0 {
		keys -= n * 8
	}
	if mode == 2 {
		keys -= (n + 7) / 8
	}
	if n >= 1<<21 || keys >= 1<<27 {
		panic("prepared view limit")
	}
	return fastPacked{v.ptr, uint64(n) | uint64(keys)<<21 | uint64(t>>6)<<48 | uint64(mode)<<50 | uint64(kind)<<52}
}
func (v fastPacked) parts() (n, keys, width, mode, kind int) {
	return int(v.info & ((1 << 21) - 1)), int(v.info >> 21 & ((1 << 27) - 1)), 1 << uint(v.info>>48&3), int(v.info >> 50 & 3), int(v.info >> 52 & 3)
}
func (v fastPacked) Raw() []byte {
	n, keys, width, mode, kind := v.parts()
	size := keys + n*width + 5
	if v.info&(1<<54) != 0 {
		size -= 5
	}
	if mode == 2 {
		size += (n + 7) / 8
	}
	if kind == 0 {
		size += n * 8
	}
	return unsafe.Slice(v.ptr, size)
}
func (v fastPacked) Get(key uint64) lengthValue {
	n, keys, width, mode, kind := v.parts()
	if kind != 0 {
		panic("Object required")
	}
	p := (*byte)(unsafe.Add(unsafe.Pointer(v.ptr), keys))
	i := perfFind(unsafe.Slice(p, n*width), width, key, false)
	if i < 0 {
		return lengthValue{}
	}
	cells := n * width
	if mode == 2 {
		cells += (n + 7) / 8
	}
	word := binary.LittleEndian.Uint64(unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(p), cells+i*8)), 8))
	if mode == 0 || mode == 2 && *(*byte)(unsafe.Add(unsafe.Pointer(p), n*width+i/8))&(1<<uint(i&7)) == 0 {
		return lengthWord(word)
	}
	if word>>32 == 0 {
		return lengthValue{}
	}
	return lengthValue{(*byte)(unsafe.Add(unsafe.Pointer(v.ptr), int(uint32(word)))), word >> 32}
}
func TestFastPacked(t *testing.T) {
	if unsafe.Sizeof(fastPacked{}) != 16 {
		t.Fatal("packed view size")
	}
	for _, source := range workloadFixtures(t) {
		var w lengthWriter
		raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
		var walk func(lengthView, Value)
		walk = func(view lengthView, x Value) {
			p := preparePacked(view)
			if !bytes.Equal(p.Raw(), view.Raw()) {
				t.Fatal("raw extent")
			}
			if x.Kind == Record {
				for _, f := range x.Fields {
					v := view.Get(f.Key)
					if p.Get(f.Key) != v {
						t.Fatal("packed value")
					}
					if !v.IsZero() && (f.Value.Kind == Record || f.Value.Kind == Dict || f.Value.Kind == List) {
						walk(v.Object(), f.Value)
					}
				}
			}
			if x.Kind == Dict {
				for _, e := range x.Entries {
					v := view.GetString(e.Key)
					if !v.IsZero() && (e.Value.Kind == Record || e.Value.Kind == Dict || e.Value.Kind == List) {
						walk(v.Object(), e.Value)
					}
				}
			}
			if x.Kind == List {
				for i, e := range x.Items {
					v := view.At(i)
					if !v.IsZero() && (e.Kind == Record || e.Kind == Dict || e.Kind == List) {
						walk(v.Object(), e)
					}
				}
			}
		}
		walk(lengthTrusted(raw), source)
	}
}
