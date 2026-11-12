package kvolab

// Same front framing, with the outer dispatch fused into each getter. Retain
// the initial two-call getter as a control so wrapper cost is not called format cost.
import (
	"encoding/binary"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"testing"
	"unsafe"
)

func unifiedDirectGet(v lengthView, key uint64, equal bool) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if !perfNativeWords {
		panic("native key loads unavailable")
	}
	raw := v.Raw()
	end := len(raw)
	t := raw[0]
	if t>>1&3 != 0 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		return unifiedView(v).Get(key)
	}
	n := int(binary.LittleEndian.Uint32(raw[1:]))
	width := 1 << uint(t>>6)
	mode := t >> 3 & 3
	cells := end - n*8
	codes := cells
	if mode == 2 {
		codes -= (n + 7) / 8
	}
	keys := codes - n*width
	i := perfFind(raw[keys:codes], width, key, equal)
	if i < 0 {
		return lengthValue{}
	}
	word := binary.LittleEndian.Uint64(raw[cells+i*8:])
	if mode == 0 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) == 0 {
		return lengthWord(word)
	}
	at, size := int(uint32(word)), int(word>>32)
	return lengthBytes(raw[at : at+size])
}
func unifiedDirectGetString(v lengthView, target string, equal bool) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if !perfNativeWords {
		panic("front Fast Dictionary required")
	}
	raw := v.Raw()
	end := len(raw)
	t := raw[0]
	if t>>1&3 != 1 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		return unifiedView(v).GetString(target)
	}
	n := int(binary.LittleEndian.Uint32(raw[1:]))
	if n == 0 {
		return lengthValue{}
	}
	width := 1 << uint(t>>6)
	mode := t >> 3 & 3
	codes := end
	if mode == 2 {
		codes -= (n + 7) / 8
	}
	keys := codes - n*width
	p := unsafe.Pointer(unsafe.SliceData(raw[keys:codes]))
	i := -1
	switch width {
	case 1:
		i = perfDictFind(raw, unsafe.Slice((*uint8)(p), n), target, equal)
	case 2:
		i = perfDictFind(raw, unsafe.Slice((*uint16)(p), n), target, equal)
	case 4:
		i = perfDictFind(raw, unsafe.Slice((*uint32)(p), n), target, equal)
	case 8:
		i = perfDictFind(raw, unsafe.Slice((*uint64)(p), n), target, equal)
	}
	if i < 0 {
		return lengthValue{}
	}
	at := int(getWidth(raw[keys+i*width:], width))
	size, rest := lengthU(raw[at:])
	start := len(raw) - len(rest) + int(size)
	if mode == 0 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) == 0 {
		return lengthWord(binary.LittleEndian.Uint64(raw[start:]))
	}
	finish := keys
	if i+1 < n {
		finish = int(getWidth(raw[keys+(i+1)*width:], width))
	}
	return lengthBytes(raw[start:finish])
}
func unifiedDirectAt(v lengthView, index int) lengthValue {
	if v.n == 0 {
		panic(draft.ErrIndex)
	}
	raw := v.Raw()
	end := len(raw)
	t := raw[0]
	if t>>1&3 != 2 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		return unifiedView(v).At(index)
	}
	n := int(binary.LittleEndian.Uint32(raw[1:]))
	if index < 0 || index >= n {
		panic(draft.ErrIndex)
	}
	width := 1 << uint(t>>6)
	mode := t >> 3 & 3
	codes := end
	if mode == 2 {
		codes -= (n + 7) / 8
	}
	keys := codes - n*width
	start := int(getWidth(raw[keys+index*width:], width))
	if mode == 0 || mode == 2 && raw[codes+index/8]&(1<<uint(index&7)) == 0 {
		return lengthWord(binary.LittleEndian.Uint64(raw[start:]))
	}
	finish := keys
	if index+1 < n {
		finish = int(getWidth(raw[keys+(index+1)*width:], width))
	}
	return lengthBytes(raw[start:finish])
}
func unifiedDirectPath(raw []byte, path []perfStep) lengthValue {
	var value lengthValue
	for i, s := range path {
		v := lengthTrusted(raw)
		switch s.kind {
		case 0:
			value = unifiedDirectGet(v, s.key, true)
		case 1:
			value = unifiedDirectGetString(v, s.text, true)
		case 2:
			value = unifiedDirectAt(v, int(s.key))
		}
		if i+1 < len(path) {
			raw = value.Bytes()
		}
	}
	return value
}
func TestUnifiedDirect(t *testing.T) {
	for _, source := range workloadFixtures(t) {
		for _, p := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
			var old lengthWriter
			back := lengthEncode(&old, nil, source, p)
			var w unifiedWriter
			raw := unifiedEncode(&w, nil, source, p)
			for _, leaf := range perfCollect(source, back) {
				if !equalLengthValue(unifiedDirectPath(raw, leaf.path), leaf.value) {
					t.Fatal("leaf")
				}
			}
		}
	}
}
