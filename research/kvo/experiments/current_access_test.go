package kvolab

// Fuse the selected format dispatch with the existing lookup kernels, avoiding
// an extra non-inlined wrapper around each Fast access. No wire change. The
// initial wrappers remain GetWrapped/GetStringWrapped/AtWrapped controls.
import (
	"bytes"
	"encoding/binary"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"unsafe"
)

func (v currentView) Get(key uint64) currentValue { return currentObjectGet(v.lengthView, key) }
func (v currentView) GetString(key string) currentValue {
	return currentDictionaryGet(v.lengthView, key)
}
func (v currentView) At(index int) currentValue { return currentListAt(v.lengthView, index) }

func currentObjectGet(v lengthView, key uint64) currentValue {
	if v.n == 0 {
		return currentValue{}
	}
	raw := v.Raw()
	end := len(raw) - 5
	t := raw[len(raw)-1]
	if t>>1&3 != 0 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		return currentFromCompact(oneBitTrustedGet(raw, false, key, ""))
	}
	if !perfNativeWords {
		return currentFromFast(keysLastPortableGet(v, key))
	}
	n := int(binary.LittleEndian.Uint32(raw[end:]))
	width := 1 << uint(t>>6)
	mode := t >> 3 & 3
	keys := end - n*width
	codes := keys
	if mode == 2 {
		codes -= (n + 7) / 8
	}
	cells := codes - n*8
	i := -1
	if width == 1 {
		if key <= 255 {
			if n <= 4 {
				for j, k := range raw[keys:end] {
					if k == byte(key) {
						i = j
						break
					}
				}
			} else {
				i = bytes.IndexByte(raw[keys:end], byte(key))
			}
		}
	} else {
		i = perfFind(raw[keys:end], width, key, true)
	}
	if i < 0 {
		return currentValue{}
	}
	word := binary.LittleEndian.Uint64(raw[cells+i*8:])
	if mode == 0 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) == 0 {
		return currentRawWord(word)
	}
	at, size := int(uint32(word)), int(word>>32)
	return currentBytes(raw[at : at+size])
}
func currentDictionaryGet(v lengthView, target string) currentValue {
	if v.n == 0 {
		return currentValue{}
	}
	raw := v.Raw()
	end := len(raw) - 5
	t := raw[len(raw)-1]
	if t>>1&3 != 1 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		return currentFromCompact(oneBitTrustedGet(raw, false, 0, target))
	}
	if !perfNativeWords {
		return currentFromFast(v.GetString(target))
	}
	n := int(binary.LittleEndian.Uint32(raw[end:]))
	if n == 0 {
		return currentValue{}
	}
	width := 1 << uint(t>>6)
	mode := t >> 3 & 3
	codes := end
	if mode == 2 {
		codes -= (n + 7) / 8
	}
	keys := codes - n*width
	p := unsafe.Pointer(unsafe.SliceData(raw[keys:codes]))
	i, start := -1, 0
	switch width {
	case 1:
		i, start = dictLocate(raw, unsafe.Slice((*uint8)(p), n), target)
	case 2:
		i, start = dictLocate(raw, unsafe.Slice((*uint16)(p), n), target)
	case 4:
		i, start = dictLocate(raw, unsafe.Slice((*uint32)(p), n), target)
	case 8:
		i, start = dictLocate(raw, unsafe.Slice((*uint64)(p), n), target)
	}
	if i < 0 {
		return currentValue{}
	}
	if mode == 0 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) == 0 {
		return currentRawWord(binary.LittleEndian.Uint64(raw[start:]))
	}
	finish := keys
	if i+1 < n {
		finish = int(getWidth(raw[keys+(i+1)*width:], width))
	}
	return currentBytes(raw[start:finish])
}
func currentListAt(v lengthView, index int) currentValue {
	if v.n == 0 {
		panic(draft.ErrIndex)
	}
	raw := v.Raw()
	end := len(raw) - 5
	t := raw[len(raw)-1]
	if t>>1&3 != 2 {
		panic(draft.ErrKind)
	}
	if t&1 == 0 {
		return (currentView{v}).AtWrapped(index)
	}
	n := int(binary.LittleEndian.Uint32(raw[end:]))
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
	if mode == 0 {
		return currentRawWord(binary.LittleEndian.Uint64(raw[index*8:]))
	}
	start := int(getWidth(raw[keys+index*width:], width))
	if mode == 0 || mode == 2 && raw[codes+index/8]&(1<<uint(index&7)) == 0 {
		return currentRawWord(binary.LittleEndian.Uint64(raw[start:]))
	}
	finish := keys
	if index+1 < n {
		finish = int(getWidth(raw[keys+(index+1)*width:], width))
	}
	return currentBytes(raw[start:finish])
}
