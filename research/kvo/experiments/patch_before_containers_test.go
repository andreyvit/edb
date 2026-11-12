package kvolab

// Earlier fused planner, retained to isolate the later Dictionary/List changes.
import (
	"bytes"
	"encoding/binary"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"unsafe"
)

func patchLocateFusedBeforeContainers(raw []byte, s perfStep) (at, size int, span, found bool) {
	if len(raw) == 0 || raw[len(raw)-1]&1 == 0 {
		return
	}
	if !perfNativeWords {
		return keysLastPatchLocatePrevious(raw, s)
	}
	t := raw[len(raw)-1]
	if int(t>>1&3) != s.kind {
		return
	}
	end := len(raw) - 5
	n := int(binary.LittleEndian.Uint32(raw[end:]))
	if n == 0 {
		return
	}
	width, mode := 1<<uint(t>>6), t>>3&3
	bitmap := 0
	if mode == 2 {
		bitmap = (n + 7) / 8
	}
	i := -1
	if s.kind == 0 {
		keys := end - n*width
		codes := keys - bitmap
		if width == 1 {
			if s.key <= 255 {
				if n <= 4 {
					for j, k := range raw[keys:end] {
						if k == byte(s.key) {
							i = j
							break
						}
					}
				} else {
					i = bytes.IndexByte(raw[keys:end], byte(s.key))
				}
			}
		} else {
			i = perfFind(raw[keys:end], width, s.key, true)
		}
		if i < 0 {
			return
		}
		span = mode == 1 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) != 0
		at, size = codes-n*8+i*8, 8
		if span {
			word := binary.LittleEndian.Uint64(raw[at:])
			at, size = int(uint32(word)), int(word>>32)
		}
		return at, size, span, true
	}
	codes := end - bitmap
	keys := codes - n*width
	if s.kind == 2 {
		if s.key < uint64(n) {
			i = int(s.key)
		}
	} else {
		p := unsafe.Pointer(unsafe.SliceData(raw[keys:codes]))
		switch width {
		case 1:
			i = perfDictFind(raw, unsafe.Slice((*uint8)(p), n), s.text, true)
		case 2:
			i = perfDictFind(raw, unsafe.Slice((*uint16)(p), n), s.text, true)
		case 4:
			i = perfDictFind(raw, unsafe.Slice((*uint32)(p), n), s.text, true)
		case 8:
			i = perfDictFind(raw, unsafe.Slice((*uint64)(p), n), s.text, true)
		}
	}
	if i < 0 {
		return
	}
	span = mode == 1 || mode == 2 && raw[codes+i/8]&(1<<uint(i&7)) != 0
	at = int(getWidth(raw[keys+i*width:], width))
	finish := keys
	if i+1 < n {
		finish = int(getWidth(raw[keys+(i+1)*width:], width))
	}
	if s.kind == 1 {
		n, rest := lengthU(raw[at:])
		at = len(raw) - len(rest) + int(n)
	}
	return at, finish - at, span, true
}

func patchPlanFusedBeforeContainers(base []byte, edits []perfLeaf, dst []patchRef) ([]patchRef, bool) {
	dst = dst[:0]
	for _, edit := range edits {
		if len(edit.path) == 0 {
			panic(draft.ErrKind)
		}
		raw := base
		offset := 0
		for i, s := range edit.path {
			at, size, span, found := patchLocateFusedBeforeContainers(raw, s)
			if !found {
				return dst[:0], false
			}
			if i+1 < len(edit.path) {
				if !span {
					return dst[:0], false
				}
				raw = raw[at : at+size]
				offset += at
				continue
			}
			v := edit.value
			if s.kind != 2 && v.IsZero() {
				return dst[:0], false
			}
			if span != (v.ptr != nil) || span && int(v.num) != size {
				return dst[:0], false
			}
			dst = append(dst, patchRef{offset + at, size, v})
		}
	}
	return dst, true
}
