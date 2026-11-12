package fast1

import (
	"strconv"
	"strings"
)

type objectKind byte

const (
	objectKindMap       objectKind = 0x0
	objectKindArray     objectKind = 0x1
	objectKindString    objectKind = 0x2
	objectKindRaw       objectKind = 0x3
	objectKindSet       objectKind = 0x4
	objectKindTombstone objectKind = 0x8 // mutable records only
)

func (k objectKind) String() string {
	switch k {
	case objectKindMap:
		return "map"
	case objectKindArray:
		return "array"
	case objectKindString:
		return "string"
	case objectKindRaw:
		return "raw"
	case objectKindSet:
		return "set"
	case objectKindTombstone:
		return "tombstone"
	default:
		return "objectKind(" + strconv.Itoa(int(k)) + ")"
	}
}

func packKindOffsetSize(kind objectKind, off, size uint32) uint64 {
	return uint64(kind)<<60 | uint64(off)<<32 | uint64(size)
}

func unpackKindOffsetSize(packed uint64) (kind objectKind, off, size uint32) {
	size = uint32(packed & 0xFFFF_FFFF)
	off = uint32(packed>>32) & 0x0FFF_FFFF
	kind = objectKind(packed >> 60)
	return
}

func HexString(data []uint64) string {
	var buf strings.Builder
	for i, w := range data {
		if i > 0 {
			buf.WriteByte(' ')
		}
		if w <= 0xFFFF_FFFF {
			writeHex(&buf, w)
		} else {
			wh, wl := uint32(w>>32), uint32(w)
			writeHex(&buf, uint64(wh))
			buf.WriteByte(':')
			writeHex(&buf, uint64(wl))
		}
	}
	return buf.String()
}

func writeHex(buf *strings.Builder, v uint64) {
	s := strconv.FormatUint(v, 16)
	if len(s)%2 == 1 {
		buf.WriteByte('0')
	}
	buf.WriteString(s)
}
