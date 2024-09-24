package kvo

import (
	"strconv"
	"strings"
)

const (
	objectKindMap       byte = 0x0
	objectKindArray     byte = 0x1
	objectKindString    byte = 0x2
	objectKindRaw       byte = 0x3
	objectKindSet       byte = 0x4
	objectKindTombstone byte = 0x8 // does not fit the actual flags field, but used by mutable record
)

func packKindOffsetSize(kind byte, off, size uint32) uint64 {
	return uint64(kind)<<60 | uint64(off)<<32 | uint64(size)
}

func unpackKindOffsetSize(packed uint64) (kind byte, off, size uint32) {
	size = uint32(packed & 0xFFFF_FFFF)
	off = uint32(packed>>32) & 0x0FFF_FFFF
	kind = byte(packed >> 60)
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
		// var b [8]byte
		// binary.LittleEndian.PutUint64(b[:], w)
		// buf.WriteString(hex.EncodeToString(b[:]))
	}
	return buf.String()
}

func Hex(v uint64) string {
	return strings.ToUpper(strconv.FormatUint(v, 16)) + "h"
}

func writeHex(buf *strings.Builder, v uint64) {
	s := strconv.FormatUint(v, 16)
	if len(s)%2 == 1 {
		buf.WriteByte('0')
	}
	buf.WriteString(s)
}

type keysThenValues []uint64

func (s keysThenValues) Len() int           { return len(s) / 2 }
func (s keysThenValues) Less(i, j int) bool { return s[i] < s[j] }

func (s keysThenValues) Swap(i, j int) {
	n := len(s) / 2
	s[i], s[j] = s[j], s[i]
	s[i+n], s[j+n] = s[j+n], s[i+n]
}
