package edb

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
)

// tuple format: el1 el2 ... elN len1 len2 ... lenN-1  n
type tuple [][]byte

func (tup tuple) String() string {
	var buf strings.Builder
	for i, el := range tup {
		if i > 0 {
			buf.WriteByte('|')
		}
		buf.WriteString(hex.EncodeToString(el))
	}
	return buf.String()
}

func (tup tuple) Equal(another tuple) bool {
	n := len(tup)
	if len(another) != n {
		return false
	}
	for i, b := range tup {
		if !bytes.Equal(b, another[i]) {
			return false
		}
	}
	return true
}

func (tup tuple) rawData(raw []byte, n int) []byte {
	var total int
	for i := 0; i < n; i++ {
		total += len(tup[i])
	}
	if total > len(raw) {
		panic("total > len(raw)")
	}
	return raw[:total]
}

func (tup tuple) prefixLen(n int) int {
	var total int
	for i := 0; i < n; i++ {
		total += len(tup[i])
	}
	return total
}

func decodeTuple(raw []byte) (tuple, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	c, raw := decodeRuvarint(raw)
	if c == 0 {
		return nil, nil
	}

	lens := make([]uint32, c)
	for i := int(c) - 2; i >= 0; i-- {
		lens[i], raw = decodeRuvarint(raw)
		// log.Printf("lens[%d] = %d", i, lens[i])
	}

	var explicitLen uint64
	for i := uint32(0); i < c-1; i++ {
		explicitLen += uint64(lens[i])
	}
	if explicitLen > uint64(len(raw)) {
		return nil, fmt.Errorf("invalid tuple: sum of explicit lens %d is greater than total data len %d", explicitLen, len(raw))
	}

	starts := make([]uint32, c+1)
	for i := uint32(0); i < c-1; i++ {
		starts[i+1] = starts[i] + lens[i]
	}
	starts[c] = uint32(len(raw))

	tup := make(tuple, c)
	for i := uint32(0); i < c; i++ {
		tup[i] = raw[starts[i]:starts[i+1]]
	}
	return tup, nil
}

func (tup tuple) len() int {
	var n int
	for _, item := range tup {
		n += len(item)
	}
	return n + 1 + 4*(len(tup)-1)
}

func (tup tuple) encode(buf []byte) []byte {
	var tb tupleEncoder
	for _, el := range tup {
		tb.begin(buf)
		buf = appendRaw(buf, el)
	}
	return tb.finalize(buf)
}

type tupleEncoder struct {
	startOffPlus1 int
	lens          []int
}

func (tb *tupleEncoder) count() int {
	return len(tb.lens) + 1
}

func (tb *tupleEncoder) begin(buf []byte) {
	off := tb.startOffPlus1
	if off < 0 {
		panic("tupleEncoder finalized")
	} else if off != 0 {
		itemLen := len(buf) + 1 - off
		tb.lens = append(tb.lens, itemLen)
	}
	tb.startOffPlus1 = len(buf) + 1
}

func (tb *tupleEncoder) finalize(buf []byte) []byte {
	for _, v := range tb.lens {
		buf = appendRuvarint(buf, uint32(v))
	}
	buf = appendRuvarint(buf, uint32(tb.count()))
	return buf
}

func (tb *tupleEncoder) prefixLen(n int) int {
	var total int
	for _, len := range tb.lens[:n] {
		total += len
	}
	return total
}

// Reverse Uvarint is just byte-reversed Uvarint, for right-to-left reading
func appendRuvarint(buf []byte, v uint32) []byte {
	var vb [binary.MaxVarintLen32]byte
	vn := binary.PutUvarint(vb[:], uint64(v))
	off, buf := grow(buf, vn)
	for i, b := range vb[:vn] {
		buf[off+vn-i-1] = b
	}
	return buf
}

func decodeRuvarint(buf []byte) (uint32, []byte) {
	var vb [binary.MaxVarintLen32]byte
	n := len(buf)
	if n == 0 {
		panic("decodeRuvarint: empty buf")
	}
	c := binary.MaxVarintLen32
	if n < c {
		c = n
	}
	for i := 0; i < c; i++ {
		vb[i] = buf[n-i-1]
	}
	v, vn := binary.Uvarint(vb[:])
	if vn < 0 {
		panic(fmt.Errorf("invalid ruvarint in %x", buf))
	}
	return uint32(v), buf[:n-vn]
}
