package edb

import (
	"bytes"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// Go map iteration order can change between two encodes of the same struct.
// Compare maps by encoded key, arrays in order, and scalar payloads exactly.
// Unknown legacy fields must survive both round trips before conversion.
func sameLegacyPayload(a, b []byte, depth int) bool {
	if bytes.Equal(a, b) {
		return true
	}
	if len(a) == 0 || len(b) == 0 || depth > 256 {
		return false
	}
	ra, rb := bytes.NewReader(a), bytes.NewReader(b)
	da, db := msgpack.NewDecoder(ra), msgpack.NewDecoder(rb)
	switch c := a[0]; {
	case msgpcode.IsFixedMap(c) || c == msgpcode.Map16 || c == msgpcode.Map32:
		n, e := da.DecodeMapLen()
		m, f := db.DecodeMapLen()
		if e != nil || f != nil || n != m || n < 0 || n > len(a)/2 {
			return false
		}
		vals := make(map[string][]byte, n)
		for range n {
			k, e := da.DecodeRaw()
			v, f := da.DecodeRaw()
			if e != nil || f != nil || vals[string(k)] != nil {
				return false
			}
			vals[string(k)] = v
		}
		for range m {
			k, e := db.DecodeRaw()
			v, f := db.DecodeRaw()
			if e != nil || f != nil || !sameLegacyPayload(vals[string(k)], v, depth+1) {
				return false
			}
			delete(vals, string(k))
		}
	case msgpcode.IsFixedArray(c) || c == msgpcode.Array16 || c == msgpcode.Array32:
		n, e := da.DecodeArrayLen()
		m, f := db.DecodeArrayLen()
		if e != nil || f != nil || n < 0 || n != m {
			return false
		}
		for range n {
			x, e := da.DecodeRaw()
			y, f := db.DecodeRaw()
			if e != nil || f != nil || !sameLegacyPayload(x, y, depth+1) {
				return false
			}
		}
	default:
		return false
	}
	return ra.Len() == 0 && rb.Len() == 0
}
