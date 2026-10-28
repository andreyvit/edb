package kvolab

// Trusted lookup controls avoid re-validating preceding values. Retain the
// iterator-based lookups to distinguish grammar costs from implementation costs.
import (
	"bytes"
	"fmt"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

func selectorHeader(raw []byte, shift uint) (n int, shared byte, payload, meta []byte) {
	footer, b := tagTail(raw)
	if footer>>1&3 != 0 {
		panic(draft.ErrKind)
	}
	at := len(b) - int(footer>>3)
	payload, meta = b[:at], b[at:]
	h, rest := lengthU(meta)
	return int(h >> shift), byte(h) & byte(1<<shift-1), payload, rest
}
func selectorNumber(meta []byte, shared, mixed byte, shift uint) (delta uint64, code byte, rest []byte) {
	u, rest := lengthU(meta)
	if shared != mixed {
		return u, shared, rest
	}
	code = byte(u) & mixed
	if code != mixed {
		return u >> shift, code, rest
	}
	delta, rest = lengthU(rest)
	return delta, rest[0], rest[1:]
}
func selectorTrustedGet(raw []byte, mode selectorMode, target uint64) (lengthValue, bool) {
	if len(raw) == 0 {
		return lengthValue{}, false
	}
	switch mode {
	case selectorLiteral:
		return presenceLookup(lengthTrusted(raw), target)
	case selectorRounded:
		return selectorRoundedGet(raw, target)
	case selectorTagged, selectorTaggedWide:
		return selectorTagGet(raw, target, mode == selectorTaggedWide)
	default:
		panic(draft.ErrFormat)
	}
}
func selectorRoundedGet(raw []byte, target uint64) (lengthValue, bool) {
	n, shared, payload, meta := selectorHeader(raw, 3)
	var key uint64
	if shared <= 4 {
		width := int([5]byte{0, 1, 2, 4, 8}[shared])
		for i := 0; i < n; i++ {
			d, r := lengthU(meta)
			meta = r
			key += d
			if key == target {
				return lengthCell(payload, i*width, width, byte(width)), true
			}
			if key > target {
				break
			}
		}
		return lengthValue{}, false
	}
	pos := 0
	for i := 0; i < n; i++ {
		d, code, r := selectorNumber(meta, shared, 7, 3)
		meta = r
		key += d
		if key > target {
			break
		}
		size := 0
		span := code == 5
		if span {
			u, r := lengthU(meta)
			meta = r
			size = int(u)
		} else {
			size = int([5]byte{0, 1, 2, 4, 8}[code])
		}
		if key == target {
			if span {
				return lengthBytes(payload[pos : pos+size]), true
			}
			return lengthCell(payload, pos, size, byte(size)), true
		}
		pos += size
	}
	return lengthValue{}, false
}
func selectorTagGet(raw []byte, target uint64, wide bool) (lengthValue, bool) {
	n, shared, payload, meta := selectorHeader(raw, 2)
	var key uint64
	if shared == 1 || shared == 2 {
		width := 4
		if shared == 2 {
			width = 8
		}
		for i := 0; i < n; i++ {
			d, r := lengthU(meta)
			meta = r
			key += d
			if key == target {
				return lengthCell(payload, i*width, width, byte(width)), true
			}
			if key > target {
				break
			}
		}
		return lengthValue{}, false
	}
	sentinel := byte(191)
	longBase := 63
	if wide {
		sentinel = 249
		longBase = 121
	}
	pos := 0
	for i := 0; i < n; i++ {
		d, code, r := selectorNumber(meta, shared, 3, 2)
		meta = r
		key += d
		if key > target {
			break
		}
		size := 4
		span := false
		if code == 2 {
			size = 8
		} else if code == 0 {
			tag := meta[0]
			meta = meta[1:]
			if tag < 128 {
				if key == target {
					return lengthWord(uint64(tag)), true
				}
				continue
			}
			if tag < sentinel {
				span = true
				size = int(tag - 128)
			} else if tag == sentinel {
				u, r := lengthU(meta)
				meta = r
				span = true
				size = int(u) + longBase
			} else if wide {
				size = int(tag) - 249
				if size >= 4 {
					size++
				}
			} else {
				size = int(tag) - 192
			}
		}
		if key == target {
			if span {
				return lengthBytes(payload[pos : pos+size]), true
			}
			return lengthCell(payload, pos, size, byte(size)), true
		}
		pos += size
	}
	return lengthValue{}, false
}
func TestSelectorTrustedGet(t *testing.T) {
	all := fixtures()
	for name, v := range workloadFixtures(t) {
		all[name] = v
	}
	for _, source := range all {
		if source.Kind != Record {
			continue
		}
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			w := selectorWriter{mode: mode}
			raw := selectorEncode(&w, nil, source)
			keys := []uint64{0, 1, 2, 1000, ^uint64(0)}
			if len(source.Fields) > 0 {
				keys = append(keys, source.Fields[0].Key, source.Fields[len(source.Fields)-1].Key)
			}
			for _, key := range keys {
				a, pa := selectorGet(raw, mode, key, "")
				b, pb := selectorTrustedGet(raw, mode, key)
				c, pc := selectorTrustedGetInline(raw, mode, key)
				if mode == selectorLiteral {
					z, pz := selectorLiteralGetInline(raw, key)
					if pz != pb || z.num != b.num || (z.ptr == nil) != (b.ptr == nil) || z.ptr != nil && !bytes.Equal(z.Bytes(), b.Bytes()) {
						t.Fatal("literal specialized", key)
					}
				}
				if pb != pc || b.num != c.num || (b.ptr == nil) != (c.ptr == nil) || b.ptr != nil && !bytes.Equal(b.Bytes(), c.Bytes()) {
					t.Fatal("inline control", mode, key)
				}
				if pa != pb || a.num != b.num || (a.ptr == nil) != (b.ptr == nil) {
					t.Fatal("lookup", mode, key)
				}
				if a.ptr != nil && !bytes.Equal(a.Bytes(), b.Bytes()) {
					t.Fatal("span")
				}
			}
		}
	}
}
func BenchmarkSelectorTrustedGet(b *testing.B) {
	for _, sourceName := range []string{"local", "global", "mixed"} {
		source := numeric(128, 0)
		if sourceName == "global" {
			source = numeric(128, 1<<50)
		}
		if sourceName == "mixed" {
			for i := range source.Fields {
				if i%3 == 0 {
					source.Fields[i].Value = S("generic payload")
				}
			}
		}
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			w := selectorWriter{mode: mode}
			raw := selectorEncode(&w, nil, source)
			for _, lookup := range []string{"iterator", "helper", "inlined"} {
				b.Run(fmt.Sprintf("%s/%s/%s", sourceName, mode, lookup), func(b *testing.B) {
					b.ReportAllocs()
					var sum uint64
					for i := 0; i < b.N; i++ {
						key := source.Fields[i&127].Key
						var v lengthValue
						if lookup == "inlined" {
							v, _ = selectorTrustedGetInline(raw, mode, key)
						} else if lookup == "helper" {
							v, _ = selectorTrustedGet(raw, mode, key)
						} else {
							v, _ = selectorGet(raw, mode, key, "")
						}
						sum += v.num
					}
					sinkU = sum
					b.ReportMetric(float64(len(raw)), "encoded-B")
				})
			}
		}
	}
}

// Control for Go refusing to inline selectorNumber in the inner loop.
func selectorTrustedGetInline(raw []byte, mode selectorMode, target uint64) (lengthValue, bool) {
	if len(raw) == 0 {
		return lengthValue{}, false
	}
	switch mode {
	case selectorLiteral:
		return presenceLookup(lengthTrusted(raw), target)
	case selectorRounded:
		return selectorRoundedGetInline(raw, target)
	case selectorTagged, selectorTaggedWide:
		return selectorTagGetInline(raw, target, mode == selectorTaggedWide)
	default:
		panic(draft.ErrFormat)
	}
}
func selectorRoundedGetInline(raw []byte, target uint64) (lengthValue, bool) {
	n, shared, payload, meta := selectorHeader(raw, 3)
	var key uint64
	if shared <= 4 {
		width := int([5]byte{0, 1, 2, 4, 8}[shared])
		for i := 0; i < n; i++ {
			d, r := lengthU(meta)
			meta = r
			key += d
			if key == target {
				return lengthCell(payload, i*width, width, byte(width)), true
			}
			if key > target {
				break
			}
		}
		return lengthValue{}, false
	}
	pos := 0
	for i := 0; i < n; i++ {
		d, r := lengthU(meta)
		meta = r
		code := shared
		if shared == 7 {
			code = byte(d) & 7
			if code == 7 {
				d, meta = lengthU(meta)
				code, meta = meta[0], meta[1:]
			} else {
				d >>= 3
			}
		}
		key += d
		if key > target {
			break
		}
		size := 0
		span := code == 5
		if span {
			u, r := lengthU(meta)
			meta = r
			size = int(u)
		} else {
			size = int([5]byte{0, 1, 2, 4, 8}[code])
		}
		if key == target {
			if span {
				return lengthBytes(payload[pos : pos+size]), true
			}
			return lengthCell(payload, pos, size, byte(size)), true
		}
		pos += size
	}
	return lengthValue{}, false
}
func selectorTagGetInline(raw []byte, target uint64, wide bool) (lengthValue, bool) {
	n, shared, payload, meta := selectorHeader(raw, 2)
	var key uint64
	if shared == 1 || shared == 2 {
		width := 4
		if shared == 2 {
			width = 8
		}
		for i := 0; i < n; i++ {
			d, r := lengthU(meta)
			meta = r
			key += d
			if key == target {
				return lengthCell(payload, i*width, width, byte(width)), true
			}
			if key > target {
				break
			}
		}
		return lengthValue{}, false
	}
	sentinel := byte(191)
	longBase := 63
	if wide {
		sentinel = 249
		longBase = 121
	}
	pos := 0
	for i := 0; i < n; i++ {
		d, r := lengthU(meta)
		meta = r
		code := shared
		if shared == 3 {
			code = byte(d) & 3
			if code == 3 {
				d, meta = lengthU(meta)
				code, meta = meta[0], meta[1:]
			} else {
				d >>= 2
			}
		}
		key += d
		if key > target {
			break
		}
		size := 4
		span := false
		if code == 2 {
			size = 8
		} else if code == 0 {
			tag := meta[0]
			meta = meta[1:]
			if tag < 128 {
				if key == target {
					return lengthWord(uint64(tag)), true
				}
				continue
			}
			if tag < sentinel {
				span = true
				size = int(tag - 128)
			} else if tag == sentinel {
				u, r := lengthU(meta)
				meta = r
				span = true
				size = int(u) + longBase
			} else if wide {
				size = int(tag) - 249
				if size >= 4 {
					size++
				}
			} else {
				size = int(tag) - 192
			}
		}
		if key == target {
			if span {
				return lengthBytes(payload[pos : pos+size]), true
			}
			return lengthCell(payload, pos, size, byte(size)), true
		}
		pos += size
	}
	return lengthValue{}, false
}

// Same specialized loop as rounded3, but literal byte lengths.
func selectorLiteralGetInline(raw []byte, target uint64) (lengthValue, bool) {
	n, shared, payload, meta := selectorHeader(raw, 4)
	var key uint64
	if shared <= 8 {
		width := int(shared)
		for i := 0; i < n; i++ {
			d, r := lengthU(meta)
			meta = r
			key += d
			if key == target {
				return lengthCell(payload, i*width, width, byte(width)), true
			}
			if key > target {
				break
			}
		}
		return lengthValue{}, false
	}
	pos := 0
	for i := 0; i < n; i++ {
		d, r := lengthU(meta)
		meta = r
		code := shared
		if shared == 15 {
			code = byte(d) & 15
			if code == 15 {
				d, meta = lengthU(meta)
				code, meta = meta[0], meta[1:]
			} else {
				d >>= 4
			}
		}
		key += d
		if key > target {
			break
		}
		size := 0
		span := code == 9
		if span {
			u, r := lengthU(meta)
			meta = r
			size = int(u)
		} else {
			size = int(code)
		}
		if key == target {
			if span {
				return lengthBytes(payload[pos : pos+size]), true
			}
			return lengthCell(payload, pos, size, byte(size)), true
		}
		pos += size
	}
	return lengthValue{}, false
}

func BenchmarkSelectorLiteralControl(b *testing.B) {
	for _, name := range []string{"local", "global", "mixed"} {
		source := numeric(128, 0)
		if name == "global" {
			source = numeric(128, 1<<50)
		}
		if name == "mixed" {
			for i := range source.Fields {
				if i%3 == 0 {
					source.Fields[i].Value = S("generic payload")
				}
			}
		}
		for _, strategy := range []string{"literal-common", "literal-matched", "rounded-matched"} {
			mode := selectorLiteral
			if strategy == "rounded-matched" {
				mode = selectorRounded
			}
			w := selectorWriter{mode: mode}
			raw := selectorEncode(&w, nil, source)
			b.Run(name+"/"+strategy, func(b *testing.B) {
				var sum uint64
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					key := source.Fields[i&127].Key
					var v lengthValue
					switch strategy {
					case "literal-common":
						v, _ = presenceLookup(lengthTrusted(raw), key)
					case "literal-matched":
						v, _ = selectorLiteralGetInline(raw, key)
					case "rounded-matched":
						v, _ = selectorRoundedGetInline(raw, key)
					}
					sum += v.num
				}
				sinkU = sum
			})
		}
	}
}
