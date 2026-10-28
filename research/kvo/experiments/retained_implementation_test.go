package kvolab

// Retain implementation choices independently of the format so another CPU can
// reverse the local ranking. These copies intentionally do not replace defaults.
import (
	"bytes"
	"fmt"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"math"
	"testing"
)

func retainedLengthEncodeNoHint(w *lengthWriter, dst []byte, v Value, policy func(depth int, v Value) bool) []byte {
	w.Reset(dst)
	var walk func(uint64, string, Value, int)
	walk = func(key uint64, text string, x Value, depth int) {
		switch x.Kind {
		case Record, Dict, List:
			kind := 0
			if x.Kind == Dict {
				kind = 1
			} else if x.Kind == List {
				kind = 2
			}
			indexed := policy != nil && policy(depth, x)
			w.Begin(key, text, kind, indexed)
			switch x.Kind {
			case Record:
				for _, f := range x.Fields {
					walk(f.Key, "", f.Value, depth+1)
				}
			case Dict:
				for _, p := range x.Entries {
					walk(0, p.Key, p.Value, depth+1)
				}
			case List:
				for _, v := range x.Items {
					walk(0, "", v, depth+1)
				}
			}
			w.End()
		default:
			word := x.U
			switch x.Kind {
			case Null, False:
				word = 0
			case True:
				word = 1
			case Int:
				i := int64(x.U)
				word = uint64(i<<1) ^ uint64(i>>63)
			case Float:
				if math.Float64frombits(x.U) == 0 {
					word = 0
				}
			case Text:
				w.Put(key, text, lengthBytes(tagStringBytes(x.S)))
				return
			case Blob:
				w.Put(key, text, lengthBytes(x.B))
				return
			}
			w.Put(key, text, lengthWord(word))
		}
	}
	walk(0, "", v, 0)
	return w.BytesWritten()
}
func (v lengthView) retainedGenericGet(target uint64) lengthValue {
	if v.n == 0 {
		return lengthValue{}
	}
	if v.Kind() != 0 {
		panic(draft.ErrKind)
	}
	if v.Indexed() {
		h := lengthIndex(v.Raw())
		return h.value(retainedGenericNumber(&h, target))
	}
	_, n, shared, payload, meta := lengthHeader(v.Raw())
	var key uint64
	pos := 0
	if shared <= 8 {
		// Uniform scalar records do not need to read any value lengths.
		for i := 0; i < n; i++ {
			d, r := lengthU(meta)
			meta = r
			key += d
			if key == target {
				return lengthCell(payload, i*int(shared), int(shared), shared)
			}
			if key > target {
				break
			}
		}
		return lengthValue{}
	}
	for i := 0; i < n; i++ {
		code := shared
		var d uint64
		if shared == 15 {
			d, code, meta = lengthMixed(meta)
		} else {
			d, meta = lengthU(meta)
		}
		key += d
		size := int(code)
		if code == 9 {
			u, r := lengthU(meta)
			size = int(u)
			meta = r
		}
		if key == target {
			return lengthCell(payload, pos, size, code)
		}
		if key > target {
			break
		}
		pos += size
	}
	return lengthValue{}
}

func retainedGenericNumber(h *lengthIndexHeader, key uint64) int {
	lo, hi := 0, h.n
	for lo < hi {
		m := lo + (hi-lo)/2
		if h.number(m) < key {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo == h.n || h.number(lo) != key {
		return -1
	}
	return lo
}
func TestRetainedImplementation(t *testing.T) {
	for _, source := range fixtures() {
		var a, b lengthWriter
		for _, indexed := range []bool{false, true} {
			policy := func(int, Value) bool { return indexed }
			want := lengthEncode(&a, nil, source, policy)
			got := retainedLengthEncodeNoHint(&b, nil, source, policy)
			if !bytes.Equal(want, got) {
				t.Fatal("capacity hint changed bytes")
			}
		}
	}
	for _, source := range lengthKeyFixtures() {
		var w lengthWriter
		v := lengthTrusted(lengthEncode(&w, nil, source, func(int, Value) bool { return true }))
		for _, f := range source.Fields {
			if v.Get(f.Key).Uint() != v.retainedGenericGet(f.Key).Uint() {
				t.Fatal("lookup")
			}
		}
	}
}
func BenchmarkRetainedImplementation(b *testing.B) {
	all := fixtures()
	all["deep16"] = tagDeep(16, 32768)
	for _, name := range []string{"state", "settings", "dictionary-128", "deep16"} {
		source := all[name]
		for _, hint := range []bool{false, true} {
			for _, cold := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/hint%v/cold%v", name, hint, cold), func(b *testing.B) {
					var w lengthWriter
					var out []byte
					encode := retainedLengthEncodeNoHint
					if hint {
						encode = lengthEncode
					}
					out = encode(&w, nil, source, nil)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if cold {
							var fresh lengthWriter
							out = encode(&fresh, nil, source, nil)
						} else {
							out = encode(&w, out[:0], source, nil)
						}
					}
					sinkBytes = out
				})
			}
		}
	}
	keys := lengthKeyFixtures()
	for _, name := range []string{"local", "clustered-high", "random32", "random64"} {
		source := keys[name]
		var w lengthWriter
		v := lengthTrusted(lengthEncode(&w, nil, source, func(int, Value) bool { return true }))
		for _, generic := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/generic%v", name, generic), func(b *testing.B) {
				b.ReportAllocs()
				var sum uint64
				for i := 0; i < b.N; i++ {
					k := source.Fields[query(i, 128)-1].Key
					if generic {
						sum += v.retainedGenericGet(k).Uint()
					} else {
						sum += v.Get(k).Uint()
					}
				}
				sinkU = sum
			})
		}
	}
}
