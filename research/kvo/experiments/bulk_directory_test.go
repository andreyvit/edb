package kvolab

// Reserve the known final directory extent once at End. Payloads still stream
// directly to the destination, and no child sizing pass or payload buffer is
// introduced. This control keeps the previous append-per-entry writer intact.
import (
	"bytes"
	"encoding/binary"
	"maps"
	"math"
	"math/bits"
	"slices"
	"testing"

	draft "github.com/andreyvit/edb/research/kvo/draft"
)

type bulkDirectoryWriter struct{ keysLastWriter }

func appendBulkDirectory(dst []byte, f *lengthFrame) []byte {
	n := len(f.entries)
	width := 1
	if n > 0 {
		width = tagKeyWidth(f.entries[n-1].key)
	}
	mode, bitmap := byte(0), 0
	if f.shared == 9 {
		mode = 1
	} else if f.shared == 15 && n > 0 {
		mode, bitmap = 2, (n+7)/8
	}
	size := n*width + bitmap + 5
	if f.kind == 0 {
		size += n * 8
	}
	start := len(dst)
	dst = slices.Grow(dst, size)
	dst = dst[:start+size]
	keys, codes := start, start+n*width
	if f.kind == 0 {
		codes = start + n*8
		keys = codes + bitmap
		for i, m := range f.entries {
			binary.LittleEndian.PutUint64(dst[start+i*8:], m.word)
		}
	}
	if bitmap > 0 {
		clear(dst[codes : codes+bitmap])
		for i, m := range f.entries {
			if m.wire == 9 {
				dst[codes+i/8] |= 1 << uint(i&7)
			}
		}
	}
	// Width is constant for the entire directory. Hoist its dispatch out of
	// the entry loop; explicit endian stores retain the portable baseline.
	switch width {
	case 1:
		for i, m := range f.entries {
			dst[keys+i] = byte(m.key)
		}
	case 2:
		for i, m := range f.entries {
			binary.LittleEndian.PutUint16(dst[keys+i*2:], uint16(m.key))
		}
	case 4:
		for i, m := range f.entries {
			binary.LittleEndian.PutUint32(dst[keys+i*4:], uint32(m.key))
		}
	case 8:
		for i, m := range f.entries {
			binary.LittleEndian.PutUint64(dst[keys+i*8:], m.key)
		}
	}
	footer := start + size - 5
	binary.LittleEndian.PutUint32(dst[footer:], uint32(n))
	dst[footer+4] = 1 | byte(f.kind)<<1 | mode<<3 | byte(bits.Len(uint(width))-1)<<6
	return dst
}

func (w *bulkDirectoryWriter) End() {
	if w.depth == 0 {
		panic(draft.ErrKind)
	}
	f := &w.frames[w.depth-1]
	if len(f.entries) == 0 && w.depth > 1 {
		key, rollback := f.parentKey, f.rollback
		w.out = w.out[:rollback]
		w.depth--
		if w.frames[w.depth-1].kind == 2 {
			if w.frames[w.depth-1].indexed {
				w.out = binary.LittleEndian.AppendUint64(w.out, 0)
			}
			w.add(key, 0, 0)
		}
		return
	}
	if f.indexed {
		w.out = appendBulkDirectory(w.out, f)
	} else {
		w.out = lengthAppendCompact(w.out, *f)
	}
	n := len(w.out) - f.start
	if n > tagMaxBytes {
		panic(draft.ErrLimit)
	}
	key, start := f.parentKey, f.start
	w.depth--
	if w.depth > 0 {
		w.add(key, 9, uint64(start-w.frames[w.depth-1].start)|uint64(n)<<32)
	}
}
func bulkDirectoryEncode(w *bulkDirectoryWriter, dst []byte, v Value, policy func(depth int, v Value) bool) []byte {
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
				w.ReserveEntries(len(x.Fields))
				for _, f := range x.Fields {
					walk(f.Key, "", f.Value, depth+1)
				}
			case Dict:
				w.ReserveEntries(len(x.Entries))
				for _, p := range x.Entries {
					walk(0, p.Key, p.Value, depth+1)
				}
			case List:
				w.ReserveEntries(len(x.Items))
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

func TestBulkDirectory(t *testing.T) {
	all := maps.Clone(workloadFixtures(t))
	for key, value := range fixtures() {
		all[key] = value
	}
	all["global128"] = mmapFixture("global128")
	all["extreme-key"] = Obj(Field{0, U(1)}, Field{math.MaxUint64, U(2)})
	var old appendDirectoryWriter
	var writer bulkDirectoryWriter
	for name, source := range all {
		for _, policy := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
			want := appendDirectoryEncode(&old, nil, source, policy)
			// Dirty backing storage ensures absent bitmap bits and every footer
			// byte are overwritten rather than accidentally relying on zero fill.
			storage := bytes.Repeat([]byte{0xff}, len(want)+13)
			got := bulkDirectoryEncode(&writer, storage[:0], source, policy)
			if !bytes.Equal(got, want) {
				t.Fatal(name, "bytes differ")
			}
			if _, err := keysLastOpen(got); err != nil {
				t.Fatal(name, err)
			}
		}
	}
}

func BenchmarkBulkDirectory(b *testing.B) {
	fixtures := map[string]Value{}
	for name, row := range rowFixtures(b) {
		fixtures[name] = row.source
	}
	for _, name := range []string{"local128", "global128", "nested"} {
		fixtures[name] = mmapFixture(name)
	}
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy", "local128", "global128", "nested"} {
		source := fixtures[name]
		for _, layout := range []string{"fast", "compact", "mixed"} {
			policy := func(d int, _ Value) bool { return layout == "fast" || layout == "mixed" && d == 0 }
			for _, mode := range []string{"previous", "bulk"} {
				for _, cold := range []bool{false, true} {
					label := "reuse"
					if cold {
						label = "cold"
					}
					b.Run(name+"/"+layout+"/"+mode+"/"+label, func(b *testing.B) {
						var old appendDirectoryWriter
						var writer bulkDirectoryWriter
						var out []byte
						run := func() {
							if cold {
								old = appendDirectoryWriter{}
								writer = bulkDirectoryWriter{}
								out = nil
							}
							if mode == "previous" {
								out = appendDirectoryEncode(&old, out[:0], source, policy)
							} else {
								out = bulkDirectoryEncode(&writer, out[:0], source, policy)
							}
						}
						run()
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							run()
						}
						b.StopTimer()
						sinkBytes = out
						b.ReportMetric(float64(len(out)), "row-B")
					})
				}
			}
		}
	}
}
