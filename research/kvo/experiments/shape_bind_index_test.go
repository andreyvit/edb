package kvolab

// Path catalogs follow canonical container order. Binary binding needs no
// auxiliary index. The string-keyed map control pays to build a reusable index;
// it uses unambiguous length-prefixed steps, not unchecked hash fingerprints.
import (
	"encoding/binary"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"math/bits"
	"slices"
	"strings"
	"testing"
)

func comparePerfPath(a, b []perfStep) int {
	for i := 0; i < min(len(a), len(b)); i++ {
		x, y := a[i], b[i]
		if x.kind != y.kind {
			return x.kind - y.kind
		}
		if x.kind == 1 {
			if c := strings.Compare(x.text, y.text); c != 0 {
				return c
			}
		} else if x.key < y.key {
			return -1
		} else if x.key > y.key {
			return 1
		}
	}
	return len(a) - len(b)
}

func bindShapeBinary(dst []int, leaves, edits []perfLeaf) []int {
	dst = dst[:0]
	for _, edit := range edits {
		i, ok := slices.BinarySearchFunc(leaves, edit.path, func(leaf perfLeaf, path []perfStep) int {
			return comparePerfPath(leaf.path, path)
		})
		if !ok {
			panic(draft.ErrUnknownKey)
		}
		dst = append(dst, i)
	}
	return dst
}

func appendShapePath(dst []byte, path []perfStep) []byte {
	for _, step := range path {
		dst = append(dst, byte(step.kind))
		if step.kind == 1 {
			dst = binary.AppendUvarint(dst, uint64(len(step.text)))
			dst = append(dst, step.text...)
		} else {
			dst = binary.AppendUvarint(dst, step.key)
		}
	}
	return dst
}

type shapePathIndex struct {
	slots map[string]int
	buf   []byte
}

func (x *shapePathIndex) Reset(leaves []perfLeaf) {
	if x.slots == nil {
		x.slots = make(map[string]int, len(leaves))
	} else {
		clear(x.slots)
	}
	for i, leaf := range leaves {
		x.buf = appendShapePath(x.buf[:0], leaf.path)
		x.slots[string(x.buf)] = i
	}
}

// One immutable string backs every map key. Builder.String does not copy it;
// substrings are safe Go strings and cannot be invalidated by lookup scratch.
// This removes the per-path allocation without using unsafe string aliases.
func (x *shapePathIndex) ResetArena(leaves []perfLeaf) {
	x.resetArena(leaves, 0)
}

func (x *shapePathIndex) ResetArenaSized(leaves []perfLeaf) {
	size := 0
	for _, leaf := range leaves {
		for _, step := range leaf.path {
			size++
			if step.kind == 1 {
				size += len(step.text) + (bits.Len64(uint64(len(step.text))|1)+6)/7
			} else {
				size += (bits.Len64(step.key|1) + 6) / 7
			}
		}
	}
	x.resetArena(leaves, size)
}

func (x *shapePathIndex) resetArena(leaves []perfLeaf, size int) {
	if x.slots == nil {
		x.slots = make(map[string]int, len(leaves))
	} else {
		clear(x.slots)
	}
	var builder strings.Builder
	builder.Grow(size)
	ends := make([]int, len(leaves))
	for i, leaf := range leaves {
		x.buf = appendShapePath(x.buf[:0], leaf.path)
		builder.Write(x.buf)
		ends[i] = builder.Len()
	}
	keys := builder.String()
	start := 0
	for i, end := range ends {
		x.slots[keys[start:end]] = i
		start = end
	}
}

func (x *shapePathIndex) Bind(dst []int, edits []perfLeaf) []int {
	dst = dst[:0]
	for _, edit := range edits {
		x.buf = appendShapePath(x.buf[:0], edit.path)
		i, ok := x.slots[string(x.buf)]
		if !ok {
			panic(draft.ErrUnknownKey)
		}
		dst = append(dst, i)
	}
	return dst
}

func TestShapeBindIndex(t *testing.T) {
	for name, source := range workloadFixtures(t) {
		var w lengthWriter
		raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
		leaves := perfCollect(source, raw)
		if !slices.IsSortedFunc(leaves, func(a, b perfLeaf) int { return comparePerfPath(a.path, b.path) }) {
			t.Fatal(name, "noncanonical leaf order")
		}
		var index shapePathIndex
		index.Reset(leaves)
		var arena shapePathIndex
		arena.ResetArena(leaves)
		var sized shapePathIndex
		sized.ResetArenaSized(leaves)
		if len(index.slots) != len(leaves) {
			t.Fatal(name, "duplicate path encoding")
		}
		queries := slices.Clone(leaves)
		slices.Reverse(queries)
		binarySlots := bindShapeBinary(nil, leaves, queries)
		hashSlots := index.Bind(nil, queries)
		arenaSlots := arena.Bind(nil, queries)
		sizedSlots := sized.Bind(nil, queries)
		for i := range queries {
			if binarySlots[i] != len(leaves)-1-i || hashSlots[i] != binarySlots[i] || arenaSlots[i] != binarySlots[i] || sizedSlots[i] != binarySlots[i] {
				t.Fatal(name, "binding")
			}
		}
	}
	// Prefixes, delimiters, NULs and full-width numeric keys must not alias.
	paths := [][]perfStep{
		{{kind: 0, key: 1}}, {{kind: 2, key: 1}}, {{kind: 0, key: ^uint64(0)}},
		{{kind: 1, text: ""}}, {{kind: 1, text: "a\x00b"}},
		{{kind: 1, text: "a"}, {kind: 1, text: "b"}},
		{{kind: 1, text: "a"}, {kind: 0, key: 0}},
	}
	seen := make(map[string]bool)
	for _, path := range paths {
		key := string(appendShapePath(nil, path))
		if seen[key] {
			t.Fatal("ambiguous path encoding")
		}
		seen[key] = true
	}
}

func BenchmarkShapeBindIndex(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		edits := rowEdits(r, "wide70")
		for _, mode := range []string{"linear", "binary", "hash", "hash-arena", "prepare-hash", "prepare-hash-arena", "prepare-hash-sized", "prepare-bind-write-binary", "prepare-bind-write-hash", "prepare-bind-write-hash-arena", "prepare-bind-write-hash-sized", "reused-bind-write-binary", "reused-bind-write-hash", "reused-bind-write-hash-arena"} {
			b.Run(name+"/"+mode, func(b *testing.B) {
				var index shapePathIndex
				var slots []int
				var out []byte
				var w lengthWriter
				plan := prepareShape(r)
				resetIndex := func(x *shapePathIndex) {
					if strings.HasSuffix(mode, "sized") {
						x.ResetArenaSized(r.leaves)
					} else if strings.HasSuffix(mode, "arena") {
						x.ResetArena(r.leaves)
					} else {
						x.Reset(r.leaves)
					}
				}
				resetIndex(&index)
				run := func() {
					switch mode {
					case "linear":
						slots = boundShapeEditsPrevious(r, edits)
					case "binary":
						slots = bindShapeBinary(slots, r.leaves, edits)
					case "hash", "hash-arena":
						slots = index.Bind(slots, edits)
					case "prepare-hash", "prepare-hash-arena", "prepare-hash-sized":
						var fresh shapePathIndex
						resetIndex(&fresh)
						sinkU = uint64(len(fresh.slots))
					default:
						if strings.HasPrefix(mode, "prepare-") {
							plan = prepareShape(r)
							if strings.Contains(mode, "hash") {
								index = shapePathIndex{}
								resetIndex(&index)
							}
						} else {
							plan.Reset(false)
						}
						if strings.Contains(mode, "hash") {
							slots = index.Bind(slots, edits)
						} else {
							slots = bindShapeBinary(slots, r.leaves, edits)
						}
						for i, edit := range edits {
							plan.Set(slots[i], edit.value)
						}
						out = plan.Append(&w, out[:0])
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
				if len(slots) > 0 {
					sinkU = uint64(slots[len(slots)-1])
				}
			})
		}
	}
}
