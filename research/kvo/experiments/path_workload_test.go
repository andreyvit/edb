package kvolab

// Paths and scalar/span values come from the sanitized replay. Access order is
// an explicit benchmark scenario, not a claimed production frequency trace.
import (
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"
)

type perfStep struct {
	kind int
	key  uint64
	text string
}
type perfLeaf struct {
	path  []perfStep
	value lengthValue
	kind  Kind
}
type perfRow struct {
	id     string
	source Value
	raw    []byte
	leaves []perfLeaf
	depth  int
}

var perfRowsOnce sync.Once
var perfRows map[string]perfRow

func perfChild(v lengthView, s perfStep, reader string) lengthValue {
	if reader == "common" {
		switch s.kind {
		case 0:
			return v.Get(s.key)
		case 1:
			return v.GetString(s.text)
		default:
			return v.At(int(s.key))
		}
	}
	switch s.kind {
	case 0:
		return perfGet(v, s.key, reader == "equal")
	case 1:
		return perfGetString(v, s.text, reader == "equal")
	default:
		return perfAt(v, int(s.key))
	}
}
func perfPath(v lengthView, path []perfStep, reader string) lengthValue {
	var value lengthValue
	for i, s := range path {
		value = perfChild(v, s, reader)
		if i+1 < len(path) {
			if value.IsZero() {
				return value
			}
			v = value.Object()
		}
	}
	return value
}
func perfCollect(source Value, raw []byte) []perfLeaf {
	var leaves []perfLeaf
	var walk func(Value, lengthView, []perfStep)
	walk = func(x Value, v lengthView, path []perfStep) {
		add := func(s perfStep, child Value) {
			if s.kind != 2 && tagZero(child) {
				return
			}
			p := append(append([]perfStep(nil), path...), s)
			value := perfChild(v, s, "common")
			if !value.IsZero() && (child.Kind == Record || child.Kind == Dict || child.Kind == List) {
				walk(child, value.Object(), p)
			} else {
				leaves = append(leaves, perfLeaf{p, value, child.Kind})
			}
		}
		switch x.Kind {
		case Record:
			for _, f := range x.Fields {
				add(perfStep{kind: 0, key: f.Key}, f.Value)
			}
		case Dict:
			for _, e := range x.Entries {
				add(perfStep{kind: 1, text: e.Key}, e.Value)
			}
		case List:
			for i, e := range x.Items {
				add(perfStep{kind: 2, key: uint64(i)}, e)
			}
		}
	}
	walk(source, lengthTrusted(raw), nil)
	return leaves
}
func rowFixtures(tb testing.TB) map[string]perfRow {
	all := workloadFixtures(tb)
	perfRowsOnce.Do(func() {
		perfRows = map[string]perfRow{}
		names := make([]string, 0, len(all))
		for name := range all {
			names = append(names, name)
		}
		sort.Strings(names)
		best := map[string]float64{"row-small": math.Inf(1), "row-wide": math.Inf(1), "row-deep": math.Inf(1), "row-span-heavy": math.Inf(1)}
		for _, name := range names {
			source := all[name]
			if source.Kind != Record {
				continue
			}
			var w lengthWriter
			raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
			r := perfRow{id: name, source: source, raw: raw, leaves: perfCollect(source, raw)}
			for _, l := range r.leaves {
				r.depth = max(r.depth, len(l.path))
			}
			if len(r.leaves) < 8 || len(r.leaves) > 2000 || len(raw) > 1<<20 {
				continue
			}
			scores := map[string]float64{"row-small": math.Abs(float64(len(r.leaves) - 32)), "row-wide": math.Abs(float64(len(r.leaves) - 512)), "row-deep": float64(-r.depth)*1e6 + float64(len(r.leaves)), "row-span-heavy": -float64(len(raw)) / float64(len(r.leaves))}
			for label, score := range scores {
				if score < best[label] {
					best[label] = score
					perfRows[label] = r
				}
			}
		}
	})
	return perfRows
}
func rowEdits(r perfRow, pattern string) []perfLeaf {
	leaves := append([]perfLeaf(nil), r.leaves...)
	if pattern == "few-shallow" {
		sort.SliceStable(leaves, func(i, j int) bool { return len(leaves[i].path) < len(leaves[j].path) })
	}
	if pattern == "few-deep" {
		sort.SliceStable(leaves, func(i, j int) bool { return len(leaves[i].path) > len(leaves[j].path) })
	}
	if pattern == "wide70" || pattern == "empty-shuffled" {
		rng := uint64(0x123abcde99887766)
		for i := len(leaves) - 1; i > 0; i-- {
			rng ^= rng << 13
			rng ^= rng >> 7
			rng ^= rng << 17
			j := int(rng % uint64(i+1))
			leaves[i], leaves[j] = leaves[j], leaves[i]
		}
	}
	switch pattern {
	case "few-shallow", "few-deep":
		leaves = leaves[:min(4, len(leaves))]
	case "wide70":
		leaves = leaves[:max(1, len(leaves)*7/10)]
	}
	for i := range leaves {
		v := leaves[i].value
		switch leaves[i].kind {
		case Record, Dict, List: // Empty containers in Lists remain zero.
		case Text, Blob:
			b := append([]byte(nil), v.Bytes()...)
			if len(b) == 0 {
				b = append(b, 1)
			} else {
				b[len(b)/2] ^= 1
			}
			v = lengthBytes(b)
		default:
			v.num++
			if v.num == 0 {
				v.num = 1
			}
		}
		leaves[i].value = v
	}
	return leaves
}
func TestPerfRows(t *testing.T) {
	for name, r := range rowFixtures(t) {
		t.Logf("%s source=%s bytes=%d leaves=%d depth=%d", name, r.id, len(r.raw), len(r.leaves), r.depth)
		for _, l := range r.leaves {
			for _, reader := range []string{"common", "lower", "equal"} {
				if got := perfPath(lengthTrusted(r.raw), l.path, reader); got != l.value {
					t.Fatal(name, reader, "path")
				}
			}
		}
	}
}
func BenchmarkFastShapeReads(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, rows := range []int{1, 1024} {
			b.Run(fmt.Sprintf("%s/rows%d", name, rows), func(b *testing.B) {
				d := newMmapDataset(b, r.raw, rows)
				warmMmapPayloads(d)
				for _, reader := range []string{"common", "lower", "equal"} {
					b.Run(reader, func(b *testing.B) {
						b.ReportAllocs()
						rng := uint64(0x123abcde99887766)
						var sum uint64
						for i := 0; i < b.N; i++ {
							rng ^= rng << 13
							rng ^= rng >> 7
							rng ^= rng << 17
							v := perfPath(lengthTrusted(d.rows[int(rng>>16)&(rows-1)]), r.leaves[int(rng%uint64(len(r.leaves)))].path, reader)
							sum += v.num
						}
						sinkU = sum
						b.ReportMetric(float64(len(r.raw)), "row-B")
					})
				}
			})
		}
	}
}
