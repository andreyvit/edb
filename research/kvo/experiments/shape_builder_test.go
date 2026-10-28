package kvolab

// A prepared shape makes leaf slots explicit. Dictionary keys and List lengths
// belong to this plan; changing them invalidates it. This is the optional dense
// decoded/bound control, not a replacement for a general row editor.
import (
	"testing"
	"unsafe"
)

type shapeEvent struct {
	key        uint64
	text       string
	kind, slot int
}
type shapeBuilder struct {
	events           []shapeEvent
	values, original []lengthValue
}

func prepareShape(r perfRow) shapeBuilder {
	var b shapeBuilder
	var walk func(uint64, string, Value, lengthValue)
	walk = func(key uint64, text string, x Value, value lengthValue) {
		if x.Kind == Record || x.Kind == Dict || x.Kind == List {
			kind := int(tagContainerKind(x.Kind))
			b.events = append(b.events, shapeEvent{key, text, kind, -1})
			view := value.Object()
			add := func(k uint64, s string, c Value) {
				if kind != 2 && tagZero(c) {
					return
				}
				v := perfChild(view, perfStep{kind, k, s}, "common")
				if v.IsZero() {
					slot := len(b.values)
					b.values = append(b.values, v)
					b.events = append(b.events, shapeEvent{k, s, 3, slot})
				} else {
					walk(k, s, c, v)
				}
			}
			switch x.Kind {
			case Record:
				for _, f := range x.Fields {
					add(f.Key, "", f.Value)
				}
			case Dict:
				for _, e := range x.Entries {
					add(0, e.Key, e.Value)
				}
			case List:
				for i, e := range x.Items {
					add(uint64(i), "", e)
				}
			}
			b.events = append(b.events, shapeEvent{kind: 4})
			return
		}
		slot := len(b.values)
		b.values = append(b.values, value)
		b.events = append(b.events, shapeEvent{key, text, 3, slot})
	}
	walk(0, "", r.source, lengthBytes(r.raw))
	b.original = append([]lengthValue(nil), b.values...)
	return b
}
func (b *shapeBuilder) Reset(empty bool) {
	if empty {
		clear(b.values)
	} else {
		copy(b.values, b.original)
	}
}
func (b *shapeBuilder) Set(slot int, v lengthValue) { b.values[slot] = v }
func (b *shapeBuilder) Append(w *lengthWriter, dst []byte) []byte {
	w.Reset(dst)
	for _, e := range b.events {
		switch e.kind {
		case 0, 1, 2:
			w.Begin(e.key, e.text, e.kind, true)
		case 3:
			w.Put(e.key, e.text, b.values[e.slot])
		case 4:
			w.End()
		}
	}
	return w.BytesWritten()
}
func boundShapeEditsPrevious(r perfRow, edits []perfLeaf) []int {
	slots := make([]int, len(edits))
	for i, e := range edits {
		slots[i] = -1
		for j, l := range r.leaves {
			if samePerfPath(e.path, l.path) {
				slots[i] = j
				break
			}
		}
		if slots[i] < 0 {
			panic("unknown bound path")
		}
	}
	return slots
}

// Canonically ordered leaf catalogs need no auxiliary index. Keep the old
// all-leaves scan below as an explicitly named historical comparison.
func boundShapeEdits(r perfRow, edits []perfLeaf) []int {
	return bindShapeBinary(make([]int, 0, len(edits)), r.leaves, edits)
}

// Deliberately measure the simple reference path-to-slot binding utility too.
// It is quadratic and excluded from already-bound application timings; those
// timings must never be presented as the cost of arbitrary dynamic path input.
func BenchmarkShapeBind(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		edits := rowEdits(r, "wide70")
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				slots := boundShapeEditsPrevious(r, edits)
				sinkU = uint64(slots[len(slots)-1])
			}
		})
	}
}
func TestShapeBuilder(t *testing.T) {
	for name, r := range rowFixtures(t) {
		for _, pattern := range []string{"wide70", "empty-shuffled"} {
			edits := rowEdits(r, pattern)
			slots := boundShapeEdits(r, edits)
			b := prepareShape(r)
			if len(b.values) != len(r.leaves) {
				t.Fatal("slots")
			}
			b.Reset(pattern == "empty-shuffled")
			for i, e := range edits {
				b.Set(slots[i], e.value)
			}
			var w lengthWriter
			out := b.Append(&w, nil)
			for _, e := range edits {
				if !equalLengthValue(perfPath(lengthTrusted(out), e.path, "common"), e.value) {
					t.Fatal(name, "bound shape")
				}
			}
		}
	}
}
func BenchmarkShapeBuilder(b *testing.B) {
	for _, name := range []string{"row-small", "row-wide", "row-deep", "row-span-heavy"} {
		r := rowFixtures(b)[name]
		for _, pattern := range []string{"wide70", "empty-shuffled", "prepare"} {
			b.Run(name+"/"+pattern, func(b *testing.B) {
				if pattern == "prepare" {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						p := prepareShape(r)
						sinkU = uint64(len(p.values))
					}
					return
				}
				edits := rowEdits(r, pattern)
				slots := boundShapeEdits(r, edits)
				p := prepareShape(r)
				var w lengthWriter
				var out []byte
				run := func() {
					p.Reset(pattern == "empty-shuffled")
					for i, e := range edits {
						p.Set(slots[i], e.value)
					}
					out = p.Append(&w, out[:0])
				}
				run()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					run()
				}
				b.StopTimer()
				sinkBytes = out
				b.ReportMetric(float64(cap(p.events)*int(unsafe.Sizeof(shapeEvent{}))), "binding-B")
				b.ReportMetric(float64(cap(p.values)*16+cap(p.original)*16), "values-B")
			})
		}
	}
}
