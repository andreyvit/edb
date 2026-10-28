package kvolab

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"
)

type perfContainer struct {
	id      string
	source  Value
	entries int
	raw     []byte
}

var perfContainersOnce sync.Once
var perfContainers map[string]perfContainer

func batchFixtures(tb testing.TB) map[string]perfContainer {
	all := workloadFixtures(tb)
	perfContainersOnce.Do(func() {
		perfContainers = map[string]perfContainer{}
		type target struct {
			name string
			kind Kind
			n    int
		}
		targets := []target{{"object8", Record, 8}, {"object64", Record, 64}, {"object128", Record, 128}, {"dictionary8", Dict, 8}, {"dictionary64", Dict, 64}, {"dictionary-largest", Dict, 256}}
		best := map[string]int{}
		for _, x := range targets {
			best[x.name] = math.MaxInt
		}
		names := make([]string, 0, len(all))
		for n := range all {
			names = append(names, n)
		}
		sort.Strings(names)
		var walk func(string, Value)
		walk = func(id string, v Value) {
			count := 0
			switch v.Kind {
			case Record:
				for _, f := range v.Fields {
					if !tagZero(f.Value) {
						count++
					}
				}
			case Dict:
				for _, e := range v.Entries {
					if !tagZero(e.Value) {
						count++
					}
				}
			}
			for _, x := range targets {
				if v.Kind == x.kind && count > 0 {
					dist := count - x.n
					if dist < 0 {
						dist = -dist
					}
					if dist < best[x.name] {
						best[x.name] = dist
						perfContainers[x.name] = perfContainer{id: id, source: v, entries: count}
					}
				}
			}
			switch v.Kind {
			case Record:
				for i, f := range v.Fields {
					walk(fmt.Sprintf("%s/f%d", id, i), f.Value)
				}
			case Dict:
				for i, e := range v.Entries {
					walk(fmt.Sprintf("%s/d%d", id, i), e.Value)
				}
			case List:
				for i, e := range v.Items {
					walk(fmt.Sprintf("%s/l%d", id, i), e)
				}
			}
		}
		for _, n := range names {
			walk(n, all[n])
		}
		for name, c := range perfContainers {
			var w lengthWriter
			c.raw = lengthEncode(&w, nil, c.source, func(int, Value) bool { return true })
			perfContainers[name] = c
		}
	})
	return perfContainers
}

type perfEdit struct {
	key   uint64
	text  string
	value lengthValue
}

func batchEdits(c perfContainer, pattern string) []perfEdit {
	it := lengthTrusted(c.raw).Iter()
	var all []perfEdit
	for {
		k, text, v, ok := it.Next()
		if !ok {
			break
		}
		if v.ptr == nil {
			word := v.num + 1
			if word == 0 {
				word = 1
			}
			v = lengthWord(word)
		}
		// A whole child is already prepared and assigned opaquely; it stays valid.
		// Span resizing and deep leaf changes are measured separately.
		all = append(all, perfEdit{k, string(text), v})
	}
	if pattern != "empty-sorted" {
		rng := uint64(0x7e1b652ba019aacd)
		for i := len(all) - 1; i > 0; i-- {
			rng ^= rng << 13
			rng ^= rng >> 7
			rng ^= rng << 17
			j := int(rng % uint64(i+1))
			all[i], all[j] = all[j], all[i]
		}
	}
	switch pattern {
	case "few4", "repeat4":
		all = all[:min(4, len(all))]
	case "wide70", "interleave70":
		all = all[:max(1, len(all)*7/10)]
	}
	if pattern == "repeat4" {
		short := all
		all = nil
		for i := 0; i < 8; i++ {
			all = append(all, short...)
		}
	}
	return all
}
func TestBatchFixtures(t *testing.T) {
	names := make([]string, 0, len(batchFixtures(t)))
	for n := range batchFixtures(t) {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		c := batchFixtures(t)[name]
		t.Logf("%s source=%s entries=%d bytes=%d", name, c.id, c.entries, len(c.raw))
		for _, pattern := range []string{"few4", "wide70", "empty-shuffled", "empty-sorted", "interleave70", "repeat4"} {
			edits := batchEdits(c, pattern)
			base := lengthTrusted(c.raw)
			if pattern == "empty-shuffled" || pattern == "empty-sorted" {
				base = lengthView{}
			}
			var want []byte
			for _, mode := range []string{"sorted", "append", "lazy", "upsert", "hash"} {
				var e batchEditor
				e.Reset(base, lengthTrusted(c.raw).Kind(), mode)
				for _, m := range edits {
					e.Set(m.key, m.text, m.value, false)
					if pattern == "interleave70" {
						_ = e.Get(m.key, m.text)
					}
				}
				var w lengthWriter
				out := e.Append(&w, nil, true)
				if mode == "sorted" {
					want = bytes.Clone(out)
				} else if !bytes.Equal(out, want) {
					t.Fatal(name, pattern, mode, "different output")
				}
			}
		}
	}
}
func BenchmarkBatchOverlay(b *testing.B) {
	names := []string{"object8", "object64", "object128", "dictionary8", "dictionary64", "dictionary-largest"}
	for _, name := range names {
		c := batchFixtures(b)[name]
		for _, pattern := range []string{"few4", "wide70", "empty-shuffled", "empty-sorted", "interleave70", "repeat4"} {
			edits := batchEdits(c, pattern)
			base := lengthTrusted(c.raw)
			kind := base.Kind()
			if pattern == "empty-shuffled" || pattern == "empty-sorted" {
				base = lengthView{}
			}
			for _, mode := range []string{"sorted", "append", "lazy", "upsert", "hash"} {
				for _, op := range []string{"stage", "stage-read", "finish"} {
					// Interleaved reads are already part of staging in this scenario.
					if pattern == "interleave70" && op == "stage-read" {
						continue
					}
					b.Run(name+"/"+pattern+"/"+mode+"/"+op, func(b *testing.B) {
						var e batchEditor
						var w lengthWriter
						var out []byte
						run := func() {
							e.Reset(base, kind, mode)
							for _, m := range edits {
								e.Set(m.key, m.text, m.value, false)
								if pattern == "interleave70" {
									sinkValue = e.Get(m.key, m.text)
								}
							}
							if op == "stage-read" {
								m := edits[0]
								sinkValue = e.Get(m.key, m.text)
							}
							if op == "finish" {
								out = e.Append(&w, out[:0], true)
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
						sinkU = uint64(len(e.changes) + len(e.storage.edits))
						b.ReportMetric(float64(e.ScratchBytes()), "scratch-B")
						b.ReportMetric(float64(len(edits)), "edits/op")
						b.ReportMetric(float64(len(c.raw)), "base-B")
					})
				}
			}
		}
	}
}
func BenchmarkBatchCold(b *testing.B) {
	for _, name := range []string{"object64", "dictionary64"} {
		c := batchFixtures(b)[name]
		base := lengthTrusted(c.raw)
		for _, pattern := range []string{"few4", "wide70"} {
			edits := batchEdits(c, pattern)
			for _, mode := range []string{"sorted", "append", "lazy", "upsert", "hash"} {
				b.Run(name+"/"+pattern+"/"+mode, func(b *testing.B) {
					b.ReportAllocs()
					var out []byte
					for i := 0; i < b.N; i++ {
						var e batchEditor
						var w lengthWriter
						e.Reset(base, base.Kind(), mode)
						for _, m := range edits {
							e.Set(m.key, m.text, m.value, false)
						}
						out = e.Append(&w, nil, true)
					}
					sinkBytes = out
				})
			}
		}
	}
}

var sinkValue lengthValue
