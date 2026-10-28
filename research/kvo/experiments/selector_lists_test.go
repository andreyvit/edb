package kvolab

import (
	"fmt"
	"testing"
)

func selectorListFixtures() map[string]Value {
	out := make(map[string]Value)
	for _, name := range []string{"zero", "boolean", "small", "uint16", "mostly-word8", "alternating", "spans"} {
		v := Value{Kind: List}
		for i := 0; i < 128; i++ {
			x := U(0)
			switch name {
			case "boolean":
				x = U(uint64(i & 1))
			case "small":
				x = U(uint64(i))
			case "uint16":
				x = U(uint64(256 + i))
			case "mostly-word8":
				x = U(uint64(1<<56 + i))
				if i == 64 {
					x = U(1)
				}
			case "alternating":
				x = U(uint64(i))
				if i&1 == 1 {
					x = U(uint64(1<<56 + i))
				}
			case "spans":
				x = S(string(make([]byte, i+1)))
			}
			v.Items = append(v.Items, x)
		}
		out[name] = v
	}
	return out
}

func TestSelectorLists(t *testing.T) {
	for name, v := range selectorListFixtures() {
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			w := selectorWriter{mode: mode}
			raw := selectorEncode(&w, nil, v)
			checkSelector(t, v, raw, mode)
			for i, item := range v.Items {
				got := selectorAt(raw, mode, i)
				if item.Kind == Text {
					if string(got.Bytes()) != item.S {
						t.Fatal(name, mode, i)
					}
				} else if got.Uint() != item.U {
					t.Fatal(name, mode, i)
				}
			}
		}
	}
}

func BenchmarkSelectorLists(b *testing.B) {
	all := selectorListFixtures()
	for _, name := range []string{"zero", "boolean", "small", "uint16", "mostly-word8", "alternating", "spans"} {
		v := all[name]
		for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
			w := selectorWriter{mode: mode}
			raw := selectorEncode(&w, nil, v)
			for _, op := range []string{"write", "at", "scan"} {
				b.Run(fmt.Sprintf("%s/%s/%s", name, mode, op), func(b *testing.B) {
					dst := make([]byte, 0, len(raw))
					var sum uint64
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						switch op {
						case "write":
							dst = selectorEncode(&w, dst[:0], v)
						case "at":
							sum += selectorAt(raw, mode, i&127).num
						case "scan":
							it := selectorIter(raw, mode)
							for {
								_, _, x, ok := it.Next()
								if !ok {
									break
								}
								sum += x.num
							}
						}
					}
					sinkU, sinkBytes = sum, dst
					b.ReportMetric(float64(len(raw)), "encoded-B")
				})
			}
		}
	}
}
