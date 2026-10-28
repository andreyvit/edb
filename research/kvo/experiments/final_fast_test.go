package kvolab

import (
	"fmt"
	"testing"
)

// Final comparison uses identical query streams, matching numeric keys and full
// payload warmup for every codec, including Immutable. Cached roots are reported
// separately; all other variants pay root opening inside the timed loop.
func BenchmarkFinalFast(b *testing.B) {
	for _, fixture := range []string{"local128", "global128", "nested"} {
		source := mmapFixture(fixture)
		for _, rows := range []int{1, 32768} {
			for _, mode := range []string{"common", "fused-equal", "packed-open", "packed-cache", "front", "unified", "immutable"} {
				b.Run(fmt.Sprintf("%s/rows%d/%s", fixture, rows, mode), func(b *testing.B) {
					codec := "indexed-all"
					if mode == "immutable" {
						codec = mode
					}
					raw := mmapEncode(source, codec)
					if mode == "front" {
						var fw frontWriter
						raw = frontEncode(&fw, nil, source)
					}
					if mode == "unified" {
						var uw unifiedWriter
						raw = unifiedEncode(&uw, nil, source, func(int, Value) bool { return true })
					}
					d := newMmapDataset(b, raw, rows)
					warmMmapPayloads(d)
					var cache []fastPacked
					if mode == "packed-cache" {
						cache = make([]fastPacked, rows)
						for i, r := range d.rows {
							cache[i] = preparePacked(lengthTrusted(r))
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					var sum uint64
					rng := uint64(0xdedabcde12345678)
					for i := 0; i < b.N; i++ {
						rng ^= rng << 13
						rng ^= rng >> 7
						rng ^= rng << 17
						row := int(rng>>16) & (rows - 1)
						r := d.rows[row]
						if mode == "common" || mode == "immutable" {
							sum += mmapRead(r, codec, fixture, rng)
							continue
						}
						key := rng&127 + 1
						if fixture == "global128" {
							key += 1 << 50
						}
						if fixture == "nested" {
							key = rng&7 + 1
						}
						var v lengthValue
						switch mode {
						case "unified":
							v = unifiedTrusted(r).Get(key)
						case "front":
							v = frontGet(lengthTrusted(r), key, true)
						case "fused-equal":
							v = perfGet(lengthTrusted(r), key, true)
						case "packed-open":
							v = preparePacked(lengthTrusted(r)).Get(key)
						case "packed-cache":
							v = cache[row].Get(key)
						}
						if fixture == "nested" {
							childKey := rng>>3&15 + 1
							if mode == "unified" {
								v = unifiedTrusted(v.Bytes()).Get(childKey)
							} else if mode == "front" {
								v = frontGet(v.Object(), childKey, true)
							} else if mode == "fused-equal" {
								v = perfGet(v.Object(), childKey, true)
							} else {
								v = preparePacked(v.Object()).Get(childKey)
							}
						}
						sum += v.Uint()
					}
					b.StopTimer()
					sinkU = sum
					b.ReportMetric(float64(len(raw)), "row-B")
					if mode == "packed-cache" {
						b.ReportMetric(float64(16*rows), "cache-B")
					}
				})
			}
		}
	}
}
