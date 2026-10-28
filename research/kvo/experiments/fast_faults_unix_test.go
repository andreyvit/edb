//go:build darwin || linux

package kvolab

import (
	"fmt"
	"syscall"
	"testing"
)

func BenchmarkFastFaults(b *testing.B) {
	source := numeric(128, 0)
	var w lengthWriter
	raw := lengthEncode(&w, nil, source, func(int, Value) bool { return true })
	for _, codec := range []string{"narrow", "wide", "immutable"} {
		for _, warm := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/warm%v", codec, warm), func(b *testing.B) {
				data := raw
				if codec == "wide" {
					data = attrForcedWidth(raw, 8)
				} else if codec == "immutable" {
					data = oldKVO(source).Bytes()
				}
				d := newMmapDataset(b, data, 32768)
				if warm {
					warmMmapPayloads(d)
				}
				var before, after syscall.Rusage
				if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				var sum uint64
				rng := uint64(0xdedabcde12345678)
				mode := "typed-keys"
				if codec == "immutable" {
					mode = "immutable"
				}
				for i := 0; i < b.N; i++ {
					rng ^= rng << 13
					rng ^= rng >> 7
					rng ^= rng << 17
					sum += attrGet(d.rows[int(rng>>16)&32767], rng&127+1, mode)
				}
				b.StopTimer()
				sinkU = sum
				if err := syscall.Getrusage(syscall.RUSAGE_SELF, &after); err != nil {
					b.Fatal(err)
				}
				b.ReportMetric(float64(after.Minflt-before.Minflt)/float64(b.N), "minor-faults/op")
				b.ReportMetric(float64(after.Majflt-before.Majflt)/float64(b.N), "major-faults/op")
			})
		}
	}
}
