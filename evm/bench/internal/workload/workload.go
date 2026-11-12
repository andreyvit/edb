// Package workload contains shared fixtures for the EVM and Lua benchmarks.
package workload

import (
	_ "embed"
	"fmt"
	"strings"
	"testing"
)

//go:embed testdata/guest.go
var GuestGo string

//go:embed testdata/guest.lua
var Lua string

//go:embed testdata/expected.txt
var expected string

type Sensors struct{ Reading, Limit [256]int }

func NewSensors() *Sensors {
	s := new(Sensors)
	for i := range s.Reading {
		s.Reading[i] = (i*73 + 19) % 211
		s.Limit[i] = 90 + i*17%67
	}
	return s
}

type Runner func(int) (int, error)
type Setup func(testing.TB, string) Runner

func answers(t testing.TB, name string) [4]int {
	t.Helper()
	var out [4]int
	for i := range out {
		out[i] = -1
	}
	for _, row := range strings.Split(strings.TrimSpace(expected), "\n") {
		var entry string
		var seed, answer int
		if _, err := fmt.Sscan(row, &entry, &seed, &answer); err != nil {
			t.Fatal(err)
		}
		if entry == name {
			out[seed] = answer
		}
	}
	for _, answer := range out {
		if answer < 0 {
			t.Fatalf("missing answers for %s", name)
		}
	}
	return out
}

func check(t testing.TB, run Runner, seed, want int) {
	t.Helper()
	got, err := run(seed)
	if err != nil || got != want {
		t.Fatalf("seed %d: got %d, %v; want %d", seed, got, err, want)
	}
}

var names = []string{"Price", "Scan", "Fib", "Sieve", "Mandelbrot"}

func Verify(t *testing.T, setup Setup) {
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			run := setup(t, name)
			for seed, want := range answers(t, name) {
				check(t, run, seed, want)
			}
		})
	}
}

func Benchmark(b *testing.B, setup Setup) {
	for _, name := range names {
		BenchmarkEntry(b, name, setup)
	}
}

func BenchmarkEntry(b *testing.B, name string, setup Setup) {
	b.Run(name, func(b *testing.B) {
		run := setup(b, name)
		want := answers(b, name)
		for i := range 32 {
			check(b, run, i%4, want[i%4])
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			seed := i % 4
			got, err := run(seed)
			if err != nil || got != want[seed] {
				b.Fatalf("seed %d: got %d, %v; want %d", seed, got, err, want[seed])
			}
		}
	})
}
