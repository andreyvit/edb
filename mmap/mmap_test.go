package mmap

import (
	"os"
	"testing"
)

func TestOptionsHas(t *testing.T) {
	var o Options = Writable | Prefault
	if !o.Has(Writable) || o.Has(SequentialAccess) {
		t.Fatalf("Options.Has returned unexpected results for %v", o)
	}
}

func TestMmapAndMunmap(t *testing.T) {
	f := must(os.CreateTemp("", "mmap_test_*"))
	defer os.Remove(f.Name())
	defer f.Close()

	const size = 4096
	if err := f.Truncate(size); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	b, err := Mmap(f, 0, size, Writable)
	if err != nil {
		t.Fatalf("Mmap: %v", err)
	}
	if len(b) != size {
		t.Fatalf("len(mmap) = %d, wanted %d", len(b), size)
	}
	b[0] = 0x42
	if err := Fdatasync(f, b); err != nil {
		t.Fatalf("Fdatasync: %v", err)
	}
	if err := Munmap(b); err != nil {
		t.Fatalf("Munmap: %v", err)
	}
}

func TestMmap_PanicsOnNonZeroOffset(t *testing.T) {
	f := must(os.CreateTemp("", "mmap_test_*"))
	defer os.Remove(f.Name())
	defer f.Close()

	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic")
		}
	}()
	_, _ = Mmap(f, 1, 1, 0)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

