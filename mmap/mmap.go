package mmap

import (
	"os"
)

type Options uint

const (
	// Writable opens the file for writing (otherwise, it's opened read-only).
	Writable Options = 1 << 0

	// SequentialAccess is a hint requesting aggressive read-ahead.
	// Incompatible with RandomAccess. Maps to MADV_SEQUENTIAL on Unix.
	SequentialAccess Options = 1 << 1

	// RandomAccess is a hint that read ahead is less useful than normally.
	// Incompatible with SequentialAccess. Maps to MADV_RANDOM on Unix.
	RandomAccess Options = 1 << 2

	// Prefault is a hint requesting the entire file to be loaded in memory
	// for fastest access. Maps to MAP_POPULATE on Linux.
	Prefault Options = 1 << 3
)

func (o Options) Has(v Options) bool {
	return o&v != 0
}

// mmap memory maps a DB's data file.
func Mmap(f *os.File, offset, size int, opt Options) ([]byte, error) {
	if offset != 0 {
		panic("non-zero offset not yet supported")
	}
	return mmap(f, size, opt)
}

// Munmap unmaps the given slice from memory. The slice must have been returned
// by Mmap.
func Munmap(b []byte) error {
	return munmap(b)
}
