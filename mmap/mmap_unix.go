//go:build unix

package mmap

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func mmap(f *os.File, size int, opt Options) ([]byte, error) {
	prot := syscall.PROT_READ
	if opt.Has(Writable) {
		prot |= syscall.PROT_WRITE
	}

	flags := syscall.MAP_SHARED
	if opt.Has(Prefault) {
		flags |= MAP_POPULATE
	}

	b, err := unix.Mmap(int(f.Fd()), 0, size, prot, flags)
	if err != nil {
		return nil, err
	}

	if opt.Has(SequentialAccess) {
		err = unix.Madvise(b, syscall.MADV_SEQUENTIAL)
		if err != nil && !errors.Is(err, syscall.ENOSYS) {
			// Ignore not implemented error in kernel because it still works.
			return nil, errors.Join(fmt.Errorf("madvise(MADV_SEQUENTIAL): %w", err), unix.Munmap(b))
		}
	} else if opt.Has(RandomAccess) {
		err = unix.Madvise(b, syscall.MADV_RANDOM)
		if err != nil && !errors.Is(err, syscall.ENOSYS) {
			// Ignore not implemented error in kernel because it still works.
			return nil, errors.Join(fmt.Errorf("madvise(MADV_RANDOM): %w", err), unix.Munmap(b))
		}
	}

	return b, nil
}

func munmap(b []byte) error {
	return unix.Munmap(b)
}
