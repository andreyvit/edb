package mmap

import (
	"os"

	"golang.org/x/sys/unix"
)

func fdatasync(f *os.File, mapping []byte) error {
	if mapping != nil {
		return unix.Msync(mapping, unix.MS_INVALIDATE)
	} else {
		return f.Sync()
	}
}
