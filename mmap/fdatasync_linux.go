package mmap

import (
	"os"
	"syscall"
)

func fdatasync(f *os.File, _ []byte) error {
	return syscall.Fdatasync(int(f.Fd()))
}
