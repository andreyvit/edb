//go:build windows || (unix && !plan9 && !linux && !openbsd)

package mmap

import "os"

func fdatasync(f *os.File, _ []byte) error {
	return f.Sync()
}
