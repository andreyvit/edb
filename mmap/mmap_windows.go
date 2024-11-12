package mmap

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

func mmap(file *os.File, size int, opt Options) ([]byte, error) {
	var sizelo, sizehi uint32

	prot := uint32(syscall.PAGE_READONLY)

	if opt.Has(Writable) {
		if err := file.Truncate(int64(size)); err != nil {
			return nil, fmt.Errorf("truncate: %s", err)
		}
		sizehi = uint32(uint64(size) >> 32)
		sizelo = uint32(uint64(size))
		prot = syscall.PAGE_READWRITE
	}

	h, errno := syscall.CreateFileMapping(syscall.Handle(file.Fd()), nil, prot, sizehi, sizelo, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, 0)
	if addr == 0 {
		_ = syscall.CloseHandle(h)
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	if err := syscall.CloseHandle(h); err != nil {
		_ = syscall.UnmapViewOfFile(addr)
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	return unsafe.Slice((*byte)(unsafe.Pointer(addr)), size), nil
}

func munmap(b []byte) error {
	addr := (uintptr)(unsafe.Pointer(&b[0]))
	err := syscall.UnmapViewOfFile(addr)
	if err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
