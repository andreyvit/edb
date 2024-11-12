//go:build mips || mipsle

package mmap

// MaxSize represents the largest supported mmap size.
const MaxSize = 0x40000000 // 1GB
