//go:build mips64 || mips64le

package mmap

// MaxSize represents the largest supported mmap size.
const MaxSize = 0x8000000000 // 512GB
