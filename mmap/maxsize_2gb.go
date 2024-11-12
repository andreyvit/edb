//go:build 386 || arm || ppc

package mmap

// MaxSize represents the largest supported mmap size.
const MaxSize = 0x7FFFFFFF // 2GB
