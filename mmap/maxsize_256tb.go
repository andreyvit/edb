//go:build amd64 || arm64 || loong64 || ppc64 || ppc64le || riscv64 || s390x

package mmap

// MaxSize represents the largest supported mmap size.
const MaxSize = 0xFFFFFFFFFFFF // 256TB
