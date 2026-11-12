package fast1

import (
	"github.com/andreyvit/edb/kvo"
	"iter"
)

type Packable interface {
	// Pack returns ImmutableRecordData of this record. Must return nil if
	// called on a nil struct pointer.
	Pack() ImmutableRecordData
}

type AnyFast1Record interface {
	AnyRoot() AnyFast1Map
	Packable
}

type AnyFast1Map interface {
	Type() kvo.AnyType
	IsMissing() bool
	KeyCount() int
	Keys() []uint64
	KeySeq() iter.Seq[uint64]
	Get(key uint64) uint64
	GetAnyMap(key uint64) AnyFast1Map
	Packable() Packable
	Dump() string
}
