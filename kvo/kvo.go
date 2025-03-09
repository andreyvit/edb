// Package kvo (stands for Key Value Objects) manages trees of objects mainly
// represented as key-value pairs.
//
// It has 3 goals:
//
// 1. Manipulation of data objects in a generic way (e.g. handling all integer
// ledger stats with the same code).
//
// 2. A compact binary data representation that can be read from a memory-mapped
// file without deserialization.
//
// 3. A diff/change format that can be used within a transaction (including to
// serve reads) and then quickly merged with the memory-mapped data when doing
// updates.
//
// Potentially we also want to have a diff format that can be persisted
// long-term as part of change history. This might or might not match #3.
package kvo

import "iter"

type Packable interface {
	// Pack returns ImmutableRecordData of this record. Must return nil if
	// called on a nil struct pointer.
	Pack() ImmutableRecordData
}

type AnyRecord interface {
	AnyRoot() AnyMap
	Packable
}

type AnyMap interface {
	Type() AnyType
	IsMissing() bool
	KeyCount() int
	Keys() []uint64
	KeySeq() iter.Seq[uint64]
	Get(key uint64) uint64
	GetAnyMap(key uint64) AnyMap
	Packable() Packable
	Dump() string
}

type FmtContext struct {
	// data map[any]any
}
