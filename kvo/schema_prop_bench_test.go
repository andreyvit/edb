package kvo

import (
	"fmt"
	"testing"
)

// Match the code-only scalar references without importing record (which itself
// depends on model). Codes exceed the runtime's cached small-interface integers.
type benchmarkPropRef[T any] struct {
	_    [0]*T
	code uint64
}

func (p benchmarkPropRef[T]) Code() uint64 { return p.code }

func (b *ModelBuilder) benchmarkInterfaceProp(p PropertyCode, opts ...any) {
	b.addProp(p.Code(), opts...)
}

var benchmarkModelSink *Model

func BenchmarkModelPropertyRegistration(b *testing.B) {
	s := NewSchema()
	var codes [16]benchmarkPropRef[int64]
	var declarations [16]*Prop
	for i := range codes {
		code := uint64(1000 + i)
		NewProp(s, code, fmt.Sprintf("customer:field%d", i), TInt64, nil)
		codes[i] = benchmarkPropRef[int64]{code: code}
		declarations[i] = s.MustPropByCode(code).(*Prop)
	}
	build := func() *ModelBuilder {
		return &ModelBuilder{model: &Model{schema: s, propsByCode: make(map[PropCode]*PropInstance), propsByName: make(map[string]*PropInstance)}}
	}
	b.Run("TypedCode/Interface", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := build()
			for _, p := range codes {
				builder.benchmarkInterfaceProp(p, PropRequired)
			}
			benchmarkModelSink = builder.model
		}
	})
	b.Run("TypedCode/Generic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := build()
			for _, p := range codes {
				builder.Prop(p, PropRequired)
			}
			benchmarkModelSink = builder.model
		}
	})
	b.Run("Declaration/Interface", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := build()
			for _, p := range declarations {
				builder.benchmarkInterfaceProp(p, PropRequired)
			}
			benchmarkModelSink = builder.model
		}
	})
	b.Run("Declaration/Generic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			builder := build()
			for _, p := range declarations {
				builder.Prop(p, PropRequired)
			}
			benchmarkModelSink = builder.model
		}
	})
}
