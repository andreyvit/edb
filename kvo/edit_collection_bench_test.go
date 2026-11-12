package kvo_test

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func BenchmarkDictionaryBatchUpdates(b *testing.B) {
	type row struct {
		Payload string            `kvo:"payload"`
		IDs     map[uint32]string `kvo:"ids"`
	}
	s := kvo.NewSchema()
	ids := kvo.NewMapProp(s, 1, "ids", kvo.MapOf(kvo.TUint32, kvo.TMachineString), values.UnsignedCodec[uint32](), values.StringCodec[string]())
	payload := kvo.NewStringProp[string](s, 2, "payload", kvo.TMachineString)
	m := kvo.NewModel(s, kvo.NewEntityType(s, "MapBatch", reflect.TypeFor[row]()), func(b *kvo.ModelBuilder) { b.Prop(ids); b.Prop(payload) })
	binding := kvo.Bind(reflect.TypeFor[row](), m)
	src := &row{Payload: strings.Repeat("x", 8192), IDs: make(map[uint32]string)}
	for i := range uint32(32) {
		src.IDs[i] = "original"
	}
	var changes [16]string
	for i := range changes {
		changes[i] = "updated-" + strconv.Itoa(i)
	}
	for _, format := range []string{"MessagePack", "Fast2", "Compact2"} {
		var r kvo.Record
		if format == "MessagePack" {
			r = binding.MessagePack(msgpack.Encode(nil, reflect.ValueOf(src)))
		} else {
			layout := row2.Fast2
			if format == "Compact2" {
				layout = row2.Compact2
			}
			r = binding.Encoded(row2.Open(binding.Encode(nil, reflect.ValueOf(src).Elem(), layout)))
		}
		for _, count := range []int{1, 4, 16} {
			for _, serialize := range []string{"once", "each"} {
				b.Run(fmt.Sprintf("%s/%d/%s", format, count, serialize), func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						e := r.Edit()
						m := e.Map(ids)
						for i := range count {
							m.Set(uint32(i%4), changes[i])
							if serialize == "each" {
								if format == "MessagePack" {
									sharedData = e.MessagePack()
								} else {
									sharedData = e.Snapshot().Raw()
								}
							}
						}
						if format == "MessagePack" {
							sharedData = e.MessagePack()
						} else {
							sharedData = e.Snapshot().Raw()
						}
					}
				})
			}
		}
		b.Run(format+"/borrowed", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				m := r.Get(ids)
				n := len(m.Get(7))
				for _, v := range m.Iter() {
					n += len(v)
				}
				if n != 264 {
					b.Fatal(n)
				}
			}
		})
	}
}
