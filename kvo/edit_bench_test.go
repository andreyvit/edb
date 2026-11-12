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

func BenchmarkRecordBatchUpdates(b *testing.B) {
	type row struct {
		Payload string `kvo:"payload"`
		A       string `kvo:"a"`
		B       string `kvo:"b"`
		C       string `kvo:"c"`
		D       string `kvo:"d"`
	}
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "Batch", reflect.TypeFor[row]())
	for i, name := range []string{"payload", "a", "b", "c", "d"} {
		kvo.NewProp(s, uint64(i+1), name, kvo.TMachineString, nil)
	}
	m := kvo.NewModel(s, typ, func(b *kvo.ModelBuilder) {
		for i := uint64(1); i <= 5; i++ {
			b.Prop(s.MustPropByCode(i))
		}
	})
	binding := kvo.Bind(reflect.TypeFor[row](), m)
	src := &row{Payload: strings.Repeat("x", 8192), A: "before", B: "before", C: "before", D: "before"}
	mp := binding.MessagePack(msgpack.Encode(nil, reflect.ValueOf(src)))
	var changes [16]values.Value
	for i := range changes {
		changes[i] = values.String("updated-" + strconv.Itoa(i))
	}
	cases := []struct {
		name string
		r    kvo.Record
	}{{"MessagePack", mp}}
	for _, layout := range []row2.Layout{row2.Fast2, row2.Compact2} {
		cases = append(cases, struct {
			name string
			r    kvo.Record
		}{layout.String(), binding.Encoded(row2.Open(binding.Encode(nil, reflect.ValueOf(src).Elem(), layout)))})
	}
	for _, c := range cases {
		for _, count := range []int{1, 4, 16} {
			b.Run(fmt.Sprintf("%s/%d", c.name, count), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					e := c.r.Edit()
					for i := range count {
						e.SetValue(uint64(2+i%4), changes[i])
					}
					if c.name == "MessagePack" {
						sharedData = e.MessagePack()
					} else {
						sharedData = e.Snapshot().Raw()
					}
				}
			})
		}
	}
}
