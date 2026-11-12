package evmcompiler_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

type listBenchRow struct {
	Tags []string `kvo:"tags" msgpack:"t"`
}

func BenchmarkRecordListCell(b *testing.B) {
	s := kvo.NewSchema()
	entity := kvo.NewEntityType(s, "listbench", reflect.TypeFor[listBenchRow]())
	kvo.NewProp(s, 1, "tags", kvo.ListOf(kvo.TMachineString), nil)
	model := kvo.NewModel(s, entity, func(m *kvo.ModelBuilder) { m.Prop(s.MustPropByCode(1)) })
	binding := kvo.Bind(reflect.TypeFor[listBenchRow](), model)
	pkg := evm.Package{Path: "db", Records: []*evm.RecordType{evm.NewRecordType("Customer", model, false)}}
	mod, err := evmcompiler.Compile("listbench.go", `import "db"
func Range(c *db.Customer, n int) int {
 sum:=0
 for i:=0;i<n;i++ {for _,tag:=range c.tags {if tag=="hit" {sum++}}}
 return sum
}
func Len(c *db.Customer,n int) int {sum:=0;for i:=0;i<n;i++ {sum+=len(c.tags)};return sum}
func Index(c *db.Customer,n int) int {sum:=0;for i:=0;i<n;i++ {if c.tags[0]=="hit" {sum++}};return sum}
func Reverse(c *db.Customer,n int) int {
 sum:=0
 for i:=0;i<n;i++ {tags:=c.tags;for j:=len(tags)-1;j>=0;j-- {if tags[j]=="hit" {sum++}}}
 return sum
}
`, pkg)
	if err != nil {
		b.Fatal(err)
	}
	prog, err := evm.Link(mod, pkg)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	for _, size := range []int{3, 32} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			rows := make([]*listBenchRow, 64)
			formats := []struct {
				name string
				rows []kvo.Record
			}{{name: "Fast2"}, {name: "Compact2"}, {name: "MessagePack"}, {name: "Native"}, {name: "Memory"}}
			for i := range rows {
				row := &listBenchRow{Tags: make([]string, size)}
				for j := range row.Tags {
					if j%3 == 0 {
						row.Tags[j] = "hit"
					} else {
						row.Tags[j] = fmt.Sprintf("tag-%d-%d", i, j)
					}
				}
				rows[i] = row
				fast := binding.Encoded(row2.Trusted(binding.Encode(nil, reflect.ValueOf(row).Elem(), row2.Fast2)))
				for j, r := range []kvo.Record{fast, binding.Encoded(row2.Trusted(binding.Encode(nil, reflect.ValueOf(row).Elem(), row2.Compact2))), binding.MessagePack(msgpack.Encode(nil, reflect.ValueOf(row))), binding.Record(reflect.ValueOf(row)), kvo.Encoded(fast.Snapshot()).WithModel(model).Fastest()} {
					formats[j].rows = append(formats[j].rows, r)
				}
			}
			for _, format := range formats {
				b.Run(format.name, func(b *testing.B) {
					for _, fn := range []string{"Range", "Len", "Index", "Reverse"} {
						b.Run(fn, func(b *testing.B) {
							vm := evm.New(prog)
							const repeats = 16
							args := [2]values.Value{{}, values.Int(repeats)}
							var out [1]values.Value
							want := repeats * ((size + 2) / 3)
							if fn == "Len" {
								want = repeats * size
							}
							if fn == "Index" {
								want = repeats
							}
							run := func(i int) {
								args[0] = format.rows[i%len(format.rows)].Value()
								if err := vm.Run(ctx, fn, args[:], out[:]); err != nil {
									b.Fatal(err)
								}
								if out[0].AsInt() != int64(want) {
									b.Fatal(fn, out[0].AsInt(), want)
								}
							}
							for i := range format.rows {
								run(i)
							}
							b.ReportAllocs()
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								run(i)
							}
						})
					}
				})
			}
		})
	}
}
