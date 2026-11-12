package kvo_test

import (
	"reflect"
	"strconv"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// This is the original proposed returned-by-value cursor, retained as a control.
type largeStringCursor struct {
	raw       kvo.ListValueIterator
	codec     *values.Codec[string]
	remaining int
}

func (c *largeStringCursor) Next() (string, bool) {
	v, ok := c.raw.Next()
	if !ok {
		c.remaining = 0
		return "", false
	}
	c.remaining--
	return c.codec.Decode(v), true
}
func largeStringGet(r kvo.Record, key uint64, codec *values.Codec[string]) largeStringCursor {
	l := r.ListValue(key)
	return largeStringCursor{l.Iter(), codec, l.Len()}
}

type typedListRow struct {
	Tags []string `kvo:"tags" msgpack:"t"`
}

var typedListSink kvo.List[string]
var largeCursorSink largeStringCursor

func BenchmarkTypedList(b *testing.B) {
	s := kvo.NewSchema()
	typ := kvo.NewEntityType(s, "typedListBench", reflect.TypeFor[typedListRow]())
	tags := kvo.NewListProp(s, 3, "tags", kvo.ListOf(kvo.TMachineString), values.StringCodec[string]())
	model := kvo.NewModel(s, typ, func(m *kvo.ModelBuilder) { m.Prop(tags) })
	binding := kvo.Bind(reflect.TypeFor[typedListRow](), model)
	codec := values.StringCodec[string]()
	for _, n := range []int{0, 3, 32} {
		b.Run(fmtSize(n), func(b *testing.B) {
			row := &typedListRow{Tags: make([]string, n)}
			for i := range row.Tags {
				row.Tags[i] = fmtSize(i)
			}
			fast := binding.Encoded(row2.Trusted(binding.Encode(nil, reflect.ValueOf(row).Elem(), row2.Fast2)))
			cases := []struct {
				name string
				r    kvo.Record
			}{
				{"Fast2", fast},
				{"Compact2", binding.Encoded(row2.Trusted(binding.Encode(nil, reflect.ValueOf(row).Elem(), row2.Compact2)))},
				{"MessagePack", binding.MessagePack(msgpack.Encode(nil, reflect.ValueOf(row)))},
				{"Native", binding.Record(reflect.ValueOf(row))},
				{"Memory", kvo.Encoded(fast.Snapshot()).WithModel(model).Fastest()},
			}
			for _, f := range cases {
				b.Run(f.name, func(b *testing.B) {
					_ = f.r.Get(tags).Len()
					b.Run("CellHandle", func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							typedListSink = f.r.Get(tags)
						}
					})
					b.Run("LargeHandle", func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							largeCursorSink = largeStringGet(f.r, tags.Code(), &codec)
						}
					})
					b.Run("Raw", func(b *testing.B) {
						sum := 0
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							it := f.r.ListValue(tags.Code()).Iter()
							for {
								v, ok := it.Next()
								if !ok {
									break
								}
								sum += len(v.AsString())
							}
						}
						accessSum = uint64(sum)
					})
					b.Run("LargeCursor", func(b *testing.B) {
						sum := 0
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							it := largeStringGet(f.r, tags.Code(), &codec)
							for {
								v, ok := it.Next()
								if !ok {
									break
								}
								sum += len(v)
							}
						}
						accessSum = uint64(sum)
					})
					b.Run("CellCursor", func(b *testing.B) {
						sum := 0
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							it := f.r.Get(tags).Cursor()
							for {
								v, ok := it.Next()
								if !ok {
									break
								}
								sum += len(v)
							}
						}
						accessSum = uint64(sum)
					})
					b.Run("CellRange", func(b *testing.B) {
						sum := 0
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							for v := range f.r.Get(tags).Iter() {
								sum += len(v)
							}
						}
						accessSum = uint64(sum)
					})
				})
			}
		})
	}
}
func fmtSize(n int) string { return strconv.Itoa(n) }
func TestTypedCollectionSize(t *testing.T) {
	if reflect.TypeFor[kvo.List[string]]().ConvertibleTo(reflect.TypeFor[kvo.List[int]]()) {
		t.Fatal("collection element types can be cast away")
	}

	if unsafe.Sizeof(kvo.List[string]{}) != 16 || unsafe.Sizeof(kvo.RecordList{}) != 16 {
		t.Fatal("collection handle exceeds one cell")
	}
	t.Logf("list=%d object-list=%d cursor=%d old-returned-cursor=%d", unsafe.Sizeof(kvo.List[string]{}), unsafe.Sizeof(kvo.RecordList{}), unsafe.Sizeof(kvo.Cursor[string]{}), unsafe.Sizeof(largeStringCursor{}))
}
