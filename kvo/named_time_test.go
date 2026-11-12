package kvo

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	wire "github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/vmihailenco/msgpack/v5"
)

type migrationTime time.Time

func (v *migrationTime) EncodeMsgpack(e *msgpack.Encoder) error {
	if time.Time(*v).IsZero() {
		return e.EncodeNil()
	}
	return e.EncodeString("named:" + time.Time(*v).Format(time.RFC3339Nano))
}
func (v *migrationTime) DecodeMsgpack(d *msgpack.Decoder) error {
	s, err := d.DecodeString()
	if err != nil {
		return err
	}
	if s == "" {
		*v = migrationTime{}
		return nil
	}
	if !strings.HasPrefix(s, "named:") {
		return fmt.Errorf("missing custom prefix")
	}
	tm, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(s, "named:"))
	*v = migrationTime(tm)
	return err
}

type migrationTimeRow struct {
	At migrationTime `kvo:"at" msgpack:"at"`
}

func TestNamedTimeMigrationEdits(t *testing.T) {
	s := NewSchema()
	at := NewTimeProp(s, 1, "at")
	m := NewModel(s, NewEntityType(s, "timed", reflect.TypeFor[migrationTimeRow]()), func(b *ModelBuilder) { b.Prop(at) })
	p := Project(reflect.TypeFor[migrationTimeRow](), m, nil)
	b := Bind(reflect.TypeFor[migrationTimeRow](), m)
	for _, tm := range []time.Time{{}, time.Date(2021, 4, 5, 6, 7, 8, 123456789, time.UTC)} {
		for _, format := range []string{"native", "MessagePack", "Fast2", "Compact2"} {
			t.Run(fmt.Sprint(tm, "/", format), func(t *testing.T) {
				input := &migrationTimeRow{At: migrationTime(tm)}
				r := p.Record(reflect.ValueOf(input))
				switch format {
				case "MessagePack":
					r = p.MessagePack(wire.Encode(nil, reflect.ValueOf(input)))
				case "Fast2":
					r = b.Encoded(row2.Open(b.Encode(nil, reflect.ValueOf(input).Elem(), row2.Fast2)))
				case "Compact2":
					r = b.Encoded(row2.Open(b.Encode(nil, reflect.ValueOf(input).Elem(), row2.Compact2)))
				}
				if got := r.Get(at); !got.Equal(tm) {
					t.Fatal(got, tm)
				}
				if got := r.Has(at.Code()); got != !tm.IsZero() {
					t.Fatalf("Has=%v, time=%v", got, tm)
				}
				var property migrationTime
				if err := p.TryDecodeProperty(reflect.ValueOf(&property).Elem(), r, at.Code()); err != nil {
					t.Fatal(err)
				}
				if !time.Time(property).Equal(tm) {
					t.Fatal("property decode changed named time")
				}
				e := r.Edit()
				if got := e.Has(at); got != !tm.IsZero() {
					t.Fatalf("Editor.Has=%v, time=%v", got, tm)
				}
				next := time.Unix(123, 456).UTC()
				e.Set(at, next)
				if !e.Get(at).Equal(next) || !e.Has(at) {
					t.Fatal("edit lost precision")
				}
				if format == "native" && !time.Time(input.At).Equal(next) {
					t.Fatal("did not write through")
				}
				var decoded migrationTimeRow
				if format == "MessagePack" {
					if err := wire.TryDecode(e.MessagePack(), reflect.ValueOf(&decoded)); err != nil {
						t.Fatal(err)
					}
				} else {
					b.Decode(e.Record().Snapshot().Raw(), reflect.ValueOf(&decoded).Elem())
				}
				if !time.Time(decoded.At).Equal(next) {
					t.Fatal("decoded wrong named time")
				}
			})
		}
	}
	for _, raw := range [][]byte{{0x80}, {0x81, 0xa2, 'a', 't', 0xc0}} {
		r := p.MessagePack(raw)
		if r.Has(at.Code()) || r.Edit().Has(at) {
			t.Fatal("missing/nil named timestamp is present")
		}
		property := migrationTime(time.Now())
		if err := p.TryDecodeProperty(reflect.ValueOf(&property).Elem(), r, at.Code()); err != nil {
			t.Fatal(err)
		}
		if !time.Time(property).IsZero() {
			t.Fatal("missing/nil property did not clear destination")
		}
	}
	source := string(b.Generate("kvo", "MigrationTimeBinding"))
	if !strings.Contains(source, "out=migrationTime(v.AsTime())") && !strings.Contains(source, "out = migrationTime(v.AsTime())") {
		t.Fatal("generated decoder omitted named conversion")
	}
}
