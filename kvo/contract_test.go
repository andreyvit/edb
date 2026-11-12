package kvo_test

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/nativebind"
	"github.com/andreyvit/edb/values"
)

func recordContractCases(binding *kvo.Binding, model *kvo.Model) []struct {
	name string
	open func(*statrow.State) kvo.Record
} {
	generated := binding.WithGenerated(&statrow.Static, nativebind.StaticOnly)
	return []struct {
		name string
		open func(*statrow.State) kvo.Record
	}{
		{"Fast2", func(row *statrow.State) kvo.Record {
			return binding.Encoded(row2.Open(binding.Encode(nil, reflect.ValueOf(row).Elem(), row2.Fast2)))
		}},
		{"Compact2", func(row *statrow.State) kvo.Record {
			return binding.Encoded(row2.Open(binding.Encode(nil, reflect.ValueOf(row).Elem(), row2.Compact2)))
		}},
		{"MessagePack", func(row *statrow.State) kvo.Record {
			return binding.MessagePack(msgpack.Encode(nil, reflect.ValueOf(row)))
		}},
		{"Reflected", func(row *statrow.State) kvo.Record { return binding.Record(reflect.ValueOf(row)) }},
		{"Generated", func(row *statrow.State) kvo.Record { return generated.Record(reflect.ValueOf(row)) }},
		{"Memory", func(row *statrow.State) kvo.Record {
			raw := binding.Encode(nil, reflect.ValueOf(row).Elem(), row2.Fast2)
			return kvo.Encoded(row2.Open(raw)).WithModel(model).Fastest()
		}},
	}
}

func TestRecordOperationContract(t *testing.T) {
	schema, model := statrow.Model()
	binding := kvo.Bind(reflect.TypeFor[statrow.State](), model)
	earned := kvo.BindSignedProp[int64](schema, 19)
	tags := kvo.BindListProp(schema, 15, values.UnsignedCodec[uint64]())
	for _, tc := range recordContractCases(binding, model) {
		t.Run(tc.name, func(t *testing.T) {
			source := statrow.Sample(7)
			source.Key = 0
			want := *source
			r := tc.open(source)
			original := r.Clone()
			assertRow := func(raw row2.View, want statrow.State) {
				t.Helper()
				var got statrow.State
				binding.Decode(raw.Raw(), reflect.ValueOf(&got).Elem())
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("row = %#v, want %#v", got, want)
				}
			}
			assertRow(r.Snapshot(), want)
			if r.Model() != model || r.Get(earned) != want.Earned || !slices.Equal(r.Get(tags).Slice(), want.Tags) {
				t.Fatal("typed reads disagree with the native row")
			}
			ed := r.Edit()
			ed.Set(earned, int64(-19))
			ed.FastSetPathValue([]row2.Step{{Key: 9}, {Key: 5}}, values.Uint(901))
			tags.SetSlice(ed, []uint64{8, 0, 11})
			want.Earned, want.Profile.Balance, want.Tags = -19, 901, []uint64{8, 0, 11}
			assertRow(ed.Snapshot(), want)
			if ed.Get(earned) != want.Earned || !slices.Equal(ed.Get(tags).Slice(), want.Tags) {
				t.Fatal("editor reads disagree with saved data")
			}
			ed.Set(earned, int64(0))
			tags.SetSlice(ed, []uint64{})
			want.Earned, want.Tags = 0, []uint64{}
			assertRow(ed.Snapshot(), want)
			ed.SetValue(15, values.Value{})
			want.Tags = nil
			assertRow(ed.Snapshot(), want)
			if original.Get(earned) != 98 || original.Child(row2.Step{Key: 9}).GetValue(5).AsUint() != 107 {
				t.Fatal("editing changed an independent clone")
			}
		})
	}
}

type assignmentFailure struct{ field string }

func (e *assignmentFailure) Error() string { return "rejected " + e.field }

func TestRecordAssignmentErrorContract(t *testing.T) {
	schema, model := statrow.Model()
	binding := kvo.Bind(reflect.TypeFor[statrow.State](), model)
	cause := &assignmentFailure{"earned"}
	codec := values.SignedCodec[int64]()
	codec.Encode = func(int64) values.Value { panic(cause) }
	prop := kvo.BindProp(schema, 19, codec)
	for _, tc := range recordContractCases(binding, model) {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.open(statrow.Sample(7))
			ed := r.Edit()
			err := ed.TrySet(prop, int64(123))
			var typed *assignmentFailure
			if !errors.Is(err, cause) || !errors.As(err, &typed) || typed != cause {
				t.Fatalf("TrySet lost error identity: %v", err)
			}
			func() {
				defer func() {
					p, ok := recover().(error)
					if !ok || !errors.Is(p, cause) {
						t.Errorf("Set panic = %v, want original cause", p)
					}
				}()
				ed.Set(prop, int64(123))
			}()
			if ed.GetValue(19).AsInt() != 98 || ed.Changes() != 0 {
				t.Fatal("failed assignment changed the record")
			}
			textCodec := values.SignedCodec[int64]()
			textCodec.Encode = func(int64) values.Value { panic("invalid assignment") }
			if err := ed.TrySet(kvo.BindProp(schema, 19, textCodec), int64(1)); err == nil || !strings.Contains(err.Error(), "invalid assignment") {
				t.Fatalf("non-error panic = %v", err)
			}
		})
	}
}

func TestBindingErrorIdentity(t *testing.T) {
	_, model := statrow.Model()
	base := kvo.Bind(reflect.TypeFor[statrow.State](), model)
	for name, binding := range map[string]*kvo.Binding{"reflected": base, "generated": base.WithGenerated(&statrow.Static, nativebind.StaticOnly)} {
		t.Run(name, func(t *testing.T) {
			var dst statrow.State
			if _, err := binding.TryEncode(nil, reflect.ValueOf(&dst).Elem(), row2.Layout(255)); !errors.Is(err, row2.ErrLayout) {
				t.Fatalf("encode error = %v", err)
			}
			for _, tc := range []struct {
				name  string
				raw   []byte
				cause error
			}{
				{"malformed", []byte{0xff}, row2.ErrFormat},
				{"wrong kind", []byte{0, 12}, row2.ErrKind},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if err := binding.TryDecode(tc.raw, reflect.ValueOf(&dst).Elem()); !errors.Is(err, tc.cause) {
						t.Fatalf("decode error = %v", err)
					}
					if err := binding.TryValidate(tc.raw); !errors.Is(err, tc.cause) {
						t.Fatalf("validation error = %v", err)
					}
					var w row2.Writer
					w.Begin(0, "", row2.Object, row2.Fast2)
					w.Put(9, "", values.Bytes(tc.raw), row2.Data)
					w.End()
					r := binding.Encoded(row2.Open(w.BytesWritten()))
					var child statrow.Profile
					if err := binding.TryDecodeProperty(reflect.ValueOf(&child).Elem(), r, 9); !errors.Is(err, tc.cause) {
						t.Fatalf("property error = %v", err)
					}
				})
			}
		})
	}
}
