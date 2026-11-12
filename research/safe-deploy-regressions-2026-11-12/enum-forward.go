//go:build ignore

// Candidate-only forward-compatibility probe; synthetic values, no database.
package main

import (
	"fmt"
	"reflect"

	"github.com/andreyvit/edb/kvo"
	wire "github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/prairiegroupinc/fire/bm/msocmon"
	"github.com/prairiegroupinc/fire/bpub"
	_ "github.com/prairiegroupinc/fire/nm"
	"github.com/vmihailenco/msgpack/v5"
)

type enumRow struct {
	Origin   bpub.OriginCategory `msgpack:"o" kvo:"origin"`
	Platform msocmon.Platform    `msgpack:"p" kvo:"platform"`
}

func main() {
	s := kvo.NewSchema()
	o := kvo.NewSignedProp[bpub.OriginCategory](s, 1, "origin", kvo.NewScalarSubtype[bpub.OriginCategory]("audit.origin", kvo.TInt))
	p := kvo.NewSignedProp[msocmon.Platform](s, 2, "platform", kvo.NewScalarSubtype[msocmon.Platform]("audit.platform", kvo.TInt))
	m := kvo.NewModel(s, kvo.NewEntityType(s, "enumRow", reflect.TypeFor[enumRow]()), func(b *kvo.ModelBuilder) { b.Prop(o); b.Prop(p) })
	proj := kvo.Project(reflect.TypeFor[enumRow](), m, nil)
	checks := 0
	for origin := 0; origin <= 17; origin++ {
		for platform := 0; platform <= 8; platform++ {
			want := enumRow{bpub.OriginCategory(origin), msocmon.Platform(platform)}
			oText, _ := want.Origin.MarshalText()
			pText, _ := want.Platform.MarshalText()
			forms := func(n int, text []byte) []any {
				return []any{string(text), text, int8(n), int16(n), int32(n), int64(n), uint8(n), uint16(n), uint32(n), uint64(n)}
			}
			for i, a := range forms(origin, oText) {
				for j, b := range forms(platform, pText) {
					raw, err := msgpack.Marshal(map[string]any{"o": a, "p": b})
					if err != nil {
						panic(err)
					}
					var native enumRow
					if err := msgpack.Unmarshal(raw, &native); err != nil {
						panic(fmt.Sprintf("native %d/%d forms %d/%d: %v", origin, platform, i, j, err))
					}
					if native != want {
						panic("native changed enum values")
					}
					r := proj.MessagePack(raw)
					if r.Get(o) != want.Origin || r.Get(p) != want.Platform {
						panic("record changed enum values")
					}
					var decoded enumRow
					proj.DecodeProperty(reflect.ValueOf(&decoded.Origin).Elem(), r, o.Code())
					proj.DecodeProperty(reflect.ValueOf(&decoded.Platform).Elem(), r, p.Code())
					if decoded != want {
						panic("property decode changed enum values")
					}
					e := r.Edit()
					e.Set(o, want.Origin)
					e.Set(p, want.Platform)
					var edited enumRow
					wire.Decode(e.MessagePack(), reflect.ValueOf(&edited).Elem())
					if edited != want {
						panic("edit changed enum values")
					}
					if r.Get(o) != want.Origin || r.Get(p) != want.Platform {
						panic("edit changed retained record")
					}
					checks++
				}
			}
		}
	}
	fmt.Printf("PASS %d mixed encoding pairs; native, record, property, editor and retained reads\n", checks)
}
