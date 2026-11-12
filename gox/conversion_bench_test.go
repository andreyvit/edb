package gox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"html/template"
	"testing"

	"github.com/andreyvit/edb/gox"
)

func TestValueRenderingMatchesJSONSemantics(t *testing.T) {
	data := map[string]any{"name": "<Alice>", "rows": []any{map[string]any{"n": json.Number("18446744073709551615"), "tags": []any{"vip", "member"}}, nil, json.Number("-7")}}
	reference := template.Must(template.New("values").Parse(`{{range .Times}}<div title="{{$.Data}}">{{$.Data}}</div><script>const data = {{$.Data}};</script>{{end}}`))
	var want bytes.Buffer
	if err := reference.Execute(&want, struct {
		Data  any
		Times [16]int
	}{Data: data}); err != nil {
		t.Fatal(err)
	}
	if got := render(t, conversionTemplate); got != want.String() {
		t.Fatalf("rendered %s, want %s", got, want.String())
	}
}

const conversionTemplate = `func Page(b *gox.Builder) {
 data := map[string]any{"name": "<Alice>", "rows": []any{map[string]any{"n": uint64(18446744073709551615), "tags": []string{"vip", "member"}}, nil, int8(-7)}}
 for i := 0; i < 16; i++ {
  <div title={data}>{data}</div>
  <script>const data = {=data};</script>
 }
}`

func BenchmarkValueRendering(b *testing.B) {
	tmpl, err := gox.Compile("values.gox", conversionTemplate)
	if err != nil {
		b.Fatal(err)
	}
	p, err := tmpl.Link()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := p.Render(context.Background(), nil, gox.RenderOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
