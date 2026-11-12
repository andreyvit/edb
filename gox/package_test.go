package gox_test

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/gox"
	"github.com/andreyvit/edb/values"
)

func TestCompilationEntryPoints(t *testing.T) {
	compilers := []struct {
		name    string
		compile func(string) (*gox.Template, error)
	}{
		{"Compile", func(s string) (*gox.Template, error) { return gox.Compile("page.gox", s) }},
		{"CompileFiles/file", func(s string) (*gox.Template, error) {
			return gox.CompileFiles("page.gox", map[string]string{"page.gox": s})
		}},
		{"CompileFiles/function", func(s string) (*gox.Template, error) {
			return gox.CompileFiles("Page", map[string]string{"page.gox": s})
		}},
		{"CompileSources", func(s string) (*gox.Template, error) {
			return gox.CompileSources(map[string]string{"page.gox": s})
		}},
		{"Options.Compile", func(s string) (*gox.Template, error) {
			return (gox.Options{}).Compile("page.gox", s)
		}},
		{"Options.CompileFiles", func(s string) (*gox.Template, error) {
			return (gox.Options{}).CompileFiles("Page", map[string]string{"page.gox": s})
		}},
		{"Options.CompileSources", func(s string) (*gox.Template, error) {
			return (gox.Options{}).CompileSources(map[string]string{"page.gox": s})
		}},
	}
	const source = `// A builder-taking helper is not a render entry.
func label(b *gox.Builder, name string) string { return name }
func Page(b *gox.Builder, name string) {
 <p class="greeting">{label(name)}</p>
}`
	var generated map[string]string
	for _, compiler := range compilers {
		t.Run(compiler.name, func(t *testing.T) {
			tmpl, err := compiler.compile(source)
			if err != nil {
				t.Fatal(err)
			}
			if generated == nil {
				generated = tmpl.GeneratedSources
			} else if !maps.Equal(tmpl.GeneratedSources, generated) {
				t.Fatal("compile entry points generated different Go sources")
			}
			if tmpl.Entry != "" && (tmpl.Entry != "Page" || !reflect.DeepEqual(tmpl.Parameters, []gox.Parameter{{Name: "name", Type: "string"}})) {
				t.Fatalf("entry %q parameters %+v", tmpl.Entry, tmpl.Parameters)
			}
			if tmpl.DynamicClasses || len(tmpl.Classes) != 1 || tmpl.Classes[0].Name != "greeting" {
				t.Fatalf("classes %+v, dynamic %v", tmpl.Classes, tmpl.DynamicClasses)
			}
			program, err := tmpl.Link()
			if err != nil {
				t.Fatal(err)
			}
			args := []values.Value{values.String("<Ada>")}
			got, err := program.RenderFunc(context.Background(), "Page", args, gox.RenderOptions{})
			if err != nil || got != `<p class="greeting">&lt;Ada&gt;</p>` {
				t.Fatalf("render: %q, %v", got, err)
			}
			if _, err := program.RenderFunc(context.Background(), "Page", args, gox.RenderOptions{MaxOutputBytes: 1}); !errors.Is(err, evm.ErrOutput) {
				t.Fatalf("output limit: %v", err)
			}
			for _, bad := range []struct{ source, message string }{
				{"// anonymous renderers are not Go declarations\n() {\n <p />\n}", "page.gox:2:1: expected declaration"},
				{"func Page() {\n <p />\n}", "page.gox:1:1: rendering functions require an explicit first *gox.Builder parameter"},
				{"func Page(b *gox.Builder) int {\n <p />\n return 1\n}", "page.gox:1:1: rendering functions must not return values"},
				{"// source positions survive preprocessing\nfunc Page(b *gox.Builder) {\n <p>{missing}</p>\n}", "page.gox:3:"},
				{"func Page(b *gox.Builder) {\n <p title={}>\n}", "page.gox:2:"},
				{source + strings.Repeat(" ", evmcompiler.MaxSourceSize), "combined GoX sources exceed"},
			} {
				if _, err := compiler.compile(bad.source); err == nil || !strings.Contains(err.Error(), bad.message) {
					t.Errorf("got %v, want %q", err, bad.message)
				}
			}
		})
	}
}

func TestNamedPackage(t *testing.T) {
	sources := map[string]string{
		"main.go": `package main
import "gox"
import "strings"
func Main(b *gox.Builder, name string) { Page(b, strings.ToUpper(name)) }
`,
		"page.gox": `package main
func Page(b *gox.Builder, name string) {
 <section>
  Title()
  <Title />
  <a href={name}>{name}</a>
 </section>
}
`,
		"title.gox": `package main
func Title(b *gox.Builder, name string) {
 <h1>{name}</h1>
}
`,
	}
	tmpl, err := gox.CompileSources(sources)
	if err != nil {
		t.Fatal(err)
	}
	p, err := tmpl.Link()
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.RenderFunc(context.Background(), "Main", []values.Value{values.String("javascript:<hi>")}, gox.RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := `<section><h1>JAVASCRIPT:&lt;HI&gt;</h1><h1>JAVASCRIPT:&lt;HI&gt;</h1><a href="#ZgotmplZ">JAVASCRIPT:&lt;HI&gt;</a></section>`; got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	// Go imports are file-local, and calls outside elements remain ordinary Go.
	sources["title.gox"] = `package main
func Title(b *gox.Builder, name string) {
 Page()
 <h1>{strings.ToUpper(name)}</h1>
}`
	_, err = gox.CompileSources(sources)
	if err == nil || !strings.Contains(err.Error(), "not enough arguments") {
		t.Fatalf("missing explicit args: %v", err)
	}
	sources["main.go"] = `package main
func Main() string { return strings.ToUpper("x") }`
	sources["title.gox"] = "package main\nfunc Title(b *gox.Builder, name string) {\n <h1>{name}</h1>\n}"
	_, err = gox.CompileSources(sources)
	if err == nil || !strings.Contains(err.Error(), "main.go:2:") || !strings.Contains(err.Error(), "undefined: strings") {
		t.Fatalf("file-local imports: %v", err)
	}
}

func TestNamedAmbient(t *testing.T) {
	tmpl, err := gox.CompileSources(map[string]string{"ambient.gox": `
func AmbientTest(b *gox.Builder) {
 var ambient *evmruntime.Ambient
 delta := map[string]any{"label": "parent", "n": 42}
 ambient = ambient.CloneWith(delta)
 delta["label"] = "mutated"
 child := ambient.CloneWith(map[string]any{"label": "child", "n": nil})
 <p>{ambient.Get("label")}{"/"}{child.Get("label")}{"/"}{ambient.Get("n")}{"/"}{child.Get("n") == nil}</p>
}

`})
	if err != nil {
		t.Fatal(err)
	}
	p, err := tmpl.Link()
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.RenderFunc(context.Background(), "AmbientTest", nil, gox.RenderOptions{})
	if err != nil || got != "<p>parent/child/42/true</p>" {
		t.Fatalf("%q: %v", got, err)
	}
}

func TestNamedSourcePositions(t *testing.T) {
	for _, directive := range []string{"//line forged.go:400", "/*line forged.go:400*/"} {
		_, err := gox.CompileSources(map[string]string{"view.gox": directive + `
func Page(b *gox.Builder) {
 <p>{unknown}</p>
}
`})
		if err == nil || !strings.Contains(err.Error(), "view.gox:3:") || strings.Contains(err.Error(), "forged.go") {
			t.Fatalf("%s: %v", directive, err)
		}
		tmpl, err := gox.CompileSources(map[string]string{"view.gox": directive + `
type Person struct { Name string }
func Page(b *gox.Builder) {
 var p *Person
 <p>{p.Name}</p>
}
`})
		if err != nil {
			t.Fatal(err)
		}
		p, err := tmpl.Link()
		if err != nil {
			t.Fatal(err)
		}
		var reports []gox.Diagnostic
		_, err = p.RenderFunc(context.Background(), "Page", nil, gox.RenderOptions{Report: func(d gox.Diagnostic) { reports = append(reports, d) }})
		if err != nil || len(reports) != 1 || reports[0].Position.File != "view.gox" || reports[0].Position.Line != 5 {
			t.Fatalf("runtime positions: %+v: %v", reports, err)
		}
	}
}

func TestNamedComponentTags(t *testing.T) {
	host := evm.Package{Path: "widgets", Functions: []evm.ExternalFunction{{Name: "Badge", ParamNames: []string{"pageTitle"}, Signature: evm.Signature{Params: []*values.Type{values.StringType}, Results: []*values.Type{values.StringType}}, Call: func(c *evm.Call) error {
		c.SetResult(0, c.Argument(0))
		return nil
	}}}}
	tmpl, err := gox.CompileSources(map[string]string{"page.gox": `
func Page(b *gox.Builder, title string) {
 Content := Card
 label := func() {
  <em>{title}</em>
 }
 <section>
  <Content pageTitle={title}>
   <slot-body>
    <label />
   </slot-body>
  </Content>
  <Card pageTitle="second"><slot-body /></Card>
  <widgets.Badge pageTitle={title} />
 </section>
}
func Card(b *gox.Builder, pageTitle string, body func()) {
 <article><h2>{pageTitle}</h2>
  body()
 </article>
}
`}, host)
	if err != nil {
		t.Fatal(err)
	}
	p, err := tmpl.Link(host)
	if err != nil {
		t.Fatal(err)
	}
	out, err := p.RenderFunc(context.Background(), "Page", []values.Value{values.String("<safe>")}, gox.RenderOptions{Strict: true})
	if err != nil || out != `<section><article><h2>&lt;safe&gt;</h2><em>&lt;safe&gt;</em></article><article><h2>second</h2></article>&lt;safe&gt;</section>` {
		t.Fatalf("%q: %v", out, err)
	}
	_, err = gox.CompileSources(map[string]string{"page.gox": "func Page(b *gox.Builder) {\n <Missing />\n}"})
	if err == nil || !strings.Contains(err.Error(), "unknown or untyped component Missing") {
		t.Fatalf("unknown component: %v", err)
	}
}
