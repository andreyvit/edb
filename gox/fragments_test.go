package gox_test

import (
	"context"
	"strings"
	"testing"

	"github.com/andreyvit/edb/gox"
	"github.com/andreyvit/edb/values"
)

func TestFragments(t *testing.T) {
	got := render(t, `func Page(b *gox.Builder, name string) {
 label := func() {
  <>
   caption()
   <em>{name}</em>
  </>
 }
 caption(name)
 <>
  "Hello, "
  caption()
  <> {"!"} </>
  Badge()
  <Badge />
  <Card>
   <slot-body>
    <>
     label()
    </>
   </slot-body>
  </Card>
 </>
 caption(name)
 <><></></>
}
func caption(name string) string { return name }
func Badge(b *gox.Builder, name string) {
 <><strong>{name}</strong></>
}
func Card(b *gox.Builder, body func()) {
 <>
  <article>
   <>
    class("card")
    body()
   </>
  </article>
 </>
}`, values.String("<Ada>"))
	if want := `Hello, &lt;Ada&gt;!<strong>&lt;Ada&gt;</strong><strong>&lt;Ada&gt;</strong><article class="card">&lt;Ada&gt;<em>&lt;Ada&gt;</em></article>`; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFragmentsControlFlow(t *testing.T) {
	source := `func Page(b *gox.Builder, stop bool) {
 <>
  for i := 0; i < 4; i++ {
   <>
    if i == 0 { continue }
    <li>
     i
     if i == 2 { break }
    </li>
   </>
  }
  for _, name := range []string{"skip", "show"} {
   <>
    <span>
     if name == "skip" { continue }
     name
    </span>
   </>
  }
  <section>
   <>
    if stop { return }
    "tail"
   </>
  </section>
 </>
 <footer>{"end"}</footer>
}`
	for _, stop := range []bool{false, true} {
		got := render(t, source, values.Bool(stop))
		want := `<li>1</li><li>2</li><span></span><span>show</span><section>`
		if stop {
			want += `</section>`
		} else {
			want += `tail</section><footer>end</footer>`
		}
		if got != want {
			t.Fatalf("stop=%v: got %q, want %q", stop, got, want)
		}
	}
}

func TestFragmentsEscapingAndNil(t *testing.T) {
	tmpl, err := gox.Compile("fragments.gox", `func Page(b *gox.Builder, value string) {
 var customer *Customer
 <>
  "before"
  customer.Name
  <a href={value}>{value}</a>
  <textarea><>{value}</></textarea>
  "after"
 </>
}
type Customer struct { Name string }
`)
	if err != nil {
		t.Fatal(err)
	}
	program, err := tmpl.Link()
	if err != nil {
		t.Fatal(err)
	}
	args := []values.Value{values.String("javascript:</textarea><script>alert(1)</script>")}
	var diagnostics []gox.Diagnostic
	got, err := program.Render(context.Background(), args, gox.RenderOptions{Report: func(d gox.Diagnostic) { diagnostics = append(diagnostics, d) }})
	if err != nil {
		t.Fatal(err)
	}
	escaped := `javascript:&lt;/textarea&gt;&lt;script&gt;alert(1)&lt;/script&gt;`
	if want := `before<a href="#ZgotmplZ">` + escaped + `</a><textarea>` + escaped + `</textarea>after`; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if len(diagnostics) != 1 || diagnostics[0].Position.File != "fragments.gox" || diagnostics[0].Position.Line != 5 {
		t.Fatalf("diagnostics: %+v", diagnostics)
	}
	if _, err := program.Render(context.Background(), args, gox.RenderOptions{Strict: true}); err == nil {
		t.Fatal("strict render succeeded")
	}
}

func TestFragmentsRequireBalancedScope(t *testing.T) {
	for _, tt := range []struct {
		body, error string
	}{
		{"</>", "unbalanced closing fragment </>"},
		{"<>", "unclosed fragment <>"},
		{"<> <p> </> </p>", "unbalanced closing fragment </>"},
		{"<p> <> </p> </>", "unbalanced closing tag p"},
		{"<>\n if name != \"\" {\n </>\n }\n </>", "if branch must contain complete elements"},
		{"for i := 0; i < 2; i++ {\n <>\n }", "loop must contain complete elements"},
		{"<>\n caption()\n </>\n caption()", "not enough arguments"},
		{"<>\n {\n name := 42\n caption()\n }\n </>", "cannot use name"},
	} {
		_, err := gox.Compile("fragments.gox", "func Page(b *gox.Builder, name string) {\n "+tt.body+"\n}\nfunc caption(name string) string { return name }")
		if err == nil || !strings.Contains(err.Error(), tt.error) || !strings.Contains(err.Error(), "fragments.gox:") {
			t.Errorf("%s: got %v, want %s", tt.body, err, tt.error)
		}
	}
}

func TestFragmentsOutputFormats(t *testing.T) {
	for _, tt := range []struct {
		format      gox.Format
		body, value string
		want        string
	}{
		{gox.HTMLFormat, "value", "<b>", "&lt;b&gt;"},
		{gox.TextFormat, "value", "<b>", "<b>"},
		{gox.CSSFormat, `".x { color: "` + "\n value\n" + `"; }"`, "red", ".x { color: red; }"},
	} {
		tmpl, err := (gox.Options{Format: tt.format}).Compile("fragments.gox", "func Page(b *gox.Builder, value string) {\n <>\n "+tt.body+"\n </>\n}")
		if err != nil {
			t.Fatal(err)
		}
		program, err := tmpl.Link()
		if err != nil {
			t.Fatal(err)
		}
		got, err := program.Render(context.Background(), []values.Value{values.String(tt.value)}, gox.RenderOptions{Strict: true})
		if err != nil || got != tt.want {
			t.Fatalf("format=%v: got %q, %v; want %q", tt.format, got, err, tt.want)
		}
	}
}
