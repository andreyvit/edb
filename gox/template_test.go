package gox_test

import (
	"context"
	"errors"
	"html/template"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/gox"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func TestGoXPreparationAndHelper(t *testing.T) {
	source := `func Page(b *gox.Builder,
 name string,
 count int,
) {
 title := heading(count)
 <section>
  <h1>{title}</h1>
  <p>
   "Hello, "
   name
   "!"
  </p>
  for i := 0; i < count; i++ {
   <span>{i}</span>
  }
 </section>
}
func heading(n int) string { return fmt.Sprintf("Customers: %d", n) }
`
	tmpl, err := gox.Compile("customers.gox", source)
	if err != nil {
		t.Fatal(err)
	}
	program, err := tmpl.Link()
	if err != nil {
		t.Fatal(err)
	}
	got, err := program.Render(context.Background(), []values.Value{values.String("<Alice>"), values.Int(2)}, gox.RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := `<section><h1>Customers: 2</h1><p>Hello, &lt;Alice&gt;!</p><span>0</span><span>1</span></section>`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func render(t *testing.T, source string, args ...values.Value) string {
	t.Helper()
	tmpl, err := gox.Compile("test.gox", source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := tmpl.Link()
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Render(context.Background(), args, gox.RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return got
}
func TestContextualEscaping(t *testing.T) {
	source := `func Page(b *gox.Builder, value string) {
 <a href={value} title={value}>{value}</a>
 <img srcset={value}>
 <div style="color: {value}"></div>
 <script>const v = {=value}; const s = "{=value}"; const r = /{=value}/; const t = ` + "`" + `{=value}` + "`" + `;</script>
 <style>.x { color: {=value}; background: url('{=value}'); }</style>
 <textarea>{value}</textarea><title>{value}</title>
 <a href="/search?q={value}"></a>
 <button onclick="return f({value})"></button>
 <meta http-equiv="refresh" content="0; url={value}">
 }`
	cases := []string{`javascript:alert(1)`, `</script><script>alert(1)</script>`, `" onmouseover="alert(1)`, "${alert(1)}", "O'Reilly & <x>", "\x00"}
	for _, value := range cases {
		got := render(t, source, values.String(value))
		reference := `<a href="{{.}}" title="{{.}}">{{.}}</a><img srcset="{{.}}"><div style="color: {{.}}"></div><script>const v = {{.}}; const s = "{{.}}"; const r = /{{.}}/; const t = ` + "`" + `{{.}}` + "`" + `;</script><style>.x { color: {{.}}; background: url('{{.}}'); }</style>`
		reference += `<textarea>{{.}}</textarea><title>{{.}}</title><a href="/search?q={{.}}"></a><button onclick="return f({{.}})"></button><meta http-equiv="refresh" content="0; url={{.}}">`
		tmpl, err := template.New("reference").Parse(reference)
		if err != nil {
			t.Fatal(err)
		}
		var b strings.Builder
		if err = tmpl.Execute(&b, value); err != nil {
			t.Fatal(err)
		}
		if got != b.String() {
			t.Errorf("%q:\ngot  %s\nwant %s", value, got, b.String())
		}
	}
}
func TestNilOutputAndStrict(t *testing.T) {
	source := `func Page(b *gox.Builder) {
 var customer *Customer
 <p>
  "before"
  customer.Name
  "after"
 </p>
 }
 type Customer struct { Name string }
 `
	tmpl, err := gox.Compile("nil.gox", source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := tmpl.Link()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostics []gox.Diagnostic
	got, err := p.Render(context.Background(), nil, gox.RenderOptions{Report: func(d gox.Diagnostic) { diagnostics = append(diagnostics, d) }})
	if err != nil || got != "<p>beforeafter</p>" || len(diagnostics) != 1 {
		t.Fatalf("%q %v %v", got, err, diagnostics)
	}
	if diagnostics[0].Position.File != "nil.gox" || diagnostics[0].Position.Line != 5 {
		t.Fatalf("position: %+v", diagnostics)
	}
	_, err = p.Render(context.Background(), nil, gox.RenderOptions{Strict: true})
	if err == nil {
		t.Fatal("strict render succeeded")
	}
}
func TestAttributesFromHelper(t *testing.T) {
	source := `func Page(b *gox.Builder, disabled bool) {
 <button class="base">
  decorate(disabled)
  "Save"
 </button>
 }
 func decorate(b *gox.Builder, disabled bool) {
  b.Class("button")
  b.Attr("aria-disabled", disabled)
  b.Attr("disabled", disabled)
 }
 `
	got := render(t, source, values.Bool(false))
	if got != `<button class="base button" aria-disabled="false">Save</button>` {
		t.Fatal(got)
	}
}
func TestRejectsBrokenStructureAndTypes(t *testing.T) {
	for _, source := range []string{
		`func Page(b *gox.Builder) {` + "\n" + `<div>` + "\n" + `if true {` + "\n" + `</div>` + "\n" + `} }`,
		`func Page(b *gox.Builder) {` + "\n" + `<p>{unknown}</p>` + "\n" + `}`,
		`func Page(b *gox.Builder) {` + "\n" + `<script type={"x"}>const x=1;</script>` + "\n" + `}`,
	} {
		_, err := gox.Compile("bad.gox", source)
		if err == nil {
			t.Errorf("accepted %s", source)
		}
	}
}

func TestComponentsSlotsAndImplicitInputs(t *testing.T) {
	sources := map[string]string{
		"page.gox": `func Page(b *gox.Builder, page string) {
 <main>
  for i:=0;i<2;i++ {
   <Wrapper label={fmt.Sprintf("item %d",i)}>
    <slot-body>
     page
     i
    </slot-body>
   </Wrapper>
  }
 </main>
 }`,
		"wrapper.gox": `func Wrapper(b *gox.Builder, page string, label string, body func()) {
 <Button label={label}>
  <slot-body>
   body()
  </slot-body>
 </Button>
 }`,
		"button.gox": `func Button(b *gox.Builder, page string,label string,body func()) {
 <button title={page}>
  label
  ": "
  body()
 </button>
 }`,
	}
	tmpl, err := gox.CompileFiles("page.gox", sources)
	if err != nil {
		t.Fatal(err)
	}
	p, err := tmpl.Link()
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Render(context.Background(), []values.Value{values.String("Root")}, gox.RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := `<main><button title="Root">item 0: Root0</button><button title="Root">item 1: Root1</button></main>`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
func TestLocalAndPolymorphicComponents(t *testing.T) {
	source := `func Page(b *gox.Builder, prefix string) {
 item := func(value string) {
  <b>
   prefix
   value
  </b>
 }
 chosen := item
 <div>
  <chosen value="one" />
  item("two")
 </div>
 }`
	got := render(t, source, values.String("@"))
	if got != `<div><b>@one</b><b>@two</b></div>` {
		t.Fatal(got)
	}
}
func TestLexicalAndWhitespaceBoundaries(t *testing.T) {
	source := "" + `func Page(b *gox.Builder) {
 text := ` + "`" + `
 <div>not markup</div>
 ` + "`" + `
 // <span>not markup</span>
 /*
 <em>not markup</em>
 */
 <pre>{text}</pre>
 <p>{"{"}</p>
 }`
	got := render(t, source)
	want := "<pre>\n &lt;div&gt;not markup&lt;/div&gt;\n </pre><p>{</p>"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRenderHelpersCanChangeImplementation(t *testing.T) {
	for _, helper := range []string{
		`func greeting(name string) gox.HTML { return gox.HTML("<b>" + name + "</b>") }`,
		`func greeting(b *gox.Builder,name string) { b.HTML(gox.HTML("<b>" + name + "</b>")) }`,
	} {
		source := `func Page(b *gox.Builder, name string) {
 <p>
  greeting(name)
 </p>
 }
 ` + helper
		if got := render(t, source, values.String("Alice")); got != `<p><b>Alice</b></p>` {
			t.Fatal(got)
		}
	}
}
func TestControlFlowAndClasses(t *testing.T) {
	source := `func Page(b *gox.Builder) {
 <ul class="list">
  for i:=0;i<4;i++ {
   if i==0 {continue}
   <li>
    class("row active")
    if i==2 {continue}
    i
    if i==3 {break}
   </li>
  }
 </ul>
 }
 `
	tmpl, err := gox.Compile("loop.gox", source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := tmpl.Link()
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Render(context.Background(), nil, gox.RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got != `<ul class="list"><li class="row active">1</li><li class="row active"></li><li class="row active">3</li></ul>` {
		t.Fatal(got)
	}
	names := map[string]bool{}
	for _, cl := range tmpl.Classes {
		names[cl.Name] = true
	}
	if !names["list"] || !names["row"] || !names["active"] {
		t.Fatal(tmpl.Classes)
	}
	if got := render(t, `func Page(b *gox.Builder) {
 <div />
 <p> {"ok"} </p>
 }`); got != `<div></div><p>ok</p>` {
		t.Fatal(got)
	}
}
func TestPipelinesAndTypedSlots(t *testing.T) {
	source := `func Page(b *gox.Builder, name string) {
 <section>
  name | upper
  name | withPrefix("!")
  <rows>
   <slot-row>
    <b>{item}</b>
   </slot-row>
  </rows>
 </section>
 }
 func upper(s string)string{return strings.ToUpper(s)}
 func withPrefix(prefix, s string)string{return prefix+s}
 func rows(b *gox.Builder, row func(item string)) {
  <div>
   row("first")
   row("second")
  </div>
 }
 `
	got := render(t, source, values.String("alice"))
	if got != `<section>ALICE!alice<div><b>first</b><b>second</b></div></section>` {
		t.Fatal(got)
	}
}
func TestKVOInputs(t *testing.T) {
	schema := kvo.NewSchema()
	entity := kvo.NewEntityType(schema, "customer")
	name := kvo.NewProp(schema, 1, "name", kvo.TMachineString, nil)
	model := kvo.NewModel(schema, entity, func(b *kvo.ModelBuilder) { b.Prop(schema.MustPropByCode(name)) })
	pkg := evm.Package{Path: "db", Records: []*evm.RecordType{evm.NewRecordType("Customer", model, false)}}
	tmpl, err := gox.Compile("record.gox", `func Page(b *gox.Builder, customer *db.Customer) {
 <p>{customer.name}</p>
 }`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := tmpl.Link(pkg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, scope := evm.WithRecords(context.Background())
	defer scope.Close()
	edit := row2.Edit(row2.Empty(row2.Compact2))
	edit.Set([]row2.Step{{Key: name}}, values.Bytes([]byte("<Alice>")), row2.Data)
	got, err := p.Render(ctx, []values.Value{scope.Read(model, edit.Snapshot())}, gox.RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got != `<p>&lt;Alice&gt;</p>` {
		t.Fatal(got)
	}
}
func TestLinkingLimitsAndConcurrency(t *testing.T) {
	pkg := evm.Package{Path: "host", Functions: []evm.ExternalFunction{{Name: "Label", Signature: evm.Signature{Results: []*values.Type{values.StringType}}}}}
	tmpl, err := gox.Compile("host.gox", `func Page(b *gox.Builder) {
 <p>{host.Label()}</p>
 }`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	for _, label := range []string{"one", "two"} {
		pkg.Functions[0].Call = func(c *evm.Call) error { c.SetResult(0, values.String(label)); return nil }
		p, err := tmpl.Link(pkg)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, err := p.Render(context.Background(), nil, gox.RenderOptions{})
				if err != nil || got != "<p>"+label+"</p>" {
					t.Errorf("%s %v", got, err)
				}
			}()
		}
		wg.Wait()
		_, err = p.Render(context.Background(), nil, gox.RenderOptions{MaxOutputBytes: 3})
		if !errors.Is(err, evm.ErrOutput) {
			t.Fatalf("limit: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = p.Render(ctx, nil, gox.RenderOptions{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
	}
}
func BenchmarkRender(b *testing.B) {
	source := `func Page(b *gox.Builder, rows []string) {
 <ul>
  for _, name := range rows {
   <li class="row">
    name
   </li>
  }
 </ul>
 }`
	tmpl, err := gox.Compile("bench.gox", source)
	if err != nil {
		b.Fatal(err)
	}
	program, err := tmpl.Link()
	if err != nil {
		b.Fatal(err)
	}
	helperTemplate, err := gox.Compile("helper.gox", strings.ReplaceAll(source, `<li class="row">`, "<li>\n class(\"row\")"))
	if err != nil {
		b.Fatal(err)
	}
	helperProgram, err := helperTemplate.Link()
	if err != nil {
		b.Fatal(err)
	}
	old := template.Must(template.New("old").Parse(`<ul>{{range .Rows}}<li class="row">{{.}}</li>{{end}}</ul>`))
	rows := make([]string, 100)
	for i := range rows {
		rows[i] = "Alice & Bob"
	}
	data := struct{ Rows []string }{rows}
	cells := make([]values.Value, len(rows))
	for i, row := range rows {
		cells[i] = values.String(row)
	}
	args := []values.Value{values.SliceValue(unsafe.Pointer(unsafe.SliceData(cells)), uint32(len(cells)), uint32(len(cells)))}
	b.Run("GoX", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_, err := program.Render(context.Background(), args, gox.RenderOptions{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("GoTemplate", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			var out strings.Builder
			if err := old.Execute(&out, data); err != nil {
				b.Fatal(err)
			}
			_ = out.String()
		}
	})
	b.Run("GoXClassHelper", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := helperProgram.Render(context.Background(), args, gox.RenderOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestInlineContextsNilAndSourceErrors(t *testing.T) {
	source := `func Page(b *gox.Builder) {
 var customer *Customer
 <a title="prefix {customer.Name} suffix">{"ok"}</a>
 <script>const name={=customer.Name};const text="{=customer.Name}";</script>
 }
 type Customer struct{Name string}`
	got := render(t, source)
	if got != `<a>ok</a><script>const name= null ;const text="";</script>` {
		t.Fatal(got)
	}
	source = `// Header comment
 func Page(b *gox.Builder,
  n int,
 ) {
 <div>{100 / n}</div>
 }`
	tmpl, err := gox.Compile("division.gox", source)
	if err != nil {
		t.Fatal(err)
	}
	p, err := tmpl.Link()
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Render(context.Background(), []values.Value{values.Int(0)}, gox.RenderOptions{})
	if !errors.Is(err, evm.ErrDivideByZero) || !strings.Contains(err.Error(), "division.gox:5") {
		t.Log(tmpl.GeneratedSource)
		var execution *evm.ExecutionError
		if errors.As(err, &execution) {
			t.Logf("position: %+v", execution.Position)
		}
		t.Fatal(err)
	}
}
func TestAutoimportShadowingAndTypeSwitch(t *testing.T) {
	source := `func Page(b *gox.Builder) {
 fmt := struct{Title string}{"local"}
 var value any = "hello"
 <div>
  fmt.Title
  switch v := value.(type) {
  case string:
   <b>{v}</b>
  default:
   "other"
  }
 </div>
 }`
	if got := render(t, source); got != `<div>local<b>hello</b></div>` {
		t.Fatal(got)
	}
}

func TestTextAndCSSFormats(t *testing.T) {
	text, err := (gox.Options{Format: gox.TextFormat}).Compile("message.gox", `func Page(b *gox.Builder, name string) {
 "Hello, "
 name
 "!\n"
 }`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := text.Link()
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Render(context.Background(), []values.Value{values.String("<Alice>")}, gox.RenderOptions{})
	if err != nil || got != "Hello, <Alice>!\n" {
		t.Fatalf("%q %v", got, err)
	}
	css, err := (gox.Options{Format: gox.CSSFormat}).Compile("theme.gox", `func Page(b *gox.Builder, color string) {
 ".button { color: "
 color
 "; }"
 }`)
	if err != nil {
		t.Fatal(err)
	}
	p, err = css.Link()
	if err != nil {
		t.Fatal(err)
	}
	got, err = p.Render(context.Background(), []values.Value{values.String("expression(alert(1))")}, gox.RenderOptions{})
	if err != nil || got != ".button { color: ZgotmplZ; }" {
		t.Fatalf("%q %v", got, err)
	}
}

func TestNilGuardsPreserveEvaluationOrder(t *testing.T) {
	source := `func Page(b *gox.Builder) {
 var customer *Customer
 calls:=0
 next:=func() *Customer {calls++;return nil}
 <div>
  fmt.Sprintf("name: %s",next().Name)
  false && next().Active
  true || next().Active
  customer == nil || customer.Active
  calls
 </div>
 }
 type Customer struct{Name string;Active bool}`
	got := render(t, source)
	if got != `<div>falsetruetrue1</div>` {
		t.Fatal(got)
	}
	got = render(t, `func Page(b *gox.Builder) {
 var customer *Customer
 <div>
  greet(customer.Name)
  "after"
 </div>
 }
 type Customer struct{Name string}
 func greet(b *gox.Builder,name string) { b.Text(name) }`)
	if got != `<div>after</div>` {
		t.Fatal(got)
	}
}

func TestBuilderHelperEscapingAndTagLifetime(t *testing.T) {
	source := `func Page(b *gox.Builder, name, url string) {
 <div>
  link(name,url)
 </div>
 }
 func link(b *gox.Builder,name,url string) {
  b.Open("a")
  b.Attr("href",url)
  b.Text(name)
  b.Close()
 }`
	if got := render(t, source, values.String("<Alice>"), values.String("javascript:alert(1)")); got != `<div><a href="#ZgotmplZ">&lt;Alice&gt;</a></div>` {
		t.Fatal(got)
	}
	got := render(t, `func Page(b *gox.Builder) {
 <button disabled>
  attr("disabled",false)
  icon()
 </button>
 }
 func icon(b *gox.Builder) {
  b.Open("img")
  b.Attr("src","https://example.com/icon.png")
 }`)
	if got != `<button><img src="https://example.com/icon.png"></button>` {
		t.Fatal(got)
	}
	tmpl, err := gox.Compile("late.gox", `func Page(b *gox.Builder) {
 <p>
  "text"
  class("late")
 </p>
 }`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := tmpl.Link()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Render(context.Background(), nil, gox.RenderOptions{}); err == nil {
		t.Fatal("late attribute accepted")
	}
}

func FuzzCompileGoX(f *testing.F) {
	for _, source := range []string{
		`func Page(b *gox.Builder) {}`, `func Page(b *gox.Builder) {` + "\n" + `<p>{"hello"}</p>` + "\n" + `}`, `func Page(b *gox.Builder, x *X) {` + "\n" + `<p>{x.Name}</p>` + "\n" + `} type X struct{Name string}`,
		`func Page(b *gox.Builder) {` + "\n" + `<Unknown />` + "\n" + `}`, `func Page(b *gox.Builder, x int) {` + "\n" + `<div>{x + "bad"}</div>` + "\n" + `}`,
		`func Page(b *gox.Builder) {` + "\n" + `<script>const x={=unknown};</script>` + "\n" + `}`,
	} {
		f.Add(source)
	}
	f.Fuzz(func(t *testing.T, source string) {
		if len(source) > 2048 {
			t.Skip()
		}
		_, _ = gox.Compile("fuzz.gox", source)
	})
}

func TestRenderingEffectsCannotBypassContextualEscaping(t *testing.T) {
	for _, source := range []string{
		`func Page(b *gox.Builder, payload string) {
 <script>{=hidden(payload)}</script>
 }
 func hidden(b *gox.Builder,payload string)int {b.Text(payload);return 0}`,
		`func Page(b *gox.Builder, payload string) {
 <a title={hidden(payload)}>{"ok"}</a>
 }
 func hidden(b *gox.Builder,payload string)string {b.Text(payload);return "value"}`,
		`func Page(b *gox.Builder, payload string) {
 body := func() {
  <p>{payload}</p>
 }
 <script>{=invoke(body)}</script>
 }
 func invoke(body func())int {body();return 0}`,
		`func Page(b *gox.Builder, payload string) {
 <host>
  <slot-body>
   payload
  </slot-body>
 </host>
 }
 func host(b *gox.Builder, body func()) {
  <script>{=invoke(body)}</script>
 }
 func invoke(body func())int {body();return 0}`,
		`func Page(b *gox.Builder, payload string) {
 <host>
  <slot-body>
   "alert(1);"
  </slot-body>
 </host>
 }
 func host(b *gox.Builder, body func()) {
  <script>{=invoke(body)}</script>
 }
 func invoke(body func())int {body();return 0}`,
		`func Page(b *gox.Builder, payload string) {
 <host>
  <slot-body>
   payload
  </slot-body>
 </host>
 }
 func host(b *gox.Builder, body func()) {
  <a title={invoke(body)}>{"ok"}</a>
 }
 func invoke(body func())int {body();return 0}`,
	} {
		tmpl, err := gox.Compile("effect.gox", source)
		if err != nil {
			t.Fatal(err)
		}
		p, err := tmpl.Link()
		if err != nil {
			t.Fatal(err)
		}
		got, err := p.Render(context.Background(), []values.Value{values.String("alert(1);")}, gox.RenderOptions{})
		if err == nil || got != "" {
			t.Fatalf("unsafe rendering effect: %q %v", got, err)
		}
	}
}

func TestRenderingClosureCannotReturnValue(t *testing.T) {
	_, err := gox.Compile("effect.gox", `func Page(b *gox.Builder) {
 body := func()int {
  <p>{"unsafe"}</p>
  return 0
 }
 <script>{=body()}</script>
 }`)
	if err == nil || !strings.Contains(err.Error(), "rendering functions must not return values") {
		t.Fatal(err)
	}
}

func TestUnicodeAndNativeScriptDelimiters(t *testing.T) {
	source := `func Page(b *gox.Builder) {
 <p title={"Привет 🌍"}>{"Здравствуйте"}</p>
 <script>const k="K";const s={="</script>"};const z="</scriptish>";</SCRIPT>
 <style />
 <textarea />
 <p>{"done"}</p>
 }`
	got := render(t, source)
	want := `<p title="Привет 🌍">Здравствуйте</p><script>const k="K";const s="\u003c/script\u003e";const z="\x3C/scriptish>";</script><style></style><textarea></textarea><p>done</p>`
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestStaticClassInventory(t *testing.T) {
	tmpl, err := gox.Compile("classes.gox", `func Page(b *gox.Builder, tone string) {
 const spacing = "px-4 " + "py-2"
 <div class="base bg-{tone} {"rounded"} suffix">
  class(spacing)
  attr("class", "font-bold")
 </div>
 }`)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, class := range tmpl.Classes {
		seen[class.Name] = true
	}
	for _, name := range []string{"base", "rounded", "suffix", "px-4", "py-2", "font-bold"} {
		if !seen[name] {
			t.Errorf("missing %s: %v", name, tmpl.Classes)
		}
	}
	if len(seen) != 6 || !tmpl.DynamicClasses {
		t.Fatalf("%v dynamic=%v", tmpl.Classes, tmpl.DynamicClasses)
	}
	tmpl, err = gox.Compile("static.gox", `func Page(b *gox.Builder) {
 <div>
  class("static")
 </div>
 }`)
	if err != nil {
		t.Fatal(err)
	}
	if tmpl.DynamicClasses {
		t.Fatal("synthetic helper marked static template dynamic")
	}
}

func TestRootComponentReturningHTML(t *testing.T) {
	got := render(t, `func Page(b *gox.Builder) {
 <hello />
 }
 func hello() gox.HTML { return gox.HTML("<p>hello</p>") }`)
	if got != "<p>hello</p>" {
		t.Fatal(got)
	}
}

func TestNestedAndEmptySlots(t *testing.T) {
	got := render(t, `func Page(b *gox.Builder) {
 <panel>
  <slot-body>
   <panel>
    <slot-body>
     "inner"
    </slot-body>
   </panel>
  </slot-body>
 </panel>
 <panel><slot-body /></panel>
 }
 func panel(b *gox.Builder, body func()) {
  <section>
   body()
  </section>
 }`)
	if got != `<section><section>inner</section></section><section></section>` {
		t.Fatal(got)
	}
}

func TestRendererFunctionsAsValues(t *testing.T) {
	sources := map[string]string{
		"page.gox": `func Page(b *gox.Builder, title string) {
 chosen := Button
 <Wrapper component={chosen} />
 <div>
  Button()
 </div>
 }`,
		"button.gox": `func Button(b *gox.Builder, title string) {
 <button>{title}</button>
 }`,
		"wrapper.gox": `func Wrapper(b *gox.Builder, component func(b *gox.Builder,title string),title string) {
 <section>
  <component />
 </section>
 }`,
	}
	tmpl, err := gox.CompileFiles("page.gox", sources)
	if err != nil {
		t.Fatal(err)
	}
	program, err := tmpl.Link()
	if err != nil {
		t.Fatal(err)
	}
	got, err := program.Render(context.Background(), []values.Value{values.String("Save")}, gox.RenderOptions{Strict: true})
	if err != nil {
		t.Fatal(err)
	}
	if got != `<section><button>Save</button></section><div><button>Save</button></div>` {
		t.Fatal(got)
	}
}
