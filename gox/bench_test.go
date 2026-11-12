package gox_test

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"html/template"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/gox"
	"github.com/andreyvit/edb/values"
)

//go:embed testdata/bench/*.gox testdata/bench/*.gohtml
var benchmarkSources embed.FS

type Screen struct {
	Key, Heading, Caption, SignIn, Notice string
	ReadOnly, Compact                     bool
}
type Palette struct{ Section, Tile, Action, Category, Detail, Date, Expiry, Accent string }
type Icon struct{ URL, Alt string }
type Field struct{ Key, Label, Value, Hint string }
type Choice struct {
	Key, Label string
	Checked    bool
}
type Tile struct {
	Key, Title, Description, Mode, URL, CTA, Consent, Error string
	Icon                                                    *Icon
	Amount                                                  int
	Complete, Disabled                                      bool
	Fields                                                  []*Field
	Choices                                                 []*Choice
}
type Record struct {
	Key, Code, CopyData, Description, Date, Expires string
	Amount                                          int
	Credit, Expired                                 bool
}
type Cell struct {
	Text              string
	Included, Current bool
}
type Feature struct {
	Title string
	Cells []*Cell
}
type Plan struct {
	Key, Name, Subtitle, Threshold, URL string
	Icon                                *Icon
	Current                             bool
	Features                            []*Feature
}
type benchScene struct {
	Screen   *Screen
	Palette  *Palette
	Tiles    []*Tile
	Records  []*Record
	Plans    []*Plan
	Features []*Feature
}
type tileScope struct {
	Scene *benchScene
	Tile  *Tile
}
type recordScope struct {
	Scene  *benchScene
	Record *Record
}

func benchmarkScene(n int, hostile bool) *benchScene {
	suffix := " & more"
	url := "https://assets.example.invalid/item?q=a&view=wide"
	if hostile {
		suffix = ` <img src=x onerror="alert(1)"> " + `
		url = "javascript:alert(1)"
	}
	s := &benchScene{
		Screen:  &Screen{Key: "workspace", Heading: "Your workshop" + suffix, Caption: "Choose your next activity", SignIn: url, Notice: "Changes saved", ReadOnly: hostile, Compact: hostile},
		Palette: &Palette{Section: "space-y-6 px-4", Tile: "bg-slate-50 text-slate-900 p-6", Action: "rounded px-4 py-3 font-medium", Category: "rounded-full px-4 py-2", Detail: "text-sm leading-relaxed", Date: "text-xs tabular-nums", Expiry: "text-xs italic", Accent: "#245678"},
	}
	for i := range n {
		key := strconv.Itoa(i)
		tile := &Tile{Key: key, Title: "Activity " + key + suffix, Description: "A short explanation with a useful next step" + suffix,
			Mode: []string{"fields", "choices", "link", "plain"}[i%4], URL: url, CTA: "Continue", Consent: "Keep me informed", Amount: 25 * (i + 1),
			Complete: i%3 == 0, Disabled: i%5 == 0, Icon: &Icon{url, "Activity icon" + suffix}}
		for j := range 3 {
			tile.Fields = append(tile.Fields, &Field{Key: strconv.Itoa(j), Label: []string{"Year", "Month", "Day"}[j], Value: strconv.Itoa(j + 1), Hint: "Enter value" + suffix})
		}
		for j := range 4 {
			tile.Choices = append(tile.Choices, &Choice{Key: strconv.Itoa(j), Label: "Option " + strconv.Itoa(j) + suffix, Checked: j == i%4})
		}
		if i%6 == 1 {
			tile.Error = "Please review your entry"
		}
		if i%7 == 2 {
			tile.Icon = nil
			tile.Consent = ""
		}
		s.Tiles = append(s.Tiles, tile)
		record := &Record{Key: key, Description: "Scheduled adjustment " + key + suffix, Date: "2030-04-17 10:30", Amount: (i%9 - 4) * 75, Credit: i%2 == 0, Expired: i%5 == 0}
		if i%3 == 0 {
			record.Code = "SAMPLE-" + key + suffix
			record.CopyData = `{"items":[{"sku":"sample","quantity":2}]}`
		}
		if i%4 != 1 {
			record.Expires = "2031-08-12"
		}
		s.Records = append(s.Records, record)
	}
	plans := 4
	if n > 24 {
		plans = 8
	}
	for i := range plans {
		s.Plans = append(s.Plans, &Plan{Key: strconv.Itoa(i), Name: "Edition " + strconv.Itoa(i) + suffix, Subtitle: "Designed for your next stage", Threshold: fmt.Sprintf("%d credits", i*500), URL: url, Icon: &Icon{url, "Edition icon"}, Current: i == 1})
	}
	for i := range plans * 2 {
		feature := &Feature{Title: "Capability " + strconv.Itoa(i) + suffix}
		for j, p := range s.Plans {
			cell := &Cell{Current: p.Current, Included: (i+j)%3 == 0}
			if (i+j)%3 == 1 {
				cell.Text = fmt.Sprintf("%d per month", (i+1)*(j+1))
			}
			feature.Cells = append(feature.Cells, cell)
			p.Features = append(p.Features, &Feature{Title: feature.Title, Cells: []*Cell{cell}})
		}
		s.Features = append(s.Features, feature)
	}
	return s
}

// The adapter is setup-only: each scalar, pointer and slice occupies one EVM cell.
func benchmarkValue(v reflect.Value) values.Value {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return values.Value{}
		}
		return benchmarkValue(v.Elem())
	case reflect.Struct:
		cells := make([]values.Value, v.NumField())
		for i := range cells {
			cells[i] = benchmarkValue(v.Field(i))
		}
		return values.Value{P: unsafe.Pointer(unsafe.SliceData(cells))}
	case reflect.Slice:
		cells := make([]values.Value, v.Len())
		for i := range cells {
			cells[i] = benchmarkValue(v.Index(i))
		}
		return values.SliceValue(unsafe.Pointer(unsafe.SliceData(cells)), uint32(len(cells)), uint32(len(cells)))
	case reflect.String:
		return values.String(v.String())
	case reflect.Bool:
		return values.Bool(v.Bool())
	case reflect.Int:
		return values.Int(v.Int())
	default:
		panic(v.Type())
	}
}
func benchmarkTypes() string {
	var out strings.Builder
	for _, typ := range []reflect.Type{reflect.TypeFor[Screen](), reflect.TypeFor[Palette](), reflect.TypeFor[Icon](), reflect.TypeFor[Field](), reflect.TypeFor[Choice](), reflect.TypeFor[Tile](), reflect.TypeFor[Record](), reflect.TypeFor[Cell](), reflect.TypeFor[Feature](), reflect.TypeFor[Plan]()} {
		fmt.Fprintf(&out, "\ntype %s struct {\n", typ.Name())
		for i := range typ.NumField() {
			f := typ.Field(i)
			fmt.Fprintf(&out, "%s %s\n", f.Name, strings.ReplaceAll(f.Type.String(), "gox_test.", ""))
		}
		out.WriteString("}\n")
	}
	return out.String()
}

type benchRenderers struct {
	gox      *gox.Program
	standard *template.Template
	args     []values.Value
	scene    *benchScene
}

func prepareBenchmark(tb testing.TB, name string, n int, hostile bool) benchRenderers {
	tb.Helper()
	scene := benchmarkScene(n, hostile)
	sources := map[string]string{}
	for _, file := range []string{name + ".gox", "action.gox"} {
		if file == "action.gox" && name != "workshop" {
			continue
		}
		data, err := benchmarkSources.ReadFile("testdata/bench/" + file)
		if err != nil {
			tb.Fatal(err)
		}
		sources[file] = string(data)
	}
	sources[name+".gox"] += benchmarkTypes()
	tmpl, err := gox.CompileFiles(name+".gox", sources)
	if err != nil {
		tb.Fatal(err)
	}
	p, err := tmpl.Link()
	if err != nil {
		tb.Fatal(err)
	}
	data, err := benchmarkSources.ReadFile("testdata/bench/" + name + ".gohtml")
	if err != nil {
		tb.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	for i := range lines {
		lines[i] = strings.TrimSpace(lines[i])
	}
	standard, err := template.New(name).Funcs(template.FuncMap{
		"tileScope":   func(s *benchScene, t *Tile) tileScope { return tileScope{s, t} },
		"recordScope": func(s *benchScene, r *Record) recordScope { return recordScope{s, r} },
	}).Parse(strings.Join(lines, ""))
	if err != nil {
		tb.Fatal(err)
	}
	roots := []any{scene.Screen, scene.Palette}
	switch name {
	case "workshop":
		roots = append(roots, scene.Tiles)
	case "journal":
		roots = append(roots, scene.Records)
	case "comparison":
		roots = append(roots, scene.Plans, scene.Features)
	}
	args := make([]values.Value, len(roots))
	for i, v := range roots {
		args[i] = benchmarkValue(reflect.ValueOf(v))
	}
	r := benchRenderers{p, standard, args, scene}
	r.check(tb)
	return r
}
func (r benchRenderers) check(tb testing.TB) {
	tb.Helper()
	got, err := r.gox.Render(context.Background(), r.args, gox.RenderOptions{Strict: true})
	if err != nil {
		tb.Fatal(err)
	}
	var buf bytes.Buffer
	if err := r.standard.Execute(&buf, r.scene); err != nil {
		tb.Fatal(err)
	}
	want := buf.String()
	if got != want {
		i := 0
		for i < min(len(got), len(want)) && got[i] == want[i] {
			i++
		}
		tb.Fatalf("output differs at %d:\nGoX: %q\nGo:  %q", i, got[max(0, i-80):min(len(got), i+180)], want[max(0, i-80):min(len(want), i+180)])
	}
}
func TestApplicationTemplates(t *testing.T) {
	for _, name := range []string{"workshop", "journal", "comparison"} {
		for _, hostile := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/hostile=%v", name, hostile), func(t *testing.T) { prepareBenchmark(t, name, 12, hostile) })
		}
	}
}

var benchmarkOutput string

func BenchmarkApplication(b *testing.B) {
	for _, name := range []string{"workshop", "journal", "comparison"} {
		for _, n := range []int{12, 48} {
			b.Run(fmt.Sprintf("%s/%d", name, n), func(b *testing.B) {
				r := prepareBenchmark(b, name, n, false)
				b.Run("GoX", func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						var err error
						benchmarkOutput, err = r.gox.Render(context.Background(), r.args, gox.RenderOptions{})
						if err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("GoTemplate", func(b *testing.B) {
					var buf bytes.Buffer
					if err := r.standard.Execute(&buf, r.scene); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						buf.Reset()
						if err := r.standard.Execute(&buf, r.scene); err != nil {
							b.Fatal(err)
						}
						benchmarkOutput = buf.String()
					}
				})
			})
		}
	}
}
