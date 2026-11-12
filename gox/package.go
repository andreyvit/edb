package gox

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/scanner"
	"go/token"
	"maps"
	"path"
	"slices"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
)

// CompileSources compiles ordinary Go and GoX files into one package. Each GoX
// renderer declares its name and builder parameter. Use RenderFunc to select one.
func CompileSources(sources map[string]string, packages ...evm.Package) (*Template, error) {
	return Options{}.CompileSources(sources, packages...)
}
func (options Options) CompileSources(sources map[string]string, packages ...evm.Package) (*Template, error) {
	return options.compileSources("", sources, packages...)
}

// CompileFiles compiles one package and selects the default renderer by function
// name or filename. Component tags use exact Go callable names.
func CompileFiles(entry string, sources map[string]string, packages ...evm.Package) (*Template, error) {
	return Options{}.CompileFiles(entry, sources, packages...)
}
func (options Options) CompileFiles(entry string, sources map[string]string, packages ...evm.Package) (*Template, error) {
	return options.compileSources(entry, sources, packages...)
}

func (options Options) compileSources(entry string, sources map[string]string, packages ...evm.Package) (*Template, error) {
	if options.Format > CSSFormat {
		return nil, fmt.Errorf("invalid output format")
	}
	c := &compiler{
		files:    token.NewFileSet(),
		mappings: map[string][]evm.SourcePosition{}, originals: map[string]*token.File{},
		outputMaps: map[string]*generatedMap{}, implicitCalls: map[*ast.CallExpr]bool{},
		packages: append(slices.Clone(packages), runtimePackage()),
		template: &Template{sources: maps.Clone(sources), format: options.Format, GeneratedSources: map[string]string{}},
	}
	keys := slices.Sorted(maps.Keys(sources))
	if len(keys) == 0 {
		return nil, fmt.Errorf("a source file is required")
	}
	pkgName := "plugin"
	for _, name := range keys {
		var scan scanner.Scanner
		source := sources[name]
		scan.Init(token.NewFileSet().AddFile(name, -1, len(source)), []byte(source), nil, 0)
		if _, kind, _ := scan.Scan(); kind == token.PACKAGE {
			_, _, pkgName = scan.Scan()
			break
		}
	}
	total := 0
	for _, name := range keys {
		source := sources[name]
		if name == "_gox_helpers.go" {
			return nil, fmt.Errorf("%s is reserved for GoX helpers", name)
		}
		total += len(source)
		if total > evmcompiler.MaxSourceSize {
			return nil, fmt.Errorf("combined GoX sources exceed %d bytes", evmcompiler.MaxSourceSize)
		}
		generated := source
		var mapping []int
		if path.Ext(name) == ".gox" {
			var marks []markup
			var err error
			generated, marks, mapping, err = preprocess(name, source, len(c.marks))
			if err != nil {
				return nil, err
			}
			c.marks = append(c.marks, marks...)
		} else if path.Ext(name) != ".go" {
			return nil, fmt.Errorf("%s: source filename must end in .go or .gox", name)
		}
		positions := make([]evm.SourcePosition, len(source)+1)
		line, column := 1, 1
		for i := range positions {
			positions[i] = evm.SourcePosition{File: name, Line: line, Column: column}
			if i < len(source) && source[i] == '\n' {
				line, column = line+1, 1
			} else {
				column++
			}
		}
		if mapping != nil {
			mapped := make([]evm.SourcePosition, len(mapping))
			for i, offset := range mapping {
				mapped[i] = positions[offset]
			}
			positions = mapped
		}
		var scan scanner.Scanner
		scan.Init(token.NewFileSet().AddFile(name, -1, len(generated)), []byte(generated), nil, 0)
		if _, kind, _ := scan.Scan(); kind != token.PACKAGE {
			prefix := "package " + pkgName + "\n"
			generated = prefix + generated
			positions = append(make([]evm.SourcePosition, len(prefix)), positions...)
		}
		c.mappings[name] = positions
		file, err := parser.ParseFile(c.files, name, generated, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return nil, c.parseError(err)
		}
		c.originals[name] = c.files.File(file.Pos())
		c.syntax = append(c.syntax, file)
		for _, d := range file.Decls {
			f, ok := d.(*ast.FuncDecl)
			if !ok || f.Body == nil {
				continue
			}
			builder := findBuilder(f.Type)
			if hasMarkup(f.Body) && builder == "" {
				return nil, c.error(f.Pos(), "rendering functions require an explicit first *gox.Builder parameter")
			}
			if builder == "" {
				continue
			}
			if len(f.Type.Params.List[0].Names) != 1 || f.Type.Params.List[0].Names[0].Name != builder {
				return nil, c.error(f.Pos(), "the builder must be the first parameter")
			}
			if f.Type.Results != nil && len(f.Type.Results.List) > 0 {
				if hasMarkup(f.Body) {
					return nil, c.error(f.Pos(), "rendering functions must not return values")
				}
				// Ordinary Go helpers may accept a builder and return a value.
				// They are callable from renderers, but are not render entries.
				continue
			}
			if f.Recv != nil {
				continue
			}
			if f.Name.Name == entry || name == entry && c.template.Entry == "" {
				c.template.Entry = f.Name.Name
				for _, field := range f.Type.Params.List[1:] {
					for _, id := range field.Names {
						c.template.Parameters = append(c.template.Parameters, Parameter{id.Name, nodeString(field.Type)})
					}
				}
			}
		}
		if path.Ext(name) == ".gox" {
			c.file = file
			c.autoImports()
		}
	}
	if entry != "" && c.template.Entry == "" {
		return nil, fmt.Errorf("renderer %q is missing", entry)
	}
	// Helpers occupy their own file, so normal Go imports remain file-local.
	helper, err := parser.ParseFile(c.files, "_gox_helpers.go", "package "+c.syntax[0].Name.Name+`
import "gox"
func _goxMark(id int, args ...any) {}
func class(b *gox.Builder, value string) { gox.Class(b, value) }
func attr(b *gox.Builder, name string, value any) { gox.Attr(b, name, value) }
`, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	c.syntax = append(c.syntax, helper)
	for round := 0; ; round++ {
		if round >= 64 {
			return nil, fmt.Errorf("GoX type lowering did not converge")
		}
		c.pkg, c.info, err = evmcompiler.AnalyzeFiles(c.files, c.syntax, c.packages...)
		if c.info == nil {
			return nil, err
		}
		changed := false
		for _, file := range c.syntax {
			if path.Ext(c.files.File(file.Pos()).Name()) != ".gox" {
				continue
			}
			c.file = file
			c.markImplicitCalls()
			changed = c.pipelines() || changed
			changed = c.resolveCallsAndFields() || changed
		}
		if changed {
			continue
		}
		for _, file := range c.syntax {
			c.file = file
			componentChanged, componentErr := c.components()
			if componentErr != nil {
				return nil, componentErr
			}
			if componentChanged {
				changed = true
				c.autoImports()
			}
		}
		if !changed {
			break
		}
	}
	c.pkg, c.info, err = evmcompiler.AnalyzeFiles(c.files, c.syntax, c.packages...)
	if c.info == nil {
		return nil, err
	}
	for _, file := range c.syntax {
		c.file = file
		if file != helper {
			c.collectClasses()
		}
		if path.Ext(c.files.File(file.Pos()).Name()) != ".gox" {
			continue
		}
		for _, d := range file.Decls {
			f, ok := d.(*ast.FuncDecl)
			if !ok || f.Body == nil {
				continue
			}
			c.builder = findBuilder(f.Type)
			state := c.initialState(c.builder != "")
			if err := c.block(f.Body, &state); err != nil {
				return nil, err
			}
			if len(state.tags) != 0 {
				return nil, c.error(f.End(), state.unclosedMessage())
			}
			if c.template.format == CSSFormat && c.builder != "" {
				if _, err := state.context.Text("</style>"); err != nil {
					return nil, c.error(f.End(), err.Error())
				}
			}
			if err := state.context.Finish(); err != nil {
				return nil, c.error(f.End(), err.Error())
			}
		}
		c.batchOperations()
	}
	helper.Decls = slices.DeleteFunc(helper.Decls, func(d ast.Decl) bool {
		f, ok := d.(*ast.FuncDecl)
		return ok && f.Name.Name == "_goxMark"
	})
	for _, file := range c.syntax {
		c.file = file
		name := c.files.File(file.Pos()).Name()
		if path.Ext(name) == ".gox" {
			c.pruneImports()
		}
		var output bytes.Buffer
		if err := (&printer.Config{Mode: printer.UseSpaces | printer.TabIndent | printer.SourcePos, Tabwidth: 8}).Fprint(&output, c.files, file); err != nil {
			return nil, err
		}
		c.template.GeneratedSources[name+".go"] = output.String()
		c.template.GeneratedSource += output.String() + "\n"
	}
	module, err := evmcompiler.CompileFiles(c.template.GeneratedSources, c.packages...)
	if err != nil {
		return nil, c.remapCompileError(err)
	}
	c.remapModule(module)
	c.template.Module = module
	return c.template, nil
}

// Mark calls while the original element and fragment boundaries still exist.
// Statements outside them retain ordinary Go argument checking.
func (c *compiler) markImplicitCalls() {
	for _, d := range c.file.Decls {
		f, ok := d.(*ast.FuncDecl)
		if !ok || f.Body == nil {
			continue
		}
		depth := 0
		ast.Inspect(f.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if m, yes := c.marker(call); yes {
				if _, _, component := c.resolveComponent(m.name, call.Pos()); component {
					m.void = m.selfClosing
				}
				for _, arg := range call.Args[1:] {
					ast.Inspect(arg, func(n ast.Node) bool {
						if call, ok := n.(*ast.CallExpr); ok {
							c.implicitCalls[call] = true
						}
						return true
					})
				}
				if m.kind == markupOpen && !m.void {
					depth++
				} else if m.kind == markupClose {
					depth--
				}
			} else if depth > 0 {
				c.implicitCalls[call] = true
			}
			return true
		})
	}
}

type generatedMap struct {
	file  *token.File
	files *token.FileSet
	lines []int
}

func (c *compiler) generatedPosition(pos evm.SourcePosition) evm.SourcePosition {
	source, ok := c.template.GeneratedSources[pos.File]
	if !ok {
		return pos
	}
	m := c.outputMaps[pos.File]
	if m == nil {
		m = &generatedMap{files: token.NewFileSet(), lines: []int{0}}
		m.file = m.files.AddFile(pos.File, -1, len(source))
		var scan scanner.Scanner
		scan.Init(m.file, []byte(source), nil, scanner.ScanComments)
		for {
			if _, t, _ := scan.Scan(); t == token.EOF {
				break
			}
		}
		for i := range source {
			if source[i] == '\n' {
				m.lines = append(m.lines, i+1)
			}
		}
		c.outputMaps[pos.File] = m
	}
	offset := 0
	if pos.Line > 0 && pos.Line <= len(m.lines) {
		offset = m.lines[pos.Line-1]
	}
	offset = min(len(source), offset+max(0, pos.Column-1))
	p := m.files.PositionFor(m.file.Pos(offset), true)
	if original := c.originals[p.Filename]; original != nil && p.Line > 0 && p.Line <= original.LineCount() {
		offset := original.Offset(original.LineStart(p.Line)) + max(0, p.Column-1)
		if positions := c.mappings[p.Filename]; offset < len(positions) && positions[offset].File != "" {
			return positions[offset]
		}
	}
	return evm.SourcePosition{File: p.Filename, Line: p.Line, Column: p.Column}
}
