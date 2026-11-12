package evmcompiler

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"go/types"
	"go/version"
	"maps"
	"slices"
	"strings"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
	"golang.org/x/tools/go/ssa"
)

type typedCompiler struct {
	ownsDeferStack map[*ssa.Function]bool
	deferValues    map[ssa.Value]bool
	deferBuiltins  map[*ssa.Defer]evm.FunctionRef
	hostDynamic    []types.Type
	dynamic        map[*values.Shape]uint32
	files          *token.FileSet
	filename       string
	addedLines     map[string]int
	errors         Diagnostics
	env            *typeEnvironment
	pkg            *ssa.Package
	module         evm.Module
	functions      map[*ssa.Function]uint32
	globals        map[*ssa.Global]uint32
	imports        map[importKey]uint32
	words          map[uint64]uint32
	strings        map[string]uint32
	conversions    map[evm.Conversion]uint16
	shapes         map[*values.Shape]uint16
	refs           map[*ssa.Function]uint32
	invokes        map[evm.Invoke]uint32
	callSignatures map[*values.Shape]uint32
}

func compileTyped(filename, source string, packages []evm.Package, options Options) (*evm.Module, error) {
	return compileTypedFiles(map[string]string{filename: source}, packages, options)
}

func compileTypedFiles(sources map[string]string, packages []evm.Package, options Options) (*evm.Module, error) {
	filenames := slices.Sorted(maps.Keys(sources))
	if len(filenames) == 0 {
		return nil, Diagnostics{{Message: "a source file is required"}}
	}
	filename := filenames[0]
	c := &typedCompiler{addedLines: make(map[string]int), invokes: make(map[evm.Invoke]uint32), dynamic: make(map[*values.Shape]uint32), filename: filename, files: token.NewFileSet(), functions: make(map[*ssa.Function]uint32), globals: make(map[*ssa.Global]uint32), imports: make(map[importKey]uint32), words: make(map[uint64]uint32), strings: make(map[string]uint32), conversions: make(map[evm.Conversion]uint16), shapes: make(map[*values.Shape]uint16), refs: make(map[*ssa.Function]uint32), callSignatures: make(map[*values.Shape]uint32)}
	c.module.NoOptimization = options.NoOptimization
	c.module.StrictCallWindows = true
	total := 0
	for _, source := range sources {
		total += len(source)
	}
	if total > MaxSourceSize {
		c.fail(token.NoPos, "source exceeds %d bytes", MaxSourceSize)
		return nil, c.errors
	}
	var err error
	c.env, err = newTypeEnvironment(packages)
	if err != nil {
		c.fail(token.NoPos, "%v", err)
		return nil, c.errors
	}
	c.env.files = c.files
	c.module.Sources = maps.Clone(sources)
	packageName := "plugin"
	for _, filename := range filenames {
		source := sources[filename]
		var scan scanner.Scanner
		scan.Init(token.NewFileSet().AddFile(filename, -1, len(source)), []byte(source), nil, 0)
		if _, kind, _ := scan.Scan(); kind == token.PACKAGE {
			_, _, packageName = scan.Scan()
			break
		}
	}
	var syntax []*ast.File
	for _, filename := range filenames {
		source := sources[filename]
		var scan scanner.Scanner
		scan.Init(token.NewFileSet().AddFile(filename, -1, len(source)), []byte(source), nil, 0)
		if _, kind, _ := scan.Scan(); kind != token.PACKAGE {
			source = "package " + packageName + "\n" + source
			c.addedLines[filename] = 1
		}
		file, err := parser.ParseFile(c.files, filename, source, parser.SkipObjectResolution)
		if err != nil {
			for _, e := range err.(scanner.ErrorList) {
				var pos token.Pos
				c.files.Iterate(func(f *token.File) bool {
					if f.Name() == filename {
						pos = f.Pos(e.Pos.Offset)
						return false
					}
					return true
				})
				c.fail(pos, "%s", e.Msg)
			}
			continue
		}
		if version.Compare(file.GoVersion, "go1.23") > 0 {
			c.fail(file.Pos(), "language version %s is not supported; use Go 1.23", file.GoVersion)
		}
		syntax = append(syntax, file)
	}
	if len(c.errors) > 0 {
		return nil, c.errors
	}
	file := syntax[0]
	aliases := map[string]string{}
	check := func() (*types.Package, *types.Info, error) {
		info := &types.Info{Implicits: make(map[ast.Node]types.Object), FileVersions: make(map[*ast.File]string), Types: make(map[ast.Expr]types.TypeAndValue), Defs: make(map[*ast.Ident]types.Object), Uses: make(map[*ast.Ident]types.Object), Selections: make(map[*ast.SelectorExpr]*types.Selection), Scopes: make(map[ast.Node]*types.Scope), Instances: make(map[*ast.Ident]types.Instance)}
		config := types.Config{GoVersion: "go1.23", Sizes: types.SizesFor("gc", "amd64"), Importer: c.env, Error: func(err error) {
			e := err.(types.Error)
			msg := e.Msg
			if strings.Contains(msg, "Evm$") {
				for internal, public := range c.env.recordOperations {
					msg = strings.ReplaceAll(msg, internal, public)
				}
			}
			c.fail(e.Pos, "%s", msg)
		}}
		pkg := types.NewPackage("plugin", "")
		c.env.guest = pkg
		for name, path := range aliases {
			owner, err := c.env.Import(path)
			if err != nil {
				c.fail(file.Pos(), "%v", err)
				return nil, nil, err
			}
			pkg.Scope().Insert(owner.Scope().Lookup(name))
		}
		err := types.NewChecker(&config, c.files, pkg, info).Files(syntax)
		return pkg, info, err
	}
	for _, p := range packages {
		if len(p.Records) > 0 {
			c.env.recordFacade = true
			break
		}
	}
	pkg, info, err := check()
	rewritten := false
	if len(c.env.recordOperations) > 0 {
		for _, file := range syntax {
			rewritten = c.env.rewriteRecordOperations(file, info, aliases) || rewritten
		}
	}
	if rewritten {
		// The first check identifies types, including local aliases. Check the
		// rewritten calls against their receiver-free signatures for diagnostics.
		c.errors = nil
		pkg, info, err = check()
	}
	if err != nil {
		return nil, c.errors
	}
	if c.env.recordFacade && len(c.env.recordFields) > 0 {
		for _, file := range syntax {
			rewriteRecordFields(file, info, c.env.recordFields)
		}
		c.env, err = newTypeEnvironment(packages)
		if err != nil {
			return nil, err
		}
		c.env.files = c.files
		pkg, info, err = check()
		if err != nil {
			return nil, c.errors
		}
	}
	needsInit := false
	for _, file := range syntax {
		ast.Inspect(file, func(n ast.Node) bool {
			switch n.(type) {
			case *ast.GoStmt, *ast.ChanType, *ast.SendStmt, *ast.SelectStmt:
				c.fail(n.Pos(), "channels and goroutines are not supported")
			}
			return true
		})
		for _, decl := range file.Decls {
			if d, ok := decl.(*ast.GenDecl); ok && d.Tok == token.VAR {
				needsInit = true
			}
			if f, ok := decl.(*ast.FuncDecl); ok && f.Name.Name == "init" {
				needsInit = true
			}
			if f, ok := decl.(*ast.FuncDecl); ok && f.Body == nil {
				c.fail(f.Pos(), "function body is required")
			}
		}
		if len(c.errors) > 0 {
			return nil, c.errors
		}
	}
	prog := ssa.NewProgram(c.files, ssa.InstantiateGenerics|ssa.SanityCheckFunctions)
	var createPackage func(*types.Package)
	created := make(map[*types.Package]bool)
	createPackage = func(p *types.Package) {
		if created[p] {
			return
		}
		created[p] = true
		for _, child := range p.Imports() {
			createPackage(child)
		}
		if std := c.env.standard[p.Path()]; std != nil {
			prog.CreatePackage(p, std.files, std.info, true).Build()
		} else {
			prog.CreatePackage(p, nil, nil, true)
		}
	}
	for _, p := range pkg.Imports() {
		createPackage(p)
	}
	// Host signatures can reference source packages without a direct guest import.
	for _, path := range slices.Sorted(maps.Keys(c.env.imported)) {
		createPackage(c.env.imported[path])
	}
	c.pkg = prog.CreatePackage(pkg, syntax, info, true)
	c.pkg.Build()
	allDecls := &ast.File{}
	for _, file := range syntax {
		allDecls.Decls = append(allDecls.Decls, file.Decls...)
	}
	functions := c.discoverFunctions(prog, allDecls, info, needsInit)
	c.markDeferValues(functions)
	if needsInit {
		c.module.Init = "init"
		var globals []*ssa.Global
		for _, m := range c.pkg.Members {
			if g, ok := m.(*ssa.Global); ok {
				globals = append(globals, g)
			}
		}
		slices.SortFunc(globals, func(a, b *ssa.Global) int { return strings.Compare(a.Name(), b.Name()) })
		for _, g := range globals {
			s, err := c.env.shape(g.Type().Underlying().(*types.Pointer).Elem())
			if err != nil {
				c.fail(g.Pos(), "%v", err)
				continue
			}
			c.globals[g] = uint32(len(c.module.Globals))
			c.module.Globals = append(c.module.Globals, s)
		}
	}
	for _, f := range functions {
		sig, err := c.env.signature(f.Signature)
		if err != nil {
			c.fail(f.Pos(), "%v", err)
			continue
		}
		c.functions[f] = uint32(len(c.module.Functions))
		name, internal := c.functionName(f, len(c.module.Functions))
		c.module.Functions = append(c.module.Functions, evm.Function{Method: f.Signature.Recv() != nil, RecoverTransparent: strings.HasPrefix(f.Synthetic, "bound method wrapper") || strings.HasPrefix(f.Synthetic, "wrapper for") || strings.HasPrefix(f.Synthetic, "thunk for"), Internal: internal, Name: name, Position: c.position(f.Pos()), Signature: sig})
	}
	if len(c.errors) > 0 {
		return nil, c.errors
	}
	for _, p := range packages {
		if len(p.Records) > 0 {
			c.registerJSONTypes(token.NoPos)
			break
		}
	}
	c.prepareDeferBuiltins(functions)
	for _, t := range c.hostDynamic {
		c.dynamicType(t, token.NoPos)
	}
	if !options.NoOptimization {
		for _, f := range functions {
			c.simplifyScalars(f)
		}
	}
	for _, f := range functions {
		c.lowerFunction(f)
	}
	if len(c.module.Functions) == 0 && len(c.errors) == 0 {
		c.fail(file.End(), "a function is required")
	}
	if len(c.errors) > 0 {
		return nil, c.errors
	}
	for _, s := range c.env.shapeOrder {
		if _, ok := c.shapes[s]; !ok {
			if len(c.module.Shapes) >= 65536 {
				c.fail(token.NoPos, "too many storage shapes")
				break
			}
			c.shapes[s] = uint16(len(c.module.Shapes))
			c.module.Shapes = append(c.module.Shapes, s)
		}
	}
	if len(c.errors) > 0 {
		return nil, c.errors
	}
	for _, pkg := range c.env.standard {
		c.module.Sources[c.files.PositionFor(pkg.files[0].Pos(), false).Filename] = pkg.source
	}
	return &c.module, nil
}
func (c *typedCompiler) position(pos token.Pos) evm.SourcePosition {
	p := c.files.PositionFor(pos, false)
	if p.Filename == "" {
		p.Filename = c.filename
	}
	return evm.SourcePosition{File: p.Filename, Line: max(1, p.Line-c.addedLines[p.Filename]), Column: max(1, p.Column)}
}
func (c *typedCompiler) fail(pos token.Pos, format string, args ...any) {
	c.errors = append(c.errors, Diagnostic{Position: c.position(pos), Message: fmt.Sprintf(format, args...)})
}
func (c *typedCompiler) word(x uint64) uint32 {
	if i, ok := c.words[x]; ok {
		return i
	}
	i := uint32(len(c.module.Words))
	c.words[x] = i
	c.module.Words = append(c.module.Words, x)
	return i
}
func (c *typedCompiler) string(s string) uint32 {
	if i, ok := c.strings[s]; ok {
		return i
	}
	i := uint32(len(c.module.Strings))
	c.strings[s] = i
	c.module.Strings = append(c.module.Strings, s)
	return i
}
