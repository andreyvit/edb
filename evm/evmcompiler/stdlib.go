package evmcompiler

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"slices"

	"golang.org/x/tools/go/ssa"

	"github.com/andreyvit/edb/evm"
)

type standardPackage struct {
	source string
	files  []*ast.File
	info   *types.Info
}

func (e *typeEnvironment) importStandard(path, source string) (*types.Package, error) {
	file, err := parser.ParseFile(e.files, "evm/_stdlib/"+path+".go", source, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	for _, decl := range file.Decls {
		if d, ok := decl.(*ast.GenDecl); ok && d.Tok == token.VAR {
			return nil, fmt.Errorf("virtual package %q cannot declare globals", path)
		}
		if d, ok := decl.(*ast.FuncDecl); ok && d.Recv == nil && d.Name.Name == "init" {
			return nil, fmt.Errorf("virtual package %q cannot declare init functions", path)
		}
	}
	info := &types.Info{Implicits: make(map[ast.Node]types.Object), FileVersions: make(map[*ast.File]string), Types: make(map[ast.Expr]types.TypeAndValue), Defs: make(map[*ast.Ident]types.Object), Uses: make(map[*ast.Ident]types.Object), Selections: make(map[*ast.SelectorExpr]*types.Selection), Scopes: make(map[ast.Node]*types.Scope), Instances: make(map[*ast.Ident]types.Instance)}
	config := types.Config{GoVersion: "go1.23", Sizes: types.SizesFor("gc", "amd64"), Importer: e}
	pkg, err := config.Check(path, e.files, []*ast.File{file}, info)
	if err != nil {
		return nil, err
	}
	e.imported[path] = pkg
	e.standard[path] = &standardPackage{source: source, files: []*ast.File{file}, info: info}
	for _, decl := range file.Decls {
		f, ok := decl.(*ast.FuncDecl)
		if !ok || f.Body != nil {
			continue
		}
		obj := info.Defs[f.Name].(*types.Func)
		// Defer VM type/layout construction until this leaf is reachable.
		e.natives[obj] = nativeBinding{importKey{pkg: path, name: obj.Name()}, nil}
	}
	return pkg, nil
}

func (c *typedCompiler) debugPrintImport(args []ssa.Value, newline bool, pos token.Pos) (evm.FunctionRef, evm.Signature) {
	var sig evm.Signature
	for _, v := range args {
		s, err := c.env.shape(v.Type())
		if err != nil {
			c.fail(pos, "%v", err)
			return evm.FunctionRef{}, sig
		}
		sig.Params = append(sig.Params, s.Type)
	}
	name := "print"
	if newline {
		name = "println"
	}
	for i := range c.module.Imports {
		imp := &c.module.Imports[i]
		if imp.Package == "evm/debug" && imp.Name == name && slices.Equal(imp.Signature.Params, sig.Params) {
			return evm.FunctionRef{Native: true, Index: uint32(i)}, sig
		}
	}
	idx := uint32(len(c.module.Imports))
	c.module.Imports = append(c.module.Imports, evm.Import{Package: "evm/debug", Name: name, Signature: sig})
	return evm.FunctionRef{Native: true, Index: idx}, sig
}
func (f *lowerer) printBuiltin(call *ssa.Call, newline bool) {
	ref, sig := f.c.debugPrintImport(call.Call.Args, newline, call.Pos())
	layout, err := evm.LayoutOf(sig.Params)
	if err != nil {
		f.c.fail(call.Pos(), "%v", err)
		return
	}
	f.outR = max(f.outR, layout.RCount)
	f.outP = max(f.outP, layout.PCount)
	for i, v := range call.Call.Args {
		f.outMove(layout.Slots[i], f.regs[v][0], false)
	}
	f.emit(evm.WithBC(evm.OpCallHost, 0, ref.Index))
}

// SourceImporter exposes only EVM standard packages when checking host declarations.
func SourceImporter(files *token.FileSet) types.Importer {
	e, err := newTypeEnvironment(nil)
	if err != nil {
		panic(err)
	}
	e.files = files
	return &sourceImporter{e}
}

type sourceImporter struct{ *typeEnvironment }

func (s *sourceImporter) Import(path string) (*types.Package, error) {
	if path == "github.com/andreyvit/edb/evm/evmruntime/log" {
		source, _ := evm.StandardLibrarySource("evmruntime/log")
		return s.importStandard(path, source)
	}
	return s.typeEnvironment.Import(path)
}
