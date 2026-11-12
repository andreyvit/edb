package evmcompiler

import (
	"go/ast"
	"go/token"
	"go/types"

	"github.com/andreyvit/edb/evm"
)

// Analyze checks a frontend's AST against the same virtual packages as Compile.
// It returns partial type information with diagnostics so a frontend can resolve
// its own syntax, then require a successful check after lowering to ordinary Go.
func Analyze(files *token.FileSet, file *ast.File, packages ...evm.Package) (*types.Package, *types.Info, error) {
	return AnalyzeFiles(files, []*ast.File{file}, packages...)
}

// AnalyzeFiles checks a package while preserving each file's import scope.
func AnalyzeFiles(files *token.FileSet, syntax []*ast.File, packages ...evm.Package) (*types.Package, *types.Info, error) {
	e, err := newTypeEnvironment(packages)
	if err != nil {
		return nil, nil, err
	}
	e.files = files
	e.recordFacade = true
	e.guest = types.NewPackage("plugin", "")
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}, Scopes: map[ast.Node]*types.Scope{}, Implicits: map[ast.Node]types.Object{}, Instances: map[*ast.Ident]types.Instance{}, FileVersions: map[*ast.File]string{}}
	var diagnostics Diagnostics
	cfg := types.Config{GoVersion: "go1.23", Sizes: types.SizesFor("gc", "amd64"), Importer: e, Error: func(err error) {
		x := err.(types.Error)
		p := files.Position(x.Pos)
		diagnostics = append(diagnostics, Diagnostic{Position: evm.SourcePosition{File: p.Filename, Line: p.Line, Column: p.Column}, Message: x.Msg})
	}}
	err = types.NewChecker(&cfg, files, e.guest, info).Files(syntax)
	if err != nil {
		return e.guest, info, diagnostics
	}
	return e.guest, info, nil
}
