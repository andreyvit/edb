package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	fmt.Println("package fdb\nimport \"github.com/prairiegroupinc/fire/nm\"\nvar auditProperties=[]any{")
	files, _ := filepath.Glob(filepath.Join(os.Args[1], "*.go"))
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, e := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if e != nil {
			panic(e)
		}
		for _, d := range f.Decls {
			g, ok := d.(*ast.GenDecl)
			if !ok || g.Tok != token.VAR {
				continue
			}
			for _, sp := range g.Specs {
				v := sp.(*ast.ValueSpec)
				for i, x := range v.Values {
					call, ok := x.(*ast.CallExpr)
					if !ok {
						continue
					}
					fun := call.Fun
					if it, ok := fun.(*ast.IndexExpr); ok {
						fun = it.X
					}
					if it, ok := fun.(*ast.IndexListExpr); ok {
						fun = it.X
					}
					sel, ok := fun.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					if strings.Contains(sel.Sel.Name, "Prop") && (strings.Contains(sel.Sel.Name, "List") || strings.Contains(sel.Sel.Name, "Map") || sel.Sel.Name == "Prop") {
						fmt.Println("nm." + v.Names[i].Name + ",")
					}
				}
			}
		}
	}
	fmt.Println("}")
}
