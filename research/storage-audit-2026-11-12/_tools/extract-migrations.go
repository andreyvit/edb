package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"strings"
)

func main() {
	fset := token.NewFileSet()
	f, e := parser.ParseFile(fset, os.Args[1], nil, 0)
	if e != nil {
		panic(e)
	}
	fmt.Println("package fdb\nimport(")
	for _, i := range f.Imports {
		var b bytes.Buffer
		format.Node(&b, fset, i)
		fmt.Println(b.String())
	}
	fmt.Println(")")
	fmt.Println("var auditOldMigrators=map[string]func(*edb.Tx,any,uint64){")
	for _, decl := range f.Decls {
		g, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range g.Specs {
			v, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, x := range v.Values {
				call, ok := x.(*ast.CallExpr)
				if !ok || len(v.Names) <= i {
					continue
				}
				name := v.Names[i].Name
				var cb *ast.FuncLit
				ast.Inspect(call, func(n ast.Node) bool {
					c, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					fun := c.Fun
					if a, ok := fun.(*ast.IndexListExpr); ok {
						fun = a.X
					}
					if a, ok := fun.(*ast.IndexExpr); ok {
						fun = a.X
					}
					sel, ok := fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					if sel.Sel.Name == "AddTable" && len(c.Args) > 4 {
						cb, _ = c.Args[4].(*ast.FuncLit)
					}
					if sel.Sel.Name == "Migrate" && len(c.Args) > 0 {
						cb, _ = c.Args[0].(*ast.FuncLit)
					}
					return true
				})
				if cb == nil {
					continue
				}
				var typ, body bytes.Buffer
				format.Node(&typ, fset, cb.Type.Params.List[1].Type)
				format.Node(&body, fset, cb)
				s := strings.ReplaceAll(body.String(), "*edb.IndexBuilder", "*auditOldBuilder")
				fmt.Printf("%q: func(tx *edb.Tx,v any,old uint64){ (%s)(tx,v.(%s),old)},\n", name, s, typ.String())
			}
		}
	}
	fmt.Println("}")
}
