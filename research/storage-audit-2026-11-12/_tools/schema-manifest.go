package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
)

type table struct {
	Name     string
	Version  int
	Indices  []string
	Tags     []string
	Suppress bool
}
type index struct {
	Name    string
	Unique  bool
	Options []string
}
type manifest struct {
	Tables  map[string]*table
	Indices map[string]*index
}

func method(e ast.Expr) string {
	switch f := e.(type) {
	case *ast.SelectorExpr:
		return f.Sel.Name
	case *ast.IndexExpr:
		return method(f.X)
	case *ast.IndexListExpr:
		return method(f.X)
	}
	return ""
}
func str(e ast.Expr) string {
	if s, ok := e.(*ast.BasicLit); ok && s.Kind == token.STRING {
		v, _ := strconv.Unquote(s.Value)
		return v
	}
	return ""
}
func ident(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return v.Sel.Name
	}
	return ""
}
func main() {
	out := manifest{map[string]*table{}, map[string]*index{}}
	for _, path := range os.Args[1:] {
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			panic(err)
		}
		var inspectValue func(string, ast.Expr)
		inspectValue = func(name string, x ast.Expr) {
			c, ok := x.(*ast.CallExpr)
			if !ok {
				return
			}
			kind := method(c.Fun)
			switch kind {
			case "Unique":
				inspectValue(name, c.Fun.(*ast.SelectorExpr).X)
				if idx := out.Indices[name]; idx != nil {
					idx.Unique = true
				}
				return
			case "AddIndex":
				if len(c.Args) == 0 || str(c.Args[0]) == "" {
					return
				}
				idx := &index{Name: str(c.Args[0])}
				for _, a := range c.Args[1:] {
					opt := ident(a)
					if opt == "IndexOptDeprecatedUnique" {
						idx.Unique = true
					} else if opt != "" && opt != "nil" {
						idx.Options = append(idx.Options, opt)
					}
				}
				sort.Strings(idx.Options)
				out.Indices[name] = idx
			case "AddTable", "DefineTable":
				t := &table{Name: str(c.Args[1]), Version: 1}
				out.Tables[name] = t
				if kind == "AddTable" {
					n, _ := strconv.Atoi(c.Args[2].(*ast.BasicLit).Value)
					t.Version = n
					if a, ok := c.Args[5].(*ast.CompositeLit); ok {
						for _, x := range a.Elts {
							t.Indices = append(t.Indices, ident(x))
						}
					}
					for _, x := range c.Args[6:] {
						v := ident(x)
						if v == "SuppressContentWhenLogging" {
							t.Suppress = true
						} else {
							t.Tags = append(t.Tags, v)
						}
					}
				}
				for _, a := range c.Args {
					cb, ok := a.(*ast.FuncLit)
					if !ok {
						continue
					}
					ast.Inspect(cb.Body, func(n ast.Node) bool {
						if as, ok := n.(*ast.AssignStmt); ok {
							for i, r := range as.Rhs {
								if i < len(as.Lhs) {
									inspectValue(ident(as.Lhs[i]), r)
									if call, ok := r.(*ast.CallExpr); ok && method(call.Fun) == "AddIndex" {
										t.Indices = append(t.Indices, ident(as.Lhs[i]))
									}
								}
							}
						}
						if call, ok := n.(*ast.CallExpr); ok {
							switch method(call.Fun) {
							case "SetSchemaVersion":
								n, _ := strconv.Atoi(call.Args[0].(*ast.BasicLit).Value)
								t.Version = n
							case "Tag":
								t.Tags = append(t.Tags, ident(call.Args[0]))
							case "SuppressContentWhenLogging":
								t.Suppress = true
							case "AddIndex":
								if len(call.Args) > 0 && str(call.Args[0]) == "" {
									t.Indices = append(t.Indices, ident(call.Args[0]))
								}
							}
						}
						return true
					})
				}
				sort.Strings(t.Indices)
				sort.Strings(t.Tags)
			}
		}
		for _, d := range f.Decls {
			if g, ok := d.(*ast.GenDecl); ok {
				for _, sp := range g.Specs {
					if v, ok := sp.(*ast.ValueSpec); ok {
						for i, x := range v.Values {
							if i < len(v.Names) {
								inspectValue(v.Names[i].Name, x)
							}
						}
					}
				}
			}
		}
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
}
