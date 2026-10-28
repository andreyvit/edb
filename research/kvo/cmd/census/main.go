// Census inventories table registration sites and reachable Go payload shapes.
// It parses source only: no application package is imported or executed.
// Names are emitted only with -private; default output uses stable anonymous IDs.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type source struct {
	path, pkg string
	file      *ast.File
	imports   map[string]string
}
type decl struct {
	src  *source
	expr ast.Expr
}
type shape struct {
	Structs     int `json:"structs"`
	Fields      int `json:"fields"`
	StringMaps  int `json:"string_maps"`
	NumericMaps int `json:"numeric_maps"`
	OtherMaps   int `json:"other_maps"`
	Lists       int `json:"lists"`
	Bytes       int `json:"byte_sequences"`
	Optional    int `json:"pointers"`
	Dynamic     int `json:"dynamic_values"`
	Strings     int `json:"strings"`
	Unresolved  int `json:"unresolved_types"`
}
type registration struct {
	ID    string `json:"id"`
	Site  string `json:"site"`
	Kind  string `json:"kind"`
	Name  string `json:"name,omitempty"`
	Row   string `json:"row,omitempty"`
	Shape shape  `json:"shape"`
}

var fset = token.NewFileSet()
var declarations = map[string]decl{}
var root, module string
var unresolvedDetails = map[string]bool{}
var otherMapDetails = map[string]bool{}

func ident(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return ident(e.X) + "." + e.Sel.Name
	case *ast.StarExpr:
		return ident(e.X)
	case *ast.IndexExpr:
		return ident(e.X)
	case *ast.IndexListExpr:
		return ident(e.X)
	}
	return ""
}
func key(e ast.Expr, s *source) string {
	switch e := e.(type) {
	case *ast.Ident:
		return s.pkg + "." + e.Name
	case *ast.SelectorExpr:
		return s.imports[ident(e.X)] + "." + e.Sel.Name
	}
	return ""
}
func underlying(e ast.Expr, s *source, seen map[string]bool) string {
	if i, ok := e.(*ast.Ident); ok {
		switch i.Name {
		case "string":
			return "string"
		case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "uintptr", "byte", "rune":
			return "number"
		case "any":
			return "dynamic"
		case "bool", "float32", "float64":
			return "scalar"
		}
	}
	k := key(e, s)
	if k != "" && !seen[k] {
		seen[k] = true
		if d, ok := declarations[k]; ok {
			return underlying(d.expr, d.src, seen)
		}
	}
	return "other"
}
func walk(e ast.Expr, s *source, out *shape, seen map[string]bool, depth int) {
	if depth > 64 {
		out.Unresolved++
		return
	}
	switch e := e.(type) {
	case *ast.StarExpr:
		out.Optional++
		walk(e.X, s, out, seen, depth+1)
	case *ast.MapType:
		switch underlying(e.Key, s, map[string]bool{}) {
		case "string":
			out.StringMaps++
		case "number":
			out.NumericMaps++
		default:
			out.OtherMaps++
			otherMapDetails[key(e.Key, s)] = true
		}
		walk(e.Value, s, out, seen, depth+1)
	case *ast.ArrayType:
		if i, ok := e.Elt.(*ast.Ident); ok && (i.Name == "byte" || i.Name == "uint8") {
			out.Bytes++
			return
		}
		out.Lists++
		walk(e.Elt, s, out, seen, depth+1)
	case *ast.StructType:
		out.Structs++
		for _, field := range e.Fields.List {
			if field.Tag != nil {
				tag, _ := strconv.Unquote(field.Tag.Value)
				if reflect.StructTag(tag).Get("msgpack") == "-" {
					continue
				}
			}
			if len(field.Names) > 0 && !field.Names[0].IsExported() {
				continue
			}
			out.Fields += max(1, len(field.Names))
			walk(field.Type, s, out, seen, depth+1)
		}
	case *ast.InterfaceType:
		out.Dynamic++
	case *ast.IndexExpr:
		walk(e.X, s, out, seen, depth+1)
		walk(e.Index, s, out, seen, depth+1)
	case *ast.IndexListExpr:
		walk(e.X, s, out, seen, depth+1)
		for _, a := range e.Indices {
			walk(a, s, out, seen, depth+1)
		}
	case *ast.Ident, *ast.SelectorExpr:
		switch underlying(e, s, map[string]bool{}) {
		case "string":
			out.Strings++
			return
		case "number", "scalar":
			return
		case "dynamic":
			out.Dynamic++
			return
		}
		k := key(e, s)
		if seen[k] {
			return
		}
		seen[k] = true
		if d, ok := declarations[k]; ok {
			walk(d.expr, d.src, out, seen, depth+1)
		} else if ident(e) != "time.Time" && ident(e) != "time.Duration" {
			out.Unresolved++
			unresolvedDetails[k] = true
		}
	}
}
func main() {
	shapeTypes := flag.String("shape-types", "", "comma-separated fully qualified type names")
	private := flag.Bool("private", false, "include source names for local analysis")
	flag.Parse()
	if flag.NArg() != 1 {
		panic("usage: census [-private] SOURCE_ROOT")
	}
	root = flag.Arg(0)
	mod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		panic(err)
	}
	for _, l := range strings.Split(string(mod), "\n") {
		if strings.HasPrefix(l, "module ") {
			module = strings.TrimSpace(strings.TrimPrefix(l, "module "))
			break
		}
	}
	var sources []*source
	err = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != root && (strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), "_") || d.Name() == "vendor" || d.Name() == "node_modules" || d.Name() == "third_party" || d.Name() == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, filepath.Dir(p))
		pkg := module
		if rel != "." {
			pkg += "/" + rel
		}
		s := &source{p, pkg, f, map[string]string{}}
		for _, im := range f.Imports {
			v, _ := strconv.Unquote(im.Path.Value)
			n := filepath.Base(v)
			if im.Name != nil {
				n = im.Name.Name
			}
			s.imports[n] = v
		}
		sources = append(sources, s)
		for _, d := range f.Decls {
			g, ok := d.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, sp := range g.Specs {
				if t, ok := sp.(*ast.TypeSpec); ok {
					declarations[pkg+"."+t.Name.Name] = decl{s, t.Type}
				}
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	if *shapeTypes != "" {
		out := map[string]shape{}
		for _, name := range strings.Split(*shapeTypes, ",") {
			d, ok := declarations[name]
			if !ok {
				panic("type not found: " + name)
			}
			var sh shape
			walk(d.expr, d.src, &sh, map[string]bool{}, 0)
			out[name] = sh
		}
		if err := json.NewEncoder(os.Stdout).Encode(out); err != nil {
			panic(err)
		}
		return
	}
	var regs []registration
	for _, s := range sources {
		ast.Inspect(s.file, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := c.Fun.(*ast.SelectorExpr)
			if !ok || s.imports[ident(sel.X)] != "github.com/andreyvit/edb" {
				return true
			}
			kind := sel.Sel.Name
			switch kind {
			case "AddTable", "DefineTable", "DefineKVTable", "DefineRawTable", "AddKVMap":
			default:
				return true
			}
			p := fset.Position(c.Pos())
			rel, _ := filepath.Rel(root, p.Filename)
			r := registration{Site: fmt.Sprintf("%s:%d", rel, p.Line), Kind: kind}
			if len(c.Args) > 1 {
				if lit, ok := c.Args[1].(*ast.BasicLit); ok {
					r.Name, _ = strconv.Unquote(lit.Value)
				} else {
					r.Name = "<computed>"
				}
			}
			var row ast.Expr
			for _, a := range c.Args {
				fn, ok := a.(*ast.FuncLit)
				if !ok || len(fn.Type.Params.List) == 0 {
					continue
				}
				pt := fn.Type.Params.List[0].Type
				if ptr, ok := pt.(*ast.StarExpr); ok {
					pt = ptr.X
				}
				if kind == "AddTable" {
					row = pt
					break
				}
				if idx, ok := pt.(*ast.IndexListExpr); ok && ident(idx.X) == "edb.TableBuilder" {
					row = idx.Indices[0]
					break
				}
			}
			if row != nil {
				r.Row = ident(row)
				walk(row, s, &r.Shape, map[string]bool{}, 0)
			}
			regs = append(regs, r)
			return true
		})
	}
	sort.Slice(regs, func(i, j int) bool { return regs[i].Site < regs[j].Site })
	for i := range regs {
		regs[i].ID = fmt.Sprintf("R%03d", i+1)
		if !*private {
			regs[i].Name = ""
			regs[i].Row = ""
		}
	}
	if *private {
		for k := range unresolvedDetails {
			fmt.Fprintln(os.Stderr, "unresolved:", k)
		}
		for k := range otherMapDetails {
			fmt.Fprintln(os.Stderr, "other-map-key:", k)
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(regs); err != nil {
		panic(err)
	}
}
