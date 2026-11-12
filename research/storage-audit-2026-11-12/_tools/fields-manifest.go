package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

type field struct {
	Type string
	Tag  string
}

func main() {
	root := os.Args[1]
	out := map[string]field{}
	for _, dir := range []string{"bm", "bpub", "internal", "firejobs", "bubbleflake", "tristate"} {
		filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err == nil && d.IsDir() && (d.Name() == "testdata" || strings.HasPrefix(d.Name(), "_")) {
				return filepath.SkipDir
			}
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fs := token.NewFileSet()
			f, err := parser.ParseFile(fs, path, nil, 0)
			if err != nil {
				panic(err)
			}
			rel, _ := filepath.Rel(root, path)
			var walk func(string, *ast.StructType)
			walk = func(prefix string, s *ast.StructType) {
				for _, f := range s.Fields.List {
					var b bytes.Buffer
					format.Node(&b, fs, f.Type)
					typ := b.String()
					tag := ""
					if f.Tag != nil {
						raw, _ := strconv.Unquote(f.Tag.Value)
						tag = reflect.StructTag(raw).Get("msgpack")
					}
					names := f.Names
					if len(names) == 0 {
						names = []*ast.Ident{{Name: "@" + typ}}
					}
					for _, n := range names {
						key := prefix + "." + n.Name
						if child, ok := f.Type.(*ast.StructType); ok {
							out[key] = field{"struct", tag}
							walk(key, child)
						} else {
							out[key] = field{typ, tag}
						}
					}
				}
			}
			for _, decl := range f.Decls {
				g, ok := decl.(*ast.GenDecl)
				if !ok {
					continue
				}
				for _, spec := range g.Specs {
					if t, ok := spec.(*ast.TypeSpec); ok {
						if st, ok := t.Type.(*ast.StructType); ok {
							walk(rel+":"+t.Name.Name, st)
						}
					}
				}
			}
			return nil
		})
	}
	raw, _ := json.Marshal(out)
	fmt.Println(string(raw))
}
