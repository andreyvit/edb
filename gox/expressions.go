package gox

import (
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"path"
	"strconv"
	"strings"
	"unicode"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"golang.org/x/tools/go/ast/astutil"
)

func (c *compiler) pipelines() bool {
	changed := false
	astutil.Apply(c.file, nil, func(cursor *astutil.Cursor) bool {
		b, ok := cursor.Node().(*ast.BinaryExpr)
		if !ok || b.Op != token.OR || c.info.TypeOf(b) != nil {
			return true
		}
		rhs := unwrap(b.Y)
		var fn ast.Expr
		var args []ast.Expr
		if call, ok := rhs.(*ast.CallExpr); ok {
			fn = call.Fun
			args = call.Args
		} else {
			fn = rhs
		}
		t := c.info.TypeOf(fn)
		if t == nil {
			return true
		}
		sig, ok := t.Underlying().(*types.Signature)
		if !ok || sig.Results().Len() != 1 {
			return true
		}
		if !sig.Variadic() && sig.Params().Len() != len(args)+1 {
			return true
		}
		cursor.Replace(&ast.CallExpr{Fun: fn, Args: append(append([]ast.Expr(nil), args...), b.X), Lparen: b.OpPos})
		changed = true
		return true
	})
	return changed
}
func (c *compiler) collectClasses() {
	ast.Inspect(c.file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || call.Pos() == token.NoPos {
			return true
		}
		name := ""
		switch f := call.Fun.(type) {
		case *ast.Ident:
			name = f.Name
		case *ast.SelectorExpr:
			name = f.Sel.Name
		}
		if name == "attr" || name == "Attr" {
			if len(call.Args) < 2 {
				return true
			}
			key := c.info.Types[unwrap(call.Args[len(call.Args)-2])].Value
			if key == nil || key.Kind() != constant.String || constant.StringVal(key) != "class" {
				return true
			}
		} else if name != "class" && name != "Class" {
			return true
		}
		if len(call.Args) == 0 {
			return true
		}
		arg := unwrap(call.Args[len(call.Args)-1])
		value := c.info.Types[arg].Value
		if value != nil && value.Kind() == constant.String {
			for _, class := range strings.Fields(constant.StringVal(value)) {
				c.template.Classes = append(c.template.Classes, Class{class, c.position(arg.Pos())})
			}
		} else {
			c.template.DynamicClasses = true
		}
		return true
	})
}

func (c *compiler) pruneImports() {
	_, info, _ := evmcompiler.AnalyzeFiles(c.files, c.syntax, c.packages...)
	used := map[string]bool{}
	if info != nil {
		ast.Inspect(c.file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				if p, ok := info.Uses[id].(*types.PkgName); ok {
					used[p.Name()] = true
				}
			}
			return true
		})
	}
	imports := append([]*ast.ImportSpec(nil), c.file.Imports...)
	for _, imp := range imports {
		value, _ := strconv.Unquote(imp.Path.Value)
		name := path.Base(value)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if !used[name] {
			astutil.DeleteNamedImport(c.files, c.file, name, value)
		}
	}
}

func (c *compiler) recordClassAttribute(a attribute, args []ast.Expr, pos evm.SourcePosition) {
	type fragment struct {
		text  string
		known bool
	}
	fragments := make([]fragment, len(a.parts))
	for i, p := range a.parts {
		if p.arg == 0 {
			fragments[i] = fragment{p.text, true}
			continue
		}
		value := c.info.Types[unwrap(args[p.arg-1])].Value
		if value != nil && value.Kind() == constant.String {
			fragments[i] = fragment{constant.StringVal(value), true}
		} else {
			c.template.DynamicClasses = true
		}
	}
	// Coalesce known fragments before deciding which tokens have dynamic prefixes.
	for i := 0; i < len(fragments); i++ {
		if !fragments[i].known {
			continue
		}
		start := i
		text := fragments[i].text
		for i+1 < len(fragments) && fragments[i+1].known {
			i++
			text += fragments[i].text
		}
		names := strings.Fields(text)
		if len(names) == 0 {
			continue
		}
		if start > 0 && len(text) > 0 && !unicode.IsSpace(rune(text[0])) {
			names = names[1:]
		}
		if i+1 < len(fragments) && len(names) > 0 && len(text) > 0 && !unicode.IsSpace(rune(text[len(text)-1])) {
			names = names[:len(names)-1]
		}
		for _, name := range names {
			c.template.Classes = append(c.template.Classes, Class{name, pos})
		}
	}
}
