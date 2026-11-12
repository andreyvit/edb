package gox

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strconv"

	"github.com/andreyvit/edb/evm"
)

// Nil handling is ordinary Go control flow. The skip label is outside temporary
// variable scopes, so failure neither unwinds a defer nor evaluates later operands.
type nilLowerer struct {
	c          *compiler
	label      string
	missing    operation
	statements []ast.Stmt
}

func (c *compiler) needsNilGuard(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if found {
			return false
		}
		if _, ok := node.(*ast.FuncLit); ok {
			return false
		}
		if s, ok := node.(*ast.SelectorExpr); ok && c.pointerField(s) {
			found = true
			return false
		}
		return true
	})
	return found
}
func (c *compiler) pointerField(s *ast.SelectorExpr) bool {
	selection := c.info.Selections[s]
	if selection == nil || selection.Kind() != types.FieldVal {
		return false
	}
	typ := c.info.TypeOf(s.X)
	if typ == nil {
		return false
	}
	_, ok := typ.Underlying().(*types.Pointer)
	return ok
}
func (c *compiler) guarded(expr ast.Expr, output ast.Stmt, pos evm.SourcePosition, fallback ...bool) []ast.Stmt {
	if !c.needsNilGuard(expr) {
		return []ast.Stmt{output}
	}
	c.sequence++
	n := &nilLowerer{c: c, label: "_goxSkip" + strconv.Itoa(c.sequence), missing: operation{kind: opMissing, position: pos}}
	if len(fallback) > 0 && fallback[0] {
		call := output.(*ast.ExprStmt).X.(*ast.CallExpr)
		id, _ := strconv.Atoi(call.Args[1].(*ast.BasicLit).Value)
		n.missing.filter = c.template.operations[id].filter
		n.missing.input = c.template.operations[id].input
		n.missing.content = c.template.operations[id].content
		n.missing.fallback = true
	}
	value := n.expression(expr)
	original := output.(*ast.ExprStmt).X.(*ast.CallExpr)
	call := *original
	call.Args = append([]ast.Expr(nil), original.Args...)
	call.Args[len(call.Args)-1] = value
	n.statements = append(n.statements, &ast.ExprStmt{X: &call})
	return []ast.Stmt{&ast.BlockStmt{List: []ast.Stmt{&ast.BlockStmt{List: n.statements}, &ast.LabeledStmt{Label: ast.NewIdent(n.label), Stmt: &ast.EmptyStmt{Implicit: true}}}}}
}
func (n *nilLowerer) temporary() string {
	n.c.sequence++
	return "_goxValue" + strconv.Itoa(n.c.sequence)
}

func (c *compiler) guardedCall(expr ast.Expr) []ast.Stmt {
	if !c.needsNilGuard(expr) {
		return []ast.Stmt{&ast.ExprStmt{X: expr}}
	}
	c.sequence++
	n := &nilLowerer{c: c, label: "_goxSkip" + strconv.Itoa(c.sequence), missing: operation{kind: opMissing, position: c.position(expr.Pos())}}
	call := n.expression(expr)
	n.statements = append(n.statements, &ast.ExprStmt{X: call})
	return []ast.Stmt{&ast.BlockStmt{List: []ast.Stmt{&ast.BlockStmt{List: n.statements}, &ast.LabeledStmt{Label: ast.NewIdent(n.label), Stmt: &ast.EmptyStmt{Implicit: true}}}}}
}
func (n *nilLowerer) hold(expr ast.Expr) ast.Expr {
	if tv, ok := n.c.info.Types[expr]; ok && (tv.Value != nil || tv.IsType()) {
		return expr
	}
	if isIdent(expr, "nil") {
		return expr
	}
	switch e := expr.(type) {
	case *ast.BasicLit:
		return expr
	case *ast.Ident:
		switch n.c.info.Uses[e].(type) {
		case *types.Func, *types.Builtin, *types.TypeName, *types.Const:
			return expr
		}
	case *ast.SelectorExpr:
		if id, ok := e.X.(*ast.Ident); ok {
			if _, packageName := n.c.info.Uses[id].(*types.PkgName); packageName {
				return expr
			}
		}

	}
	name := n.temporary()
	n.statements = append(n.statements, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(name)}, Tok: token.DEFINE, Rhs: []ast.Expr{expr}})
	return ast.NewIdent(name)
}
func (n *nilLowerer) expression(expr ast.Expr) ast.Expr {
	if !n.c.needsNilGuard(expr) {
		return expr
	}
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return &ast.ParenExpr{X: n.expression(e.X)}
	case *ast.SelectorExpr:
		receiver := n.expression(e.X)
		if n.c.pointerField(e) {
			receiver = n.hold(receiver)
			n.statements = append(n.statements, &ast.IfStmt{Cond: &ast.BinaryExpr{X: receiver, Op: token.EQL, Y: ast.NewIdent("nil")}, Body: &ast.BlockStmt{List: []ast.Stmt{n.c.op(n.missing), &ast.BranchStmt{Tok: token.GOTO, Label: ast.NewIdent(n.label)}}}})
		}
		return &ast.SelectorExpr{X: receiver, Sel: e.Sel}
	case *ast.CallExpr:
		fn := n.hold(n.expression(e.Fun))
		args := make([]ast.Expr, len(e.Args))
		for i, arg := range e.Args {
			args[i] = n.hold(n.expression(arg))
		}
		return &ast.CallExpr{Fun: fn, Args: args, Ellipsis: e.Ellipsis}
	case *ast.BinaryExpr:
		left := n.hold(n.expression(e.X))
		if e.Op == token.LAND || e.Op == token.LOR {
			name := n.temporary()
			typ := ast.Expr(ast.NewIdent("bool"))
			if t := n.c.info.TypeOf(e); t != nil {
				if parsed, err := parser.ParseExpr(types.TypeString(t, func(p *types.Package) string { return p.Name() })); err == nil {
					typ = parsed
				}
			}
			n.statements = append(n.statements, &ast.DeclStmt{Decl: &ast.GenDecl{Tok: token.VAR, Specs: []ast.Spec{&ast.ValueSpec{Names: []*ast.Ident{ast.NewIdent(name)}, Type: typ, Values: []ast.Expr{left}}}}})
			outer := n.statements
			n.statements = nil
			right := n.expression(e.Y)
			n.statements = append(n.statements, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(name)}, Tok: token.ASSIGN, Rhs: []ast.Expr{right}})
			condition := ast.Expr(ast.NewIdent(name))
			if e.Op == token.LOR {
				condition = &ast.UnaryExpr{Op: token.NOT, X: condition}
			}
			n.statements = append(outer, &ast.IfStmt{Cond: condition, Body: &ast.BlockStmt{List: n.statements}})
			return ast.NewIdent(name)
		}
		right := n.expression(e.Y)
		return &ast.BinaryExpr{X: left, Op: e.Op, Y: right}
	case *ast.UnaryExpr:
		return &ast.UnaryExpr{Op: e.Op, X: n.expression(e.X)}
	case *ast.StarExpr:
		return &ast.StarExpr{X: n.expression(e.X)}
	case *ast.IndexExpr:
		return &ast.IndexExpr{X: n.hold(n.expression(e.X)), Index: n.expression(e.Index)}
	case *ast.SliceExpr:
		x := n.hold(n.expression(e.X))
		var low, high, max ast.Expr
		if e.Low != nil {
			low = n.hold(n.expression(e.Low))
		}
		if e.High != nil {
			high = n.hold(n.expression(e.High))
		}
		if e.Max != nil {
			max = n.expression(e.Max)
		}
		return &ast.SliceExpr{X: x, Low: low, High: high, Max: max, Slice3: e.Slice3}
	case *ast.TypeAssertExpr:
		return &ast.TypeAssertExpr{X: n.expression(e.X), Type: e.Type}
	case *ast.CompositeLit:
		copy := *e
		copy.Elts = make([]ast.Expr, len(e.Elts))
		isMap := false
		if typ := n.c.info.TypeOf(e); typ != nil {
			_, isMap = typ.Underlying().(*types.Map)
		}
		for i, item := range e.Elts {
			if pair, ok := item.(*ast.KeyValueExpr); ok {
				key := pair.Key
				if isMap {
					key = n.hold(n.expression(key))
				}
				copy.Elts[i] = &ast.KeyValueExpr{Key: key, Value: n.hold(n.expression(pair.Value))}
			} else {
				copy.Elts[i] = n.hold(n.expression(item))
			}
		}
		return &copy
	}
	return expr
}
