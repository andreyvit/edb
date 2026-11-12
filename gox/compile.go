package gox

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/scanner"
	"go/token"
	"go/types"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	ht "github.com/andreyvit/edb/gox/internal/htmltemplate"
	"github.com/andreyvit/edb/internal/naming"
	"golang.org/x/tools/go/ast/astutil"
)

type compiler struct {
	syntax        []*ast.File
	mappings      map[string][]evm.SourcePosition
	originals     map[string]*token.File
	outputMaps    map[string]*generatedMap
	implicitCalls map[*ast.CallExpr]bool

	fixedOperations map[ast.Stmt]int
	componentCalls  map[*ast.CallExpr]bool
	originalTokens  map[evm.SourcePosition]token.Pos
	branchTargets   []branchTarget
	slotFuncs       map[*ast.FuncLit]bool
	loweredFuncs    map[*ast.FuncLit]bool
	files           *token.FileSet
	file            *ast.File
	info            *types.Info
	pkg             *types.Package
	packages        []evm.Package
	marks           []markup
	template        *Template
	builder         string
	sequence        int
}
type renderState struct {
	rendering bool
	context   ht.Context
	tags      []string // An empty name denotes a fragment, which emits no tag.
}

func (s renderState) clone() renderState {
	return renderState{rendering: s.rendering, context: s.context.Clone(), tags: slices.Clone(s.tags)}
}

func (s renderState) unclosedMessage() string {
	if name := s.tags[len(s.tags)-1]; name != "" {
		return "unclosed element " + name
	}
	return "unclosed fragment <>"
}

func (c *compiler) position(pos token.Pos) evm.SourcePosition {
	p := c.files.PositionFor(pos, false)
	return c.sourcePosition(p)
}
func (c *compiler) sourcePosition(p token.Position) evm.SourcePosition {
	if positions := c.mappings[p.Filename]; p.Offset >= 0 && p.Offset < len(positions) && positions[p.Offset].File != "" {
		return positions[p.Offset]
	}
	return evm.SourcePosition{File: p.Filename, Line: max(1, p.Line), Column: max(1, p.Column)}
}
func (c *compiler) error(pos token.Pos, message string) error {
	return Diagnostic{c.position(pos), message}
}
func (c *compiler) parseError(err error) error {
	if list, ok := err.(scanner.ErrorList); ok && len(list) > 0 {
		return Diagnostic{c.sourcePosition(list[0].Pos), list[0].Msg}
	}
	return err
}
func hasMarkup(n *ast.BlockStmt) bool {
	if n == nil {
		return false
	}
	found := false
	ast.Inspect(n, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		if call, ok := n.(*ast.CallExpr); ok && isIdent(call.Fun, "_goxMark") {
			found = true
		}
		return true
	})
	return found
}
func findBuilder(ft *ast.FuncType) string {
	for _, f := range ft.Params.List {
		if nodeString(f.Type) == "*gox.Builder" && len(f.Names) > 0 {
			return f.Names[0].Name
		}
	}
	return ""
}
func isIdent(e ast.Expr, name string) bool { x, ok := e.(*ast.Ident); return ok && x.Name == name }
func sel(pkg, name string) *ast.SelectorExpr {
	return &ast.SelectorExpr{X: ast.NewIdent(pkg), Sel: ast.NewIdent(name)}
}
func nodeString(n ast.Node) string {
	var b bytes.Buffer
	_ = format.Node(&b, token.NewFileSet(), n)
	return b.String()
}
func clearPositions(node ast.Node) {
	var walk func(reflect.Value)
	walk = func(v reflect.Value) {
		if !v.IsValid() {
			return
		}
		if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
			if !v.IsNil() {
				walk(v.Elem())
			}
			return
		}
		if v.Type() == reflect.TypeFor[token.Pos]() {
			if v.CanSet() {
				v.SetInt(0)
			}
			return
		}
		switch v.Kind() {
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if v.Type().Field(i).Name != "Obj" {
					walk(v.Field(i))
				}
			}
		case reflect.Slice:
			for i := 0; i < v.Len(); i++ {
				walk(v.Index(i))
			}
		}
	}
	walk(reflect.ValueOf(node))
}
func (c *compiler) autoImports() {
	names := map[string]string{"fmt": "fmt", "gox": "gox"}
	for _, name := range evm.StandardLibraryPaths() {
		names[path.Base(name)] = name
	}
	for _, p := range c.packages {
		names[path.Base(p.Path)] = p.Path
	}
	used := map[string]bool{"gox": true}
	ast.Inspect(c.file, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if m, ok := c.marker(call); ok && m.kind == markupOpen {
				if pkg, _, ok := strings.Cut(m.name, "."); ok {
					used[pkg] = true
				}
			}
		}
		if s, ok := n.(*ast.SelectorExpr); ok {
			if id, ok := s.X.(*ast.Ident); ok {
				used[id.Name] = true
			}
		}
		return true
	})
	keys := make([]string, 0, len(names))
	for n := range names {
		if used[n] {
			keys = append(keys, n)
		}
	}
	slices.Sort(keys)
	for _, name := range keys {
		astutil.AddNamedImport(c.files, c.file, name, names[name])
	}
}
func (c *compiler) scope(pos token.Pos) *types.Scope {
	var best *types.Scope
	for _, s := range c.info.Scopes {
		if s.Contains(pos) && (best == nil || s.Pos() >= best.Pos() && s.End() <= best.End()) {
			best = s
		}
	}
	if best == nil {
		return c.pkg.Scope()
	}
	return best
}
func (c *compiler) resolveCallsAndFields() bool {
	changed := false
	ast.Inspect(c.file, func(n ast.Node) bool {
		if s, ok := n.(*ast.SelectorExpr); ok && c.info.Selections[s] == nil {
			t := c.info.TypeOf(s.X)
			if t != nil {
				if p, ok := t.Underlying().(*types.Pointer); ok {
					t = p.Elem()
				}
				// KVO's facade already contains every model selector. Only Go
				// struct fields receive JSON and inferred snake_case aliases.
				if c.isRecordType(t) {
					return true
				}
				if st, ok := t.Underlying().(*types.Struct); ok {
					match := ""
					for i := 0; i < st.NumFields(); i++ {
						f := st.Field(i)
						alias := strings.Split(reflect.StructTag(st.Tag(i)).Get("json"), ",")[0]
						if s.Sel.Name == alias && alias != "-" || s.Sel.Name == naming.Snake(f.Name()) {
							if match != "" && match != f.Name() {
								return true
							}
							match = f.Name()
						}
					}
					if match != "" && match != s.Sel.Name {
						s.Sel.Name = match
						changed = true
					}
				}
			}
		}
		call, ok := n.(*ast.CallExpr)
		if !ok || isIdent(call.Fun, "_goxMark") {
			return true
		}
		if !c.implicitCalls[call] {
			return true
		}
		t := c.info.TypeOf(call.Fun)
		if t == nil {
			return true
		}
		sig, ok := t.Underlying().(*types.Signature)
		if !ok {
			return true
		}
		params := sig.Params()
		scope := c.scope(call.Pos())
		hasBuilder := false
		if params.Len() > 0 && len(call.Args) > 0 {
			if typ := c.info.TypeOf(call.Args[0]); typ != nil {
				hasBuilder = types.AssignableTo(typ, params.At(0).Type())
			}
		}
		if !hasBuilder && params.Len() > 0 && types.TypeString(params.At(0).Type(), func(p *types.Package) string { return p.Name() }) == "*gox.Builder" && len(call.Args) < params.Len() {
			call.Args = append([]ast.Expr{ast.NewIdent(c.builderAt(call.Pos()))}, call.Args...)
			changed = true
		}
		if sig.Variadic() {
			return true
		}
		for len(call.Args) < params.Len() {
			p := params.At(len(call.Args))
			if p.Name() == "" {
				break
			}
			_, obj := scope.LookupParent(p.Name(), call.Pos())
			if obj == nil {
				break
			}
			call.Args = append(call.Args, ast.NewIdent(p.Name()))
			changed = true
		}
		return true
	})
	return changed
}

func (c *compiler) isRecordType(t types.Type) bool {
	named, ok := types.Unalias(t).(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	for _, pkg := range c.packages {
		if pkg.Path == named.Obj().Pkg().Path() {
			for _, r := range pkg.Records {
				if r.Name == named.Obj().Name() {
					return true
				}
			}
		}
	}
	return false
}

func (c *compiler) op(op operation, args ...ast.Expr) ast.Stmt {
	id := len(c.template.operations)
	c.template.operations = append(c.template.operations, op)
	fn := "Step"
	xs := []ast.Expr{ast.NewIdent(c.builder), &ast.BasicLit{Kind: token.INT, Value: strconv.Itoa(id)}}
	if len(args) > 0 {
		fn = "Value"
		if typ := c.info.TypeOf(args[0]); typ != nil {
			if basic, ok := typ.(*types.Basic); ok {
				if basic.Kind() == types.String {
					fn = "String"
				} else if basic.Kind() == types.Int {
					fn = "Int"
				} else if basic.Kind() == types.Bool {
					fn = "Bool"
				}
			}
		}
		xs = append(xs, args...)
	}
	position := c.originalToken(op.position)
	function := sel("gox", fn)
	function.X.(*ast.Ident).NamePos = position
	function.Sel.NamePos = position
	stmt := &ast.ExprStmt{X: &ast.CallExpr{Fun: function, Args: xs, Lparen: position}}
	if len(args) == 0 {
		if c.fixedOperations == nil {
			c.fixedOperations = make(map[ast.Stmt]int)
		}
		c.fixedOperations[stmt] = id
	}
	return stmt
}
func (c *compiler) marker(call *ast.CallExpr) (markup, bool) {
	if !isIdent(call.Fun, "_goxMark") || len(call.Args) == 0 {
		return markup{}, false
	}
	n, ok := call.Args[0].(*ast.BasicLit)
	if !ok {
		return markup{}, false
	}
	id, err := strconv.Atoi(n.Value)
	if err != nil || id < 0 || id >= len(c.marks) {
		return markup{}, false
	}
	return c.marks[id], true
}
func (c *compiler) block(block *ast.BlockStmt, state *renderState) error {
	var out []ast.Stmt
	for _, stmt := range block.List {
		var literalErr error
		ast.Inspect(stmt, func(node ast.Node) bool {
			f, ok := node.(*ast.FuncLit)
			if !ok {
				return true
			}
			if c.loweredFuncs[f] {
				return false
			}
			if !hasMarkup(f.Body) && !c.slotFuncs[f] {
				return true
			}
			if f.Type.Results != nil && len(f.Type.Results.List) > 0 {
				literalErr = c.error(f.Pos(), "rendering functions must not return values")
				return false
			}
			if c.loweredFuncs == nil {
				c.loweredFuncs = map[*ast.FuncLit]bool{}
			}
			c.loweredFuncs[f] = true
			nested := renderState{rendering: c.slotFuncs[f]}
			outerTargets := c.branchTargets
			c.branchTargets = nil
			literalErr = c.block(f.Body, &nested)
			c.branchTargets = outerTargets
			if literalErr == nil && len(nested.tags) > 0 {
				literalErr = c.error(f.Pos(), "unclosed local component")
			}
			return false
		})
		if literalErr != nil {
			return literalErr
		}
		switch n := stmt.(type) {
		case *ast.ExprStmt:
			if call, ok := n.X.(*ast.CallExpr); ok {
				if m, ok := c.marker(call); ok {
					ops, err := c.markup(m, call.Args[1:], state)
					if err != nil {
						return err
					}
					out = append(out, ops...)
					continue
				}
			}
			call, _ := n.X.(*ast.CallExpr)
			if len(state.tags) > 0 || state.rendering || c.componentCalls[call] {
				op, err := c.output(n.X, state)
				if err != nil {
					return err
				}
				out = append(out, op...)
				continue
			}
		case *ast.IfStmt:
			left := state.clone()
			if err := c.block(n.Body, &left); err != nil {
				return err
			}
			if !slices.Equal(left.tags, state.tags) {
				return c.error(n.Pos(), "if branch must contain complete elements")
			}
			right := state.clone()
			if n.Else != nil {
				switch e := n.Else.(type) {
				case *ast.BlockStmt:
					if err := c.block(e, &right); err != nil {
						return err
					}
				case *ast.IfStmt:
					b := &ast.BlockStmt{List: []ast.Stmt{e}}
					if err := c.block(b, &right); err != nil {
						return err
					}
					n.Else = b
				}
			}
			if !slices.Equal(right.tags, state.tags) {
				return c.error(n.Pos(), "else branch must contain complete elements")
			}
			if err := left.context.Join(right.context); err != nil {
				return c.error(n.Pos(), err.Error())
			}
			state.context = left.context
		case *ast.ForStmt:
			loop := state.clone()
			c.branchTargets = append(c.branchTargets, branchTarget{loop: true, depth: len(state.tags)})
			err := c.block(n.Body, &loop)
			c.branchTargets = c.branchTargets[:len(c.branchTargets)-1]
			if err != nil {
				return err
			}
			if !slices.Equal(loop.tags, state.tags) {
				return c.error(n.Pos(), "loop must contain complete elements")
			}
			if err := loop.context.Join(state.context); err != nil {
				return c.error(n.Pos(), err.Error())
			}
			state.context = loop.context
		case *ast.RangeStmt:
			loop := state.clone()
			c.branchTargets = append(c.branchTargets, branchTarget{loop: true, depth: len(state.tags)})
			err := c.block(n.Body, &loop)
			c.branchTargets = c.branchTargets[:len(c.branchTargets)-1]
			if err != nil {
				return err
			}
			if !slices.Equal(loop.tags, state.tags) {
				return c.error(n.Pos(), "loop must contain complete elements")
			}
			if err := loop.context.Join(state.context); err != nil {
				return c.error(n.Pos(), err.Error())
			}
			state.context = loop.context
		case *ast.BlockStmt:
			if err := c.block(n, state); err != nil {
				return err
			}
		case *ast.TypeSwitchStmt:
			switchNode := &ast.SwitchStmt{Body: n.Body}
			block := &ast.BlockStmt{List: []ast.Stmt{switchNode}}
			if err := c.block(block, state); err != nil {
				return err
			}
		case *ast.LabeledStmt:
			block := &ast.BlockStmt{List: []ast.Stmt{n.Stmt}}
			if err := c.block(block, state); err != nil {
				return err
			}
			if len(block.List) != 1 {
				return c.error(n.Pos(), "label on generated rendering operation")
			}
			n.Stmt = block.List[0]
		case *ast.SwitchStmt:
			c.branchTargets = append(c.branchTargets, branchTarget{depth: len(state.tags)})
			var merged *ht.Context
			hasDefault := false
			for _, clause := range n.Body.List {
				cl := clause.(*ast.CaseClause)
				if cl.List == nil {
					hasDefault = true
				}
				branch := state.clone()
				b := &ast.BlockStmt{List: cl.Body}
				if err := c.block(b, &branch); err != nil {
					return err
				}
				cl.Body = b.List
				if !slices.Equal(branch.tags, state.tags) {
					return c.error(cl.Pos(), "switch case must contain complete elements")
				}
				if merged == nil {
					x := branch.context
					merged = &x
				} else if err := merged.Join(branch.context); err != nil {
					return c.error(cl.Pos(), err.Error())
				}
			}
			if merged != nil {
				if !hasDefault {
					if err := merged.Join(state.context); err != nil {
						return c.error(n.Pos(), err.Error())
					}
				}
				state.context = *merged
			}
			c.branchTargets = c.branchTargets[:len(c.branchTargets)-1]
		case *ast.ReturnStmt:
			closes, err := c.closeTags(state, 0, n.Pos())
			if err != nil {
				return err
			}
			out = append(out, closes...)
		case *ast.BranchStmt:
			if n.Tok == token.FALLTHROUGH {
				break
			}
			target := -1
			if n.Label == nil && (n.Tok == token.BREAK || n.Tok == token.CONTINUE) {
				for i := len(c.branchTargets) - 1; i >= 0; i-- {
					t := c.branchTargets[i]
					if n.Tok == token.BREAK || t.loop {
						target = t.depth
						break
					}
				}
			}
			if target >= 0 {
				closes, err := c.closeTags(state, target, n.Pos())
				if err != nil {
					return err
				}
				out = append(out, closes...)
			} else if len(state.tags) > 0 {
				return c.error(n.Pos(), "labeled jumps cannot cross open elements")
			}

		}
		out = append(out, stmt)
	}
	block.List = out
	return nil
}
func (c *compiler) markup(m markup, args []ast.Expr, s *renderState) ([]ast.Stmt, error) {
	var out []ast.Stmt
	m.name = strings.ToLower(m.name)
	fragment := m.name == "" && (m.kind == markupOpen || m.kind == markupClose)
	if c.template.format != HTMLFormat && !fragment {
		return nil, fmt.Errorf("markup requires HTML output format")
	}
	pos := evm.SourcePosition{File: m.pos.file, Line: m.pos.line, Column: m.pos.column}
	text := func(value string, kind operationKind) error {
		before := s.context.Clone()
		v, err := s.context.Text(value)
		if err != nil {
			return Diagnostic{pos, err.Error()}
		}
		if v != "" || !before.Equal(s.context) {
			out = append(out, c.op(operation{kind: kind, text: v, position: pos, input: before, content: s.context.Clone()}))
		}
		return nil
	}
	switch m.kind {
	case markupOpen:
		if fragment {
			s.tags = append(s.tags, "")
			break
		}
		if strings.HasPrefix(m.name, "c-") || strings.HasPrefix(m.name, "slot-") {
			return nil, Diagnostic{pos, "component requires call lowering"}
		}
		if !s.context.IsText() {
			return nil, Diagnostic{pos, "element in non-HTML context"}
		}
		out = append(out, c.op(operation{kind: opOpen, name: m.name, position: pos, void: m.void}))
		if _, err := s.context.Text("<" + m.name); err != nil {
			return nil, Diagnostic{pos, err.Error()}
		}
		attributes := map[string]bool{}
		for _, a := range m.attrs {
			a.name = strings.ToLower(a.name)
			if attributes[a.name] {
				return nil, Diagnostic{pos, "duplicate attribute " + a.name}
			}
			attributes[a.name] = true
			start := len(out)
			if a.name == "class" {
				c.recordClassAttribute(a, args, pos)
			}
			if (m.name == "script" || m.name == "style") && a.name == "type" {
				for _, part := range a.parts {
					if part.arg > 0 {
						return nil, Diagnostic{pos, "script/style type must be static"}
					}
				}
			}
			out = append(out, c.op(operation{kind: opAttr, name: a.name, boolean: a.boolean, position: pos}))
			prefix := " " + a.name
			if !a.boolean {
				prefix += "=\""
			}
			if _, err := s.context.Text(prefix); err != nil {
				return nil, Diagnostic{pos, err.Error()}
			}
			for _, p := range a.parts {
				if p.arg == 0 {
					value := ht.HTMLEscapeString(p.text)
					if err := text(value, opAttrText); err != nil {
						return nil, err
					}

				} else {
					before := s.context.Clone()
					f, err := s.context.Action()
					if err != nil {
						return nil, Diagnostic{pos, err.Error()}
					}
					expr := unwrap(args[p.arg-1])
					position := c.position(expr.Pos())
					op := operation{kind: opValue, filter: f, position: position, input: before, content: s.context.Clone(), attribute: true, boolean: booleanAttrs[a.name] && len(a.parts) == 1}
					out = append(out, c.guarded(expr, c.op(op, expr), position)...)
				}
			}
			if !a.boolean {
				if _, err := s.context.Text("\""); err != nil {
					return nil, Diagnostic{pos, err.Error()}
				}
			}
			out = append(out, c.op(operation{kind: opEndAttr, position: pos}))
			static := true
			for _, part := range a.parts {
				if part.arg != 0 {
					static = false
				}
			}
			if static {
				var value strings.Builder
				for _, stmt := range out[start:] {
					if id, ok := c.fixedOperations[stmt]; ok && c.template.operations[id].kind == opAttrText {
						value.WriteString(c.template.operations[id].text)
					}
				}
				out = append(out[:start], c.op(operation{kind: opSetAttr, name: a.name, text: value.String(), boolean: a.boolean, position: pos}))
			}
		}
		if _, err := s.context.Text(">"); err != nil {
			return nil, Diagnostic{pos, err.Error()}
		}
		if m.void && !voidTags[m.name] {
			if _, err := s.context.Text("</" + m.name + ">"); err != nil {
				return nil, Diagnostic{pos, err.Error()}
			}
		}
		out = append(out, c.op(operation{kind: opFinish, void: m.void, position: pos, content: s.context.Clone()}))
		allStatic := true
		for _, a := range m.attrs {
			for _, p := range a.parts {
				if p.arg != 0 {
					allStatic = false
				}
			}
		}
		if allStatic {
			openCall := out[0].(*ast.ExprStmt).X.(*ast.CallExpr)
			id, _ := strconv.Atoi(openCall.Args[1].(*ast.BasicLit).Value)
			var attrs []renderedAttribute
			var current renderedAttribute
			for _, statement := range out[1 : len(out)-1] {
				call := statement.(*ast.ExprStmt).X.(*ast.CallExpr)
				index, _ := strconv.Atoi(call.Args[1].(*ast.BasicLit).Value)
				op := c.template.operations[index]
				switch op.kind {
				case opAttr:
					current = renderedAttribute{name: op.name, boolean: op.boolean}
				case opAttrText:
					current.value += op.text
				case opEndAttr:
					attrs = append(attrs, current)
				case opSetAttr:
					attrs = append(attrs, renderedAttribute{op.name, op.text, op.boolean})
				}
			}
			c.template.operations[id].attrs = attrs
			c.template.operations[id].kind = opTag
			c.template.operations[id].content = s.context.Clone()
			var prefix strings.Builder
			prefix.WriteString("<" + m.name)
			for _, a := range attrs {
				prefix.WriteString(" " + a.name)
				if !a.boolean {
					prefix.WriteString("=\"" + a.value + "\"")
				}
			}
			prefix.WriteByte('>')
			c.template.operations[id].text = prefix.String()
			out = []ast.Stmt{out[0]}
		}

		if !m.void {
			s.tags = append(s.tags, m.name)
		}
	case markupClose:
		if len(s.tags) == 0 || s.tags[len(s.tags)-1] != m.name {
			if fragment {
				return nil, Diagnostic{pos, "unbalanced closing fragment </>"}
			}
			return nil, Diagnostic{pos, "unbalanced closing tag " + m.name}
		}
		if fragment {
			s.tags = s.tags[:len(s.tags)-1]
			break
		}
		before := s.context.Clone()
		if _, err := s.context.Text("</" + m.name + ">"); err != nil {
			return nil, Diagnostic{pos, err.Error()}
		}
		s.tags = s.tags[:len(s.tags)-1]
		out = append(out, c.op(operation{kind: opClose, name: m.name, position: pos, input: before, content: s.context.Clone()}))
	case markupRaw:
		if err := text(m.text, opRaw); err != nil {
			return nil, err
		}
	case markupValue:
		return c.output(unwrap(args[0]), s)
	}
	return out, nil
}
func unwrap(e ast.Expr) ast.Expr {
	for {
		p, ok := e.(*ast.ParenExpr)
		if !ok {
			return e
		}
		e = p.X
	}
}
func (c *compiler) output(expr ast.Expr, s *renderState) ([]ast.Stmt, error) {
	t := c.info.TypeOf(expr)
	if t != nil {
		if _, ok := t.Underlying().(*types.Signature); ok {
			return nil, c.error(expr.Pos(), "invoke a function to render it")
		}
	}
	if call, ok := expr.(*ast.CallExpr); ok {
		if tuple, ok := t.(*types.Tuple); ok && tuple.Len() == 0 {
			if !s.context.IsText() {
				return nil, c.error(expr.Pos(), "rendering calls require HTML text context")
			}
			return c.guardedCall(expr), nil
		}
		if t == nil {
			if ft := c.info.TypeOf(call.Fun); ft != nil {
				if sig, ok := ft.Underlying().(*types.Signature); ok && sig.Results().Len() == 0 {
					if !s.context.IsText() {
						return nil, c.error(expr.Pos(), "rendering calls require HTML text context")
					}
					return c.guardedCall(expr), nil
				}
			}
		}
	}
	if tuple, ok := t.(*types.Tuple); ok && tuple.Len() > 1 {
		return nil, c.error(expr.Pos(), "output expression returns multiple values")
	}
	if c.template.format == TextFormat {
		position := c.position(expr.Pos())
		if lit, ok := expr.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			value, _ := strconv.Unquote(lit.Value)
			return []ast.Stmt{c.op(operation{kind: opRaw, text: value, position: position})}, nil
		}
		return c.guarded(expr, c.op(operation{kind: opValue, position: position}, expr), position), nil
	}
	if c.template.format == CSSFormat {
		if lit, ok := expr.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			value, _ := strconv.Unquote(lit.Value)
			before := s.context.Clone()
			escaped, err := s.context.Text(value)
			if err != nil {
				return nil, c.error(expr.Pos(), err.Error())
			}
			return []ast.Stmt{c.op(operation{kind: opRaw, text: escaped, position: c.position(expr.Pos()), input: before, content: s.context.Clone()})}, nil
		}
	}
	isText := s.context.IsText()
	before := s.context.Clone()
	f, err := s.context.Action()
	if err != nil {
		return nil, c.error(expr.Pos(), err.Error())
	}
	position := c.position(expr.Pos())
	if lit, ok := expr.(*ast.BasicLit); ok && lit.Kind == token.STRING {
		v, err := strconv.Unquote(lit.Value)
		if err != nil {
			return nil, err
		}
		return []ast.Stmt{c.op(operation{kind: opRaw, text: f.Apply(v), position: position, input: before, content: s.context.Clone()})}, nil
	}
	return c.guarded(expr, c.op(operation{kind: opValue, filter: f, position: position, input: before, content: s.context.Clone()}, expr), position, !isText), nil
}

func (c *compiler) closeTags(state *renderState, depth int, pos token.Pos) ([]ast.Stmt, error) {
	var out []ast.Stmt
	context := state.context.Clone()
	for i := len(state.tags) - 1; i >= depth; i-- {
		if state.tags[i] == "" {
			continue
		}
		before := context.Clone()
		if _, err := context.Text("</" + state.tags[i] + ">"); err != nil {
			return nil, c.error(pos, err.Error())
		}
		out = append(out, c.op(operation{kind: opClose, name: state.tags[i], input: before, content: context.Clone(), position: c.position(pos)}))
	}
	return out, nil
}
func (c *compiler) remapCompileError(err error) error {
	if ds, ok := err.(evmcompiler.Diagnostics); ok {
		for i := range ds {
			ds[i].Position = c.generatedPosition(ds[i].Position)
		}
		return ds
	}
	return err
}
func (c *compiler) remapModule(m *evm.Module) {
	for i := range m.Functions {
		f := &m.Functions[i]
		f.Position = c.generatedPosition(f.Position)
		for j := range f.Positions {
			f.Positions[j] = c.generatedPosition(f.Positions[j])
			if j < len(f.Lines) {
				f.Lines[j] = f.Positions[j].Line
			}
		}
	}
	for name, src := range c.template.sources {
		m.Sources[name] = src
	}
}

type branchTarget struct {
	loop  bool
	depth int
}

func (c *compiler) originalToken(p evm.SourcePosition) token.Pos {
	if p.File == "" {
		return token.NoPos
	}
	if c.originalTokens == nil {
		c.originalTokens = map[evm.SourcePosition]token.Pos{}
		for filename, positions := range c.mappings {
			f := c.originals[filename]
			for offset, position := range positions {
				if position.File != "" {
					if _, ok := c.originalTokens[position]; !ok {
						c.originalTokens[position] = f.Pos(offset)
					}
				}
			}
		}
	}
	return c.originalTokens[p]
}

func (c *compiler) initialState(file bool) renderState {
	state := renderState{}
	if file && c.template.format != HTMLFormat {
		state.rendering = true
	}
	if file && c.template.format == CSSFormat {
		_, _ = state.context.Text("<style>")
	}
	return state
}
