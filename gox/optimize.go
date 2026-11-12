package gox

import (
	"go/ast"
	"strings"
)

// Batch only compiler-owned operations. Expressions, calls and Go control flow
// remain barriers, so evaluation order and attribute-context checks do not move.
func (c *compiler) batchOperations() {
	batch := func(statements []ast.Stmt) []ast.Stmt {
		var out []ast.Stmt
		for i := 0; i < len(statements); {
			if _, ok := c.fixedOperations[statements[i]]; !ok {
				out = append(out, statements[i])
				i++
				continue
			}
			first := statements[i]
			var ids []int
			for i < len(statements) {
				id, ok := c.fixedOperations[statements[i]]
				if !ok {
					break
				}
				ids = append(ids, id)
				i++
			}
			ids = c.foldFragments(ids)
			if len(ids) == 1 {
				stmt := c.op(c.template.operations[ids[0]])
				stmt.(*ast.ExprStmt).X.(*ast.CallExpr).Args[0] = first.(*ast.ExprStmt).X.(*ast.CallExpr).Args[0]
				out = append(out, stmt)
			} else {
				stmt := c.op(operation{kind: opBatch, steps: ids, position: c.template.operations[ids[0]].position})
				stmt.(*ast.ExprStmt).X.(*ast.CallExpr).Args[0] = first.(*ast.ExprStmt).X.(*ast.CallExpr).Args[0]
				out = append(out, stmt)
			}
		}
		return out
	}
	ast.Inspect(c.file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.BlockStmt:
			n.List = batch(n.List)
		case *ast.CaseClause:
			n.Body = batch(n.Body)
		}
		return true
	})
}

// A complete constant subtree has no pending tag left for a helper to mutate.
// It can be one raw write while still checking its incoming rendering context.
func (c *compiler) foldFragments(ids []int) []int {
	var out []int
	for i := 0; i < len(ids); i++ {
		first := c.template.operations[ids[i]]
		if first.kind != opTag {
			out = append(out, ids[i])
			continue
		}
		var text strings.Builder
		var tags []string
		end := -1
		for j := i; j < len(ids); j++ {
			op := c.template.operations[ids[j]]
			switch op.kind {
			case opTag:
				text.WriteString(op.text)
				if op.void {
					if !voidTags[op.name] {
						text.WriteString("</" + op.name + ">")
					}
				} else {
					tags = append(tags, op.name)
				}
			case opRaw:
				text.WriteString(op.text)
			case opClose:
				if len(tags) == 0 || tags[len(tags)-1] != op.name {
					goto done
				}
				tags = tags[:len(tags)-1]
				text.WriteString("</" + op.name + ">")
			default:
				goto done
			}
			if len(tags) == 0 {
				end = j
				break
			}
		}
	done:
		if end < 0 {
			out = append(out, ids[i])
			continue
		}
		op := operation{kind: opRaw, text: text.String(), position: first.position, input: first.input, content: c.template.operations[ids[end]].content}
		out = append(out, len(c.template.operations))
		c.template.operations = append(c.template.operations, op)
		i = end
	}
	return out
}
