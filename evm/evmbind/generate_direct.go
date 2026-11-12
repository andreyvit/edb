package evmbind

import (
	"fmt"
	"strings"
)

// Live native pointers need no conversion frame. VM-cell pointers use the
// general alias-aware conversion, including cycles through reference fields.
func writeDirect(line func(string, ...any), op *operation, ids map[*node]string, typ func(*node) string) {
	var result *node
	var resultType string
	if len(op.outs) != 0 {
		result = op.outs[0]
		resultType = typ(result)
	}
	line("%q:func(host any) evm.DirectFunc {", op.key)
	inputs := op.args
	if op.recv == nil {
		params := []string{"*evm.Call"}
		for _, n := range inputs {
			params = append(params, typ(n))
		}
		line("h:=host.(interface{%s(%s) %s})", op.name, strings.Join(params, ","), resultType)
	} else {
		inputs = append([]*node{op.recv}, inputs...)
	}
	line("return func(c *evm.Call,x,y values.Value)(v values.Value,err error){")
	line("defer evmbind.Catch(%q,&err)", op.key)
	regs := [2]string{"x", "y"}
	var guards []string
	for i, n := range inputs {
		if n.kind == nodePointer {
			guards = append(guards, fmt.Sprintf("(%s.P!=nil && %s.R!=values.NativeAddress)", regs[i], regs[i]))
		}
	}
	emitCall := func(slow bool) {
		var args []string
		frame := "nil"
		if slow {
			line("f:=evmbind.Frame{Call:c}")
			frame = "&f"
		}
		for i, n := range inputs {
			arg := "arg" + regs[i]
			if n.kind == nodePointer && !slow {
				line("%s:=(%s)(%s.P)", arg, typ(n), regs[i])
			} else {
				line("%s:=read%s(%s,%s)", arg, ids[n], frame, regs[i])
			}
			args = append(args, arg)
		}
		receiver := "h"
		if op.recv != nil {
			receiver, args = args[0], args[1:]
		} else {
			args = append([]string{"c"}, args...)
		}
		var results []string
		if result != nil {
			results = []string{"out"}
		}
		writeInvocation(line, receiver+"."+op.name, args, results)
		if slow {
			line("f.CopyBack()")
		}
		if result == nil {
			line("return v,nil")
			return
		}
		switch result.kind {
		case nodePointer:
			line("v=write%s(%s,out)", ids[result], frame)
		case nodeString:
			line("v=values.String(string(out))") // The VM charges direct string results.
		default:
			line("v=write%s(nil,out)", ids[result])
		}
		line("return v,nil")
	}
	if len(guards) > 0 {
		line("if %s {", strings.Join(guards, "||"))
		emitCall(true)
		line("}")
	}
	emitCall(false)
	line("}},")
}
