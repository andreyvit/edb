// Package evmasm assembles textual EVM code without a Go compiler.
package evmasm

import (
	"bufio"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/values"
)

type line struct {
	number int
	words  []string
}
type body struct {
	lines     []line
	labels    map[string]int
	registers bool
}

// Assemble resolves labels and functions. Link performs structural validation
// and resolves external imports. Extra types are opaque, host-owned descriptors.
func Assemble(source string, extraTypes ...*values.Type) (*evm.Module, error) {
	types := make(map[string]*values.Type)
	for _, t := range append([]*values.Type{values.Int64, values.Uint64, values.Float64, values.Boolean, values.StringType}, extraTypes...) {
		if t == nil || types[t.Name()] != nil {
			return nil, fmt.Errorf("nil or duplicate type")
		}
		types[t.Name()] = t
	}
	m := &evm.Module{Sources: map[string]string{"": source}}
	var bodies []body
	functions, imports := make(map[string]uint32), make(map[string]uint32)
	current := -1
	scanner := bufio.NewScanner(strings.NewReader(source))
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for n := 1; scanner.Scan(); n++ {
		words, err := split(scanner.Text())
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		if len(words) == 0 {
			continue
		}
		fail := func(s string) (*evm.Module, error) { return nil, fmt.Errorf("line %d: %s", n, s) }
		switch words[0] {
		case "func", "import":
			if current >= 0 || len(words) < 3 {
				return fail("expected top-level function/import signature")
			}
			sigWords := words[2:]
			var receiver *values.Type
			if words[0] == "import" && sigWords[0] == "method" {
				if len(sigWords) < 3 {
					return fail("expected method receiver and signature")
				}
				receiver = types[sigWords[1]]
				if receiver == nil {
					return fail("unknown method receiver")
				}
				sigWords = sigWords[2:]
			}
			sig, err := signature(sigWords, types)
			if receiver != nil {
				sig.Params = append([]*values.Type{receiver}, sig.Params...)
			}
			if err != nil {
				return nil, fmt.Errorf("line %d: %w", n, err)
			}
			name := words[1]
			if words[0] == "func" {
				if _, ok := functions[name]; ok || uint64(len(m.Functions)) > math.MaxUint32 {
					return fail("duplicate function or too many functions")
				}
				functions[name] = uint32(len(m.Functions))
				current = len(m.Functions)
				m.Functions = append(m.Functions, evm.Function{Name: name, Signature: sig})
				bodies = append(bodies, body{labels: make(map[string]int)})
			} else {
				dot := strings.LastIndexByte(name, '.')
				if dot <= 0 || dot == len(name)-1 {
					return fail("import must be package.Function")
				}
				symbol := name
				if receiver != nil {
					symbol = receiver.Name() + "." + name[dot+1:]
				}
				if _, ok := imports[symbol]; ok || uint64(len(m.Imports)) > math.MaxUint32 {
					return fail("duplicate import or too many imports")
				}
				imports[symbol] = uint32(len(m.Imports))
				m.Imports = append(m.Imports, evm.Import{Receiver: receiver, Package: name[:dot], Name: name[dot+1:], Signature: sig})
			}
		case "end":
			if current < 0 || len(words) != 1 {
				return fail("unexpected end")
			}
			if !bodies[current].registers {
				return fail("function needs registers RCount PCount")
			}
			current = -1
		case "registers":
			if current < 0 || len(words) != 3 || bodies[current].registers || len(bodies[current].lines) != 0 {
				return fail("expected one registers declaration before instructions")
			}
			counts := [2]int{}
			for i := range counts {
				v, err := strconv.ParseUint(words[i+1], 10, 32)
				if err != nil || v > 65536 {
					return fail("register count must be in 0..65536")
				}
				counts[i] = int(v)
			}
			m.Functions[current].RCount, m.Functions[current].PCount = counts[0], counts[1]
			bodies[current].registers = true
		default:
			if current < 0 {
				return fail("instruction outside function")
			}
			b := &bodies[current]
			if len(words) == 1 && strings.HasSuffix(words[0], ":") {
				name := strings.TrimSuffix(words[0], ":")
				if _, ok := b.labels[name]; ok || name == "" {
					return fail("empty or duplicate label")
				}
				b.labels[name] = len(b.lines)
			} else {
				b.lines = append(b.lines, line{n, words})
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if current >= 0 {
		return nil, fmt.Errorf("missing end for %s", m.Functions[current].Name)
	}
	if len(m.Functions) == 0 {
		return nil, fmt.Errorf("no functions")
	}
	for i, b := range bodies {
		for pc, l := range b.lines {
			ins, err := assembleInstruction(l.words, pc, b.labels, functions, imports, m)
			if err != nil {
				return nil, fmt.Errorf("line %d: %w", l.number, err)
			}
			m.Functions[i].Code = append(m.Functions[i].Code, ins)
			m.Functions[i].Lines = append(m.Functions[i].Lines, l.number)
		}
	}
	return m, nil
}

func signature(words []string, types map[string]*values.Type) (evm.Signature, error) {
	var sig evm.Signature
	arrow := -1
	for i, w := range words {
		if w == "->" {
			if arrow >= 0 {
				return sig, fmt.Errorf("duplicate -> in signature")
			}
			arrow = i
		}
	}
	if arrow < 0 {
		return sig, fmt.Errorf("signature requires ->")
	}
	read := func(words []string) ([]*values.Type, error) {
		if len(words) == 1 && words[0] == "-" {
			return nil, nil
		}
		var result []*values.Type
		for _, w := range words {
			t := types[w]
			if t == nil {
				return nil, fmt.Errorf("unknown type %q", w)
			}
			result = append(result, t)
		}
		return result, nil
	}
	var err error
	sig.Params, err = read(words[:arrow])
	if err != nil {
		return sig, err
	}
	sig.Results, err = read(words[arrow+1:])
	return sig, err
}

// split keeps quoted strings intact; # starts a comment outside a string.
func split(s string) ([]string, error) {
	var words []string
	for i := 0; i < len(s); {
		if s[i] == '#' {
			break
		}
		if s[i] == ' ' || s[i] == '\t' || s[i] == '\r' || s[i] == ',' {
			i++
			continue
		}
		start := i
		if s[i] == '"' {
			i++
			for i < len(s) && s[i] != '"' {
				if s[i] == '\\' {
					i++
				}
				i++
			}
			if i >= len(s) {
				return nil, fmt.Errorf("unterminated string")
			}
			i++
			if i < len(s) && !strings.ContainsRune(" \t\r,#", rune(s[i])) {
				return nil, fmt.Errorf("expected separator after string")
			}
		} else {
			for i < len(s) && !strings.ContainsRune(" \t\r,#", rune(s[i])) {
				i++
			}
		}
		words = append(words, s[start:i])
	}
	return words, nil
}

func assembleInstruction(w []string, pc int, labels map[string]int, functions, imports map[string]uint32, m *evm.Module) (evm.Instruction, error) {
	var op evm.Opcode
	for candidate := evm.OpConst; candidate <= evm.OpReturn; candidate++ {
		if candidate.String() == w[0] {
			op = candidate
			break
		}
	}
	switch w[0] {
	case "float":
		op = evm.OpConst
	case evm.OpLoopAddLtI64.String():
		op = evm.OpLoopAddLtI64
	case evm.OpLoopAddLeI64.String():
		op = evm.OpLoopAddLeI64
	case evm.OpMulAddF64.String():
		op = evm.OpMulAddF64
	case evm.OpAddMulF64.String():
		op = evm.OpAddMulF64
	case evm.OpSubAddF64.String():
		op = evm.OpSubAddF64
	case evm.OpAddImmI64.String():
		op = evm.OpAddImmI64
	case evm.OpLoopLeI64.String():
		op = evm.OpLoopLeI64
	case evm.OpLeI64.String():
		op = evm.OpLeI64
	case evm.OpLeU64.String():
		op = evm.OpLeU64
	case evm.OpLoadIndexByte.String():
		op = evm.OpLoadIndexByte
	case evm.OpStoreIndexByte.String():
		op = evm.OpStoreIndexByte
	case evm.OpLoadIndexWord.String():
		op = evm.OpLoadIndexWord
	case evm.OpStoreIndexWord.String():
		op = evm.OpStoreIndexWord
	case evm.OpLoadIndexPtr.String():
		op = evm.OpLoadIndexPtr
	case evm.OpStoreIndexPtr.String():
		op = evm.OpStoreIndexPtr
	case evm.OpLoadIndexR.String():
		op = evm.OpLoadIndexR
	case evm.OpStoreIndexR.String():
		op = evm.OpStoreIndexR
	case evm.OpJNil.String():
		op = evm.OpJNil
	case evm.OpJNotNil.String():
		op = evm.OpJNotNil
	case evm.OpLoopLtI64.String():
		op = evm.OpLoopLtI64
	case evm.OpCallHostRR.String():
		op = evm.OpCallHostRR
	case evm.OpCallHostRP.String():
		op = evm.OpCallHostRP
	case evm.OpConcatStr.String():
		op = evm.OpConcatStr
	case evm.OpEqStr.String():
		op = evm.OpEqStr
	case evm.OpLtStr.String():
		op = evm.OpLtStr
	}
	if op == 0 && strings.HasPrefix(w[0], "call.host.") {
		for candidate := evm.OpCallHostU; candidate <= evm.OpCallHostMethodPV; candidate++ {
			if _, _, ok := candidate.HostRegisters(); ok && candidate.String() == w[0] {
				op = candidate
				break
			}
		}
	}
	if op == 0 {
		return 0, fmt.Errorf("unknown instruction %q", w[0])
	}
	args := w[1:]
	var a, b, c uint16
	var bc uint32
	var err error
	expect := func(n int) bool {
		if len(args) != n {
			err = fmt.Errorf("%s expects %d operands", w[0], n)
			return false
		}
		return true
	}
	reg := func(s string, bank byte) uint16 {
		if err != nil {
			return 0
		}
		if len(s) < 2 || s[0] != bank {
			err = fmt.Errorf("expected %c register, got %q", bank, s)
			return 0
		}
		v, e := strconv.ParseUint(s[1:], 10, 16)
		if e != nil {
			err = fmt.Errorf("invalid register %q", s)
			return 0
		}
		return uint16(v)
	}
	branch := func(name string) {
		target, ok := labels[name]
		if !ok {
			err = fmt.Errorf("unknown label %q", name)
			return
		}
		d := int64(target) - int64(pc) - 1
		if d < math.MinInt32 || d > math.MaxInt32 {
			err = fmt.Errorf("branch out of range")
			return
		}
		bc = uint32(int32(d))
	}
	callTarget := func(s string, table map[string]uint32, prefix string, bits int) uint32 {
		if index, ok := table[s]; ok {
			if bits == 16 && index > math.MaxUint16 {
				err = fmt.Errorf("call index out of range")
			}
			return index
		}
		index, e := strconv.ParseUint(strings.TrimPrefix(s, prefix), 10, bits)
		if e != nil {
			err = fmt.Errorf("invalid call target %q", s)
		}
		return uint32(index)
	}
	if pa, pb, ok := op.HostRegisters(); ok {
		if expect(3) {
			if pa != 0 {
				a = reg(args[0], pa)
			} else if args[0] != "0" {
				err = fmt.Errorf("%s requires zero A", op)
			}
			if pb != 0 {
				b = reg(args[1], pb)
			} else if args[1] != "0" {
				err = fmt.Errorf("%s requires zero B", op)
			}
			c = uint16(callTarget(args[2], imports, "imp", 16))
		}
		return evm.ABC(op, a, b, c), err
	}
	combinedBC := false
	switch op {
	case evm.OpConst:
		if expect(2) {
			a = reg(args[0], 'r')
			var value uint64
			var e error
			if w[0] == "float" {
				var f float64
				f, e = strconv.ParseFloat(args[1], 64)
				value = math.Float64bits(f)
			} else if strings.HasPrefix(args[1], "-") {
				var v int64
				v, e = strconv.ParseInt(args[1], 0, 64)
				value = uint64(v)
			} else {
				value, e = strconv.ParseUint(args[1], 0, 64)
			}
			if e != nil {
				err = fmt.Errorf("invalid constant %q", args[1])
			}
			if uint64(len(m.Words)) > math.MaxUint32 {
				err = fmt.Errorf("too many constants")
			}
			bc = uint32(len(m.Words))
			m.Words = append(m.Words, value)
		}
		combinedBC = true
	case evm.OpString:
		if expect(2) {
			a = reg(args[0], 'v')
			s, e := strconv.Unquote(args[1])
			if e != nil {
				err = e
			}
			if uint64(len(m.Strings)) > math.MaxUint32 {
				err = fmt.Errorf("too many strings")
			}
			bc = uint32(len(m.Strings))
			m.Strings = append(m.Strings, s)
		}
		combinedBC = true
	case evm.OpMoveR, evm.OpMoveP, evm.OpMoveV:
		if expect(2) {
			bank := byte('r')
			if op == evm.OpMoveP {
				bank = 'p'
			}
			if op == evm.OpMoveV {
				bank = 'v'
			}
			a, b = reg(args[0], bank), reg(args[1], bank)
		}
	case evm.OpClearP:
		if expect(1) {
			a = reg(args[0], 'p')
		}
	case evm.OpJump:
		if expect(1) {
			branch(args[0])
		}
		combinedBC = true
	case evm.OpJz, evm.OpJnz, evm.OpJNil, evm.OpJNotNil:
		if expect(2) {
			bank := byte('r')
			if op == evm.OpJNil || op == evm.OpJNotNil {
				bank = 'p'
			}
			a = reg(args[0], bank)
			branch(args[1])
		}
		combinedBC = true
	case evm.OpCallFunc, evm.OpCallHost:
		if expect(1) {
			table, prefix := functions, "fun"
			if op == evm.OpCallHost {
				table, prefix = imports, "imp"
			}
			bc = callTarget(args[0], table, prefix, 32)
		}
		combinedBC = true
	case evm.OpCallHostRR, evm.OpCallHostRP:
		if expect(3) {
			dst := byte('r')
			if op == evm.OpCallHostRP {
				dst = 'p'
			}
			a, b = reg(args[0], dst), reg(args[1], 'r')
			c = uint16(callTarget(args[2], imports, "imp", 16))
		}
	case evm.OpMulAddF64, evm.OpAddMulF64, evm.OpSubAddF64:
		if expect(4) {
			a, b, c, d := reg(args[0], 'r'), reg(args[1], 'r'), reg(args[2], 'r'), reg(args[3], 'r')
			if err != nil {
				return 0, err
			}
			if a|b|c|d > 0xfff {
				return 0, fmt.Errorf("compact operand exceeds 12 bits")
			}
			return evm.ABCD12(op, a, b, c, d), nil
		}
	case evm.OpLoopAddLtI64, evm.OpLoopAddLeI64:
		if expect(4) {
			a, b, c := reg(args[0], 'r'), reg(args[1], 'r'), reg(args[2], 'r')
			branch(args[3])
			if err != nil {
				return 0, err
			}
			if a|b|c > 0xfff || int32(bc) < -2048 || int32(bc) > 2047 {
				return 0, fmt.Errorf("compact loop operand out of range")
			}
			return evm.ABCD12(op, a, b, c, uint16(bc)&0xfff), nil
		}
	case evm.OpAddImmI64:
		if expect(3) {
			a, b = reg(args[0], 'r'), reg(args[1], 'r')
			var value int64
			if err == nil {
				value, err = strconv.ParseInt(args[2], 0, 16)
			}
			c = uint16(value)
		}
	case evm.OpLoadIndexPtr:
		if expect(3) {
			a, b, c = reg(args[0], 'p'), reg(args[1], 'v'), reg(args[2], 'r')
		}
	case evm.OpStoreIndexPtr:
		if expect(3) {
			a, b, c = reg(args[0], 'v'), reg(args[1], 'r'), reg(args[2], 'p')
		}
	case evm.OpLoadIndexR, evm.OpLoadIndexByte, evm.OpLoadIndexWord:
		if expect(3) {
			a, b, c = reg(args[0], 'r'), reg(args[1], 'v'), reg(args[2], 'r')
		}
	case evm.OpStoreIndexR, evm.OpStoreIndexByte, evm.OpStoreIndexWord:
		if expect(3) {
			a, b, c = reg(args[0], 'v'), reg(args[1], 'r'), reg(args[2], 'r')
		}
	case evm.OpLoopLtI64, evm.OpLoopLeI64:
		if expect(3) {
			a, b = reg(args[0], 'r'), reg(args[1], 'r')
			branch(args[2])
			if int32(bc) != int32(int16(bc)) {
				err = fmt.Errorf("loop branch out of range")
			}
			c = uint16(bc)
		}
	case evm.OpReturn:
		expect(0)
	default:
		if expect(3) {
			dst, src := byte('r'), byte('r')
			switch op {
			case evm.OpEqP:
				src = 'p'
			case evm.OpConcatStr:
				dst, src = 'v', 'v'
			case evm.OpLtStr, evm.OpEqStr:
				src = 'v'
			}
			a, b, c = reg(args[0], dst), reg(args[1], src), reg(args[2], src)
		}
	}
	if err != nil {
		return 0, err
	}
	if combinedBC {
		return evm.WithBC(op, a, bc), nil
	}
	return evm.ABC(op, a, b, c), nil
}
