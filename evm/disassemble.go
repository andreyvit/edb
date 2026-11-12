package evm

import (
	"fmt"
	"github.com/andreyvit/edb/values"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Disassemble lists all functions in compiled module order, with source lines and decoded operands.
func (m *Module) Disassemble() string {
	var dst strings.Builder
	sources := make(map[string][]string)
	for j := range m.Functions {
		if j != 0 {
			dst.WriteByte('\n')
		}
		m.writeDisassembly(&dst, j, sources)
	}
	return dst.String()
}

// DisassembleFunction lists the function at the given module index.
func (m *Module) DisassembleFunction(index int) string {
	var dst strings.Builder
	m.writeDisassembly(&dst, index, make(map[string][]string))
	return dst.String()
}

var extendedKindNames = [...]string{
	ExtendedMakeInterface: "MakeInterface",
	ExtendedAddressLoad:   "AddressLoad", ExtendedAddressStore: "AddressStore",
	ExtendedFieldStore:  "FieldStore",
	ExtendedNativeAlloc: "NativeAlloc",
	ExtendedTypeAssert:  "TypeAssert",
	ExtendedFieldAddr:   "FieldAddr",
	ExtendedField:       "Field",
	ExtendedPackedArray: "PackedArray", ExtendedPackedLoad: "PackedLoad", ExtendedPackedStore: "PackedStore",
	ExtendedIndexAddr:     "IndexAddr",
	ExtendedIndex:         "Index",
	ExtendedSlice:         "Slice",
	ExtendedMakeSlice:     "MakeSlice",
	ExtendedSliceToArray:  "SliceToArray",
	ExtendedMakeMap:       "MakeMap",
	ExtendedLookup:        "Lookup",
	ExtendedMapUpdate:     "MapUpdate",
	ExtendedDelete:        "Delete",
	ExtendedClear:         "Clear",
	ExtendedLen:           "Len",
	ExtendedCap:           "Cap",
	ExtendedAppend:        "Append",
	ExtendedCopy:          "Copy",
	ExtendedRange:         "Range",
	ExtendedNext:          "Next",
	ExtendedConvert:       "Convert",
	ExtendedEqual:         "Equal",
	ExtendedComplex:       "Complex",
	ExtendedReal:          "Real",
	ExtendedImag:          "Imag",
	ExtendedComplexBinary: "ComplexBinary",
	ExtendedComplexNegate: "ComplexNegate",
	ExtendedRecordCopy:    "RecordCopy",
	ExtendedRecordList:    "RecordList", ExtendedRecordListLen: "RecordListLen", ExtendedRecordListIndex: "RecordListIndex",
	ExtendedOwn: "Own", ExtendedGetChildKey: "GetChildKey", ExtendedGetKey: "GetKey", ExtendedSetKey: "SetKey",
	ExtendedMinMax: "MinMax",
}

func note(m *Module, pc int, i Instruction) string {
	k := i.BC()
	if i.Op().hostForm() != nil {
		return importName(m.Imports[i.C()])
	}
	switch i.Op() {
	case OpString:
		return strconv.Quote(m.Strings[k])
	case OpConst:
		return fmt.Sprintf("word %#x", m.Words[k])
	case OpCallFunc:
		return m.Functions[k].Name
	case OpCallHostRR, OpCallHostRP:
		k = uint32(i.C())
		fallthrough
	case OpCallHost:
		return importName(m.Imports[k])
	case OpLoopAddLtI64, OpLoopAddLeI64:
		return fmt.Sprintf("-> PC %d", pc+1+i.SignedD12())
	case OpLoopLtI64, OpLoopLeI64:
		return fmt.Sprintf("-> PC %d", pc+1+int(int16(i.C())))
	case OpJump, OpJz, OpJnz, OpJNil, OpJNotNil:
		return fmt.Sprintf("-> PC %d", pc+1+int(int32(k)))
	case OpExtended:
		d := m.ExtendedOps[k]
		if extendedKindNames[d.Kind] == "" {
			panic(fmt.Sprintf("missing descriptor name %d", d.Kind))
		}
		s := ""
		if d.Shape != nil {
			s = d.Shape.Type.Name()
		}
		if d.Source != nil {
			s = d.Source.Type.Name() + " -> " + s
		}
		if d.Record != nil {
			s = fmt.Sprintf("key%d %s.%s (%s)", d.Index, d.Record.Name, d.Record.Model.MustPropByCode(d.Index).Name(), s)
		}
		if d.Kind == ExtendedGetChildKey {
			prop := d.Record.Model.MustPropByCode(d.Index)
			leaf := prop.TypeModel().MustPropByCode(d.ChildKey)
			s = fmt.Sprintf("key%d.key%d %s.%s.%s (%s)", d.Index, d.ChildKey, d.Record.Name, prop.Name(), leaf.Name(), d.Shape.Type.Name())
		}
		if d.Kind == ExtendedFieldAddr || d.Kind == ExtendedField {
			field := d.Shape.Fields[d.Index&^VMFieldBase]
			s = fmt.Sprintf("%s.%s (%s)", s, field.Name, field.Shape.Type.Name())
		}
		detail := fmt.Sprintf("%s %s  %s -> %s", extendedKindNames[d.Kind], s, operands(d.In), operands(d.Out))
		if d.Kind == ExtendedMakeInterface {
			detail += fmt.Sprintf(" typ%d", d.Index)
		}
		if d.Packed {
			detail += " packed"
		}
		if d.Flags != 0 {
			detail += fmt.Sprintf(" flags=%d", d.Flags)
		}
		return detail
	case OpAlloc:
		return m.Shapes[i.B()].Type.Name()
	case OpLoad, OpStore:
		return m.Shapes[i.C()].Type.Name()
	case OpConvert:
		d := m.Conversions[i.C()]
		return numeric(d.From) + " -> " + numeric(d.To)
	case OpClosure:
		d := m.Closures[k]
		return fmt.Sprintf("%s, captures %s", functionName(m, m.FunctionRefs[d.Function]), operands(d.Captures))
	case OpCallIndirect:
		sig := m.Signatures[k]
		return fmt.Sprintf("callable v%d; %s -> %s", i.A(), typeNames(sig.Params), typeNames(sig.Results))
	case OpDefer:
		d := m.Defers[k]
		var target string
		switch d.Kind {
		case DeferDirect:
			target = functionName(m, d.Target)
		case DeferIndirect:
			target = fmt.Sprintf("callable v%d", d.Function.Slot)
		case DeferInvoke:
			target = fmt.Sprintf("v%d %s", d.Receiver, invokeName(m.Invokes[d.Invoke]))
		case DeferPanic:
			target = "panic"
		case DeferRecover:
			target = "recover"
		}
		if len(d.Args) > 0 {
			target += ", capture " + operands(d.Args)
		}
		if d.ExternalStack {
			target += fmt.Sprintf(", owner r%d", i.A())
		}
		return target
	case OpCallVirt:
		return invokeName(m.Invokes[k])
	case OpGlobal:
		return m.Globals[k].Type.Name()
	case OpFunction:
		return functionName(m, m.FunctionRefs[k])
	}
	return ""
}
func importName(imp Import) string {
	if imp.Receiver != nil {
		return imp.Receiver.Name() + "." + imp.Name
	}
	return imp.Package + "." + imp.Name
}
func functionName(m *Module, ref FunctionRef) string {
	if ref.Native {
		imp := m.Imports[ref.Index]
		return imp.Package + "." + imp.Name
	}
	return m.Functions[ref.Index].Name
}
func invokeName(op Invoke) string {
	if op.InterfaceName == "" {
		return op.Method.ID
	}
	return op.InterfaceName + "." + op.Method.ID
}
func typeNames(ts []*values.Type) string {
	var names []string
	for _, t := range ts {
		names = append(names, t.Name())
	}
	return "(" + strings.Join(names, ",") + ")"
}
func numeric(n NumericType) string {
	return fmt.Sprintf("%s%d", map[NumericKind]string{NumericSigned: "i", NumericUnsigned: "u", NumericFloat: "f"}[n.Kind], n.Bits)
}
func operands(os []Operand) string {
	var names []string
	for _, o := range os {
		names = append(names, fmt.Sprintf("%s%d", bankPrefix[o.Repr], o.Slot))
	}
	return "[" + strings.Join(names, ",") + "]"
}

var bankPrefix = [...]string{values.Raw: "r", values.Pointer: "p", values.Pair: "v"}

// BC is one operand. A is omitted for a BC-only instruction unless its
// reserved bits are nonzero, in which case show the unexpected value.
func (m *Module) instructionOperands(i Instruction) string {
	a, b, c := i.A(), i.B(), i.C()
	if pa, pb, ok := i.Op().HostRegisters(); ok {
		if pa == 0 {
			if pb == 0 {
				return fmt.Sprintf("%d %d imp%d", a, b, c)
			}
			return fmt.Sprintf("%d %c%d imp%d", a, pb, b, c)
		}
		if pb == 0 {
			return fmt.Sprintf("%c%d %d imp%d", pa, a, b, c)
		}
		return fmt.Sprintf("%c%d %c%d imp%d", pa, a, pb, b, c)
	}
	var pa, pb, pc string
	var table string
	switch i.Op() {
	case OpJump:
		if a != 0 {
			return fmt.Sprintf("%d %d", a, int32(i.BC()))
		}
		return strconv.FormatInt(int64(int32(i.BC())), 10)
	case OpJz, OpJnz:
		return fmt.Sprintf("r%d %d", a, int32(i.BC()))
	case OpJNil, OpJNotNil:
		return fmt.Sprintf("p%d %d", a, int32(i.BC()))
	case OpLoopAddLtI64, OpLoopAddLeI64:
		a, b, c, _ := i.ABCD12()
		return fmt.Sprintf("r%d r%d r%d %d", a, b, c, i.SignedD12())
	case OpLoopLtI64, OpLoopLeI64:
		return fmt.Sprintf("r%d r%d %d", a, b, int16(c))
	case OpCallHostRR:
		pa, pb, pc = "r", "r", "imp"
	case OpCallHostRP:
		pa, pb, pc = "p", "r", "imp"
	case OpConst:
		pa, table = "r", "wrd"
	case OpString:
		pa, table = "v", "str"
	case OpClosure:
		pa, table = "v", "cls"
	case OpFunction:
		pa, table = "v", "ref"
	case OpCallIndirect:
		pa, table = "v", "sig"
	case OpGlobal:
		pa, table = "p", "gbl"
	case OpCallVirt:
		table = "mth"
	case OpExtended:
		table = "ext"
	case OpCallFunc:
		table = "fun"
	case OpCallHost:
		table = "imp"
	case OpDefer:
		table = "dfr"
		if m.Defers[i.BC()].ExternalStack {
			pa = "r"
		}
	case OpReturn, OpRunDefers:
	case OpAlloc:
		pa, pb = "p", "shp"
	case OpClearP, OpCheckNil:
		pa = "p"
	case OpCheckShift, OpDeferStack:
		pa = "r"
	case OpPanic, OpRecover:
		pa = "v"
	case OpMoveR:
		pa, pb = "r", "r"
	case OpMoveP:
		pa, pb = "p", "p"
	case OpMoveV:
		pa, pb = "v", "v"
	case OpLoad:
		pa, pb, pc = bankPrefix[m.Shapes[c].Type.Repr()], "p", "shp"
	case OpStore:
		pa, pb, pc = "p", bankPrefix[m.Shapes[c].Type.Repr()], "shp"
	case OpConvert:
		pa, pb, pc = "r", "r", "cnv"
	case OpMulAddF64, OpAddMulF64, OpSubAddF64:
		a, b, c, d := i.ABCD12()
		return fmt.Sprintf("r%d r%d r%d r%d", a, b, c, d)
	case OpAddImmI64:
		return fmt.Sprintf("r%d r%d %d", a, b, int16(c))
	case OpLoadIndexPtr:
		pa, pb, pc = "p", "v", "r"
	case OpStoreIndexPtr:
		pa, pb, pc = "v", "r", "p"
	case OpLoadIndexR, OpLoadIndexByte, OpLoadIndexWord:
		pa, pb, pc = "r", "v", "r"
	case OpStoreIndexR, OpStoreIndexByte, OpStoreIndexWord:
		pa, pb, pc = "v", "r", "r"
	case OpEqP:
		pa, pb, pc = "r", "p", "p"
	case OpEqStr, OpLtStr:
		pa, pb, pc = "r", "v", "v"
	case OpConcatStr:
		pa, pb, pc = "v", "v", "v"
	default:
		pa, pb, pc = "r", "r", "r"
	}
	if table != "" {
		operand := table + strconv.FormatUint(uint64(i.BC()), 10)
		if pa == "" && a == 0 {
			return operand
		}
		return fmt.Sprintf("%s%d %s", pa, a, operand)
	}
	prefixes := [...]string{pa, pb, pc}
	fields := [...]uint16{a, b, c}
	n := len(fields)
	for n > 0 && prefixes[n-1] == "" && fields[n-1] == 0 {
		n--
	}
	var args []string
	for j, v := range fields[:n] {
		args = append(args, prefixes[j]+strconv.Itoa(int(v)))
	}
	return strings.Join(args, " ")
}

func (m *Module) writeDisassembly(dst *strings.Builder, j int, sources map[string][]string) {
	f := &m.Functions[j]
	fmt.Fprintf(dst, "fun%d %s   R=%d P=%d\n", j, f.Name, f.RCount, f.PCount)
	var previous SourcePosition
	for pc, i := range f.Code {
		pos := f.position(pc)
		if pos.Line > 0 && (pos.File != previous.File || pos.Line != previous.Line) {
			lines, seen := sources[pos.File]
			if !seen {
				if source, ok := m.Sources[pos.File]; ok {
					lines = strings.Split(source, "\n")
				}
				sources[pos.File] = lines
			}
			fmt.Fprint(dst, "     ; ")
			if pos.File != "" {
				fmt.Fprintf(dst, "%s:", pos.File)
			}
			fmt.Fprint(dst, pos.Line)
			if pos.Line <= len(lines) {
				fmt.Fprintf(dst, "  %s", strings.TrimRight(lines[pos.Line-1], " \t\r"))
			}
			fmt.Fprintln(dst)
		}
		previous = pos
		row := strings.TrimRight(fmt.Sprintf("%3d  %-13s %s", pc, i.Op(), m.instructionOperands(i)), " ")
		fmt.Fprint(dst, row)
		msg := note(m, pc, i)
		if msg != "" {
			fmt.Fprint(dst, "  ; ")
			indent := strings.Repeat(" ", len(row)+2) + "; "
			width := 80 - len(indent)
			for utf8.RuneCountInString(msg) > width {
				// Prefer a word boundary. A long literal can move as a whole.
				cut := strings.LastIndexByte(string([]rune(msg)[:width]), ' ')
				if cut < 0 {
					break
				}
				fmt.Fprint(dst, strings.TrimRight(msg[:cut], " "), "\n", indent)
				msg = strings.TrimLeft(msg[cut:], " ")
			}
			fmt.Fprint(dst, msg)
		}
		fmt.Fprintln(dst)
	}
}
