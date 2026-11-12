package kvolab

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type StepKind byte

const (
	FieldStep StepKind = iota
	UintKeyStep
	StringKeyStep
	IndexStep
)

type Step struct {
	Kind StepKind
	U    uint64
	S    string
}
type Op byte

const (
	Set Op = iota
	Delete
)

type Change struct {
	Path  []Step
	Op    Op
	Value View
}
type ChangeSet struct {
	Schema, Base, Next uint64
	Changes            []Change
}

// AppendChanges encodes ordered logical changes with adjacent-path prefix
// compression. No sorting: parent replacements and child edits keep their order.
// Table/record identity and transaction identity belong in the outer WAL frame.
func AppendChanges(dst []byte, cs ChangeSet, prefix bool) []byte {
	dst = append(dst, 1)
	dst = binary.AppendUvarint(dst, cs.Schema)
	dst = binary.AppendUvarint(dst, cs.Base)
	dst = binary.AppendUvarint(dst, cs.Next)
	dst = binary.AppendUvarint(dst, uint64(len(cs.Changes)))
	var previous []Step
	for _, c := range cs.Changes {
		common := 0
		if prefix {
			for common < len(previous) && common < len(c.Path) && previous[common] == c.Path[common] {
				common++
			}
		}
		dst = binary.AppendUvarint(dst, uint64(common))
		dst = binary.AppendUvarint(dst, uint64(len(c.Path)-common))
		for _, s := range c.Path[common:] {
			dst = append(dst, byte(s.Kind))
			if s.Kind == StringKeyStep {
				dst = binary.AppendUvarint(dst, uint64(len(s.S)))
				dst = append(dst, s.S...)
			} else {
				dst = binary.AppendUvarint(dst, s.U)
			}
		}
		dst = append(dst, byte(c.Op))
		if c.Op == Set {
			dst = binary.AppendUvarint(dst, uint64(len(c.Value)))
			dst = append(dst, c.Value...)
		}
		previous = c.Path
	}
	return dst
}

// VisitChanges borrows payload/path slices. Supplied path scratch bounds depth;
// callbacks must copy before retaining them. Errors include malformed data.
func VisitChanges(data []byte, scratch []Step, visit func([]Step, Op, View) error) (cs ChangeSet, err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("malformed experimental change set")
		}
	}()
	if len(data) == 0 || data[0] != 1 {
		return cs, errors.New("change version")
	}
	data = data[1:]
	cs.Schema, data = takeU(data)
	cs.Base, data = takeU(data)
	cs.Next, data = takeU(data)
	count, rest := takeU(data)
	data = rest
	previous := scratch[:0]
	if count > uint64(len(data)) {
		return cs, errors.New("change count")
	}
	for i := uint64(0); i < count; i++ {
		common, rest := takeU(data)
		suffix, rest := takeU(rest)
		if common > uint64(len(previous)) || common+suffix > uint64(cap(scratch)) {
			return cs, errors.New("path depth/prefix")
		}
		path := scratch[:int(common+suffix)]
		for j := int(common); j < len(path); j++ {
			k := StepKind(rest[0])
			rest = rest[1:]
			if k > IndexStep {
				return cs, errors.New("step kind")
			}
			s := Step{Kind: k}
			if k == StringKeyStep {
				n, b := takeU(rest)
				s.S = string(b[:int(n)])
				rest = b[int(n):]
			} else {
				s.U, rest = takeU(rest)
			}
			path[j] = s
		}
		op := Op(rest[0])
		rest = rest[1:]
		var val View
		if op == Set {
			n, b := takeU(rest)
			val = b[:int(n)]
			rest = b[int(n):]
		} else if op != Delete {
			return cs, errors.New("op")
		}
		if err := visit(path, op, val); err != nil {
			return cs, err
		}
		previous = path
		data = rest
	}
	if len(data) != 0 {
		return cs, errors.New("trailing changes")
	}
	return cs, nil
}

// ApplyToFixture is a reference semantic interpreter, not an optimized editor.
// Call it on private transaction data. Replay revision gates live above this.
func ApplyToFixture(root *Value, path []Step, op Op, value Value) error {
	if len(path) == 0 {
		if op == Delete {
			return errors.New("delete root requires row operation")
		}
		*root = value
		return nil
	}
	step := path[0]
	tail := path[1:]
	switch step.Kind {
	case FieldStep, UintKeyStep:
		if root.Kind != Record {
			return errors.New("not record")
		}
		i := 0
		for i < len(root.Fields) && root.Fields[i].Key < step.U {
			i++
		}
		if i == len(root.Fields) || root.Fields[i].Key != step.U {
			if op == Delete && len(tail) == 0 {
				return nil
			}
			if len(tail) != 0 {
				return errors.New("missing parent")
			}
			root.Fields = append(root.Fields, Field{})
			copy(root.Fields[i+1:], root.Fields[i:])
			root.Fields[i] = Field{Key: step.U}
		}
		if len(tail) == 0 {
			if op == Delete {
				root.Fields = append(root.Fields[:i], root.Fields[i+1:]...)
			} else {
				root.Fields[i].Value = value
			}
			return nil
		}
		return ApplyToFixture(&root.Fields[i].Value, tail, op, value)
	case StringKeyStep:
		if root.Kind != Dict {
			return errors.New("not dictionary")
		}
		i := 0
		for i < len(root.Entries) && root.Entries[i].Key < step.S {
			i++
		}
		if i == len(root.Entries) || root.Entries[i].Key != step.S {
			if op == Delete && len(tail) == 0 {
				return nil
			}
			if len(tail) != 0 {
				return errors.New("missing parent")
			}
			root.Entries = append(root.Entries, Entry{})
			copy(root.Entries[i+1:], root.Entries[i:])
			root.Entries[i] = Entry{Key: step.S}
		}
		if len(tail) == 0 {
			if op == Delete {
				root.Entries = append(root.Entries[:i], root.Entries[i+1:]...)
			} else {
				root.Entries[i].Value = value
			}
			return nil
		}
		return ApplyToFixture(&root.Entries[i].Value, tail, op, value)
	case IndexStep:
		if root.Kind != List || step.U >= uint64(len(root.Items)) {
			return errors.New("invalid list index")
		}
		if len(tail) == 0 && op == Delete {
			return errors.New("array element deletion needs explicit splice semantics")
		}
		return ApplyToFixture(&root.Items[step.U], tail, op, value)
	default:
		return fmt.Errorf("unknown step %d", step.Kind)
	}
}

// AppendChanges2 folds field IDs/string lengths into path tags and folds the
// operation into suffix length. Arbitrary uint64 map keys retain a full varint.
// This supersedes v1 in the proposal; v1 stays for experiment history.
func AppendChanges2(dst []byte, cs ChangeSet) []byte {
	dst = append(dst, 2)
	dst = binary.AppendUvarint(dst, cs.Schema)
	dst = binary.AppendUvarint(dst, cs.Base)
	dst = binary.AppendUvarint(dst, cs.Next)
	dst = binary.AppendUvarint(dst, uint64(len(cs.Changes)))
	var previous []Step
	for _, c := range cs.Changes {
		common := 0
		for common < len(previous) && common < len(c.Path) && previous[common] == c.Path[common] {
			common++
		}
		dst = binary.AppendUvarint(dst, uint64(common))
		dst = binary.AppendUvarint(dst, uint64(len(c.Path)-common)<<1|uint64(c.Op))
		for _, s := range c.Path[common:] {
			switch s.Kind {
			case FieldStep, IndexStep:
				if s.U>>62 != 0 {
					panic("path field/index too large")
				}
				dst = binary.AppendUvarint(dst, s.U<<2|uint64(s.Kind))
			case UintKeyStep:
				dst = append(dst, 1)
				dst = binary.AppendUvarint(dst, s.U)
			case StringKeyStep:
				dst = binary.AppendUvarint(dst, uint64(len(s.S))<<2|2)
				dst = append(dst, s.S...)
			default:
				panic("step")
			}
		}
		if c.Op == Set {
			dst = binary.AppendUvarint(dst, uint64(len(c.Value)))
			dst = append(dst, c.Value...)
		}
		previous = c.Path
	}
	return dst
}
