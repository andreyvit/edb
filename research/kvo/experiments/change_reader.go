package kvolab

import "errors"

// PathRef borrows string-key bytes. Conversion to an owning string is explicit.
type PathRef struct {
	Kind           StepKind
	U              uint64
	Offset, Length int
}
type ChangeRef struct {
	Path  []PathRef
	Op    Op
	Value View
}
type ChangeReader struct {
	pos                        int
	version                    byte
	Schema, Base, NextRevision uint64
	data                       []byte
	path                       []PathRef
	remaining                  uint64
}

func OpenChanges(data []byte, path []PathRef) (r ChangeReader, err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("malformed changes")
		}
	}()
	if data[0] != 1 && data[0] != 2 {
		return r, errors.New("version")
	}
	r.version = data[0]
	data = data[1:]
	r.Schema, data = takeU(data)
	r.Base, data = takeU(data)
	r.NextRevision, data = takeU(data)
	r.remaining, data = takeU(data)
	if r.remaining > uint64(len(data)) {
		return r, errors.New("count")
	}
	r.data = data
	r.path = path[:0]
	return r, nil
}
func (r *ChangeReader) Next() (change ChangeRef, ok bool, err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("malformed change")
		}
	}()
	if r.remaining == 0 {
		if r.pos != len(r.data) {
			return change, false, errors.New("trailing bytes")
		}
		return change, false, nil
	}
	common, b := takeU(r.data[r.pos:])
	suffix, b := takeU(b)
	op := Set
	if r.version == 2 {
		op = Op(suffix & 1)
		suffix >>= 1
	}
	if common > uint64(len(r.path)) || suffix > uint64(cap(r.path))-common {
		return change, false, errors.New("depth/prefix")
	}
	r.path = r.path[:int(common+suffix)]
	for i := int(common); i < len(r.path); i++ {
		var step PathRef
		if r.version == 2 {
			tag, rest := takeU(b)
			b = rest
			step.Kind = StepKind(tag & 3)
			n := tag >> 2
			switch step.Kind {
			case FieldStep, IndexStep:
				step.U = n
			case UintKeyStep:
				if n != 0 {
					return change, false, errors.New("uint key tag")
				}
				step.U, b = takeU(b)
			case StringKeyStep:
				step.Offset = len(r.data) - len(b)
				step.Length = int(n)
				_ = b[:int(n)]
				b = b[int(n):]
			}
		} else {
			step.Kind = StepKind(b[0])
			b = b[1:]
			if step.Kind > IndexStep {
				return change, false, errors.New("key kind")
			}
			if step.Kind == StringKeyStep {
				n, rest := takeU(b)
				step.Offset = len(r.data) - len(rest)
				step.Length = int(n)
				_ = rest[:int(n)]
				b = rest[int(n):]
			} else {
				step.U, b = takeU(b)
			}
		}
		r.path[i] = step
	}
	if r.version == 1 {
		op = Op(b[0])
		b = b[1:]
	}
	var val View
	if op == Set {
		n, rest := takeU(b)
		val = rest[:int(n)]
		b = rest[int(n):]
	} else if op != Delete {
		return change, false, errors.New("operation")
	}
	r.pos = len(r.data) - len(b)
	r.remaining--
	return ChangeRef{r.path, op, val}, true, nil
}

func (r ChangeReader) Key(p PathRef) []byte { return r.data[p.Offset : p.Offset+p.Length] }
