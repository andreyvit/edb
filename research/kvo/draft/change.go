package kvodraft

import "encoding/binary"

type StepKind byte

const (
	FieldStep StepKind = iota
	NumberStep
	StringStep
	IndexStep
)

type Step struct {
	Kind   StepKind
	Number uint64
	Text   string
}
type Change struct {
	Path   []Step
	Delete bool
	Value  Value
}
type Changes struct {
	Base, Next uint64
	Items      []Change
}

// AppendChanges writes ordered logical assignments/deletions, not physical byte
// offsets. It neither applies revisions nor makes a transaction durable.
func AppendChanges(dst []byte, c Changes) []byte {
	if c.Next <= c.Base {
		panic(ErrValue)
	}
	if len(c.Items) > MaxEntries {
		panic(ErrLimit)
	}
	// Validate before changing dst. Bound total length without a temporary buffer.
	size := ulen(c.Base) + ulen(c.Next) + ulen(uint64(len(c.Items)))
	var previous []Step
	for _, x := range c.Items {
		if len(x.Path) > MaxEntries {
			panic(ErrLimit)
		}
		common := commonPath(previous, x.Path)
		size += ulen(uint64(common)) + ulen(uint64(len(x.Path)-common)<<1)
		for _, s := range x.Path[common:] {
			switch s.Kind {
			case FieldStep, IndexStep:
				if s.Number>>62 != 0 {
					panic(ErrLimit)
				}
				size += ulen(s.Number<<2 | uint64(s.Kind))
			case NumberStep:
				size += 1 + ulen(s.Number)
			case StringStep:
				if len(s.Text) > MaxBytes {
					panic(ErrLimit)
				}
				size += ulen(uint64(len(s.Text))<<2|2) + len(s.Text)
			default:
				panic(ErrValue)
			}
			if size > MaxBytes {
				panic(ErrLimit)
			}
		}
		if !x.Delete {
			if !x.Value.valid() {
				panic(ErrValue)
			}
			if len(x.Value.data) > MaxBytes {
				panic(ErrLimit)
			}
			value := x.Value
			if value.IsZero() {
				value = Value{}
			}
			n := 1 + bodyLen(value)
			size += ulen(uint64(n)) + n
		}
		if size > MaxBytes {
			panic(ErrLimit)
		}
		previous = x.Path
	}
	dst = binary.AppendUvarint(dst, c.Base)
	dst = binary.AppendUvarint(dst, c.Next)
	dst = binary.AppendUvarint(dst, uint64(len(c.Items)))
	previous = nil
	for _, x := range c.Items {
		common := commonPath(previous, x.Path)
		dst = binary.AppendUvarint(dst, uint64(common))
		suffix := uint64(len(x.Path)-common) << 1
		if x.Delete {
			suffix |= 1
		}
		dst = binary.AppendUvarint(dst, suffix)
		for _, s := range x.Path[common:] {
			switch s.Kind {
			case FieldStep, IndexStep:
				dst = binary.AppendUvarint(dst, s.Number<<2|uint64(s.Kind))
			case NumberStep:
				dst = append(dst, 1)
				dst = binary.AppendUvarint(dst, s.Number)
			case StringStep:
				dst = binary.AppendUvarint(dst, uint64(len(s.Text))<<2|2)
				dst = append(dst, s.Text...)
			}
		}
		if !x.Delete {
			value := x.Value
			if value.IsZero() {
				value = Value{}
			}
			dst = binary.AppendUvarint(dst, uint64(1+bodyLen(value)))
			dst = appendBody(append(dst, byte(value.wire)), value)
		}
		previous = x.Path
	}
	return dst
}
func commonPath(a, b []Step) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

// PathPart is pointer-free. String-key bytes are offsets in the reader's input.
type PathPart struct {
	Kind       StepKind
	Number     uint64
	start, end int
}
type ChangeView struct {
	Path   []PathPart
	Delete bool
	Value  Value
}
type ChangeReader struct {
	Base, Next uint64
	data       []byte
	pos, left  int
	path       []PathPart
	failed     bool
}

func OpenChanges(data []byte, scratch []PathPart) (r ChangeReader, err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != ErrFormat {
				panic(p)
			}
			r = ChangeReader{}
			err = ErrFormat
		}
	}()
	if len(data) < 3 || len(data) > MaxBytes {
		return r, ErrFormat
	}
	r.data = data
	r.Base, data = readU(data)
	r.Next, data = readU(data)
	count, data := readU(data)
	if r.Next <= r.Base || count > MaxEntries || count > uint64(len(data)/2) {
		return ChangeReader{}, ErrFormat
	}
	r.left = int(count)
	r.pos = len(r.data) - len(data)
	r.path = scratch[:0]
	return r, nil
}
func (r *ChangeReader) Key(p PathPart) []byte {
	if p.Kind != StringStep || p.start < 0 || p.end < p.start || p.end > len(r.data) {
		panic(ErrValue)
	}
	return r.data[p.start:p.end]
}
func (r *ChangeReader) NextChange() (out ChangeView, ok bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			if p != ErrFormat {
				panic(p)
			}
			out = ChangeView{}
			ok = false
			err = ErrFormat
			r.failed = true
		}
	}()
	if r.failed {
		return out, false, ErrFormat
	}
	if r.left == 0 {
		if r.pos != len(r.data) {
			r.failed = true
			return out, false, ErrFormat
		}
		return out, false, nil
	}
	prefix, b := readU(r.data[r.pos:])
	tag, b := readU(b)
	suffix := tag >> 1
	if prefix > uint64(len(r.path)) || suffix > uint64(len(b)) || prefix+suffix > MaxEntries {
		r.failed = true
		return out, false, ErrFormat
	}
	needed := int(prefix + suffix)
	if needed > cap(r.path) {
		grown := make([]PathPart, needed, max(needed, cap(r.path)*2))
		copy(grown, r.path[:int(prefix)])
		r.path = grown
	} else {
		r.path = r.path[:needed]
	}
	for i := int(prefix); i < len(r.path); i++ {
		code, rest := readU(b)
		p := PathPart{Kind: StepKind(code & 3)}
		b = rest
		switch p.Kind {
		case FieldStep, IndexStep:
			p.Number = code >> 2
		case NumberStep:
			if code != 1 {
				panic(ErrFormat)
			}
			p.Number, b = readU(b)
		case StringStep:
			n := code >> 2
			if n > uint64(len(b)) {
				panic(ErrFormat)
			}
			p.start = len(r.data) - len(b)
			p.end = p.start + int(n)
			b = b[int(n):]
		}
		r.path[i] = p
	}
	out.Path = r.path
	out.Delete = tag&1 != 0
	if !out.Delete {
		n, rest := readU(b)
		if n == 0 || n > uint64(len(rest)) {
			panic(ErrFormat)
		}
		value, tail := readBody(rest[1:int(n)], Wire(rest[0]))
		if len(tail) != 0 {
			panic(ErrFormat)
		}
		out.Value = value
		b = rest[int(n):]
	}
	r.pos = len(r.data) - len(b)
	r.left--
	return out, true, nil
}
