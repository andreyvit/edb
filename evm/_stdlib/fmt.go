package fmt

// Native leaves inspect externally tracked types and enforce budgets. All
// method calls and traversal run as ordinary VM code.
func scalar(value any, flags string, width, precision int, hasWidth, hasPrecision bool, verb rune, diagnostic bool) string
func describe(value any) (kind int, name string, length int, isNil bool)
func children(value any) (items []any, names []string, methods []bool)
func checkSize(size int)
func output(text string) (int, error)
func comparable(value any) bool
func integer(value any) (int, bool)
func byteSlice(value any) bool
func decodeRune(text string) (rune, int)

type State interface {
	Write([]byte) (int, error)
	Width() (int, bool)
	Precision() (int, bool)
	Flag(int) bool
}
type Formatter interface{ Format(State, rune) }
type Stringer interface{ String() string }
type GoStringer interface{ GoString() string }

type buffer struct {
	diagnostic             bool
	data                   []byte
	flags                  string
	width, precision       int
	hasWidth, hasPrecision bool
	panicking              bool
}

func (b *buffer) Write(p []byte) (int, error) {
	checkSize(len(b.data) + len(p))
	b.data = append(b.data, p...)
	return len(p), nil
}
func (b *buffer) text(s string)          { checkSize(len(b.data) + len(s)); b.data = append(b.data, s...) }
func (b *buffer) Width() (int, bool)     { return b.width, b.hasWidth }
func (b *buffer) Precision() (int, bool) { return b.precision, b.hasPrecision }
func (b *buffer) Flag(c int) bool {
	for _, r := range b.flags {
		if r == rune(c) {
			return true
		}
	}
	return false
}
func (b *buffer) reset() {
	b.flags = ""
	b.width = 0
	b.precision = 0
	b.hasWidth = false
	b.hasPrecision = false
}
func (b *buffer) leaf(v any, verb rune) {
	b.text(scalar(v, b.flags, b.width, b.precision, b.hasWidth, b.hasPrecision, verb, b.diagnostic))
}

// Each method panic is caught at its direct invocation boundary, as in fmt.
func (b *buffer) method(v any, verb rune) (handled bool) {
	name := ""
	defer func() {
		if p := recover(); p != nil {
			handled = true
			_, _, _, nilValue := describe(v)
			if nilValue {
				b.text("<nil>")
			} else {
				b.text("%!")
				b.text(string(verb))
				b.text("(PANIC=")
				b.text(name)
				b.text(" method: ")
				if b.panicking {
					panic(p)
				}
				flags, w, pr, hw, hp := b.flags, b.width, b.precision, b.hasWidth, b.hasPrecision
				b.reset()
				b.panicking = true
				b.value(p, 'v', 0, true)
				b.panicking = false
				b.flags = flags
				b.width = w
				b.precision = pr
				b.hasWidth = hw
				b.hasPrecision = hp
				b.text(")")
			}
		}
	}()
	if f, ok := v.(Formatter); ok {
		name = "Format"
		f.Format(b, verb)
		return true
	}
	if verb == 'v' && b.Flag('#') {
		if f, ok := v.(GoStringer); ok {
			name = "GoString"
			b.leaf(f.GoString(), 's')
			return true
		}
		return false
	}
	if verb == 'v' || verb == 's' || verb == 'q' || verb == 'x' || verb == 'X' {
		if f, ok := v.(error); ok {
			name = "Error"
			b.leaf(f.Error(), verb)
			return true
		}
		if f, ok := v.(Stringer); ok {
			name = "String"
			b.leaf(f.String(), verb)
			return true
		}
	}
	return false
}
func (b *buffer) value(v any, verb rune, depth int, methods bool) {
	if verb == 'p' {
		kind, _, _, _ := describe(v)
		if kind == 7 || kind == 8 {
			b.badPointer(v)
			return
		}
	}
	if verb == 'T' || verb == 'p' {
		b.leaf(v, verb)
		return
	}
	if methods && b.method(v, verb) {
		return
	}
	kind, name, n, isNil := describe(v)
	sharp := !b.diagnostic && (verb == 'v' || verb == 'w') && b.Flag('#')
	if sharp && verb == 'v' && name == "[]uint8" {
		name = "[]byte"
	}
	if kind == 6 && depth == 0 && !isNil { // pointer to aggregate
		items, _, _ := children(v)
		if len(items) > 0 {
			b.text("&")
			b.value(items[0], verb, depth+1, methods)
			return
		}
	}
	if kind != 7 && kind != 8 && kind != 9 && kind != 10 {
		b.leaf(v, verb)
		return
	}
	if kind == 9 && byteSlice(v) && (verb == 's' || verb == 'q' || verb == 'x' || verb == 'X') {
		b.leaf(v, verb)
		return
	}
	if sharp && (kind == 9 || kind == 10) && isNil {
		b.text(name)
		b.text("(nil)")
		return
	}
	if sharp {
		b.text(name)
	} else if kind == 10 {
		b.text("map")
	}
	if kind == 7 || sharp {
		b.text("{")
	} else {
		b.text("[")
	}
	items, names, allowed := children(v)
	for i := 0; i < len(items); i++ {
		if i > 0 {
			if kind == 10 && i%2 == 1 {
				b.text(":")
			} else if sharp {
				b.text(", ")
			} else {
				b.text(" ")
			}
		}
		if kind == 7 && (sharp || !b.diagnostic && b.Flag('+')) {
			b.text(names[i])
			b.text(":")
		}
		b.value(items[i], verb, depth+1, methods && allowed[i])
	}
	_ = n
	if kind == 7 || sharp {
		b.text("}")
	} else {
		b.text("]")
	}
}
func Sprint(a ...any) string {
	b := &buffer{}
	lastString := false
	for i, v := range a {
		kind, _, _, _ := describe(v)
		isString := kind == 5
		if i > 0 && !lastString && !isString {
			b.text(" ")
		}
		b.value(v, 'v', 0, true)
		lastString = isString
	}
	return string(b.data)
}
func Sprintln(a ...any) string {
	b := &buffer{}
	for i, v := range a {
		if i > 0 {
			b.text(" ")
		}
		b.value(v, 'v', 0, true)
	}
	b.text("\n")
	return string(b.data)
}
func Sprintf(format string, a ...any) string      { s, _ := formatArgs(format, a, false); return s }
func Print(a ...any) (int, error)                 { return output(Sprint(a...)) }
func Println(a ...any) (int, error)               { return output(Sprintln(a...)) }
func Printf(format string, a ...any) (int, error) { return output(Sprintf(format, a...)) }

type textError struct{ text string }

func (e *textError) Error() string { return e.text }

type wrapError struct {
	text string
	err  error
}

func (e *wrapError) Error() string { return e.text }
func (e *wrapError) Unwrap() error { return e.err }

type wrapErrors struct {
	text string
	errs []error
}

func (e *wrapErrors) Error() string   { return e.text }
func (e *wrapErrors) Unwrap() []error { return e.errs }
func Errorf(format string, a ...any) error {
	text, indices := formatArgs(format, a, true)
	if len(indices) == 0 {
		return &textError{text}
	}
	if len(indices) == 1 {
		err, _ := a[indices[0]].(error)
		return &wrapError{text, err}
	}
	// Indexed directives may reorder arguments. Go exposes wrappers in argument
	// order and includes each argument at most once for multiple %w directives.
	for i := 1; i < len(indices); i++ {
		for j := i; j > 0 && indices[j] < indices[j-1]; j-- {
			indices[j], indices[j-1] = indices[j-1], indices[j]
		}
	}
	var errs []error
	last := -1
	for _, i := range indices {
		if i != last {
			if err, ok := a[i].(error); ok {
				errs = append(errs, err)
			}
			last = i
		}
	}
	return &wrapErrors{text, errs}
}
func number(s string, i int) (n, next int) {
	next = i
	for next < len(s) && s[next] >= '0' && s[next] <= '9' {
		if n > 1000000 {
			return -1, len(s)
		}
		n = n*10 + int(s[next]-'0')
		next++
	}
	return
}
func index(s string, i int) (n, next int, found, valid bool) {
	next = i
	if i >= len(s) || s[i] != '[' {
		return
	}
	found = true
	next = i + 1
	if len(s)-i < 3 {
		return
	}
	for end := i + 1; end < len(s); end++ {
		if s[end] != ']' {
			continue
		}
		next = end + 1
		if end == i+1 {
			return
		}
		parsed, stop := number(s[i+1:end], 0)
		if parsed >= 0 && stop == end-i-1 {
			n = parsed - 1
			valid = true
		}
		return
	}
	return
}

func formatArgs(format string, a []any, wrapping bool) (string, []int) {
	b := &buffer{}
	arg := 0
	reordered := false
	var wrapped []int
	for i := 0; i < len(format); {
		start := i
		for i < len(format) && format[i] != '%' {
			i++
		}
		b.text(format[start:i])
		if i == len(format) {
			break
		}
		i++
		b.reset()
		for i < len(format) && (format[i] == '#' || format[i] == '0' || format[i] == '+' || format[i] == '-' || format[i] == ' ') {
			b.flags += string(format[i])
			i++
		}
		good := true
		afterIndex := false
		n, next, found, valid := index(format, i)
		if found {
			reordered = true
			afterIndex = valid
			good = good && valid && n >= 0 && n < len(a)
			if valid && n >= 0 && n < len(a) {
				arg = n
			}
			i = next
		}
		if i < len(format) && format[i] == '*' {
			i++
			afterIndex = false
			if arg < len(a) {
				n, ok := integer(a[arg])
				arg++
				if ok && n >= -1000000 && n <= 1000000 {
					b.width = n
					b.hasWidth = true
					if n < 0 {
						b.width = -n
						b.flags += "-"
					}
				} else {
					b.text("%!(BADWIDTH)")
				}
			} else {
				b.text("%!(BADWIDTH)")
			}
		} else {
			n, next = number(format, i)
			if next > i {
				b.width = n
				b.hasWidth = n >= 0
				i = next
				if afterIndex {
					good = false
				}
			}
		}
		if i+1 < len(format) && format[i] == '.' {
			i++
			if afterIndex {
				good = false
			}
			b.hasPrecision = true
			b.precision = 0
			n, next, found, valid = index(format, i)
			afterIndex = found && valid
			if found {
				reordered = true
				good = good && valid && n >= 0 && n < len(a)
				if valid && n >= 0 && n < len(a) {
					arg = n
				}
				i = next
			}
			if i < len(format) && format[i] == '*' {
				afterIndex = false
				i++
				if arg < len(a) {
					n, ok := integer(a[arg])
					arg++
					if ok && n >= 0 && n <= 1000000 {
						b.precision = n
					} else {
						b.hasPrecision = false
						b.text("%!(BADPREC)")
					}
				} else {
					b.hasPrecision = false
					b.text("%!(BADPREC)")
				}
			} else {
				n, next = number(format, i)
				b.precision = n
				b.hasPrecision = n >= 0
				i = next
			}
		}
		if !afterIndex {
			n, next, found, valid = index(format, i)
			if found {
				reordered = true
				good = good && valid && n >= 0 && n < len(a)
				if valid && n >= 0 && n < len(a) {
					arg = n
				}
				i = next
			}
		}
		if i == len(format) {
			b.text("%!(NOVERB)")
			break
		}
		verb, size := decodeRune(format[i:])
		i += size
		if verb == '%' {
			b.text("%")
			continue
		}
		if !good {
			b.text("%!")
			b.text(string(verb))
			b.text("(BADINDEX)")
			continue
		}
		if arg >= len(a) {
			b.text("%!")
			b.text(string(verb))
			b.text("(MISSING)")
			continue
		}
		v := a[arg]
		arg++
		if verb == 'w' {
			if wrapping {
				wrapped = append(wrapped, arg-1)
			}
			if _, ok := v.(error); wrapping && ok {
				verb = 'v'
			} else {
				kind, name, _, _ := describe(v)
				if kind == 9 && name == "[]uint8" {
					b.value(v, 'w', 0, false)
					continue
				}
				b.text("%!w(")
				if v == nil {
					b.text("<nil>")
				} else {
					b.text(scalar(v, "", 0, 0, false, false, 'T', false))
					b.text("=")
					b.value(v, 'v', 0, false)
				}
				b.text(")")
				continue
			}
		}
		b.value(v, verb, 0, true)
	}
	if !reordered && arg < len(a) {
		b.reset()
		b.text("%!(EXTRA ")
		for i := arg; i < len(a); i++ {
			if i > arg {
				b.text(", ")
			}
			if a[i] == nil {
				b.text("<nil>")
			} else {
				b.text(scalar(a[i], "", 0, 0, false, false, 'T', false))
				b.text("=")
				b.value(a[i], 'v', 0, true)
			}
		}
		b.text(")")
	}
	return string(b.data), wrapped
}

func (b *buffer) badPointer(v any) {
	b.text("%!p(")
	b.text(scalar(v, "", 0, 0, false, false, 'T', false))
	b.text("=")
	old := b.diagnostic
	b.diagnostic = true
	b.value(v, 'v', 0, false)
	b.diagnostic = old
	b.text(")")
}
