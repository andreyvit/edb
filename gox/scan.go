package gox

import (
	"fmt"
	"html"
	"strconv"
	"strings"
	"unicode"
)

type part struct {
	text string
	arg  int
}
type attribute struct {
	name    string
	parts   []part
	boolean bool
}
type markupKind uint8

const (
	markupInvalid markupKind = iota
	markupOpen
	markupClose
	markupRaw
	markupValue
)

func (k markupKind) String() string {
	switch k {
	case markupInvalid:
		return "invalid"
	case markupOpen:
		return "open"
	case markupClose:
		return "close"
	case markupRaw:
		return "raw"
	case markupValue:
		return "value"
	default:
		return "markupKind(" + strconv.Itoa(int(k)) + ")"
	}
}

type markup struct {
	kind        markupKind
	name        string
	attrs       []attribute
	text        string
	void        bool
	selfClosing bool
	pos         evmPos
}
type evmPos struct {
	file         string
	line, column int
}
type preprocessor struct {
	filename, source string
	i                int
	out              strings.Builder
	marks            []markup
	mapping          []int
}

func preprocess(filename, source string, base int) (string, []markup, []int, error) {
	p := &preprocessor{filename: filename, source: source}
	for p.i < len(source) {
		i := p.i
		c := source[i]
		if c == '<' && lineStart(source, i) {
			if err := p.markupLine(base); err != nil {
				return "", nil, nil, err
			}
			continue
		}
		end := lexicalEnd(source, i)
		if end > i {
			p.write(source[i:end], i)
			p.i = end
			continue
		}
		p.write(string(c), i)
		p.i++
	}
	return p.out.String(), p.marks, p.mapping, nil
}
func (p *preprocessor) write(s string, origin int) {
	p.out.WriteString(s)
	for range len(s) {
		p.mapping = append(p.mapping, origin)
	}
}
func lineStart(s string, i int) bool {
	for i > 0 {
		i--
		if s[i] == '\n' {
			break
		}
		if s[i] != ' ' && s[i] != '\t' && s[i] != '\r' {
			return false
		}
	}
	return true
}
func lexicalEnd(s string, i int) int {
	if i >= len(s) {
		return i
	}
	switch s[i] {
	case '"', '\'', '`':
		quote := s[i]
		for j := i + 1; j < len(s); j++ {
			if s[j] == quote {
				return j + 1
			}
			if s[j] == '\\' && quote != '`' {
				j++
			}
		}
		return len(s)
	case '/':
		if strings.HasPrefix(s[i:], "//") {
			if j := strings.IndexByte(s[i:], '\n'); j >= 0 {
				return i + j
			}
			return len(s)
		}
		if strings.HasPrefix(s[i:], "/*") {
			if j := strings.Index(s[i+2:], "*/"); j >= 0 {
				return i + 2 + j + 2
			}
			return len(s)
		}
	}
	return i
}
func (p *preprocessor) position(i int) evmPos {
	prefix := p.source[:i]
	line := strings.Count(prefix, "\n") + 1
	return evmPos{p.filename, line, i - strings.LastIndexByte(prefix, '\n')}
}
func (p *preprocessor) fail(i int, s string) error {
	q := p.position(i)
	return fmt.Errorf("%s:%d:%d: %s", q.file, q.line, q.column, s)
}
func (p *preprocessor) marker(m markup, args []string, base, origin int) {
	m.pos = p.position(origin)
	id := base + len(p.marks)
	p.marks = append(p.marks, m)
	p.write(fmt.Sprintf("_goxMark(%d", id), origin)
	search := origin
	for _, arg := range args {
		argOrigin := origin
		for _, prefix := range []string{"{", "{="} {
			if offset := strings.Index(p.source[search:], prefix+arg+"}"); offset >= 0 {
				argOrigin = search + offset + len(prefix)
				search = argOrigin + len(arg)
				break
			}
		}
		p.write(",(", argOrigin)
		for offset := range len(arg) {
			p.write(arg[offset:offset+1], min(argOrigin+offset, len(p.source)))
		}
		p.write(")", argOrigin)
	}
	p.write(");", origin)
}
func (p *preprocessor) markupLine(base int) error {
	for p.i < len(p.source) && p.source[p.i] != '\n' {
		i := p.i
		switch p.source[i] {
		case ' ', '\t', '\r':
			p.i++
		case '<':
			if strings.HasPrefix(p.source[i:], "<!--") {
				end := strings.Index(p.source[i+4:], "-->")
				if end < 0 {
					return p.fail(i, "unterminated HTML comment")
				}
				p.i = i + 4 + end + 3
				p.marker(markup{kind: markupRaw, text: p.source[i:p.i]}, nil, base, i)
				continue
			}
			if strings.HasPrefix(strings.ToLower(p.source[i:]), "<!doctype") {
				end := strings.IndexByte(p.source[i:], '>')
				if end < 0 {
					return p.fail(i, "unterminated doctype")
				}
				p.i = i + end + 1
				p.marker(markup{kind: markupRaw, text: p.source[i:p.i]}, nil, base, i)
				continue
			}
			m, args, end, err := p.tag(i)
			if err != nil {
				return err
			}
			p.i = end
			p.marker(m, args, base, i)
			if m.kind == markupOpen && !m.void && (m.name == "script" || m.name == "style") {
				if err = p.rawBody(m.name, base); err != nil {
					return err
				}
			}
		case '{':
			expr, end, err := braced(p.source, i)
			if err != nil {
				return p.fail(i, err.Error())
			}
			p.i = end
			p.marker(markup{kind: markupValue}, []string{expr}, base, i)
		default:
			return p.fail(i, "inline text must be quoted inside {expression}; put Go statements on their own lines")
		}
	}
	return nil
}
func braced(s string, start int) (string, int, error) {
	depth := 1
	for i := start + 1; i < len(s); {
		if e := lexicalEnd(s, i); e > i {
			i = e
			continue
		}
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return s[start+1 : i], i + 1, nil
			}
		}
		i++
	}
	return "", start, fmt.Errorf("unterminated interpolation")
}
func (p *preprocessor) tag(start int) (markup, []string, int, error) {
	s := p.source
	i := start + 1
	m := markup{kind: markupOpen}
	var args []string
	if i < len(s) && s[i] == '/' {
		m.kind = markupClose
		i++
	}
	if i < len(s) && s[i] == '>' {
		return m, nil, i + 1, nil
	}
	n := i
	for i < len(s) && nameByte(s[i]) {
		i++
	}
	if n == i {
		return m, nil, i, p.fail(start, "expected element name")
	}
	m.name = s[n:i]
	for {
		for i < len(s) && unicode.IsSpace(rune(s[i])) {
			i++
		}
		if i >= len(s) {
			return m, nil, i, p.fail(start, "unterminated tag")
		}
		if s[i] == '>' {
			i++
			break
		}
		if s[i] == '/' && i+1 < len(s) && s[i+1] == '>' {
			m.void = true
			m.selfClosing = true
			i += 2
			break
		}
		if m.kind == markupClose {
			return m, nil, i, p.fail(i, "closing tag has attributes")
		}
		n = i
		for i < len(s) && nameByte(s[i]) {
			i++
		}
		if n == i {
			return m, nil, i, p.fail(i, "invalid attribute name")
		}
		a := attribute{name: s[n:i], boolean: true}
		for i < len(s) && unicode.IsSpace(rune(s[i])) {
			i++
		}
		if i < len(s) && s[i] == '=' {
			a.boolean = false
			i++
			for i < len(s) && unicode.IsSpace(rune(s[i])) {
				i++
			}
			if i >= len(s) {
				return m, nil, i, p.fail(start, "missing attribute value")
			}
			if s[i] == '{' {
				expr, e, err := braced(s, i)
				if err != nil {
					return m, nil, i, p.fail(i, err.Error())
				}
				a.parts = append(a.parts, part{arg: len(args) + 1})
				args = append(args, expr)
				i = e
			} else if s[i] == '"' || s[i] == '\'' {
				q := s[i]
				i++
				n = i
				for i < len(s) && s[i] != q {
					if s[i] == '{' {
						if i > n {
							a.parts = append(a.parts, part{text: html.UnescapeString(s[n:i])})
						}
						expr, e, err := braced(s, i)
						if err != nil {
							return m, nil, i, p.fail(i, err.Error())
						}
						a.parts = append(a.parts, part{arg: len(args) + 1})
						args = append(args, expr)
						i = e
						n = i
					} else {
						i++
					}
				}
				if i >= len(s) {
					return m, nil, i, p.fail(start, "unterminated quoted attribute")
				}
				if i > n || len(a.parts) == 0 {
					a.parts = append(a.parts, part{text: html.UnescapeString(s[n:i])})
				}
				i++
			} else {
				return m, nil, i, p.fail(i, "attribute values must be quoted or enclosed in braces")
			}
		}
		for _, old := range m.attrs {
			if old.name == a.name {
				return m, nil, i, p.fail(start, "duplicate attribute "+a.name)
			}
		}
		m.attrs = append(m.attrs, a)
	}
	if voidTags[m.name] {
		m.void = true
	}
	return m, args, i, nil
}
func nameByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == ':' || c == '.'
}
func (p *preprocessor) rawBody(tag string, base int) error {
	// ASCII folding preserves byte offsets even for Unicode JS/CSS literals.
	lowerBytes := []byte(p.source)
	for i, ch := range lowerBytes {
		if ch >= 'A' && ch <= 'Z' {
			lowerBytes[i] = ch + ('a' - 'A')
		}
	}
	lower := string(lowerBytes)
	for p.i < len(p.source) {
		start := p.i
		end := rawClose(lower, tag, start)
		j := strings.Index(p.source[start:], "{=")
		if j >= 0 {
			j += start
		}
		if end >= 0 && (j < 0 || end <= j) {
			p.marker(markup{kind: markupRaw, text: p.source[start:end]}, nil, base, start)
			p.i = end
			return nil
		}
		if j < 0 {
			return p.fail(start, "unclosed "+tag)
		}
		if j > start {
			p.marker(markup{kind: markupRaw, text: p.source[start:j]}, nil, base, start)
		}
		expr, next, err := braced(p.source, j)
		if err != nil {
			return p.fail(j, err.Error())
		}
		p.marker(markup{kind: markupValue}, []string{strings.TrimPrefix(expr, "=")}, base, j)
		p.i = next
	}
	return p.fail(p.i, "unclosed "+tag)
}

func rawClose(source, tag string, start int) int {
	needle := "</" + tag
	for start < len(source) {
		i := strings.Index(source[start:], needle)
		if i < 0 {
			return -1
		}
		i += start
		end := i + len(needle)
		if end < len(source) && (source[end] == '>' || source[end] == '/' || unicode.IsSpace(rune(source[end]))) {
			return i
		}
		start = end
	}
	return -1
}

var voidTags = map[string]bool{"area": true, "base": true, "br": true, "col": true, "embed": true, "hr": true, "img": true, "input": true, "link": true, "meta": true, "param": true, "source": true, "track": true, "wbr": true}

func quote(s string) string { return strconv.Quote(s) }
