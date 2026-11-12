package htmltemplate

import (
	"fmt"
	"strings"
	"text/template/parse"
)

// Context tracks escaping state. Compiled contexts and filters are immutable;
// each render advances its own context through the compiled operations.
type Context struct{ c context }
type Filter struct {
	funcs           []func(...any) string
	strings         []func(string) string
	plainHTMLString bool
}

// ApplyString retains the upstream filter order without boxing intermediate
// strings. Unsupported filters use the ordinary trusted-type-aware path.
func (f Filter) ApplyString(s string) string {
	if f.strings == nil {
		return f.Apply(s)
	}
	for _, fn := range f.strings {
		s = fn(s)
	}
	return s
}

func filterURLString(s string) string {
	if !isSafeURL(s) {
		return "#" + filterFailsafe
	}
	return s
}
func normalizeURLString(s string) string {
	var b strings.Builder
	if processURLOnto(s, true, &b) {
		return b.String()
	}
	return s
}
func escapeURLString(s string) string {
	var b strings.Builder
	if processURLOnto(s, false, &b) {
		return b.String()
	}
	return s
}

func (f Filter) Apply(v any) string {
	if f.plainHTMLString {
		if s, ok := v.(string); ok {
			return EscapeString(s)
		}
	}
	for _, fn := range f.funcs {
		v = fn(v)
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

// EscapeString is the upstream plain-string path for HTML text and quoted
// attributes. It avoids variadic interface boxing for statically typed strings.
func EscapeString(s string) string  { return htmlReplacer(s, htmlReplacementTable, true) }
func (f Filter) IsHTMLString() bool { return f.plainHTMLString }

// AppendEscaped uses the same replacement table and preserves non-ASCII bytes,
// including invalid UTF-8, just like htmlReplacer with badRunes=true. Every
// replacement is ASCII-keyed and at least as long as its input byte.
func AppendEscaped(dst []byte, s string, limit int) ([]byte, bool) {
	if len(s) > limit-len(dst) {
		return dst, false
	}
	start := 0
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if int(ch) >= len(htmlReplacementTable) {
			continue
		}
		replacement := htmlReplacementTable[ch]
		if replacement == "" {
			continue
		}
		if i-start+len(replacement) > limit-len(dst) {
			return dst, false
		}
		dst = append(dst, s[start:i]...)
		dst = append(dst, replacement...)
		start = i + 1
	}
	if len(s)-start > limit-len(dst) {
		return dst, false
	}
	return append(dst, s[start:]...), true
}

func (c Context) Clone() Context           { return Context{c.c.clone()} }
func (c Context) IsText() bool             { return c.c.state == stateText }
func (c Context) Equal(other Context) bool { return c.c.eq(other.c) }

// Accepts checks a runtime context against the compiler's context. A branch
// join can leave URL or JavaScript position unknown without changing its state.
func (c Context) Accepts(actual Context) bool {
	a := actual.c
	if c.c.urlPart == urlPartUnknown {
		a.urlPart = urlPartUnknown
	}
	if c.c.jsCtx == jsCtxUnknown {
		a.jsCtx = jsCtxUnknown
	}
	return c.c.eq(a) || c.c.eq(nudge(a))
}
func (c *Context) Text(s string) (string, error) {
	e := makeEscaper(nil)
	n := &parse.TextNode{NodeType: parse.NodeText, Text: []byte(s)}
	c.c = e.escapeText(c.c, n)
	if c.c.err != nil {
		return "", c.c.err
	}
	if edit, ok := e.textNodeEdits[n]; ok {
		return string(edit), nil
	}
	return s, nil
}
func (c *Context) Action() (Filter, error) {
	e := makeEscaper(nil)
	n := &parse.ActionNode{NodeType: parse.NodeAction, Pipe: &parse.PipeNode{NodeType: parse.NodePipe, Cmds: []*parse.CommandNode{{NodeType: parse.NodeCommand, Args: []parse.Node{&parse.DotNode{NodeType: parse.NodeDot}}}}}}
	c.c = e.escapeAction(c.c, n)
	if c.c.err != nil {
		return Filter{}, c.c.err
	}
	f := Filter{}
	names := e.actionNodeEdits[n]
	if len(names) == 1 {
		switch names[0] {
		case "_html_template_htmlescaper", "_html_template_attrescaper", "_html_template_rcdataescaper":
			f.plainHTMLString = true
		}
	}
	for _, name := range names {
		f.funcs = append(f.funcs, funcMap[name].(func(...any) string))
		var fn func(string) string
		switch name {
		case "_html_template_htmlescaper", "_html_template_attrescaper", "_html_template_rcdataescaper":
			fn = EscapeString
		case "_html_template_urlfilter":
			fn = filterURLString
		case "_html_template_urlnormalizer":
			fn = normalizeURLString
		case "_html_template_urlescaper":
			fn = escapeURLString
		}
		f.strings = append(f.strings, fn)
	}
	for _, fn := range f.strings {
		if fn == nil {
			f.strings = nil
			break
		}
	}
	return f, nil
}
func (c *Context) Join(other Context) error {
	n := &parse.TextNode{NodeType: parse.NodeText}
	c.c = join(c.c, other.c, n, "GoX branch")
	if c.c.err != nil {
		return c.c.err
	}
	return nil
}
func (c Context) Finish() error {
	if c.c.err != nil {
		return c.c.err
	}
	if !c.IsText() {
		return fmt.Errorf("template ends in %s", c.c)
	}
	return nil
}

// Attribute plans a complete runtime attribute value through the same analyzer.
func Attribute(tag, name string, value any) (string, error) {
	f, err := AttributeFilter(tag, name)
	if err != nil {
		return "", err
	}
	return f.Apply(value), nil
}

// AttributeFilter lets a builder reuse a validated attribute plan.
func AttributeFilter(tag, name string) (Filter, error) {
	for _, s := range []string{tag, name} {
		if s == "" {
			return Filter{}, fmt.Errorf("empty tag or attribute name")
		}
		for _, r := range s {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == ':') {
				return Filter{}, fmt.Errorf("invalid name %q", s)
			}
		}
	}
	c := Context{}
	if _, err := c.Text("<" + tag + " " + name + "=\""); err != nil {
		return Filter{}, err
	}
	return c.Action()
}
