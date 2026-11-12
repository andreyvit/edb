package gox

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/andreyvit/edb/evm"
	ht "github.com/andreyvit/edb/gox/internal/htmltemplate"
)

type operationKind uint8

const (
	opInvalid operationKind = iota
	opOpen
	opClose
	opRaw
	opValue
	opAttr
	opAttrText
	opEndAttr
	opSetAttr
	opFinish
	opTag
	opMissing
	opBatch
)

func (k operationKind) String() string {
	switch k {
	case opInvalid:
		return "invalid"
	case opOpen:
		return "open"
	case opClose:
		return "close"
	case opRaw:
		return "raw"
	case opValue:
		return "value"
	case opAttr:
		return "attr"
	case opAttrText:
		return "attrtext"
	case opEndAttr:
		return "endattr"
	case opSetAttr:
		return "setattr"
	case opFinish:
		return "finish"
	case opTag:
		return "tag"
	case opMissing:
		return "missing"
	case opBatch:
		return "batch"
	default:
		return "operationKind(" + strconv.Itoa(int(k)) + ")"
	}
}

type operation struct {
	steps         []int
	input         ht.Context
	content       ht.Context
	attribute     bool
	fallback      bool
	attrs         []renderedAttribute
	kind          operationKind
	text, name    string
	filter        ht.Filter
	position      evm.SourcePosition
	boolean, void bool
}
type renderedAttribute struct {
	name, value string
	boolean     bool
}
type element struct {
	prefix  string
	shared  bool
	name    string
	attrs   []renderedAttribute
	pending bool
	void    bool
}

// Builder is the render-scoped capability supplied implicitly to GoX helpers.
// Its opening tag remains mutable until the first child output. Do not retain it
// after a render or use it concurrently.
type Builder struct {
	attributeScratch      []renderedAttribute
	classParts            []string
	classBytes            int
	attributeFilters      map[attributeKey]ht.Filter
	content               ht.Context
	format                Format
	buf                   []byte
	tags                  []element
	operations            []operation
	options               RenderOptions
	attr                  string
	attrbuf               []byte
	attrBoolean, attrOmit bool
}
type builderKey struct{}
type attributeKey struct{ tag, name string }

func (b *Builder) append(s string) error {
	if len(s) > b.options.MaxOutputBytes-len(b.buf) {
		return evm.ErrOutput
	}
	b.buf = append(b.buf, s...)
	return nil
}
func (b *Builder) flush() error {
	if len(b.tags) == 0 {
		return nil
	}
	t := &b.tags[len(b.tags)-1]
	if !t.pending {
		return nil
	}
	if t.prefix != "" {
		if err := b.append(t.prefix); err != nil {
			return err
		}
	} else {
		if err := b.append("<" + t.name); err != nil {
			return err
		}
		for _, a := range t.attrs {
			if err := b.append(" " + a.name); err != nil {
				return err
			}
			if !a.boolean {
				if err := b.append("=\""); err != nil {
					return err
				}
				if err := b.append(a.value); err != nil {
					return err
				}
				if a.name == "class" {
					for _, part := range b.classParts {
						if err := b.append(" "); err != nil {
							return err
						}
						if err := b.append(part); err != nil {
							return err
						}
					}
				}
				if err := b.append("\""); err != nil {
					return err
				}
			}
		}
		if err := b.append(">"); err != nil {
			return err
		}
	}
	t.attrs = nil
	clear(b.attributeScratch)
	b.attributeScratch = b.attributeScratch[:0]
	clear(b.classParts)
	b.classParts = b.classParts[:0]
	b.classBytes = 0
	t.pending = false
	if t.void {
		b.tags = b.tags[:len(b.tags)-1]
	}
	return nil
}
func (b *Builder) setAttr(name, value string, boolean bool) error {
	if len(b.tags) == 0 || !b.tags[len(b.tags)-1].pending {
		return fmt.Errorf("attribute %q added after child output", name)
	}
	t := &b.tags[len(b.tags)-1]
	t.prefix = ""
	if t.shared {
		b.attributeScratch = append(b.attributeScratch[:0], t.attrs...)
		t.attrs = b.attributeScratch
		t.shared = false
	}
	if t.attrs == nil {
		t.attrs = b.attributeScratch[:0]
	}
	for i := range t.attrs {
		if t.attrs[i].name == name {
			if name == "class" && value != "" && t.attrs[i].value != "" {
				if b.classBytes == 0 {
					b.classBytes = len(t.attrs[i].value)
				}
				if len(value)+1 > b.options.MaxOutputBytes-b.classBytes {
					return evm.ErrOutput
				}
				b.classParts = append(b.classParts, value)
				b.classBytes += len(value) + 1
			} else {
				t.attrs[i] = renderedAttribute{name, value, boolean}
				if name == "class" {
					clear(b.classParts)
					b.classParts = b.classParts[:0]
					b.classBytes = len(value)
				}
			}
			return nil
		}
	}
	if len(value) > b.options.MaxOutputBytes {
		return evm.ErrOutput
	}
	t.attrs = append(t.attrs, renderedAttribute{name, value, boolean})
	b.attributeScratch = t.attrs
	return nil
}
func (b *Builder) Class(value string) error {
	if b.attr != "" {
		return fmt.Errorf("attribute helper called while evaluating an attribute")
	}
	return b.setAttr("class", ht.EscapeString(value), false)
}
func (b *Builder) Attr(name string, value any) error {
	if b.attr != "" {
		return fmt.Errorf("attribute helper called while evaluating an attribute")
	}
	if len(name) > b.options.MaxOutputBytes {
		return evm.ErrOutput
	}
	name = strings.ToLower(name)
	if len(b.tags) == 0 || !b.tags[len(b.tags)-1].pending {
		return fmt.Errorf("attribute %q added after child output", name)
	}
	tag := b.tags[len(b.tags)-1].name
	if (tag == "script" || tag == "style") && name == "type" || tag == "meta" && (name == "http-equiv" || name == "content") {
		return fmt.Errorf("attribute %s on %s requires static context analysis", name, tag)
	}
	if v, ok := value.(bool); ok && booleanAttrs[name] {
		if !v {
			t := &b.tags[len(b.tags)-1]
			t.prefix = ""
			if t.shared {
				b.attributeScratch = append(b.attributeScratch[:0], t.attrs...)
				t.attrs = b.attributeScratch
				t.shared = false
			}
			for i, a := range t.attrs {
				if a.name == name {
					copy(t.attrs[i:], t.attrs[i+1:])
					t.attrs[len(t.attrs)-1] = renderedAttribute{}
					t.attrs = t.attrs[:len(t.attrs)-1]
					b.attributeScratch = t.attrs
					break
				}
			}
			return nil
		}
		return b.setAttr(name, "", true)
	}
	key := attributeKey{tag, name}
	filter, ok := b.attributeFilters[key]
	if !ok {
		var err error
		filter, err = ht.AttributeFilter(tag, name)
		if err != nil {
			return err
		}
		// Names can be data. Bound the cache retained by a pooled builder.
		if len(b.attributeFilters) < 64 && len(tag)+len(name) <= 256 {
			if b.attributeFilters == nil {
				b.attributeFilters = make(map[attributeKey]ht.Filter)
			}
			b.attributeFilters[attributeKey{strings.Clone(tag), strings.Clone(name)}] = filter
		}
	}
	return b.setAttr(name, filter.Apply(value), false)
}
func (b *Builder) Text(value string) error {
	if b.attr != "" || b.format != TextFormat && !b.content.IsText() {
		return fmt.Errorf("builder.Text requires an HTML text context")
	}
	if b.format == TextFormat {
		return b.append(value)
	}
	if err := b.flush(); err != nil {
		return err
	}
	var ok bool
	b.buf, ok = ht.AppendEscaped(b.buf, value, b.options.MaxOutputBytes)
	if !ok {
		return evm.ErrOutput
	}
	return nil
}
func (b *Builder) HTML(value HTML) error {
	if b.attr != "" || !b.content.IsText() {
		return fmt.Errorf("builder.HTML requires an HTML text context")
	}
	if err := b.flush(); err != nil {
		return err
	}
	return b.append(string(value))
}
func (b *Builder) step(id int) error {
	if id < 0 || id >= len(b.operations) {
		return fmt.Errorf("invalid render operation")
	}
	op := &b.operations[id]
	switch op.kind {
	case opBatch:
		for _, id := range op.steps {
			if err := b.step(id); err != nil {
				p := b.operations[id].position
				return fmt.Errorf("%s:%d:%d: %w", p.File, p.Line, p.Column, err)
			}
		}
		return nil
	case opTag:
		if b.attr != "" || !b.content.IsText() {
			return fmt.Errorf("component requires an HTML text context")
		}
		if err := b.flush(); err != nil {
			return err
		}
		b.tags = append(b.tags, element{name: op.name, pending: true, void: op.void, attrs: op.attrs, shared: len(op.attrs) > 0, prefix: op.text})
		b.content = op.content
		if op.void {
			if err := b.flush(); err != nil {
				return err
			}
			if !voidTags[op.name] {
				return b.append("</" + op.name + ">")
			}
		}
	case opSetAttr:
		if b.attr != "" {
			return fmt.Errorf("attribute helper called while evaluating an attribute")
		}
		return b.setAttr(op.name, op.text, op.boolean)
	case opOpen:
		if b.attr != "" || !b.content.IsText() {
			return fmt.Errorf("component requires an HTML text context")
		}
		if err := b.flush(); err != nil {
			return err
		}
		b.tags = append(b.tags, element{name: op.name, pending: true, void: op.void, attrs: op.attrs, shared: len(op.attrs) > 0})
	case opFinish:
		b.content = op.content
		if op.void {
			name := b.tags[len(b.tags)-1].name
			if err := b.flush(); err != nil {
				return err
			}
			if !voidTags[name] {
				return b.append("</" + name + ">")
			}
		}
	case opClose:
		if b.attr != "" || !op.input.Accepts(b.content) {
			return fmt.Errorf("closing element in incompatible rendering context")
		}
		b.content = op.content
		if len(b.tags) > 0 && b.tags[len(b.tags)-1].void {
			if err := b.flush(); err != nil {
				return err
			}
		}
		if len(b.tags) == 0 || b.tags[len(b.tags)-1].name != op.name {
			return fmt.Errorf("unbalanced closing tag %s", op.name)
		}
		if err := b.flush(); err != nil {
			return err
		}
		b.tags = b.tags[:len(b.tags)-1]
		return b.append("</" + op.name + ">")
	case opRaw:
		if b.attr != "" || !op.input.Accepts(b.content) {
			return fmt.Errorf("output in incompatible rendering context")
		}
		b.content = op.content
		if err := b.flush(); err != nil {
			return err
		}
		return b.append(op.text)
	case opAttr:
		b.attr = op.name
		b.attrbuf = b.attrbuf[:0]
		b.attrBoolean = op.boolean
		b.attrOmit = false
	case opAttrText:
		if len(op.text) > b.options.MaxOutputBytes-len(b.attrbuf) {
			return evm.ErrOutput
		}
		b.attrbuf = append(b.attrbuf, op.text...)
	case opEndAttr:
		name := b.attr
		b.attr = ""
		if !b.attrOmit {
			return b.setAttr(name, string(b.attrbuf), b.attrBoolean)
		}
	case opMissing:
		if b.attr != "" {
			b.attrOmit = true
		}
		d := Diagnostic{op.position, "nil value in output expression"}
		if b.options.Strict {
			return d
		}
		if b.options.Report != nil {
			b.options.Report(d)
		}
		if op.fallback && b.attr == "" {
			if !op.input.Accepts(b.content) {
				return fmt.Errorf("nil output in incompatible rendering context")
			}
			b.content = op.content
			if err := b.flush(); err != nil {
				return err
			}
			return b.append(op.filter.Apply(nil))
		}
	default:
		return fmt.Errorf("invalid render operation %q", op.kind)
	}
	return nil
}
func (b *Builder) value(id int, v any) error {
	if id < 0 || id >= len(b.operations) {
		return fmt.Errorf("invalid render value")
	}
	op := &b.operations[id]
	if b.attr != "" {
		if !op.attribute {
			return fmt.Errorf("component output while evaluating an attribute")
		}
		if x, ok := v.(bool); ok && op.boolean {
			b.attrBoolean = x
			b.attrOmit = !x
			return nil
		}
		s := op.filter.Apply(v)
		if len(s) > b.options.MaxOutputBytes-len(b.attrbuf) {
			return evm.ErrOutput
		}
		b.attrbuf = append(b.attrbuf, s...)
		return nil
	}
	if op.attribute || !op.input.Accepts(b.content) {
		return fmt.Errorf("value in incompatible rendering context")
	}
	if err := b.flush(); err != nil {
		return err
	}
	b.content = op.content
	return b.append(op.filter.Apply(v))
}

func (b *Builder) stringValue(id int, value string) error {
	if id < 0 || id >= len(b.operations) {
		return fmt.Errorf("invalid render value")
	}
	op := &b.operations[id]
	if !op.filter.IsHTMLString() {
		if b.attr != "" {
			if !op.attribute {
				return fmt.Errorf("component output while evaluating an attribute")
			}
			s := op.filter.ApplyString(value)
			if len(s) > b.options.MaxOutputBytes-len(b.attrbuf) {
				return evm.ErrOutput
			}
			b.attrbuf = append(b.attrbuf, s...)
			return nil
		}
		if op.attribute || !op.input.Accepts(b.content) {
			return fmt.Errorf("value in incompatible rendering context")
		}
		if err := b.flush(); err != nil {
			return err
		}
		b.content = op.content
		return b.append(op.filter.ApplyString(value))
	}
	var ok bool
	if b.attr != "" {
		if !op.attribute {
			return fmt.Errorf("component output while evaluating an attribute")
		}
		b.attrbuf, ok = ht.AppendEscaped(b.attrbuf, value, b.options.MaxOutputBytes)
	} else {
		if op.attribute || !op.input.Accepts(b.content) {
			return fmt.Errorf("value in incompatible rendering context")
		}
		if err := b.flush(); err != nil {
			return err
		}
		b.content = op.content
		b.buf, ok = ht.AppendEscaped(b.buf, value, b.options.MaxOutputBytes)
	}
	if !ok {
		return evm.ErrOutput
	}
	return nil
}

var booleanAttrs = map[string]bool{"allowfullscreen": true, "async": true, "autofocus": true, "autoplay": true, "checked": true, "controls": true, "default": true, "defer": true, "disabled": true, "formnovalidate": true, "hidden": true, "inert": true, "ismap": true, "itemscope": true, "loop": true, "multiple": true, "muted": true, "nomodule": true, "novalidate": true, "open": true, "playsinline": true, "readonly": true, "required": true, "reversed": true, "selected": true}

// Open and Close support rendering helpers written against the builder directly.
// Raw script/style bodies need the compiler's contextual analysis or trusted HTML.
func (b *Builder) Open(name string) error {
	if b.attr != "" || b.format != HTMLFormat || !b.content.IsText() {
		return fmt.Errorf("builder.Open requires an HTML text context")
	}
	if len(name) > b.options.MaxOutputBytes {
		return evm.ErrOutput
	}
	name = strings.ToLower(name)
	if rawContentTags[name] {
		return fmt.Errorf("%s requires a GoX element or trusted HTML", name)
	}
	if _, err := ht.Attribute(name, "class", ""); err != nil {
		return err
	}
	if err := b.flush(); err != nil {
		return err
	}
	b.tags = append(b.tags, element{name: name, pending: true, void: voidTags[name]})
	return nil
}
func (b *Builder) Close() error {
	if b.attr != "" || !b.content.IsText() {
		return fmt.Errorf("builder.Close requires an HTML text context")
	}
	if len(b.tags) == 0 {
		return fmt.Errorf("no element to close")
	}
	if b.tags[len(b.tags)-1].void {
		return b.flush()
	}
	name := b.tags[len(b.tags)-1].name
	if err := b.flush(); err != nil {
		return err
	}
	b.tags = b.tags[:len(b.tags)-1]
	return b.append("</" + name + ">")
}

var rawContentTags = map[string]bool{"script": true, "style": true, "textarea": true, "title": true, "xmp": true, "iframe": true, "noembed": true, "noframes": true, "noscript": true, "plaintext": true}
