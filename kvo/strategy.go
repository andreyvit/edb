package kvo

import (
	"bytes"
	"reflect"
	"slices"
	"strings"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Encoded attaches a native binding without decoding any fields. Fastest can
// subsequently materialize a Go struct; ordinary Get/Child still borrow bytes.
func (b *Binding) encodedView(v row2.View) recordView {
	return recordView{wire: v, plan: b.root, layout: b.root.layout, binding: b}
}
func (b *Binding) messagePackView(raw []byte) recordView    { return messagePackRecord(b.root, raw) }
func (p *Projection) messagePackView(raw []byte) recordView { return messagePackRecord(p.root, raw) }
func (b *Binding) DirectMessagePack() bool                  { return !b.root.msgFallback }
func (p *Projection) DirectMessagePack() bool               { return !p.root.msgFallback }
func messagePackRecord(p *nativePlan, raw []byte) recordView {
	if len(raw) == 0 {
		return recordView{plan: p}
	}
	r := recordView{plan: p, msg: msgpack.Trusted(raw)}
	if p.msgFallback || p.hasMessagePackInlineAlias(r.msg) {
		return r.Fastest()
	}
	return r
}

// Fast retains an existing Fast2 view; other representations use Fastest.
// A retained Fast2 view still borrows its source bytes and transaction lifetime.
func (r recordView) Fast() recordView {
	if r.wire.Layout() == row2.Fast2 {
		return r
	}
	return r.Fastest()
}

// Fastest eagerly materializes an owned Go struct when bound, or indexed
// decoded memory otherwise. Retain the result for repeated access. This is a
// workload hint, not a speed guarantee. Existing decoded objects are returned
// unchanged; a newly decoded object is independent of its encoded source.
func (r recordView) Fastest() recordView {
	if r.memory != nil || r.object != nil || r.Missing() {
		return r
	}
	if r.plan == nil {
		return recordView{memory: decodeMemory(row2.Trusted(bytes.Clone(r.wire.Raw())), r.layout, 0), layout: r.layout}
	}
	return r.decode()
}
func (r recordView) decode() recordView {

	dst := reflect.New(r.plan.typ)

	if !r.msg.Missing() {
		if err := msgpack.TryDecode(r.msg.Raw(), dst.Elem()); err != nil {
			panic(err)
		}
	} else if r.binding != nil {
		if err := r.binding.TryDecode(r.wire.Raw(), dst.Elem()); err != nil {
			panic(err)
		}
	} else {
		r.plan.read(dst.Elem(), values.Bytes(r.wire.Raw()), true, 0)
	}

	return nativeRecord(r.plan, dst)
}

func messagePackName(f reflect.StructField) string {
	name, _, _ := strings.Cut(f.Tag.Get("msgpack"), ",")
	if name == "" {
		return f.Name
	}
	return name
}

// Unusual custom representations retain the existing decoder semantics. For a
// child this fallback is local to that child; siblings stay encoded.
func messagePackFallback(t reflect.Type) bool {
	for _, typ := range []reflect.Type{t, reflect.PointerTo(t)} {
		for _, name := range []string{"EncodeMsgpack", "DecodeMsgpack", "MarshalMsgpack", "UnmarshalMsgpack", "MarshalBinary", "UnmarshalBinary", "MarshalText", "UnmarshalText"} {
			if _, ok := typ.MethodByName(name); ok {
				return true
			}
		}
	}
	if t.Kind() == reflect.Struct {
		_, ok := messagePackFields(t, make(map[string]bool))
		return !ok
	}
	return false
}

// Value struct embeddings share the parent map unless noinline is specified.
// Pointer embeddings and shadowed names retain decoder semantics.
func messagePackInline(f reflect.StructField) bool {
	return f.Anonymous && f.Type.Kind() == reflect.Struct &&
		!slices.Contains(strings.Split(f.Tag.Get("msgpack"), ",")[1:], "noinline")
}

// Validate the entire wire namespace, including fields omitted by a projection.
// This work happens once when compiling a binding, never for each record.
func messagePackFields(t reflect.Type, names map[string]bool) ([]string, bool) {
	var result []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name, tag := messagePackName(f), f.Tag.Get("msgpack")
		if name == "-" {
			continue
		}
		if f.Name == "_msgpack" || strings.Contains(tag, ",alias:") || strings.Contains(tag, ",intern") {
			return nil, false
		}
		if !f.IsExported() && !f.Anonymous {
			continue
		}
		if f.Anonymous && !slices.Contains(strings.Split(tag, ",")[1:], "noinline") {
			if !messagePackInline(f) || messagePackFallback(f.Type) {
				return nil, false
			}
			// The decoder also accepts the embedding's name as a nested field.
			if names[name] {
				return nil, false
			}
			names[name] = true
			child, ok := messagePackFields(f.Type, names)
			if !ok {
				return nil, false
			}
			result = append(result, child...)
		} else {
			if names[name] {
				return nil, false
			}
			names[name] = true
			result = append(result, name)
		}
	}
	return result, true
}

// The standard decoder also accepts an embedding under its Go/tag name.
// Such noncanonical payloads can mix nested and flattened fields in wire order;
// retain the decoder's merge behavior rather than silently read the wrong value.
func messagePackInlineAliases(t reflect.Type) []string {
	var names []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if messagePackName(f) != "-" && messagePackInline(f) {
			names = append(names, messagePackName(f))
			names = append(names, messagePackInlineAliases(f.Type)...)
		}
	}
	return names
}
func (p *nativePlan) hasMessagePackInlineAlias(raw msgpack.View) bool {
	if len(p.msgAliases) == 0 || raw.Nil() {
		return false
	}
	it := raw.Iter()
	for {
		k, _, ok := it.Next()
		if !ok {
			return false
		}
		for _, name := range p.msgAliases {
			if string(k.Bytes()) == name {
				return true
			}
		}
	}
}

func indirectPlan(p *nativePlan) *nativePlan {
	for p.kind == nativePointer || p.kind == nativeOption {
		p = p.elem
	}
	return p
}
func decodeProperty(p *nativePlan, v msgpack.View) reflect.Value {
	dst := reflect.New(p.typ)
	if !v.Missing() {
		if err := msgpack.TryDecode(v.Raw(), dst.Elem()); err != nil {
			panic(err)
		}
	}
	return dst.Elem()
}
func (r recordView) messagePackField(f *nativeField) msgpack.View {
	if f.msgName == "-" {
		return msgpack.View{}
	}
	if f.msgInline {
		return r.msg
	}
	return r.msg.Lookup(f.msgName)
}
func (r recordView) messagePackChild(s row2.Step) recordView {
	f := r.plan.field(s.Key)
	return messagePackChildValue(f.plan, r.messagePackField(f))
}
func messagePackChildValue(p *nativePlan, raw msgpack.View) recordView {
	if p.kind == nativeOption {
		if !raw.Lookup("Present").Bool() {
			return recordView{plan: indirectPlan(p)}
		}
		p = p.elem
		raw = raw.Lookup("Value")
	}
	if p.msgFallback || indirectPlan(p).msgFallback || indirectPlan(p).hasMessagePackInlineAlias(raw) {
		p, v := unwrap(p, decodeProperty(p, raw), false)
		if !v.IsValid() {
			return recordView{plan: indirectPlan(p)}
		}
		if p.kind != nativeObject {
			panic(row2.ErrKind)
		}
		return recordView{plan: p, object: v.Addr().UnsafePointer()}
	}
	if p.kind == nativePointer && raw.Nil() {
		return recordView{plan: indirectPlan(p)}
	}
	p = indirectPlan(p)
	if p.kind != nativeObject {
		panic(row2.ErrKind)
	}
	if raw.Nil() {
		raw = msgpack.Trusted(emptyMessagePackObject)
	}
	return recordView{plan: p, msg: raw}
}

var emptyMessagePackObject = []byte{0x80}

func (r recordView) messagePackGet(k uint64) values.Value {
	f := r.plan.field(k)
	return messagePackFieldValue(f, r.messagePackField(f))
}
func messagePackFieldValue(f *nativeField, raw msgpack.View) values.Value {
	p := f.plan
	if p.kind == nativeOption {
		if !raw.Lookup("Present").Bool() {
			return values.Value{}
		}
		p = p.elem
		raw = raw.Lookup("Value")
	}
	if p.msgFallback || indirectPlan(p).msgFallback {
		return propertyValue(p, raw, f.layout)
	}
	if p.kind == nativePointer && raw.Nil() {
		return values.Value{}
	}
	p = indirectPlan(p)
	if p.kind == nativeJSON {
		return propertyValue(p, raw, f.layout)
	}
	if raw.Nil() && p.msgDecoder == nil && p.kind != nativeObject && p.typ.Kind() != reflect.Array {
		return values.Value{}
	}
	switch p.kind {
	case nativeObject, nativeDictionary, nativeNumbers, nativeList:
		return p.messagePackValue(raw, f.layout)
	default:
		return p.messagePackScalar(raw)
	}
}
func propertyValue(p *nativePlan, raw msgpack.View, layout row2.Layout) values.Value {
	p, v := unwrap(p, decodeProperty(p, raw), false)
	if !v.IsValid() || p.nilable() && v.IsNil() {
		return values.Value{}
	}
	return nativeValue(p, v, layout)
}
func (r recordView) messagePackHas(k uint64) bool {
	f := r.plan.field(k)
	return messagePackHas(r.plan, f, r.messagePackField(f))
}
func messagePackHas(plan *nativePlan, f *nativeField, raw msgpack.View) bool {
	p := f.plan
	if plan.model.PropFlags(f.key)&PrimaryKey != 0 {
		return !raw.Missing() && !raw.Nil()
	}
	// Has follows the Go value's zero/presence semantics, including custom
	// decoders, JSON interfaces and fixed arrays. Decode only that property.
	if p.msgFallback || indirectPlan(p).msgFallback || p.kind == nativeJSON || p.typ.Kind() == reflect.Array {
		return nativeHas(p, decodeProperty(p, raw))
	}
	if p.kind == nativeOption {
		return raw.Lookup("Present").Bool()
	}
	switch p.kind {
	case nativePointer, nativeList, nativeBytes, nativeNumbers, nativeDictionary:
		if p.nilable() {
			return !raw.Nil()
		}
	case nativeObject:
		return true
	case nativeFloat:
		return p.messagePackScalar(raw).AsFloat() != 0
	}
	return messagePackFieldValue(f, raw).R != 0
}

// MessagePack returns the edited payload without re-encoding untouched fields.
// Nil means this editor has another backing representation.
func (e *Editor) MessagePack() []byte { e.checkAttached(); return e.stagedMessagePack() }
func (e *Editor) setMessagePack(path []row2.Step, x values.Value) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = recoveredError("MessagePack property assignment", r)
		}
	}()
	if len(path) == 0 {
		return row2.ErrKind
	}
	f := e.view.plan.field(path[0].Key)
	if len(path) == 1 && !f.msgInline {
		if f.msgName == "-" {
			panic("cannot edit a non-persistent MessagePack field")
		}
		raw := encodeMessagePackLeaf(f.plan, path[0], x)
		if !bytes.Equal(raw, e.messagePackField(f).Raw()) {
			e.stage(editField{step: path[0], raw: raw})
		}
		return nil
	}
	e.flush()
	next := setMessagePack(e.view, path, x)
	if !bytes.Equal(next.msg.Raw(), e.view.msg.Raw()) {
		e.view = next
		e.touch()
	}
	return nil
}
func setMessagePack(r recordView, path []row2.Step, x values.Value) recordView {
	s := path[0]
	f := r.plan.field(s.Key)

	if f.msgName == "-" {
		panic("cannot edit a non-persistent MessagePack field")
	}
	var raw []byte
	if len(path) > 1 {
		if f.plan.msgFallback || indirectPlan(f.plan).msgFallback || indirectPlan(f.plan).hasMessagePackInlineAlias(r.messagePackField(f)) {
			decoded := decodeProperty(f.plan, r.messagePackField(f))
			p, v := unwrap(f.plan, decoded, !(x.R == 0) || path[len(path)-1].Options&row2.Present != 0)
			if !v.IsValid() {
				return r
			}
			child := recordView{plan: p, object: v.Addr().UnsafePointer()}
			if err := child.Edit().TryFastSetPathValue(path[1:], x); err != nil {
				panic(err)
			}
			var err error
			raw, err = msgpack.TryEncode(nil, decoded)
			if err != nil {
				panic(err)
			}
		} else {
			child := r.messagePackChild(s)
			if child.Missing() {
				if x.R == 0 && path[len(path)-1].Options&row2.Present == 0 {
					return r
				}
				child = recordView{plan: indirectPlan(f.plan), msg: msgpack.Trusted(emptyMessagePackObject)}
			}
			raw = setMessagePack(child, path[1:], x).msg.Raw()
			if f.plan.kind == nativeOption {
				wrapped := r.messagePackField(f).Set("Value", raw).Set("Present", []byte{0xc3})
				raw = wrapped.Raw()
			}
		}
	} else {
		raw = encodeMessagePackLeaf(f.plan, s, x)
	}
	if f.msgInline {
		if len(path) > 1 {
			r.msg = msgpack.Trusted(raw)
		} else {
			// Replace only this embedding's fields; preserve parent siblings.
			names, _ := messagePackFields(f.plan.typ, make(map[string]bool))
			replacement := msgpack.Trusted(raw)
			for _, name := range names {
				v := replacement.Lookup(name).Raw()
				if len(v) == 0 {
					r.msg = r.msg.Delete(name)
				} else {
					r.msg = r.msg.Set(name, v)
				}
			}
		}
	} else {
		r.msg = r.msg.Set(f.msgName, raw)
	}
	return r
}

func encodeMessagePackLeaf(p *nativePlan, s row2.Step, x values.Value) []byte {
	leaf := indirectPlan(p)
	if leaf.kind >= nativeObject && leaf.kind <= nativeList && !leaf.whole(make(map[*nativePlan]bool)) {
		panic("cannot replace an aggregate containing unexposed fields; assign its properties")
	}
	v := reflect.New(p.typ).Elem()
	p.read(v, x, x.R != 0 || s.Options&row2.Present != 0, 0)
	raw, err := msgpack.TryEncode(nil, v)
	if err != nil {
		panic(err)
	}
	return raw
}

// HasNativeField reports whether a projection exposes a direct Go field index.
// EDB uses this once during registration to avoid decoding unexposed row keys.
func (p *Projection) HasNativeField(index int) bool {
	for _, f := range p.root.fields {
		if f.index == index {
			return true
		}
	}
	return false
}
