package kvo

import (
	"slices"
	"sync"

	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

// Model-only metadata is shared by encoded and generic decoded records. Type
// inspection happens once, independently of native Go bindings and row data.
type modelLayout struct {
	representations *representationSet
	valueType       *values.Type
	model           *Model
	fields          []modelField
	index           []uint16
	encodings       []byte
	base            uint64
	decodeScalars   bool
}
type modelField struct {
	key          uint64
	encoding     row2.Encoding
	writeOptions row2.KeyAccessOptions
	child        *modelLayout
	itemEncoding row2.Encoding
	list         bool
	listAccess   *ListAccess
}

var layouts = struct {
	sync.Mutex
	byModel map[*Model]*modelLayout
}{byModel: make(map[*Model]*modelLayout)}

func layoutFor(m *Model) *modelLayout {
	if m == nil {
		return nil
	}
	layouts.Lock()
	defer layouts.Unlock()
	return compileLayout(m)
}
func compileLayout(m *Model) *modelLayout {
	if m == nil {
		return nil
	}
	if p := layouts.byModel[m]; p != nil {
		return p
	}
	p := &modelLayout{model: m}
	p.valueType = values.NewOwnedType(m.Name(), values.Pair, ownValue)
	p.representations = &representationSet{layout: p}
	layouts.byModel[m] = p
	for _, prop := range m.AllProps() {
		enc := Encoding(prop.AnyType())
		itemEncoding, list := listEncoding(prop.AnyType())
		var access *ListAccess
		if typ, ok := prop.AnyType().(*ListType); ok {
			access = newListAccess(prop.Code(), typ)
		}
		p.fields = append(p.fields, modelField{key: prop.Code(), encoding: enc, writeOptions: WriteOptions(m.PropFlags(prop.Code())), child: compileLayout(prop.TypeModel()), itemEncoding: itemEncoding, list: list, listAccess: access})
		p.decodeScalars = p.decodeScalars || enc == row2.Float32
	}
	slices.SortFunc(p.fields, func(a, b modelField) int {
		if a.key < b.key {
			return -1
		}
		if a.key > b.key {
			return 1
		}
		return 0
	})
	if len(p.fields) != 0 {
		p.base = p.fields[0].key
		span := p.fields[len(p.fields)-1].key - p.base
		if span < 4096 && span < uint64(len(p.fields))*4 {
			p.index = make([]uint16, span+1)
			p.encodings = make([]byte, span+1)
			for i, f := range p.fields {
				p.index[f.key-p.base] = uint16(i + 1)
				p.encodings[f.key-p.base] = byte(f.encoding) + 1
			}
		}
	}
	return p
}

// WriteOptions converts model property flags to the options required by
// FastSetPathValue. Compilers can resolve these once when linking a field access.
func WriteOptions(flags PropFlag) row2.KeyAccessOptions {
	var options row2.KeyAccessOptions
	if flags&Compact != 0 {
		options |= row2.Compact
	}
	if flags&(PresenceIsSignificant|PrimaryKey) != 0 {
		options |= row2.Present
	}
	return options
}

func (r *recordView) propertyWriteOptions(key uint64) row2.KeyAccessOptions {
	return r.propertyField(key).writeOptions
}

func (r *recordView) propertyField(key uint64) *modelField {
	layout := r.layout
	if layout == nil && r.plan != nil {
		layout = r.plan.layout
	}
	if layout == nil {
		panic("record: property write defaults require a model")
	}
	return layout.field(key)
}
func (p *modelLayout) field(k uint64) *modelField {
	if i := k - p.base; i < uint64(len(p.index)) {
		if n := p.index[i]; n != 0 {
			return &p.fields[n-1]
		}
	}
	return p.fieldSlow(k)
}
func (p *modelLayout) fieldSlow(k uint64) *modelField {
	i, ok := slices.BinarySearchFunc(p.fields, k, func(f modelField, k uint64) int {
		if f.key < k {
			return -1
		}
		if f.key > k {
			return 1
		}
		return 0
	})
	if !ok {
		panic("record: unknown property; use GetAs for explicitly typed raw access")
	}
	return &p.fields[i]
}

// encodingSlot is small enough to inline at the access sites. Zero means the
// property needs a sparse lookup; nonzero entries are Encoding + 1.
func (p *modelLayout) encodingSlot(k uint64) byte {
	if i := k - p.base; i < uint64(len(p.encodings)) {
		return p.encodings[i]
	}
	return 0
}
func (p *modelLayout) encoding(k uint64) row2.Encoding {
	if enc := p.encodingSlot(k); enc != 0 {
		return row2.Encoding(enc - 1)
	}
	return p.fieldSlow(k).encoding
}
