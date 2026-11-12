package evm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/andreyvit/edb/values"
)

// EncodeJSON encodes a script's any value. Objects have string keys. Arrays
// and slices are JSON arrays. The byte limit also bounds expansion of shared
// subtrees. Unsupported types and cycles fail without exposing VM pointers.
func (p *Program) EncodeJSON(ctx context.Context, v values.Value, limit int) (json.RawMessage, error) {
	w := jsonValueWriter{ctx: ctx, types: p.typeShapes, limit: limit}
	if _, err := w.dynamic(v, 0); err != nil {
		return nil, err
	}
	return w.data, nil
}

// EncodeJSON encodes an any argument from this invocation.
func (c *Call) EncodeJSON(v values.Value, limit int) (json.RawMessage, error) {
	return c.program.EncodeJSON(c.Context, v, limit)
}

// EncodeJSONObject encodes a statically typed map[string]any argument.
func (c *Call) EncodeJSONObject(v values.Value, limit int) (json.RawMessage, error) {
	w := jsonValueWriter{ctx: c.Context, types: c.program.typeShapes, limit: limit}
	s := values.Shape{Kind: values.MapKind, Key: &values.Shape{Kind: values.StringKind}, Elem: &values.Shape{Kind: values.InterfaceKind}}
	if _, err := w.value(&s, v, 0); err != nil {
		return nil, err
	}
	return w.data, nil
}

// JSONValue projects a script's any value directly into an owned Go JSON tree.
// Numbers are json.Number, preserving the encoder's exact spelling and precision.
// The limit is the equivalent JSON byte length, including escaped strings.
func (p *Program) JSONValue(ctx context.Context, v values.Value, limit int) (any, error) {
	w := jsonValueWriter{ctx: ctx, types: p.typeShapes, limit: limit, mode: jsonMaterialize}
	return w.dynamic(v, 0)
}

// JSONValue also meters the materialized tree against this invocation's budget.
func (c *Call) JSONValue(v values.Value, limit int) (any, error) {
	w := jsonValueWriter{ctx: c.Context, types: c.program.typeShapes, limit: limit, mode: jsonMaterialize, charge: c.ChargeAllocation}
	return w.dynamic(v, 0)
}

type jsonConversionMode uint8

const (
	jsonEncode jsonConversionMode = iota
	jsonMaterialize
)

func (m jsonConversionMode) String() string {
	if m == jsonMaterialize {
		return "materialize"
	}
	return "encode"
}

type jsonValueWriter struct {
	ctx    context.Context
	types  *values.TypeTable
	limit  int
	data   []byte
	used   int
	mode   jsonConversionMode
	charge func(uint64) error
}

var errJSONSize = errors.New("JSON exceeds output byte limit")

func (w *jsonValueWriter) reserve(n int) error {
	if n > w.limit-w.used {
		return errJSONSize
	}
	w.used += n
	return nil
}
func (w *jsonValueWriter) append(b []byte) error {
	if err := w.reserve(len(b)); err != nil {
		return err
	}
	if w.mode == jsonEncode {
		w.data = append(w.data, b...)
	}
	return nil
}
func (w *jsonValueWriter) allocate(n uint64) error {
	if w.charge != nil {
		return w.charge(n)
	}
	return nil
}
func (w *jsonValueWriter) number(b []byte) (any, error) {
	if err := w.append(b); err != nil {
		return nil, err
	}
	if w.mode == jsonEncode {
		return nil, nil
	}
	if err := w.allocate(uint64(len(b)) + 16); err != nil {
		return nil, err
	}
	return json.Number(string(b)), nil
}

func (w *jsonValueWriter) string(s string) (any, error) {
	if len(s) > w.limit-w.used-2 {
		return nil, errJSONSize
	}
	if w.mode == jsonEncode {
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		return nil, w.append(b)
	}
	if !utf8.ValidString(s) {
		// JSON implementations differ in whether replacement runes are
		// escaped. Consult the encoder only for malformed input; no decoding
		// is needed, and ordinary strings never allocate a JSON buffer.
		if err := w.allocate(uint64(len(s))*6 + 2); err != nil {
			return nil, err
		}
		b, err := json.Marshal(s)
		if err != nil {
			return nil, err
		}
		if err := w.reserve(len(b)); err != nil {
			return nil, err
		}
		if err := w.allocate(uint64(len(s))*3 + 16); err != nil {
			return nil, err
		}
		return strings.Map(func(r rune) rune { return r }, s), nil
	}
	// Count exactly encoding/json's HTML-safe string representation without
	// producing it.
	start := w.used
	if err := w.reserve(2); err != nil {
		return nil, err
	}
	for rest := s; len(rest) > 0; {
		r, n := utf8.DecodeRuneInString(rest)
		rest = rest[n:]
		size := n
		switch r {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			size = 2
		case '<', '>', '&', '\u2028', '\u2029':
			size = 6
		default:
			if r < 0x20 {
				size = 6
			}
		}
		if err := w.reserve(size); err != nil {
			return nil, err
		}
	}
	if err := w.allocate(uint64(w.used-start) + 16); err != nil {
		return nil, err
	}
	return strings.Clone(s), nil
}

func (w *jsonValueWriter) dynamic(v values.Value, depth int) (any, error) {
	s, value, err := w.types.Unpack(v)
	if err != nil {
		return nil, err
	}
	return w.value(s, value, depth)
}

func (w *jsonValueWriter) value(s *values.Shape, v values.Value, depth int) (any, error) {
	if depth > 256 {
		return nil, errors.New("JSON value is cyclic or exceeds 256 levels")
	}
	if err := w.ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, w.append([]byte("null"))
	}
	var buf [64]byte
	switch s.Kind {
	case values.InterfaceKind:
		return w.dynamic(v, depth+1)
	case values.BoolKind:
		return v.R != 0, w.append(strconv.AppendBool(buf[:0], v.R != 0))
	case values.IntKind:
		shift := (64 - uint(s.Bits)) & 63
		return w.number(strconv.AppendInt(buf[:0], int64(v.R<<shift)>>shift, 10))
	case values.UintKind:
		return w.number(strconv.AppendUint(buf[:0], v.R, 10))
	case values.FloatKind:
		n := v.AsFloat()
		if s.Bits == 32 {
			n = float64(math.Float32frombits(uint32(v.R)))
		}
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return nil, errors.New("JSON requires finite numbers")
		}
		return w.number(strconv.AppendFloat(buf[:0], n, 'g', -1, int(s.Bits)))
	case values.TimestampKind:
		b, err := v.AsTime().MarshalJSON()
		if err != nil {
			return nil, err
		}
		return w.string(string(b[1 : len(b)-1]))
	case values.StringKind:
		return w.string(v.AsString())
	case values.MapKind:
		if s.Key.Kind != values.StringKind {
			return nil, errors.New("JSON object keys must be strings")
		}
		if v.P == nil {
			return nil, w.append([]byte("null"))
		}
		if err := w.append([]byte("{")); err != nil {
			return nil, err
		}
		m := (*values.Map)(v.P)
		if m.Len() > (w.limit-w.used)/4 {
			return nil, errJSONSize
		}
		if err := w.allocate(uint64(m.Len()) * 8); err != nil {
			return nil, err
		}
		entries := make([]*values.MapEntry, 0, m.Len())
		for _, e := range m.Entries() {
			if err := w.ctx.Err(); err != nil {
				return nil, err
			}
			if e.Live {
				entries = append(entries, e)
			}
		}
		// Also preserve duplicate-key resolution after invalid UTF-8 keys are
		// normalized, exactly as decoding the sorted encoder output would.
		slices.SortFunc(entries, func(a, b *values.MapEntry) int { return strings.Compare(a.Key.AsString(), b.Key.AsString()) })
		var out map[string]any
		if w.mode == jsonMaterialize {
			if err := w.allocate(64 + uint64(m.Len())*128); err != nil {
				return nil, err
			}
			out = make(map[string]any, m.Len())
		}
		first := true
		for _, e := range entries {
			if !e.Live {
				continue
			}
			if !first {
				if err := w.append([]byte(",")); err != nil {
					return nil, err
				}
			}
			first = false
			key, err := w.string(e.Key.AsString())
			if err != nil {
				return nil, err
			}
			if err := w.append([]byte(":")); err != nil {
				return nil, err
			}
			value, err := w.value(s.Elem, e.Value, depth+1)
			if err != nil {
				return nil, err
			}
			if out != nil {
				out[key.(string)] = value
			}
		}
		if w.mode == jsonEncode {
			return nil, w.append([]byte("}"))
		}
		return out, w.append([]byte("}"))
	case values.ArrayKind, values.SliceKind:
		n := s.Len
		if s.Kind == values.SliceKind {
			if v.P == nil {
				return nil, w.append([]byte("null"))
			}
			n = uint64(v.SliceLen())
		}
		if err := w.append([]byte("[")); err != nil {
			return nil, err
		}
		var out []any
		if w.mode == jsonMaterialize {
			if n > uint64(max(0, w.limit-w.used)) {
				return nil, errJSONSize
			}
			if err := w.allocate(24 + n*16); err != nil {
				return nil, err
			}
			out = make([]any, n)
		}
		for i := range n {
			if i != 0 {
				if err := w.append([]byte(",")); err != nil {
					return nil, err
				}
			}
			child := s.Elem.Stored(values.CellAt(v.P, i*s.Elem.Cells))
			value, err := w.value(s.Elem, child, depth+1)
			if err != nil {
				return nil, err
			}
			if out != nil {
				out[i] = value
			}
		}
		if w.mode == jsonEncode {
			return nil, w.append([]byte("]"))
		}
		return out, w.append([]byte("]"))
	default:
		return nil, fmt.Errorf("%s is not a JSON value", goTypeName(s))
	}
}
