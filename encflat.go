package edb

import (
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
)

type FlatMarshaler interface {
	MarshalFlat(buf []byte) []byte
}

type FlatUnmarshaler interface {
	UnmarshalFlat(buf []byte) error
}

type FlatMarshallable interface {
	FlatMarshaler
	FlatUnmarshaler
}

type binaryMarshallable interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

var flatMarshallableType = reflect.TypeOf((*FlatMarshallable)(nil)).Elem()
var textMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
var textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
var binaryMarshallableType = reflect.TypeOf((*binaryMarshallable)(nil)).Elem()
var timeType = reflect.TypeOf((*time.Time)(nil)).Elem()
var byteType = reflect.TypeOf((byte)(0))
var byteArrayType = reflect.TypeOf(([]byte)(nil))

func appendString(buf []byte, v string) []byte {
	n := len(v)
	off, buf := grow(buf, n)
	copy(buf[off:], v)
	return buf
}

func appendUint64(buf []byte, v uint64) []byte {
	off, buf := grow(buf, 8)
	buf[off+0] = byte(v >> 56)
	buf[off+1] = byte(v >> 48)
	buf[off+2] = byte(v >> 40)
	buf[off+3] = byte(v >> 32)
	buf[off+4] = byte(v >> 24)
	buf[off+5] = byte(v >> 16)
	buf[off+6] = byte(v >> 8)
	buf[off+7] = byte(v)
	return buf
}

func appendUint32(buf []byte, v uint32) []byte {
	off, buf := grow(buf, 4)
	buf[off+0] = byte(v >> 24)
	buf[off+1] = byte(v >> 16)
	buf[off+2] = byte(v >> 8)
	buf[off+3] = byte(v)
	return buf
}

func appendUint16(buf []byte, v uint16) []byte {
	off, buf := grow(buf, 2)
	buf[off+0] = byte(v >> 8)
	buf[off+1] = byte(v)
	return buf
}

func appendUint8(buf []byte, v uint8) []byte {
	off, buf := grow(buf, 1)
	buf[off] = v
	return buf
}

type flatEncoder struct {
	buf []byte
	tupleEncoder
}

func (fe *flatEncoder) begin() {
	fe.tupleEncoder.begin(fe.buf)
}
func (fe *flatEncoder) append(b []byte) {
	fe.buf = appendRaw(fe.buf, b)
}
func (fe *flatEncoder) finalize() []byte {
	return fe.tupleEncoder.finalize(fe.buf)
}

var flatEncodings sync.Map

type flatEncoding struct {
	typ        reflect.Type
	components []*flatComponent
}

type flatComponent struct {
	Type        reflect.Type
	Path        string
	RawParser   func(buf []byte, s string) ([]byte, error)
	RawStringer func(b []byte) (string, error)
	Parser      func(s string, val reflect.Value) error
	Stringer    func(val reflect.Value) string
	Getters     []func(v reflect.Value, init bool) reflect.Value
	Decode      func(b []byte, v reflect.Value) error
	Encode      func(fe *flatEncoder, v reflect.Value)
}

func (fc *flatComponent) valueIn(val reflect.Value, init bool) reflect.Value {
	for i := len(fc.Getters) - 1; i >= 0; i-- {
		if !val.IsValid() {
			return val
		}
		val = fc.Getters[i](val, init)
	}
	return val
}

func flatEncodingOf(typ reflect.Type) *flatEncoding {
	if e, ok := flatEncodings.Load(typ); ok {
		return e.(*flatEncoding)
	}
	enc := &flatEncoding{typ: typ}
	enumerateFlatComponents(typ, func(fc *flatComponent) {
		if fc.Type.ConvertibleTo(textMarshalerType) {
			fc.Stringer = func(val reflect.Value) string {
				return string(must(val.Interface().(encoding.TextMarshaler).MarshalText()))
			}
		}
		if reflect.PointerTo(fc.Type).ConvertibleTo(textUnmarshalerType) {
			fc.Parser = func(s string, val reflect.Value) error {
				return val.Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(s))
			}
		}
		enc.components = append(enc.components, fc)
	})
	flatEncodings.LoadOrStore(typ, enc)
	return enc
}

func (enc *flatEncoding) encode(buf []byte, val reflect.Value) []byte {
	fe := flatEncoder{buf: buf}
	enc.encodeInto(&fe, val)
	return fe.finalize()
}

func (enc *flatEncoding) encodeInto(fe *flatEncoder, val reflect.Value) {
	for _, fc := range enc.components {
		fe.begin()
		cval := fc.valueIn(val, false)
		fc.Encode(fe, cval)
	}
}

func (enc *flatEncoding) decode(buf []byte, val reflect.Value) error {
	tup, err := decodeTuple(buf)
	if err != nil {
		return err
	}

	err = enc.decodeTup(tup, val)
	if err != nil {
		return dataErrf(buf, len(buf), nil, "%s", err.Error())
	}
	return nil
}

func (enc *flatEncoding) decodeTup(tup tuple, val reflect.Value) error {
	if val.Kind() != reflect.Ptr {
		panic(fmt.Errorf("flatEncoding must be decoding into a ptr, got %v", val.Type()))
	}
	val = val.Elem()

	if len(tup) != len(enc.components) {
		return fmt.Errorf("wrong number of components: got %d, wanted at least %d", len(tup), len(enc.components))
	}

	for i, fc := range enc.components {
		cval := fc.valueIn(val, true)
		if !cval.IsValid() {
			panic(fmt.Errorf("invalid cval while decoding %v%s", enc.typ, fc.Path))
		}
		if !cval.CanSet() {
			panic(fmt.Errorf("unsettable cval while decoding %v%s", enc.typ, fc.Path))
		}
		if fc.Decode == nil {
			panic(fmt.Errorf("no flat decoder defined for %v", fc.Type))
		}
		err := fc.Decode(tup[i], cval)
		if err != nil {
			return fmt.Errorf("%s%w", pathPrefix(fc.Path), err)
		}
	}
	return nil
}

func (enc *flatEncoding) tupleToStrings(tup tuple) []string {
	n := len(enc.components)
	if len(tup) != n {
		panic(fmt.Errorf("wrong number of components: got %d, wanted %d in: %v", len(tup), len(enc.components), tup))
	}
	result := make([]string, n)
	for i, fc := range enc.components {
		// log.Printf("i=%d fc=%T %v tup=%v", i, fc, fc, tup)
		var err error
		if fc.Stringer != nil {
			val := reflect.New(fc.Type)
			err = fc.Decode(tup[i], val.Elem())
			if fc.Decode == nil {
				panic(fmt.Errorf("no Decode for %v", fc.Type))
			}
			if err != nil {
				panic(fmt.Errorf("invalid component %d: %w - in %v", i, err, tup))
			}
			result[i] = fc.Stringer(val)
		} else if fc.RawStringer != nil {
			result[i], err = fc.RawStringer(tup[i])
			if err != nil {
				panic(fmt.Errorf("invalid component %d: %w - in %v", i, err, tup))
			}
		} else {
			val := reflect.New(fc.Type)
			if fc.Decode == nil {
				panic(fmt.Errorf("no Decode for %v", fc.Type))
			}
			err = fc.Decode(tup[i], val.Elem())
			if err != nil {
				panic(fmt.Errorf("invalid component %d: %w - in %v", i, err, tup))
			}
			result[i] = fmt.Sprint(val.Interface())
		}
		// log.Printf("i=%d fc=%T %v => %q", i, fc, fc, result[i])
	}
	return result
}

func (enc *flatEncoding) stringsToRawKey(buf []byte, strs []string) ([]byte, error) {
	n := len(enc.components)
	if len(strs) != n {
		return nil, fmt.Errorf("wrong number of components: got %d, wanted %d", len(strs), len(enc.components))
	}

	var err error
	fe := flatEncoder{buf: buf}
	for i, fc := range enc.components {
		fe.begin()
		if fc.Parser != nil {
			val := reflect.New(fc.Type)
			err := fc.Parser(strs[i], val)
			if err != nil {
				return nil, fmt.Errorf("invalid component %d %q: %w", i, strs[i], err)
			}
			fc.Encode(&fe, val.Elem())
		} else {
			fe.buf, err = fc.RawParser(fe.buf, strs[i])
			if err != nil {
				return nil, fmt.Errorf("invalid component %d %q: %w", i, strs[i], err)
			}
		}
	}
	return fe.finalize(), nil
}

func enumerateFlatComponents(typ reflect.Type, f func(fc *flatComponent)) {
	if typ == timeType {
		f(&flatComponent{
			Type: typ,
			Encode: func(fe *flatEncoder, v reflect.Value) {
				value := v.Interface().(time.Time)
				fe.buf = appendUint64(fe.buf, uint64(value.Unix()))
			},
			Decode: func(b []byte, v reflect.Value) error {
				if len(b) == 8 {
					value := binary.BigEndian.Uint64(b)
					v.Set(reflect.ValueOf(time.Unix(int64(value), 0)))
					return nil
				} else {
					return fmt.Errorf("invalid time.Time data length: got %d bytes, valued %d", len(b), 8)
				}
			},
		})
		return
	}
	if typ.ConvertibleTo(flatMarshallableType) {
		f(&flatComponent{
			Type: typ,
			Encode: func(fe *flatEncoder, v reflect.Value) {
				fe.buf = v.Interface().(FlatMarshallable).MarshalFlat(fe.buf)
			},
			Decode: func(b []byte, v reflect.Value) error {
				return v.Interface().(FlatMarshallable).UnmarshalFlat(b)
			},
		})
		return
	}
	if reflect.PointerTo(typ).ConvertibleTo(binaryMarshallableType) {
		f(&flatComponent{
			Type: typ,
			Encode: func(fe *flatEncoder, v reflect.Value) {
				data, err := v.Interface().(encoding.BinaryMarshaler).MarshalBinary()
				if err != nil {
					panic(fmt.Errorf("%T.MarshalBinary: %w", v.Interface(), err))
				}
				fe.append(data)
			},
			Decode: func(b []byte, v reflect.Value) error {
				return v.Addr().Interface().(encoding.BinaryUnmarshaler).UnmarshalBinary(b)
			},
		})
		return
	}
	switch typ.Kind() {
	case reflect.String:
		f(&flatComponent{
			Type: typ,
			Encode: func(fe *flatEncoder, v reflect.Value) {
				fe.buf = appendString(fe.buf, v.String())
			},
			Decode: func(b []byte, v reflect.Value) error {
				v.Set(reflect.ValueOf(string(b)))
				return nil
			},
			RawParser: func(buf []byte, s string) ([]byte, error) {
				return appendRaw(buf, []byte(s)), nil
			},
			RawStringer: func(b []byte) (string, error) {
				if !utf8.Valid(b) {
					return "", fmt.Errorf("not a valid UTF8 string")
				}
				return string(b), nil
			},
		})
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uintptr:
		f(&flatComponent{
			Type: typ,
			// RawStringer: func(b []byte) string {

			// },
			Encode: func(fe *flatEncoder, v reflect.Value) {
				fe.buf = appendUint64(fe.buf, v.Uint())
			},
			Decode: func(b []byte, v reflect.Value) error {
				if len(b) == 8 {
					value := binary.BigEndian.Uint64(b)
					v.Set(reflect.ValueOf(value).Convert(typ))
					return nil
				} else {
					return fmt.Errorf("invalid int length: got %d bytes, valued %d", len(b), 8)
				}
			},
			RawParser: func(buf []byte, s string) ([]byte, error) {
				v, err := strconv.ParseUint(s, 10, 64)
				if err != nil {
					return nil, err
				}
				return appendUint64(buf, v), nil
			},
		})
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		f(&flatComponent{
			Type: typ,
			Encode: func(fe *flatEncoder, v reflect.Value) {
				fe.buf = appendUint64(fe.buf, uint64(v.Int()))
			},
			Decode: func(b []byte, v reflect.Value) error {
				if len(b) == 8 {
					value := int64(binary.BigEndian.Uint64(b))
					v.Set(reflect.ValueOf(value).Convert(typ))
					return nil
				} else {
					return fmt.Errorf("invalid int length: got %d bytes, valued %d", len(b), 8)
				}
			},
			RawParser: func(buf []byte, s string) ([]byte, error) {
				v, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					return nil, err
				}
				return appendUint64(buf, uint64(v)), nil
			},
		})
	case reflect.Ptr:
		elemType := typ.Elem()
		get := func(v reflect.Value, init bool) reflect.Value {
			if init {
				if v.IsNil() {
					v.Set(reflect.New(elemType))
				}
			}
			return v.Elem()
		}
		enumerateFlatComponents(typ.Elem(), func(fc *flatComponent) {
			fc.Getters = append(fc.Getters, get)
			f(fc)
		})
	case reflect.Struct:
		n := typ.NumField()
		for i := 0; i < n; i++ {
			i := i
			field := typ.Field(i)
			get := func(v reflect.Value, init bool) reflect.Value {
				return v.Field(i)
			}
			enumerateFlatComponents(field.Type, func(fc *flatComponent) {
				fc.Getters = append(fc.Getters, get)
				fc.Path = fc.Path + "." + field.Name
				f(fc)
			})
		}
	case reflect.Slice:
		if typ == byteArrayType {
			f(&flatComponent{
				Type: typ,
				Encode: func(fe *flatEncoder, v reflect.Value) {
					fe.buf = appendRaw(fe.buf, v.Convert(byteArrayType).Interface().([]byte))
				},
				Decode: func(b []byte, v reflect.Value) error {
					v.Set(reflect.ValueOf(b))
					return nil
				},
				RawParser: func(buf []byte, s string) ([]byte, error) {
					b, err := hex.DecodeString(s)
					if err != nil {
						return nil, err
					}
					return appendRaw(buf, b), nil
				},
				RawStringer: func(b []byte) (string, error) {
					return hex.EncodeToString(b), nil
				},
			})
			return
		}
		panic(fmt.Errorf("kvdb does not yet know how to encode slice %v", typ))
	case reflect.Array:
		if typ.Elem() == byteType {
			f(&flatComponent{
				Type: typ,
				Encode: func(fe *flatEncoder, v reflect.Value) {
					if !v.CanAddr() {
						panic(fmt.Errorf("non-addressable array %v %v", v.Type(), v))
					}
					fe.buf = appendRaw(fe.buf, v.Slice(0, v.Len()).Convert(byteArrayType).Interface().([]byte))
				},
			})
			return
		}
		panic(fmt.Errorf("kvdb does not yet know how to encode slice %v", typ))
	default:
		panic(fmt.Errorf("kvdb does not yet know how to encode %v", typ))
	}
}

func pathPrefix(p string) string {
	if p == "" {
		return ""
	}
	return p + ": "
}
