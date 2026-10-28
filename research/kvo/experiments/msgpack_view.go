package kvolab

import "encoding/binary"

// MPView is a minimal zero-allocation MessagePack navigator for the fixture
// types. It avoids comparing our views only against full reflection decoding.
// It is experimental: inputs must be trusted and it is not a general validator.
type MPView []byte

func mpHead(b []byte) (tag byte, n int, body []byte) {
	tag = b[0]
	body = b[1:]
	switch {
	case tag <= 0x7f || tag >= 0xe0:
		return tag, 0, body
	case tag >= 0xa0 && tag <= 0xbf:
		return tag, int(tag & 31), body
	case tag >= 0x80 && tag <= 0x9f:
		return tag, int(tag & 15), body
	}
	switch tag {
	case 0xc4, 0xd9:
		n = int(body[0])
		body = body[1:]
	case 0xc5, 0xda, 0xdc, 0xde:
		n = int(binary.BigEndian.Uint16(body))
		body = body[2:]
	case 0xc6, 0xdb, 0xdd, 0xdf:
		n = int(binary.BigEndian.Uint32(body))
		body = body[4:]
	}
	return
}
func mpSkip(b []byte) []byte {
	tag, n, body := mpHead(b)
	switch {
	case tag <= 0x7f || tag >= 0xe0:
		return body
	case tag >= 0xa0 && tag <= 0xbf:
		return body[n:]
	case tag >= 0x80 && tag <= 0x8f:
		n *= 2
	case tag >= 0x90 && tag <= 0x9f:
	default:
		switch tag {
		case 0xc0, 0xc2, 0xc3:
			return body
		case 0xcc, 0xd0:
			return body[1:]
		case 0xcd, 0xd1:
			return body[2:]
		case 0xce, 0xd2, 0xca:
			return body[4:]
		case 0xcf, 0xd3, 0xcb:
			return body[8:]
		case 0xc4, 0xc5, 0xc6, 0xd9, 0xda, 0xdb:
			return body[n:]
		case 0xde, 0xdf:
			n *= 2
		case 0xdc, 0xdd:
		default:
			panic("unsupported MessagePack tag")
		}
	}
	for i := 0; i < n; i++ {
		body = mpSkip(body)
	}
	return body
}
func (v MPView) Uint() uint64 {
	b := []byte(v)
	switch b[0] {
	case 0xcc:
		return uint64(b[1])
	case 0xcd:
		return uint64(binary.BigEndian.Uint16(b[1:]))
	case 0xce:
		return uint64(binary.BigEndian.Uint32(b[1:]))
	case 0xcf:
		return binary.BigEndian.Uint64(b[1:])
	default:
		if b[0] <= 0x7f {
			return uint64(b[0])
		}
		panic("not unsigned")
	}
}
func (v MPView) Bytes() []byte { _, n, b := mpHead(v); return b[:n] }
func (v MPView) Get(key uint64) (MPView, bool) {
	tag, n, b := mpHead(v)
	if !(tag >= 0x80 && tag <= 0x8f || tag == 0xde || tag == 0xdf) {
		panic("not map")
	}
	for i := 0; i < n; i++ {
		k := MPView(b).Uint()
		b = mpSkip(b)
		end := mpSkip(b)
		if k == key {
			return MPView(b[:len(b)-len(end)]), true
		}
		b = end
	}
	return nil, false
}
func (v MPView) GetString(key string) (MPView, bool) {
	_, n, b := mpHead(v)
	for i := 0; i < n; i++ {
		k := MPView(b).Bytes()
		b = mpSkip(b)
		end := mpSkip(b)
		if string(k) == key {
			return MPView(b[:len(b)-len(end)]), true
		}
		b = end
	}
	return nil, false
}
func (v MPView) Walk(fn func(uint64, MPView) bool) {
	_, n, b := mpHead(v)
	for i := 0; i < n; i++ {
		k := MPView(b).Uint()
		b = mpSkip(b)
		end := mpSkip(b)
		if !fn(k, MPView(b[:len(b)-len(end)])) {
			break
		}
		b = end
	}
}

func (v MPView) Int() int64 {
	switch v[0] {
	case 0xd0:
		return int64(int8(v[1]))
	case 0xd1:
		return int64(int16(binary.BigEndian.Uint16(v[1:])))
	case 0xd2:
		return int64(int32(binary.BigEndian.Uint32(v[1:])))
	case 0xd3:
		return int64(binary.BigEndian.Uint64(v[1:]))
	default:
		if v[0] >= 0xe0 {
			return int64(int8(v[0]))
		}
		return int64(v.Uint())
	}
}
