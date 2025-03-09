package kvo

import (
	"strconv"
	"strings"
)

func Dump(m AnyMap) string {
	var buf strings.Builder
	var fc FmtContext
	dump(&buf, &fc, m)
	return buf.String()
}

func dump(buf *strings.Builder, fc *FmtContext, m AnyMap) {
	if m.IsMissing() {
		buf.WriteString("<missing>")
		return
	}
	mapType := m.Type()
	buf.WriteByte('{')
	for i, k := range m.Keys() {
		if i > 0 {
			buf.WriteByte(',')
			buf.WriteByte(' ')
		}
		var kt, vt AnyType
		var prop PropImpl
		if mapType != nil {
			kt = mapType.MapKeyType()
			prop = mapType.MapProp(k)
			if prop != nil {
				vt = prop.AnyType()
			} else {
				vt = mapType.MapValueType(k)
			}
		} else {
			buf.WriteString("0x")
			buf.WriteString(strconv.FormatUint(k, 16))
		}
		if prop != nil {
			buf.WriteString(prop.Name())
		} else {
			if kt == nil {
				kt = TUknownKey
			}
			buf.WriteString(kt.FormatValue(fc, k))
		}
		buf.WriteByte(':')
		buf.WriteByte(' ')

		if vt == nil {
			vt = TUnknownUint64
		}
		switch vt.ValueKind() {
		case ValueKindWord:
			v := m.Get(k)
			buf.WriteString(vt.FormatValue(fc, v))
		case ValueKindMap:
			dump(buf, fc, m.GetAnyMap(k))
		}
	}
	buf.WriteByte('}')
}
