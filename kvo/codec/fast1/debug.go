package fast1

import (
	"github.com/andreyvit/edb/kvo"
	"strconv"
	"strings"
)

func Dump(m AnyFast1Map) string {
	var buf strings.Builder
	var fc kvo.FmtContext
	dump(&buf, &fc, m)
	return buf.String()
}

func dump(buf *strings.Builder, fc *kvo.FmtContext, m AnyFast1Map) {
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
		var kt, vt kvo.AnyType
		var prop kvo.PropImpl
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
				kt = kvo.TUknownKey
			}
			buf.WriteString(kt.FormatValue(fc, k))
		}
		buf.WriteByte(':')
		buf.WriteByte(' ')

		if vt == nil {
			vt = kvo.TUnknownUint64
		}
		switch vt.ValueKind() {
		case kvo.ValueKindWord:
			v := m.Get(k)
			buf.WriteString(vt.FormatValue(fc, v))
		case kvo.ValueKindMap:
			dump(buf, fc, m.GetAnyMap(k))
		case kvo.ValueKindUnknown, kvo.ValueKindScalarData, kvo.ValueKindPIIData:
			buf.WriteString("<unsupported>")
		}
	}
	buf.WriteByte('}')
}
