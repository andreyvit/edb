package kvo

import (
	"strconv"
	"strings"
)

func Dump(m AnyMap) string {
	var buf strings.Builder
	dump(&buf, m)
	return buf.String()
}

func dump(buf *strings.Builder, m AnyMap) {
	if m.IsMissing() {
		buf.WriteString("<missing>")
		return
	}
	model := m.Model()
	buf.WriteByte('{')
	for i, k := range m.Keys() {
		if i > 0 {
			buf.WriteByte(',')
			buf.WriteByte(' ')
		}
		v := m.Get(k)
		if model != nil {
			prop := model.MustPropByCode(k)
			buf.WriteString(prop.Name())
		} else {
			buf.WriteString("0x")
			buf.WriteString(strconv.FormatUint(k, 16))
		}
		buf.WriteByte(':')
		buf.WriteByte(' ')
		buf.WriteString(strconv.FormatUint(v, 10))
	}
	buf.WriteByte('}')
}
