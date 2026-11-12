package kvo

import (
	"reflect"

	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

func nativeRecordValue(p *nativePlan, r Record) reflect.Value {
	if n := r.Native(); n != nil {
		v := reflect.ValueOf(n)
		if v.Type() != p.typ && v.Kind() == reflect.Pointer {
			v = v.Elem()
		}
		if v.Type() == p.typ {
			return v
		}
	}
	return reflect.Value{}
}
func assignRecordValue(p *nativePlan, r Record) reflect.Value {
	if v := nativeRecordValue(p, r); v.IsValid() {
		return v
	}
	if r.representation().plan == indirectPlan(p) {
		if raw := r.MessagePack(); raw != nil {
			return decodeProperty(p, msgpack.Trusted(raw))
		}
	}
	if !indirectPlan(p).whole(make(map[*nativePlan]bool)) {
		panic("cannot replace an aggregate containing unexposed fields; assign its properties")
	}
	return nativeCollectionAssignment(p, values.Bytes(r.Snapshot().Raw()))
}
func encodeRecordMessagePack(p *nativePlan, r Record) []byte {
	if r.Missing() {
		return []byte{0xc0}
	}
	if r.representation().plan == indirectPlan(p) {
		if raw := r.MessagePack(); raw != nil {
			return raw
		}
	}
	if v := nativeRecordValue(p, r); v.IsValid() {
		return msgpack.Encode(nil, v)
	}
	return encodeMessagePackLeaf(p, row2.Step{Options: row2.Present}, values.Bytes(r.Snapshot().Raw()))
}
