package fast1

import (
	"fmt"
	"github.com/andreyvit/edb/kvo"
)

func ensureCanAccessKey(typ kvo.AnyType, key uint64) {
	if typ == nil {
		return
	}
	valueType := typ.MapValueType(key)
	if valueType == nil {
		reportCannotAccessKey(typ, key)
	}
}

func reportCannotAccessKey(typ kvo.AnyType, key uint64) {
	if schema := typ.Schema(); schema != nil {
		if prop := schema.PropByCode(key); prop != nil {
			panic(fmt.Sprintf("type %s does not allow key %s", typ.Name(), prop.Name()))
		}
	}
	panic(fmt.Sprintf("type %s does not allow key %d", typ.Name(), key))
}
