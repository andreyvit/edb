package kvo

import "testing"

func TestNativeFieldLookupSparseCodes(t *testing.T) {
	for _, keys := range [][]uint64{{0, 48, 844}, {5, 4100}, {5, 4101}, {0, ^uint64(0)}} {
		p := &nativePlan{}
		for _, key := range keys {
			p.fields = append(p.fields, nativeField{key: key})
		}
		p.indexFields()
		for i, key := range keys {
			if p.field(key) != &p.fields[i] {
				t.Fatalf("wrong field for %d", key)
			}
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("missing field did not panic")
				}
			}()
			p.field(3)
		}()
	}
}
