package edb

import (
	"reflect"
	"testing"
)

func TestTable_KeyStringAndParseKey_RoundTrip(t *testing.T) {
	t.Run("simple key", func(t *testing.T) {
		key := ID(42)
		s := usersTable.KeyString(key)
		got, err := usersTable.ParseKey(s)
		if err != nil {
			t.Fatalf("ParseKey(%q) failed: %v", s, err)
		}
		if !reflect.DeepEqual(got, key) {
			t.Fatalf("ParseKey(KeyString(%v)) = %T %v, wanted %T %v", key, got, got, key, key)
		}
	})

	t.Run("composite key", func(t *testing.T) {
		key := AB{A: 1, B: 2}
		s := widgetsTable.KeyString(key)
		got, err := widgetsTable.ParseKey(s)
		if err != nil {
			t.Fatalf("ParseKey(%q) failed: %v", s, err)
		}
		if !reflect.DeepEqual(got, key) {
			t.Fatalf("ParseKey(KeyString(%v)) = %T %v, wanted %T %v", key, got, got, key, key)
		}
	})

	t.Run("invalid key string", func(t *testing.T) {
		_, err := usersTable.ParseKey("not-a-number")
		if err == nil {
			t.Fatalf("ParseKey should fail for invalid key string")
		}
	})
}

