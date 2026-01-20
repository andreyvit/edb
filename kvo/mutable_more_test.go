package kvo

import "testing"

func TestMutableMap_BasicsAndGetAnyMap(t *testing.T) {
	root := NewRecord(TDoodad)
	rec := root.Record()
	if rec == nil {
		t.Fatalf("Record() = nil")
	}
	_ = rec.Original()

	if !root.IsEmpty() || root.IsMissing() {
		t.Fatalf("unexpected IsEmpty/IsMissing for new record")
	}

	if root.Type() != TDoodad {
		t.Fatalf("Type() returned unexpected value")
	}
	if root.Packable() == nil {
		t.Fatalf("Packable() = nil")
	}

	root.Set(KFoo, 0x42)
	eq(t, root.Get(KFoo), uint64(0x42))

	bar := root.UpdateMap(KBar)
	bar.Set(100, 42)

	am := root.GetAnyMap(KBar)
	if am.IsMissing() || am.KeyCount() == 0 {
		t.Fatalf("GetAnyMap(KBar) returned missing/empty map")
	}
	if len(root.Keys()) == 0 {
		t.Fatalf("Keys() returned empty slice, wanted keys")
	}
	for range root.KeySeq() {
		break
	}
	if root.Dump() == "" {
		t.Fatalf("Dump() returned empty string")
	}

	t.Run("unknown key panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic")
			}
		}()
		_ = root.Get(999999)
	})
}
