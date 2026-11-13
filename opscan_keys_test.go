package edb

import (
	"fmt"
	"slices"
	"testing"
)

func TestKeyCursorSets(t *testing.T) {
	t.Run("uint", func(t *testing.T) { testKeyCursorSets(t, []ID{1, 2, 3, 4, 5}) })
	t.Run("string", func(t *testing.T) { testKeyCursorSets(t, []string{"a", "aa", "b", "c", "z"}) })
	t.Run("composite", func(t *testing.T) { testKeyCursorSets(t, []AB{{1, 1}, {1, 2}, {2, 1}, {2, 2}, {3, 1}}) })
}
func testKeyCursorSets[Key comparable](t *testing.T, keys []Key) {
	db, rows, _, group := typedTestTable[Key](t, MessagePack)
	records := rows.Records()
	db.Write(TxMetadata{}, func(tx *Tx) {
		for i, key := range keys {
			g := "vip"
			if i%2 != 0 {
				g = "regular"
			}
			rows.Put(tx, &typedTestRow[Key]{key, fmt.Sprint(i), g})
			putRawTableValue(t, tx, rows.Table(), rows.Table().EncodeKey(key), []byte{0xff})
		}
	})
	db.Read(func(tx *Tx) {
		for _, reverse := range []bool{false, true} {
			a := func() *KeyCursor[Key] {
				scan := records.bindIndex[string](group).Exact(tx, "vip")
				if reverse {
					scan = scan.Reversed()
				}
				return scan.KeyCursor()
			}
			b := func() *KeyCursor[Key] {
				scan := rows.Range(tx, keys[1], keys[3])
				if reverse {
					scan = scan.Reversed()
				}
				return scan.KeyCursor()
			}
			empty := func() *KeyCursor[Key] {
				scan := records.bindIndex[string](group).Exact(tx, "missing")
				if reverse {
					scan = scan.Reversed()
				}
				return scan.KeyCursor()
			}
			check := func(c *KeyCursor[Key], want []Key) {
				t.Helper()
				want = slices.Clone(want)
				if reverse {
					slices.Reverse(want)
				}
				if got := slices.Collect(c.Keys()); !slices.Equal(got, want) {
					t.Fatalf("reverse=%v: got %v want %v", reverse, got, want)
				}
				if c.Next() {
					t.Fatal("restarted exhausted set")
				}
			}
			check(a().Union(b()), keys)
			check(a().Intersect(b()), keys[2:3])
			check(a().Union(a(), b(), b()), keys)
			check(a().Union(empty()), []Key{keys[0], keys[2], keys[4]})
			check(a().Intersect(empty()), nil)
			check(empty().Union(empty()), nil)
			check(a().Union(b()).Intersect(b()), keys[1:4])
			check(a().Union(), []Key{keys[0], keys[2], keys[4]})
			check(a().Intersect(), []Key{keys[0], keys[2], keys[4]})
		}
	})
}
func TestKeyCursorMergeValidation(t *testing.T) {
	db, rows, _, group := typedTestTable[ID](t, Fast2)
	db.Read(func(tx *Tx) {
		valid := func() *KeyCursor[ID] { return rows.Scan(tx).KeyCursor() }
		assertPanics(t, func() { valid().Union(rows.Scan(tx).Reversed().KeyCursor()) })
		assertPanics(t, func() { valid().Intersect(rows.bindIndex[string](group).Scan(tx).KeyCursor()) })
		assertPanics(t, func() { rows.bindIndex[string](group).Scan(tx).KeyCursor().Union(valid()) })
		c := valid()
		assertPanics(t, func() { c.Union(c) })
		assertPanics(t, func() { valid().Union(c, c) })
		db.Read(func(other *Tx) { assertPanics(t, func() { valid().Union(rows.Scan(other).KeyCursor()) }) })
	})
}

func TestKeyMergeDetectsNonMonotonicIndexKeys(t *testing.T) {
	db, rows, _, group := typedTestTable[string](t, Fast2)
	// A complete encoded key can be a prefix of another when the string embeds
	// tuple-footer bytes. Appending the index footer can then change their order.
	db.Write(TxMetadata{}, func(tx *Tx) {
		rows.Put(tx, &typedTestRow[string]{"a", "one", "vip"})
		rows.Put(tx, &typedTestRow[string]{"a\x01\x00", "two", "vip"})
	})
	db.Read(func(tx *Tx) {
		for _, reverse := range []bool{false, true} {
			scan := rows.bindIndex[string](group).Exact(tx, "vip")
			if reverse {
				scan = scan.Reversed()
			}
			assertPanics(t, func() {
				for range scan.KeyCursor().Union().Keys() {
				}
			})
			other := rows.Exact(tx, "a")
			if reverse {
				other = other.Reversed()
			}
			assertPanics(t, func() { scan.KeyCursor().Intersect(other.KeyCursor()) })
		}
	})
}
