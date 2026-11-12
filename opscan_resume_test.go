package edb

import (
	"fmt"
	"slices"
	"testing"
)

func TestScanBookmark(t *testing.T) {
	t.Run("uint", func(t *testing.T) { testScanBookmark(t, []ID{1, 2, 3, 4, 5, 6}) })
	t.Run("string", func(t *testing.T) { testScanBookmark(t, []string{"a", "aa", "b", "ba", "z", "zz"}) })
	t.Run("composite", func(t *testing.T) { testScanBookmark(t, []AB{{1, 0}, {1, 1}, {1, 2}, {2, 0}, {2, 1}, {2, 2}}) })
}

func testScanBookmark[Key comparable](t *testing.T, keys []Key) {
	db, rows, name, group := typedTestTable[Key](t, Fast2)
	keyIndex := rows.Table().IndexNamed("key")
	db.Write(func(tx *Tx) {
		for i, key := range keys {
			rows.Put(tx, &typedTestRow[Key]{key, fmt.Sprintf("name%d", i), []string{"a", "b", "a", "b", "aa", "b"}[i]})
		}
	})
	cases := []struct {
		idx *IndexDef
		opt ScanOptions
	}{
		{nil, FullScan()}, {nil, RangeScan(keys[1], keys[4], true, false)},
		{nil, ExactScan(keys[3])}, {nil, LowerBoundScan(keys[2], false)},
		{keyIndex, FullScan()}, {keyIndex, ExactScan(keys[2])},
		{group, FullScan()}, {group, ExactScan("a")}, {group, ExactScan("missing")},
		{group, RangeScan("a", "b", true, true)}, {group, RangeScan("a", "b", false, false)},
		{group, ExactScan("b").PrimaryKeyRange(keys[1], keys[5], false, true)},
		{name, FullScan()}, {name, ExactScan("name3")}, {name, RangeScan("name1", "name4", false, true)},
	}
	if _, ok := any(keys[0]).(AB); ok {
		cases = append(cases, struct {
			idx *IndexDef
			opt ScanOptions
		}{nil, ExactScan(keys[0]).Prefix(1)},
			struct {
				idx *IndexDef
				opt ScanOptions
			}{keyIndex, ExactScan(keys[0]).Prefix(1)})
	}
	for ci, tt := range cases {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse=%v", ci, reverse), func(t *testing.T) {
				opt := tt.opt
				opt.Reverse = reverse
				open := func(tx *Tx, b ScanBookmark) RawCursor {
					if tt.idx != nil {
						return tx.IndexScan(tt.idx, opt.Resume(b))
					}
					return tx.TableScan(rows.Table(), opt.Resume(b))
				}
				var want []Key
				db.Read(func(tx *Tx) { want = AllKeys[Key](open(tx, ScanBookmark{})) })
				for split := 0; split <= len(want); split++ {
					var mark ScanBookmark
					db.Read(func(tx *Tx) {
						c := open(tx, mark)
						for range split {
							if !c.Next() {
								t.Fatal("unexpected end")
							}
						}
						mark = c.Bookmark()
					})
					db.Read(func(tx *Tx) {
						c := open(tx, mark)
						var got []Key
						for c.Next() {
							got = append(got, decodeTypedKey[Key](rows.Table(), c.RawKey()))
						}
						if !slices.Equal(got, want[split:]) {
							t.Fatalf("split %d: got %v, want %v", split, got, want[split:])
						}
						if c.Next() {
							t.Fatal("cursor restarted after exhaustion")
						}
						if len(got) > 0 && c.Bookmark().IsZero() {
							t.Fatal("bookmark lost at exhaustion")
						}
					})
				}
			})
		}
	}
}

func TestScanBookmarkDeletedPositionAndValidation(t *testing.T) {
	db, rows, _, group := typedTestTable[ID](t, Fast2)
	db.Write(func(tx *Tx) {
		for i := ID(1); i <= 4; i++ {
			rows.Put(tx, &typedTestRow[ID]{i, fmt.Sprint(i), "vip"})
		}
	})
	for _, reverse := range []bool{false, true} {
		var mark ScanBookmark
		db.Read(func(tx *Tx) {
			opt := ExactScan("vip")
			opt.Reverse = reverse
			c := tx.IndexScan(group, opt)
			if !c.Next() {
				t.Fatal("empty")
			}
			mark = c.Bookmark()
		})
		key := ID(1)
		if reverse {
			key = 4
		}
		db.Write(func(tx *Tx) { rows.Delete(tx, key) })
		db.Read(func(tx *Tx) {
			scan := rows.Records().bindIndex[string](group).Exact(tx, "vip")
			if reverse {
				scan = scan.Reversed()
			}
			got := slices.Collect(scan.Resume(mark).Keys())
			want := []ID{2, 3, 4}
			if reverse {
				want = []ID{3, 2}
			}
			if !slices.Equal(got, want) {
				t.Fatal(got, want)
			}
			assertPanics(t, func() { rows.Scan(tx).Resume(mark).Raw() })
			assertPanics(t, func() { opt := FullScan(); opt.Reverse = !reverse; tx.IndexScan(group, opt.Resume(mark)) })
			assertPanics(t, func() {
				opt := FullScan()
				opt.Reverse = reverse
				tx.IndexScan(rows.Table().IndexNamed("name"), opt.Resume(mark))
			})
		})
	}
}

func TestScanBookmarkOwnsPosition(t *testing.T) {
	table := new(TableDef)
	raw := []byte("original")
	c := RawTableCursor{table: table, k: raw}
	b := c.Bookmark()
	raw[0] = 'X'
	if b.position != "original" {
		t.Fatal("borrowed bookmark")
	}
}

func TestScanBookmarkNarrowerBounds(t *testing.T) {
	db, rows, _, group := typedTestTable[ID](t, Fast2)
	db.Write(func(tx *Tx) {
		for i := ID(1); i <= 4; i++ {
			rows.Put(tx, &typedTestRow[ID]{i, fmt.Sprint(i), fmt.Sprint(i)})
		}
	})
	for _, reverse := range []bool{false, true} {
		var mark ScanBookmark
		db.Read(func(tx *Tx) {
			opt := FullScan()
			opt.Reverse = reverse
			c := tx.IndexScan(group, opt)
			c.Next()
			mark = c.Bookmark()
		})
		db.Read(func(tx *Tx) {
			scan := rows.Records().bindIndex[string](group).Range(tx, "2", "3")
			if reverse {
				scan = scan.Reversed()
			}
			want := []ID{2, 3}
			if reverse {
				want = []ID{3, 2}
			}
			if got := slices.Collect(scan.Resume(mark).Keys()); !slices.Equal(got, want) {
				t.Fatal(got, want)
			}
		})
	}
}

func TestIndexRangeInterleavedStringKeys(t *testing.T) {
	db, rows, _, group := typedTestTable[string](t, Fast2)
	db.Write(func(tx *Tx) {
		rows.Put(tx, &typedTestRow[string]{"a", "one", "a"})
		rows.Put(tx, &typedTestRow[string]{"z", "two", "aa"})
		rows.Put(tx, &typedTestRow[string]{"b", "three", "a"})
	})
	db.Read(func(tx *Tx) {
		for _, reverse := range []bool{false, true} {
			for _, tt := range []struct {
				lo, hi string
				inc    bool
				want   []string
			}{
				{"a", "b", false, []string{"z"}},
				{"a", "a", true, []string{"a", "b"}},
				{"aa", "b", true, []string{"z"}},
			} {
				opt := RangeScan(tt.lo, tt.hi, tt.inc, tt.inc)
				opt.Reverse = reverse
				want := slices.Clone(tt.want)
				if reverse {
					slices.Reverse(want)
				}
				got := AllKeys[string](tx.IndexScan(group, opt))
				if !slices.Equal(got, want) {
					t.Fatalf("%s..%s reverse=%v: got %v, want %v", tt.lo, tt.hi, reverse, got, want)
				}
			}
		}
	})
}
