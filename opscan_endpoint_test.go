package edb

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/values"
)

func TestIndexPrimaryKeyEndpointsWithEmbeddedFooter(t *testing.T) {
	type row struct {
		ID    string `msgpack:"-"`
		Name  string
		Group uint64
	}
	schema := new(Schema)
	var fixed, variable *IndexDef
	table := DefineTable(schema, "rows", testIndexAccess[row](indexName, indexGroupNumber), func(b *TableBuilder[string]) {
		fixed = b.AddIndex[uint64]("fixed", func(_ *Tx, _ values.Value, r kvo.Record, yield func(uint64)) { yield(r.Get(indexGroupNumber)) }).IndexDef()
		variable = b.AddIndex[string]("variable", func(_ *Tx, _ values.Value, r kvo.Record, yield func(string)) { yield(r.Get(indexName)) }).IndexDef()
	}).TableDef()
	db := setup(t, schema)
	keys := []string{"a", "a\x01\x00", "a\x01\x02", "a\x01\x08", "b", "z"}
	db.Write(TxMetadata{}, func(tx *Tx) {
		for _, key := range keys {
			Put(tx, &row{key, "b", 1})
		}
	})
	for _, tt := range []struct {
		name string
		idx  *IndexDef
		opt  ScanOptions
	}{
		{"fixed exact", fixed, ExactScan(uint64(1))},
		{"fixed range", fixed, RangeScan(uint64(1), uint64(1), true, true)},
		{"variable exact", variable, ExactScan("b")},
		{"variable range", variable, RangeScan("b", "b", true, true)},
	} {
		for _, lower := range keys {
			for _, upper := range keys {
				for flags := 0; flags < 8; flags++ {
					t.Run(fmt.Sprintf("%s/%q-%q/%d", tt.name, lower, upper, flags), func(t *testing.T) {
						db.Read(func(tx *Tx) {
							loInc, hiInc, reverse := flags&1 != 0, flags&2 != 0, flags&4 != 0
							opt := tt.opt.PrimaryKeyRange(lower, upper, loInc, hiInc)
							opt.Reverse = reverse
							all := FullScan()
							all.Reverse = reverse
							lo, hi := table.EncodeKey(lower), table.EncodeKey(upper)
							var want []string
							for c := tx.IndexScan(tt.idx, all); c.Next(); {
								key := c.RawKey()
								l, h := bytes.Compare(key, lo), bytes.Compare(key, hi)
								if (l > 0 || l == 0 && loInc) && (h < 0 || h == 0 && hiInc) {
									want = append(want, c.Key().(string))
								}
							}
							if got := AllKeys[string](tx.IndexScan(tt.idx, opt)); !slices.Equal(got, want) {
								t.Fatalf("got %q, want %q", got, want)
							}
						})
					})
				}
			}
		}
	}
}

func TestEqualVariableIndexRangePaginationWork(t *testing.T) {
	db, rows, _, idx := typedTestTable[ID](t, MessagePack)
	db.Write(TxMetadata{}, func(tx *Tx) {
		for id := ID(1); id <= 1000; id++ {
			rows.Put(tx, &typedTestRow[ID]{id, fmt.Sprint(id), "b"})
		}
		rows.Put(tx, &typedTestRow[ID]{1001, "collision", "b\x00"})
	})
	db.Read(func(tx *Tx) {
		opt := RangeScan("b", "b", true, true).PrimaryKeyRange(ID(1), nil, true, true)
		opt.PrefixValue, opt.PrefixEls = reflect.ValueOf("different"), 1
		if got := AllKeys[ID](tx.IndexScan(idx, opt)); len(got) != 0 {
			t.Fatal(got)
		}
	})
	for _, reverse := range []bool{false, true} {
		after := ID(0)
		if reverse {
			after = 1001
		}
		total, steps := 0, 0
		for {
			n := 0
			db.Read(func(tx *Tx) {
				opt := RangeScan("b", "b", true, true)
				if after != 0 {
					opt = opt.PrimaryKeyRange(after, nil, false, true)
				}
				if reverse {
					opt = RangeScan("b", "b", true, true).PrimaryKeyRange(nil, after, true, false).Reversed()
				}
				c := tx.IndexScan(idx, opt)
				counted := &paginationCursor{storageCursor: c.icur}
				c.icur = counted
				for n < 100 && c.Next() {
					want := ID(total + 1)
					if reverse {
						want = 1000 - ID(total)
					}
					after = c.Key().(ID)
					if after != want {
						t.Fatalf("reverse=%v row=%d got %d, want %d", reverse, total, after, want)
					}
					n++
					total++
				}
				steps += counted.steps
			})
			if n == 0 {
				break
			}
		}
		if total != 1000 || steps > 1030 {
			t.Fatalf("reverse=%v rows=%d advances=%d, want 1000 rows and <=1030 advances", reverse, total, steps)
		}
		t.Logf("reverse=%v rows=%d advances=%d", reverse, total, steps)
	}
}

func TestVariablePrimaryKeyEndpointSeekWork(t *testing.T) {
	db, rows, _, idx := typedTestTable[string](t, MessagePack)
	db.Write(TxMetadata{}, func(tx *Tx) {
		for id := 1; id <= 1000; id++ {
			key := fmt.Sprintf("%08d", id)
			rows.Put(tx, &typedTestRow[string]{key, key, "b"})
		}
	})
	for _, method := range []ScanMethod{ScanMethodExact, ScanMethodRange} {
		for _, reverse := range []bool{false, true} {
			after := ""
			total, steps := 0, 0
			for {
				n := 0
				db.Read(func(tx *Tx) {
					opt := RangeScan("b", "b", true, true)
					if method == ScanMethodExact {
						opt = ExactScan("b")
					}
					opt.Reverse = reverse
					if after != "" {
						if reverse {
							opt = opt.PrimaryKeyRange(nil, after, true, false)
						} else {
							opt = opt.PrimaryKeyRange(after, nil, false, true)
						}
					}
					c := tx.IndexScan(idx, opt)
					counted := &paginationCursor{storageCursor: c.icur}
					c.icur = counted
					for n < 100 && c.Next() {
						after = c.Key().(string)
						expected := total + 1
						if reverse {
							expected = 1000 - total
						}
						if want := fmt.Sprintf("%08d", expected); after != want {
							t.Fatalf("method=%v reverse=%v row=%d got %q, want %q", method, reverse, total, after, want)
						}
						n++
						total++
					}
					steps += counted.steps
				})
				if n == 0 {
					break
				}
			}
			if total != 1000 || steps > 1020 {
				t.Fatalf("method=%v reverse=%v rows=%d advances=%d, want 1000 rows and <=1020 advances", method, reverse, total, steps)
			}
			t.Logf("method=%v reverse=%v rows=%d advances=%d", method, reverse, total, steps)
		}
	}
}

func TestVariablePrimaryKeyUpperEnvelopeDoesNotVisitSuccessorPrefix(t *testing.T) {
	db, rows, _, idx := typedTestTable[string](t, MessagePack)
	db.Write(TxMetadata{}, func(tx *Tx) {
		rows.Put(tx, &typedTestRow[string]{"a", "wanted", "b"})
		for i := range 1000 {
			key := fmt.Sprintf("a\x02%04d", i)
			rows.Put(tx, &typedTestRow[string]{key, key, "b"})
		}
	})
	for _, reverse := range []bool{false, true} {
		db.Read(func(tx *Tx) {
			opt := ExactScan("b").PrimaryKeyRange(nil, "a", true, true)
			opt.Reverse = reverse
			c := tx.IndexScan(idx, opt)
			counted := &paginationCursor{storageCursor: c.icur}
			c.icur = counted
			if got := AllKeys[string](c); !slices.Equal(got, []string{"a"}) {
				t.Fatalf("reverse=%v got %q", reverse, got)
			}
			if counted.steps > 2 {
				t.Fatalf("reverse=%v advances=%d, want <=2", reverse, counted.steps)
			}
		})
	}
}

func TestPrimaryKeyUpperEnvelopeMultibyteCount(t *testing.T) {
	enc := &flatEncoding{components: make([]*flatComponent, 128)}
	for _, tt := range []struct {
		upper []byte
		want  []byte
	}{
		{[]byte{'a', 1, 0x80}, []byte{'a', 1, 0x80}},
		{[]byte{'a', 1, 0x80, 'b', 1, 0x80}, []byte{'a', 1, 0x80}},
		{[]byte{'a', 0x80, 1, 'b', 1, 0x80}, []byte{'a', 0x80, 1, 'b', 1, 0x80}},
	} {
		if got := primaryKeyUpperEnvelope(enc, tt.upper); !bytes.Equal(got, tt.want) {
			t.Fatalf("upper=%x got %x, want %x", tt.upper, got, tt.want)
		}
	}
}

func TestFixedWidthReverseExclusiveRangeWork(t *testing.T) {
	db := setup(t, basicSchema)
	db.Write(TxMetadata{}, func(tx *Tx) {
		for day := 1; day <= 3; day++ {
			for id := 1; id <= 1000; id++ {
				Put(tx, &Widget{Key: AB{id, day}, Name: "x"})
			}
		}
	})
	var mark ScanBookmark
	total, steps := 0, 0
	for {
		n := 0
		db.Read(func(tx *Tx) {
			opt := RangeScan(CD{1, 1}, CD{1, 3}, true, false).Prefix(1).Reversed().Resume(mark)
			c := tx.IndexScan(widgetsByCD, opt)
			counted := &paginationCursor{storageCursor: c.icur}
			c.icur = counted
			for n < 100 && c.Next() {
				position := 1999 - total
				want := AB{position%1000 + 1, position/1000 + 1}
				if got := c.Key().(AB); got != want {
					t.Fatalf("row=%d got %v, want %v", total, got, want)
				}
				n++
				total++
			}
			mark = c.Bookmark()
			steps += counted.steps
		})
		if n == 0 {
			break
		}
	}
	if total != 2000 || steps > 2030 {
		t.Fatalf("rows=%d advances=%d, want 2000 rows and <=2030 advances", total, steps)
	}
}

func TestStorageSeekBeforeAndResume(t *testing.T) {
	db, rows, _, _ := typedTestTable[ID](t, MessagePack)
	db.Read(func(tx *Tx) {
		c := tx.TableScan(rows.Table(), FullScan()).dcur
		if key, _ := c.SeekBefore(rows.Table().EncodeKey(ID(3))); key != nil {
			t.Fatal(key)
		}
	})
	db.Write(TxMetadata{}, func(tx *Tx) {
		for _, key := range []ID{1, 3, 5} {
			rows.Put(tx, &typedTestRow[ID]{key, fmt.Sprint(key), "b"})
		}
	})
	for _, deleted := range []bool{false, true} {
		if deleted {
			db.Write(TxMetadata{}, func(tx *Tx) { rows.Delete(tx, 3) })
		}
		db.Read(func(tx *Tx) {
			for _, tt := range []struct {
				bound, after, want ID
			}{
				{0, 0, 0}, {1, 0, 0}, {2, 0, 1}, {3, 0, 1}, {6, 0, 5},
				{6, 3, 1}, {6, 4, 3}, {2, 4, 1}, {1, 4, 0},
			} {
				c := tx.TableScan(rows.Table(), FullScan()).dcur
				if tt.after != 0 {
					c = &resumeStorageCursor{c, string(rows.Table().EncodeKey(tt.after))}
				}
				want := tt.want
				if deleted && want == 3 {
					want = 1
				}
				key, _ := c.SeekBefore(rows.Table().EncodeKey(tt.bound))
				var got ID
				if key != nil {
					got = decodeTypedKey[ID](rows.Table(), key)
				}
				if got != want {
					t.Fatalf("deleted=%v bound=%d after=%d got %d, want %d", deleted, tt.bound, tt.after, got, want)
				}
			}
		})
	}
}

func TestVariablePrimaryKeyBoundsAcrossFixedIndexValues(t *testing.T) {
	type row struct {
		ID    string `msgpack:"-"`
		Group uint64
	}
	schema := new(Schema)
	var idx *IndexDef
	table := DefineTable(schema, "rows", testIndexAccess[row](indexGroupNumber), func(b *TableBuilder[string]) {
		idx = b.AddIndex[uint64]("group", func(_ *Tx, _ values.Value, r kvo.Record, yield func(uint64)) { yield(r.Get(indexGroupNumber)) }).IndexDef()
	}).TableDef()
	db := setup(t, schema)
	db.Write(TxMetadata{}, func(tx *Tx) {
		for _, r := range []row{{"a", 1}, {"a\x01\x00", 1}, {"z", 1}, {"b", 2}, {"b\x01\x00", 2}, {"y", 2}, {"outside", 3}} {
			Put(tx, &r)
		}
	})
	for _, deletedUpper := range []bool{false, true} {
		if deletedUpper {
			db.Write(TxMetadata{}, func(tx *Tx) { table.Rows[row, string]().Delete(tx, "b") })
		}
		for flags := 0; flags < 8; flags++ {
			db.Read(func(tx *Tx) {
				loInc, hiInc, reverse := flags&1 != 0, flags&2 != 0, flags&4 != 0
				opt := RangeScan(uint64(1), uint64(2), true, true).PrimaryKeyRange("a", "b", loInc, hiInc)
				opt.Reverse = reverse
				all := FullScan()
				all.Reverse = reverse
				lo, hi := table.EncodeKey("a"), table.EncodeKey("b")
				var want []string
				for c := tx.IndexScan(idx, all); c.Next(); {
					value, _ := c.Row()
					r := value.(*row)
					l, h := bytes.Compare(c.RawKey(), lo), bytes.Compare(c.RawKey(), hi)
					if r.Group == 1 && (l > 0 || l == 0 && loInc) || r.Group == 2 && (h < 0 || h == 0 && hiInc) {
						want = append(want, r.ID)
					}
				}
				if got := AllKeys[string](tx.IndexScan(idx, opt)); !slices.Equal(got, want) {
					t.Fatalf("deletedUpper=%v flags=%d got %q, want %q", deletedUpper, flags, got, want)
				}
			})
		}
	}
}

func TestVariableIndexRangeBookmarkWork(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		for _, move := range []bool{false, true} {
			t.Run(fmt.Sprintf("reverse=%v/move=%v", reverse, move), func(t *testing.T) {
				db, rows, _, idx := typedTestTable[string](t, MessagePack)
				db.Write(TxMetadata{}, func(tx *Tx) {
					for id := 1; id <= 1000; id++ {
						key := fmt.Sprintf("%08d", id)
						rows.Put(tx, &typedTestRow[string]{key, key, "b"})
					}
					for id := 1; id <= 20; id++ {
						key := fmt.Sprintf("collision%02d", id)
						rows.Put(tx, &typedTestRow[string]{key, key, fmt.Sprintf("b%08d", id)})
						key = fmt.Sprintf("outside%02d", id)
						rows.Put(tx, &typedTestRow[string]{key, key, "c0"})
					}
				})
				var want []string
				db.Read(func(tx *Tx) {
					opt := FullScan()
					opt.Reverse = reverse
					for c := tx.IndexScan(idx, opt); c.Next(); {
						row, _ := c.Row()
						group := row.(*typedTestRow[string]).Group
						if group >= "b" && group <= "c" {
							want = append(want, c.Key().(string))
						}
					}
				})
				var mark ScanBookmark
				var got []string
				steps := 0
				for {
					n := 0
					last := ""
					db.Read(func(tx *Tx) {
						opt := RangeScan("b", "c", true, true).Resume(mark)
						opt.Reverse = reverse
						c := tx.IndexScan(idx, opt)
						counted := &paginationCursor{storageCursor: c.icur}
						c.icur = counted
						for n < 100 && c.Next() {
							last = c.Key().(string)
							got = append(got, last)
							n++
						}
						if n != 0 {
							mark = c.Bookmark()
						}
						steps += counted.steps
					})
					if n == 0 {
						break
					}
					db.Write(TxMetadata{}, func(tx *Tx) {
						if move {
							row := rows.Get(tx, last)
							row.Group = "z"
							rows.Put(tx, row)
						} else {
							rows.Delete(tx, last)
						}
					})
				}
				if !slices.Equal(got, want) {
					t.Fatalf("got %q, want %q", got, want)
				}
				if len(got) != 1020 || steps > 1060 {
					t.Fatalf("rows=%d advances=%d, want 1020 rows and <=1060 advances", len(got), steps)
				}
				t.Logf("rows=%d advances=%d", len(got), steps)
			})
		}
	}
}
