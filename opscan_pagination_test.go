package edb

import (
	"slices"
	"testing"
)

type paginationCursor struct {
	storageCursor
	steps int
}

func (c *paginationCursor) Next() ([]byte, []byte) { c.steps++; return c.storageCursor.Next() }
func (c *paginationCursor) Prev() ([]byte, []byte) { c.steps++; return c.storageCursor.Prev() }

func TestFixedWidthIndexPaginationWork(t *testing.T) {
	db := setup(t, basicSchema)
	db.Write(TxMetadata{}, func(tx *Tx) {
		for day := 1; day <= 2; day++ {
			for id := 1; id <= 1000; id++ {
				Put(tx, &Widget{Key: AB{id, day}, Name: "x"})
			}
		}
	})

	for _, upperDay := range []int{1, 2} {
		for flags := 0; flags < 8; flags++ {
			db.Read(func(tx *Tx) {
				lowerInc, upperInc, reverse := flags&1 != 0, flags&2 != 0, flags&4 != 0
				opt := RangeScan(CD{1, 1}, CD{1, upperDay}, true, true).Prefix(1).PrimaryKeyRange(AB{200, 1}, AB{800, upperDay}, lowerInc, upperInc)
				opt.Reverse = reverse
				var want []AB
				for day := 1; day <= upperDay; day++ {
					for id := 1; id <= 1000; id++ {
						if day == 1 && (id < 200 || id == 200 && !lowerInc) {
							continue
						}
						if day == upperDay && (id > 800 || id == 800 && !upperInc) {
							continue
						}
						want = append(want, AB{id, day})
					}
				}
				if reverse {
					slices.Reverse(want)
				}
				got := AllKeys[AB](tx.IndexScan(widgetsByCD, opt))
				if !slices.Equal(got, want) {
					t.Fatalf("upperDay=%d flags=%d got %v, want %v", upperDay, flags, got, want)
				}
			})
		}
	}
	for _, reverse := range []bool{false, true} {
		db.Read(func(tx *Tx) {
			day, id := 1, 0
			if reverse {
				day, id = 2, 1001
			}
			total, steps := 0, 0
			for {
				opt := RangeScan(CD{1, day}, CD{1, 2}, true, true).Prefix(1).PrimaryKeyRange(AB{id, day}, nil, false, true)
				if reverse {
					opt = RangeScan(CD{1, 1}, CD{1, day}, true, true).Prefix(1).PrimaryKeyRange(nil, AB{id, day}, true, false).Reversed()
				}
				c := tx.IndexScan(widgetsByCD, opt)
				counted := &paginationCursor{storageCursor: c.icur}
				c.icur = counted
				n := 0
				for n < 100 && c.Next() {
					key := c.Key().(AB)
					position := total
					if reverse {
						position = 1999 - total
					}
					want := AB{position%1000 + 1, position/1000 + 1}
					if key != want {
						t.Fatalf("reverse=%v row %d got %v, want %v", reverse, total, key, want)
					}
					total++
					n++
					day, id = key.B, key.A
				}
				steps += counted.steps
				if n == 0 {
					break
				}
			}
			if total != 2000 {
				t.Fatalf("reverse=%v returned %d, want 2000", reverse, total)
			}
			if steps > 2100 {
				t.Fatalf("reverse=%v cursor advances=%d, want <=2100", reverse, steps)
			}
		})
	}
}
