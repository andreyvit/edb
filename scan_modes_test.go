package edb

import (
	"testing"
)

func TestTableScan_EdgeCases(t *testing.T) {
	u1 := &User{ID: 1, Name: "a", Email: "a"}
	u2 := &User{ID: 2, Name: "b", Email: "b"}
	u3 := &User{ID: 3, Name: "c", Email: "c"}
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) { Put(tx, u1, u2, u3) })

	db.Read(func(tx *Tx) {
		assertPanics(t, func() {
			_ = All(TableScan[User](tx, RangeScan(ID(2), nil, false, false)))
		})

		keys := AllKeys[ID](TableScan[User](tx, UpperBoundScan(ID(2), false)).Raw())
		if len(keys) != 1 || keys[0] != 1 {
			t.Fatalf("UpperBoundScan(2, excl) keys = %v, wanted [1]", keys)
		}

		keys = AllKeys[ID](TableScan[User](tx, UpperBoundScan(ID(2), true)).Raw())
		if len(keys) != 2 || keys[0] != 1 || keys[1] != 2 {
			t.Fatalf("UpperBoundScan(2, incl) keys = %v, wanted [1 2]", keys)
		}

		keys = AllKeys[ID](TableScan[User](tx, LowerBoundScan(ID(2), true)).Raw())
		if len(keys) != 2 || keys[0] != 2 || keys[1] != 3 {
			t.Fatalf("LowerBoundScan(2, incl) keys = %v, wanted [2 3]", keys)
		}
	})
}

func TestIndexScan_Range_InclusiveExclusive(t *testing.T) {
	u1 := &User{ID: 1, Name: "n", Email: "a"}
	u2 := &User{ID: 2, Name: "n", Email: "b"}
	u3 := &User{ID: 3, Name: "n", Email: "c"}
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) { Put(tx, u1, u2, u3) })

	db.Read(func(tx *Tx) {
		ids := AllKeys[ID](IndexScan[User](tx, usersByEmail, RangeScan("b", nil, false, false)).Raw())
		if len(ids) != 1 || ids[0] != 3 {
			t.Fatalf("email in (b, +inf) ids = %v, wanted [3]", ids)
		}

		ids = AllKeys[ID](IndexScan[User](tx, usersByEmail, RangeScan("a", "c", true, false)).Raw())
		if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
			t.Fatalf("email in [a, c) ids = %v, wanted [1 2]", ids)
		}

		ids = AllKeys[ID](IndexScan[User](tx, usersByEmail, RangeScan("a", "c", true, false).Reversed()).Raw())
		if len(ids) != 2 || ids[0] != 2 || ids[1] != 1 {
			t.Fatalf("email in [a, c) rev ids = %v, wanted [2 1]", ids)
		}

		ids = AllKeys[ID](IndexScan[User](tx, usersByEmail, RangeScan(nil, "c", false, false).Reversed()).Raw())
		if len(ids) != 2 || ids[0] != 2 || ids[1] != 1 {
			t.Fatalf("email in (-inf, c) rev ids = %v, wanted [2 1]", ids)
		}
	})
}

func TestIndexScan_ExactIDRangeScan_UniquePanics(t *testing.T) {
	db := setup(t, basicSchema)
	db.Read(func(tx *Tx) {
		assertPanics(t, func() {
			_ = IndexScan[User](tx, usersByEmail, ExactIDRangeScan("a", ID(1), ID(2), true, true))
		})
	})
}

func assertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic")
		}
	}()
	fn()
}

