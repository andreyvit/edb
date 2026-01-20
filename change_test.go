package edb

import "testing"

func TestChangeFlags_Contains(t *testing.T) {
	f := ChangeFlagNotify | ChangeFlagIncludeKey
	if !f.Contains(ChangeFlagNotify) || !f.ContainsAny(ChangeFlagIncludeKey|ChangeFlagIncludeRow) {
		t.Fatalf("Contains/ContainsAny returned unexpected values for %v", f)
	}
	if f.Contains(ChangeFlagIncludeRow) || f.ContainsAny(0) {
		t.Fatalf("Contains/ContainsAny returned unexpected values for %v", f)
	}

	if OpPut.String() != "put" || OpDelete.String() != "delete" || OpNone.String() != "none" {
		t.Fatalf("unexpected Op.String values")
	}
	if got := Op(999).String(); got == "put" || got == "delete" || got == "none" {
		t.Fatalf("unexpected Op(999).String() = %q", got)
	}
}

func TestTx_OnChange_PutAndDelete(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	db := setup(t, basicSchema)

	var got []*Change
	db.Write(func(tx *Tx) {
		tx.OnChange(map[*Table]ChangeFlags{
			usersTable: ChangeFlagNotify | ChangeFlagIncludeKey | ChangeFlagIncludeRow | ChangeFlagIncludeOldRow,
		}, func(tx *Tx, chg *Change) {
			got = append(got, chg)
		})

		Put(tx, u1)
		u1.Name = "bar"
		Put(tx, u1)
		DeleteByKey[User](tx, ID(1))
	})

	if len(got) != 3 {
		t.Fatalf("got %d changes, wanted 3", len(got))
	}

	if got[0].Table() != usersTable || got[0].Op() != OpPut || !got[0].HasKey() || !got[0].HasRow() || got[0].HasOldRow() {
		t.Fatalf("change[0] fields not set as expected: %+v", got[0])
	}
	_ = got[0].RawKey()
	_ = got[0].KeyVal()
	_ = got[0].Key()
	_ = got[0].RowVal()
	_ = got[0].Row()

	if got[1].Op() != OpPut || !got[1].HasOldRow() {
		t.Fatalf("change[1] fields not set as expected: %+v", got[1])
	}
	_ = got[1].OldRowVal()
	_ = got[1].OldRow()

	if got[2].Op() != OpDelete || !got[2].HasKey() || !got[2].HasRow() {
		t.Fatalf("change[2] fields not set as expected: %+v", got[2])
	}
	_ = got[2].RawKey()
	_ = got[2].Key()
	_ = got[2].Row()
}
