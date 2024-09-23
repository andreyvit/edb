package edb

import (
	"encoding/hex"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

type (
	ID uint64

	User struct {
		ID    ID     `msgpack:"-"`
		Email string `msgpack:"e"`
		Name  string `msgpack:"n"`
	}

	AB struct {
		A int `msgpack:"a"`
		B int `msgpack:"b"`
	}
	CD struct {
		C int
		D int
	}
	Widget struct {
		Key   AB     `msgpack:"-"`
		Name  string `msgpack:"n"`
		Email string `msgpack:"e"`
	}
	Post struct {
		ID      string    `msgpack:"-"`
		Time    time.Time `msgpack:"tm"`
		Content string    `msgpack:"c"`
	}
)

var (
	basicSchema = &Schema{}
	usersTable  = AddTable(basicSchema, "Users", 1, func(row *User, ib *IndexBuilder) {
		ib.Add(usersByEmail, row.Email)
		if row.Name != "" {
			ib.Add(usersByName, row.Name)
		}
	}, nil, []*Index{usersByEmail, usersByName})
	usersByEmail = AddIndex[string]("Email").Unique()
	usersByName  = AddIndex[string]("Name")
	widgetsTable = AddTable(basicSchema, "Widgets", 1, func(row *Widget, ib *IndexBuilder) {
		ib.Add(widgetsByCD, CD{len(row.Name), row.Key.B})
		ib.Add(widgetsByAB, row.Key)
	}, nil, []*Index{widgetsByCD, widgetsByAB})
	widgetsByCD = AddIndex[CD]("by_CD")
	widgetsByAB = AddIndex[AB]("by_AB").Unique()
	booTable    = AddTable[Post](basicSchema, "posts", 1, nil, nil, nil)
	kubets      = DefineKVTable(basicSchema, "kubets", nil, nil, nil)
)

func init() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
}

func TestDB(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	u2 := &User{ID: 2, Name: "bar", Email: "bar@example.com"}

	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u1)
		Put(tx, u2)
	})

	db.Read(func(tx *Tx) {
		deepEqual(t, Get[User](tx, 1), u1)
	})
	db.Read(func(tx *Tx) {
		deepEqual(t, Lookup[User](tx, usersByEmail, "foo@example.com"), u1)
	})
	db.Read(func(tx *Tx) {
		deepEqual(t, Lookup[User](tx, usersByName, "foo"), u1)

		isnil(t, Lookup[User](tx, usersByName, "fo"))
		isnil(t, Lookup[User](tx, usersByName, "f"))
		isnil(t, Lookup[User](tx, usersByName, ""))
		isnil(t, Lookup[User](tx, usersByName, "fox"))
	})
	db.Read(func(tx *Tx) {
		deepEqual(t, AllTableRows[User](tx), []*User{u1, u2})
	})
	db.Write(func(tx *Tx) {
		DeleteRow(tx, u1)
		isnil(t, Lookup[User](tx, usersByEmail, "foo@example.com"))
	})
}

func TestDBScan(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}
	u2 := &User{ID: 2, Name: "bubble", Email: "bubble@example.com"}
	u3 := &User{ID: 3, Name: "bar", Email: "bar@example.com"}
	u4 := &User{ID: 4, Name: "bar", Email: "bar2@example.com"}
	u5 := &User{ID: 5, Name: "bar", Email: "bar3@example.com"}

	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u1)
		Put(tx, u2)
		Put(tx, u3)
		Put(tx, u4)
		Put(tx, u5)
	})

	db.Read(func(tx *Tx) {
		rows := All(IndexScan[User](tx, usersByName, FullScan()))
		deepEqual(t, rows, []*User{u3, u4, u5, u2, u1})

		rows = All(IndexScan[User](tx, usersByName, FullScan().Reversed()))
		deepEqual(t, rows, []*User{u1, u2, u5, u4, u3})

		rows = All(IndexScan[User](tx, usersByName, ExactScan("bar")))
		deepEqual(t, rows, []*User{u3, u4, u5})

		rows = All(IndexScan[User](tx, usersByName, ExactScan("bar").Reversed()))
		deepEqual(t, rows, []*User{u5, u4, u3})

		rows = All(IndexScan[User](tx, usersByName, ExactScan("foo")))
		deepEqual(t, rows, []*User{u1})
		rows = All(IndexScan[User](tx, usersByName, ExactScan("bubble")))
		deepEqual(t, rows, []*User{u2})
		isempty(t, All(IndexScan[User](tx, usersByName, ExactScan("ba"))))
		isempty(t, All(IndexScan[User](tx, usersByName, ExactScan("xxx"))))
		isempty(t, All(IndexScan[User](tx, usersByName, ExactScan("a"))))
		isempty(t, All(IndexScan[User](tx, usersByName, ExactScan(""))))

		rows = All(IndexScan[User](tx, usersByEmail, ExactScan("bar2@example.com")))
		deepEqual(t, rows, []*User{u4})

		rows = All(IndexScan[User](tx, usersByEmail, ExactScan("bar2@example.com").Reversed()))
		deepEqual(t, rows, []*User{u4})

		rows = All(TableScan[User](tx, FullScan().Reversed()))
		deepEqual(t, rows, []*User{u5, u4, u3, u2, u1})

		rows = All(TableScan[User](tx, RangeScan(u2.ID, u4.ID, true, true)))
		deepEqual(t, rows, []*User{u2, u3, u4})

		rows = All(TableScan[User](tx, RangeScan(u2.ID, u4.ID, true, false)))
		deepEqual(t, rows, []*User{u2, u3})

		rows = All(TableScan[User](tx, RangeScan(u2.ID, ID(8), true, true)))
		deepEqual(t, rows, []*User{u2, u3, u4, u5})

		rows = All(TableScan[User](tx, RangeScan(u2.ID, nil, true, true)))
		deepEqual(t, rows, []*User{u2, u3, u4, u5})

		rows = All(TableScan[User](tx, RangeScan(nil, u4.ID, true, false)))
		deepEqual(t, rows, []*User{u1, u2, u3})

		rows = All(IndexScan[User](tx, usersByName, RangeScan("ba", "bo", true, true)))
		deepEqual(t, rows, []*User{u3, u4, u5})

		rows = All(IndexScan[User](tx, usersByName, RangeScan("bar", "bar", true, true)))
		deepEqual(t, rows, []*User{u3, u4, u5})

		rows = All(IndexScan[User](tx, usersByName, RangeScan("foo", "foo", true, true)))
		deepEqual(t, rows, []*User{u1})

		rows = All(IndexScan[User](tx, usersByName, RangeScan("bubble", "bubble", true, true)))
		deepEqual(t, rows, []*User{u2})

		isempty(t, All(IndexScan[User](tx, usersByName, RangeScan("bubbl", "bubbl", true, true))))
		isempty(t, All(IndexScan[User](tx, usersByName, RangeScan("a", "a", true, true))))
		isempty(t, All(IndexScan[User](tx, usersByName, RangeScan("b", "b", true, true))))

		rows = All(IndexScan[User](tx, usersByName, RangeScan("b", "bz", true, true)))
		deepEqual(t, rows, []*User{u3, u4, u5, u2})

		rows = All(IndexScan[User](tx, usersByName, RangeScan("ba", "zo", true, true)))
		deepEqual(t, rows, []*User{u3, u4, u5, u2, u1})

		rows = All(IndexScan[User](tx, usersByName, RangeScan("ba", "bubble", true, true)))
		deepEqual(t, rows, []*User{u3, u4, u5, u2})

		rows = All(IndexScan[User](tx, usersByName, RangeScan("ba", "bubble", true, true).Reversed()))
		deepEqual(t, rows, []*User{u2, u5, u4, u3})

		rows = All(IndexScan[User](tx, usersByName, RangeScan("ba", "bubble", true, false)))
		deepEqual(t, rows, []*User{u3, u4, u5})

		rows = All(IndexScan[User](tx, usersByName, RangeScan("bar", "bubble", false, true)))
		deepEqual(t, rows, []*User{u2})

		rows = All(IndexScan[User](tx, usersByName, RangeScan("bar", "bubble", false, true).Reversed()))
		deepEqual(t, rows, []*User{u2})

		rows = All(IndexScan[User](tx, usersByName, RangeScan(nil, "bubble", false, true).Reversed()))
		deepEqual(t, rows, []*User{u2, u5, u4, u3})

		rows = All(IndexScan[User](tx, usersByName, RangeScan(nil, "bubble", false, false).Reversed()))
		deepEqual(t, rows, []*User{u5, u4, u3})
	})
}

func TestDBCompositeKey(t *testing.T) {
	u1 := &Widget{Key: AB{1, 43}, Name: "foo", Email: "foo@example.com"}
	u2 := &Widget{Key: AB{1, 42}, Name: "bubble", Email: "bubble@example.com"}
	u3 := &Widget{Key: AB{3, 11}, Name: "bar", Email: "bar@example.com"}
	u4 := &Widget{Key: AB{2, 11}, Name: "bar", Email: "bar2@example.com"}
	u5 := &Widget{Key: AB{2, 12}, Name: "bar", Email: "bar3@example.com"}

	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u1)
		Put(tx, u2)
		Put(tx, u3)
		Put(tx, u4)
		Put(tx, u5)
	})

	db.Read(func(tx *Tx) {
		deepEqual(t, Get[Widget](tx, AB{1, 42}), u2)
		isnil(t, Get[Widget](tx, AB{1, 44}))
		isnil(t, Get[Widget](tx, AB{}))

		rows := All(TableScan[Widget](tx, FullScan()))
		deepEqual(t, rows, []*Widget{u2, u1, u4, u5, u3})

		// CD

		rows = All(IndexScan[Widget](tx, widgetsByCD, FullScan()))
		deepEqual(t, rows, []*Widget{u4, u3, u5, u1, u2})

		rows = All(IndexScan[Widget](tx, widgetsByCD, ExactScan(CD{3, 11})))
		deepEqual(t, rows, []*Widget{u4, u3})
		rows = All(IndexScan[Widget](tx, widgetsByCD, ExactScan(CD{6, 42})))
		deepEqual(t, rows, []*Widget{u2})
		isempty(t, All(IndexScan[Widget](tx, widgetsByCD, ExactScan(CD{6, 44}))))
		isempty(t, All(IndexScan[Widget](tx, widgetsByCD, ExactScan(CD{6, 0}))))

		rows = All(IndexScan[Widget](tx, widgetsByCD, ExactScan(CD{3, 0}).Prefix(1)))
		isempty(t, All(IndexScan[Widget](tx, widgetsByCD, ExactScan(CD{2, 0}).Prefix(1))))

		// AB

		rows = All(IndexScan[Widget](tx, widgetsByAB, FullScan()))
		deepEqual(t, rows, []*Widget{u2, u1, u4, u5, u3})

		rows = All(IndexScan[Widget](tx, widgetsByAB, ExactScan(AB{2, 0}).Prefix(1)))
		deepEqual(t, rows, []*Widget{u4, u5})

		// rows = All(IndexScan[User](tx, usersByName, FullScan().Reversed()))
		// deepEqual(t, rows, []*Widget{u1, u2, u5, u4, u3})

		// rows = All(IndexScan[User](tx, usersByName, ExactScan("bar")))
		// deepEqual(t, rows, []*Widget{u3, u4, u5})

		// rows = All(IndexScan[User](tx, usersByName, ExactScan("bar").Reversed()))
		// deepEqual(t, rows, []*User{u5, u4, u3})

		// rows = All(IndexScan[User](tx, usersByName, ExactScan("foo")))
		// deepEqual(t, rows, []*User{u1})
		// rows = All(IndexScan[User](tx, usersByName, ExactScan("bubble")))
		// deepEqual(t, rows, []*User{u2})
		// isempty(t, All(IndexScan[User](tx, usersByName, ExactScan("ba"))))
		// isempty(t, All(IndexScan[User](tx, usersByName, ExactScan("xxx"))))
		// isempty(t, All(IndexScan[User](tx, usersByName, ExactScan("a"))))
		// isempty(t, All(IndexScan[User](tx, usersByName, ExactScan(""))))

		// rows = All(IndexScan[User](tx, usersByEmail, ExactScan("bar2@example.com")))
		// deepEqual(t, rows, []*User{u4})

		// rows = All(IndexScan[User](tx, usersByEmail, ExactScan("bar2@example.com").Reversed()))
		// deepEqual(t, rows, []*User{u4})
	})
}

func TestDBReverseScanBug(t *testing.T) {
	u3 := &User{ID: 3, Name: "bar", Email: "bar@example.com"}
	u4 := &User{ID: 4, Name: "bar", Email: "bar2@example.com"}
	u5 := &User{ID: 5, Name: "bar", Email: "bar3@example.com"}

	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u3)
		Put(tx, u4)
		Put(tx, u5)
	})

	db.Read(func(tx *Tx) {
		rows := All(IndexScan[User](tx, usersByName, ExactScan("bar").Reversed()))
		deepEqual(t, rows, []*User{u5, u4, u3})
	})
}

func TestUpdateIndexValue(t *testing.T) {
	u1 := &User{ID: 1, Name: "foo", Email: "foo@example.com"}

	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		Put(tx, u1)
		isnonnil(t, Lookup[User](tx, usersByName, "foo"))
		isnil(t, Lookup[User](tx, usersByName, "bar"))

		u1.Name = "bar"
		Put(tx, u1)
		isnonnil(t, Lookup[User](tx, usersByName, "bar"))
		isnil(t, Lookup[User](tx, usersByName, "foo"))
	})
}

func TestRawScan(t *testing.T) {
	var (
		kb = x("10 12 14 40 44 47")
		k1 = x("10 12 14 40 44 48")
		k2 = x("10 12 14 40 44 49")
		k3 = x("10 12 14 40 44 50")
		k4 = x("10 12 14 40 44 51")
		ke = x("10 12 14 40 44 52")
		p  = x("10 12 14")
	)
	db := setup(t, basicSchema)
	db.Write(func(tx *Tx) {
		tx.KVPutRaw(kubets, k1, []byte{})
		tx.KVPutRaw(kubets, k2, []byte{})
		tx.KVPutRaw(kubets, k3, []byte{})
		tx.KVPutRaw(kubets, k4, []byte{})
	})

	o := func(name string, rang RawRange, exp ...[]byte) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			db.Read(func(tx *Tx) {
				slog.Debug(t.Name())
				tableScan(t, tx, kubets, rang, exp...)
			})
		})
	}

	o("prefix", RawRange{Prefix: p}, k1, k2, k3, k4)
	o("prefix reverse", RawRange{Prefix: p, Reverse: true}, k4, k3, k2, k1)

	o("prefix + lower inc", RawRange{Prefix: p, Lower: k2, LowerInc: true}, k2, k3, k4)
	o("prefix + lower exc", RawRange{Prefix: p, Lower: k2, LowerInc: false}, k3, k4)
	o("prefix + lower inc reverse", RawRange{Prefix: p, Lower: k2, LowerInc: true, Reverse: true}, k4, k3, k2)
	o("prefix + lower exc reverse", RawRange{Prefix: p, Lower: k2, LowerInc: false, Reverse: true}, k4, k3)
	o("prefix + upper inc", RawRange{Prefix: p, Upper: k3, UpperInc: true}, k1, k2, k3)
	o("prefix + upper exc", RawRange{Prefix: p, Upper: k3, UpperInc: false}, k1, k2)
	o("prefix + upper inc reverse", RawRange{Prefix: p, Upper: k3, UpperInc: true, Reverse: true}, k3, k2, k1)
	o("prefix + upper exc reverse", RawRange{Prefix: p, Upper: k3, UpperInc: false, Reverse: true}, k2, k1)

	o("lower inc", RawRange{Lower: k2, LowerInc: true}, k2, k3, k4)
	o("lower exc", RawRange{Lower: k2, LowerInc: false}, k3, k4)
	o("lower inc reverse", RawRange{Lower: k2, LowerInc: true, Reverse: true}, k4, k3, k2)
	o("lower exc reverse", RawRange{Lower: k2, LowerInc: false, Reverse: true}, k4, k3)

	o("upper inc", RawRange{Upper: k3, UpperInc: true}, k1, k2, k3)
	o("upper exc", RawRange{Upper: k3, UpperInc: false}, k1, k2)
	o("upper inc reverse", RawRange{Upper: k3, UpperInc: true, Reverse: true}, k3, k2, k1)
	o("upper exc reverse", RawRange{Upper: k3, UpperInc: false, Reverse: true}, k2, k1)

	o("first lower inc", RawRange{Lower: kb, LowerInc: true}, k1, k2, k3, k4)
	o("first lower exc", RawRange{Lower: kb, LowerInc: false}, k1, k2, k3, k4)
	o("first lower inc reverse", RawRange{Lower: kb, LowerInc: true, Reverse: true}, k4, k3, k2, k1)
	o("first lower exc reverse", RawRange{Lower: kb, LowerInc: false, Reverse: true}, k4, k3, k2, k1)

	o("last upper inc", RawRange{Upper: ke, UpperInc: true}, k1, k2, k3, k4)
	o("last upper exc", RawRange{Upper: ke, UpperInc: false}, k1, k2, k3, k4)
	o("last upper inc reverse", RawRange{Upper: ke, UpperInc: true, Reverse: true}, k4, k3, k2, k1)
	o("last upper exc reverse", RawRange{Upper: ke, UpperInc: false, Reverse: true}, k4, k3, k2, k1)
}

func setup(t testing.TB, schema *Schema) *DB {
	t.Helper()

	dbFile := must(os.CreateTemp("", "db_test_*.db"))
	t.Logf("DB: %s", dbFile.Name())
	dbFile.Close()

	db := must(Open(dbFile.Name(), schema, Options{
		IsTesting: true,
	}))
	t.Cleanup(db.Close)
	return db
}

func deepEqual[T any](t testing.TB, a, e T) {
	if !reflect.DeepEqual(a, e) {
		t.Helper()
		t.Errorf("** got %v, wanted %v", a, e)
	}
}

func isempty[T any, S ~[]T](t testing.TB, a S) {
	if len(a) > 0 {
		t.Helper()
		t.Errorf("** got %v, wanted empty slice", a)
	}
}

func isnil[T any, P ~*T](t testing.TB, a P) {
	if a != nil {
		t.Helper()
		t.Errorf("** got &%v, wanted nil", *a)
	}
}

func isnonnil[T any](t testing.TB, a *T) {
	if a == nil {
		t.Helper()
		t.Errorf("** got nil %T, wanted non-nil", a)
	}
}

func x(data string) []byte {
	data = strings.ReplaceAll(data, " ", "")
	return must(hex.DecodeString(data))
}

func tableScan(t testing.TB, tx *Tx, tbl *KVTable, rang RawRange, exp ...[]byte) {
	t.Helper()
	var out []string
	for k := range tx.KVTableScan(tbl, rang).Keys() {
		out = append(out, hex.EncodeToString(k))
	}
	var expstr []string
	for _, k := range exp {
		expstr = append(expstr, hex.EncodeToString(k))
	}
	deepEqual(t, out, expstr)
}
