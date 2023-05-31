package edb

import (
	"os"
	"reflect"
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
)

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
