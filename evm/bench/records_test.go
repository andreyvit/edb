package bench_test

import (
	"context"
	_ "embed"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"

	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/kvo/codec/row2"
	"github.com/andreyvit/edb/values"
)

//go:embed testdata/customers.go
var customerScript string

const (
	customerCount = 512
	idKey         = 1
	activeKey     = 3
	balanceKey    = 5
	rateKey       = 7
	profileKey    = 9
	revisionKey   = 11
)

type recordHost struct {
	db         *edb.DB
	table      *edb.TableDef
	tx         *edb.Tx
	scope      *evm.RecordScope
	cursors    []tableCursor
	editModels map[*values.Type]*kvo.Model
	origins    map[*kvo.Editor]editOrigin
}

type editOrigin struct {
	key  kvo.PropCode
	id   uint64
	edit *edb.RecordEdit
}

type tableCursor struct {
	table  *edb.TableDef
	cursor *edb.RawTableCursor
}

func stored(t *edb.TableDef, tx *edb.Tx, id uint64) kvo.Record {
	v, _, err := tx.TryGetRecord(t, id)
	if err != nil {
		panic(err)
	}
	return v
}
func insert(t *edb.TableDef, tx *edb.Tx, id uint64, v row2.View) {
	if _, err := tx.TryPutRecordChecked(t, id, kvo.Encoded(v), edb.ValueMeta{}); err != nil {
		panic(err)
	}
}
func (h *recordHost) closeTransaction() {
	if h.tx != nil {
		h.scope.Close()
		h.scope = nil
		h.tx.Close()
		h.tx = nil
		h.cursors = nil
		h.origins = nil
	}
}
func newRecordBench(t testing.TB, extra string, rows int) (*recordHost, *evm.VM) {
	return newRecordBenchOn(t, extra, rows, edb.InMemory, edb.Options{}, row2.Fast2)
}

func newRecordBenchOn(t testing.TB, extra string, rows int, path string, opts edb.Options, layout row2.Layout) (*recordHost, *evm.VM) {
	return newRecordFixture(t, extra, rows, path, opts, layout, false)
}
func newRecordFixture(t testing.TB, extra string, rows int, path string, opts edb.Options, layout row2.Layout, legacy bool) (*recordHost, *evm.VM) {
	t.Helper()
	s, cm := customerModel()
	schema := new(edb.Schema)
	format := edb.Compact2
	if layout == row2.Fast2 {
		format = edb.Fast2
	}
	h := &recordHost{table: edb.DefineTable(schema, "customers", cm, func(b *edb.TableBuilder[uint64]) {
		if !legacy {
			b.SetRecordStorage(edb.RecordStorage{Format: format})
		}
	}).TableDef()}
	var err error
	h.db, err = edb.Open(path, schema, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.db.Close() })
	h.db.Write(edb.TxMetadata{}, func(tx *edb.Tx) {
		for id := uint64(1); id <= uint64(rows); id++ {
			if legacy {
				edb.Put(tx, statrow.Sample(id))
			} else {
				insert(h.table, tx, id, customerRow(id, layout))
			}
		}
	})
	pkg := h.guestPackage(t, s)
	m, err := evmcompiler.Compile("customers.go", customerScript+extra, pkg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	return h, evm.New(p)
}

func customerModel() (*kvo.Schema, *kvo.Model) {
	return statrow.Model()
}

func customerRow(id uint64, layout row2.Layout) row2.View {
	var w row2.Writer
	w.Begin(0, "", row2.Object, layout)
	w.Put(idKey, "", values.Uint(id), row2.Word)
	if id%5 != 0 {
		w.Put(activeKey, "", values.Uint(1), row2.Word)
	}
	w.Begin(profileKey, "", row2.Object, layout)
	w.Put(balanceKey, "", values.Uint(100+id%37), row2.Word)
	w.Put(rateKey, "", values.Uint(1+id%9), row2.Word)
	w.End()
	w.End()
	return row2.Trusted(w.BytesWritten())
}

func (h *recordHost) guestPackage(t testing.TB, s *kvo.Schema) evm.Package {
	t.Helper()
	ctx := handleType("m", "Context")
	pkg, err := evm.ModelPackage("m", s)
	if err != nil {
		t.Fatal(err)
	}
	pkg.Types = append(pkg.Types, ctx)
	bindMethod(&pkg, ctx, "BeginWrite", nil, nil, func(*evm.Call) error {
		if h.tx != nil || h.scope == nil {
			return fmt.Errorf("expected one transaction per invocation")
		}
		h.tx = h.db.BeginUpdate(edb.TxMetadata{})
		return nil
	})
	bindMethod(&pkg, ctx, "Commit", nil, nil, func(*evm.Call) error {
		if h.tx == nil || !h.tx.IsWritable() {
			return fmt.Errorf("no write transaction")
		}
		err := h.tx.Commit()
		h.closeTransaction()
		return err
	})
	h.bindPut(&pkg, ctx)
	h.bindTable(&pkg, ctx, "Customers", "Customer", h.table, idKey)
	return pkg
}

func handleType(path, name string) evm.TypeDecl {
	return evm.TypeDecl{Name: name, Shape: &values.Shape{Kind: values.UintKind, Type: values.NewType(path+"."+name, values.Raw), Bits: 64, Cells: 1}}
}
func bindMethod(pkg *evm.Package, recv evm.TypeDecl, name string, params, results []*values.Type, call evm.Native) {
	pkg.Methods = append(pkg.Methods, evm.ExternalMethod{Name: name, Receiver: recv.Shape.Type, Signature: evm.Signature{Params: params, Results: results}, Call: call})
}

// Only editable root types provided by a table implement db.RowEdit.
func (h *recordHost) bindPut(pkg *evm.Package, ctx evm.TypeDecl) {
	marker := &values.Shape{Type: values.NewType("func()", values.Pair), Kind: values.FunctionKind, Cells: 1}
	iface := &values.Shape{Type: values.NewType(pkg.Path+".RowEdit", values.Pair), Kind: values.InterfaceKind, Cells: 1, Methods: []values.Method{{ID: pkg.Path + ".rowEdit", Signature: marker}}}
	pkg.Types = append(pkg.Types, evm.TypeDecl{Name: "RowEdit", Shape: iface})
	h.editModels = make(map[*values.Type]*kvo.Model)
	bindMethod(pkg, ctx, "Put", []*values.Type{values.Uint64, iface.Type}, nil, func(c *evm.Call) error {
		if h.tx == nil || !h.tx.IsWritable() {
			return fmt.Errorf("no write transaction")
		}
		v := c.Argument(2)
		model := h.editModels[c.Program().InterfaceType(v)]
		if model == nil {
			return fmt.Errorf("expected editable table row")
		}
		row, err := h.scope.MutableRecord(v, model)
		if err != nil {
			return err
		}
		origin, ok := h.origins[row]
		if !ok {
			return fmt.Errorf("edit does not belong to this transaction")
		}
		if id := c.Argument(1).R; id != origin.id || id != row.GetValue(origin.key).AsUint() {
			return fmt.Errorf("wrong key")
		}
		_, err = h.tx.TrySaveRecord(origin.edit)
		return err
	})
}

// Each table supplies its schema and primary key. The receiver type pins its
// methods to that table at link time. No runtime dispatch by table name.
func (h *recordHost) bindTable(pkg *evm.Package, ctx evm.TypeDecl, name, rowName string, table *edb.TableDef, primaryKey kvo.PropCode) {

	cursor := handleType(pkg.Path, name+"Cursor")
	pkg.Types = append(pkg.Types, cursor)
	read := pkg.Record(table.Model(), false)
	edit := pkg.Record(table.Model(), true)
	if h.editModels[edit.Pointer] == nil {
		h.editModels[edit.Pointer] = table.Model()
		pkg.Methods = append(pkg.Methods, evm.ExternalMethod{Name: "rowEdit", Receiver: edit.Pointer, Call: func(*evm.Call) error { return nil }})
	}
	suffix := ""
	if rowName != read.Name {
		suffix = rowName
	}
	pkg.Functions = append(pkg.Functions, evm.ExternalFunction{On: read, Name: "Scan" + suffix, Signature: evm.Signature{Params: []*values.Type{ctx.Shape.Type}, Results: []*values.Type{cursor.Shape.Type}}, Call: func(c *evm.Call) error {
		if h.tx == nil {
			return fmt.Errorf("no transaction")
		}
		h.cursors = append(h.cursors, tableCursor{table, h.tx.TableScan(table, edb.FullScan())})
		c.SetResult(0, values.Uint(uint64(len(h.cursors))))
		return nil
	}})
	bindMethod(pkg, cursor, "Next", nil, []*values.Type{read.Pointer}, func(c *evm.Call) error {
		n := c.Argument(0).R
		if n == 0 || n > uint64(len(h.cursors)) {
			return fmt.Errorf("invalid cursor")
		}
		slot := h.cursors[n-1]
		if slot.table != table {
			return fmt.Errorf("cursor belongs to another table")
		}
		cur := slot.cursor
		if cur.Next() {
			v, _, err := cur.TryRecord()
			if err != nil {
				return err
			}
			return c.SetRecordResult(table.Model(), v)
		}
		c.SetResult(0, values.Value{})
		return nil
	})
	pkg.Functions = append(pkg.Functions, evm.ExternalFunction{On: read, Name: "Edit" + suffix, Signature: evm.Signature{Params: []*values.Type{ctx.Shape.Type, values.Uint64}, Results: []*values.Type{edit.Pointer}}, Call: func(c *evm.Call) error {
		if h.tx == nil || !h.tx.IsWritable() {
			return fmt.Errorf("no write transaction")
		}
		id := c.Argument(1).R
		rec, err := h.tx.TryOpenRecordEdit(table, id)
		if err != nil {
			return err
		}
		if rec == nil {
			return fmt.Errorf("%s row missing", table.Name())
		}
		delta := rec.Editor()
		if h.origins == nil {
			h.origins = make(map[*kvo.Editor]editOrigin)
		}
		h.origins[delta] = editOrigin{primaryKey, id, rec}
		c.SetResult(0, h.scope.EditRecord(table.Model(), delta))
		return nil
	}})
}

func (h *recordHost) run(vm *evm.VM, entry string, revision uint64) (uint64, error) {
	if err := vm.Reset(); err != nil {
		return 0, err
	}
	ctx, s := evm.WithRecords(context.Background())
	h.scope = s
	defer s.Close()
	defer h.closeTransaction()
	if entry == "ReadCustomers" {
		h.tx = h.db.BeginRead()
	}
	args := [2]values.Value{{}, values.Uint(revision)}
	var result [1]values.Value
	err := vm.Run(ctx, entry, args[:], result[:])
	return result[0].R, err
}
func recordAnswer(entry string, revision uint64) uint64 {
	var total uint64
	for id := uint64(1); id <= customerCount; id++ {
		if id%5 == 0 {
			continue
		}
		rate := 1 + id%9
		if entry == "ReadCustomers" {
			total += (100+id%37)*rate + id
		} else {
			total += id*rate + revision
		}
	}
	return total
}
func BenchmarkKVO(b *testing.B)     { benchmarkKVO(b, false, row2.Fast2) }
func BenchmarkKVOBolt(b *testing.B) { benchmarkKVO(b, true, row2.Fast2) }

func BenchmarkKVOCompact(b *testing.B)     { benchmarkKVO(b, false, row2.Compact2) }
func BenchmarkKVOCompactBolt(b *testing.B) { benchmarkKVO(b, true, row2.Compact2) }
func benchmarkKVO(b *testing.B, bolt bool, layout row2.Layout) {
	for _, entry := range []string{"ReadCustomers", "WriteCustomers"} {
		b.Run(entry, func(b *testing.B) {
			path, opts := edb.InMemory, edb.Options{}
			if bolt {
				path, opts = filepath.Join(b.TempDir(), "data.db"), edb.Options{IsTesting: true}
			}
			h, vm := newRecordBenchOn(b, "", customerCount, path, opts, layout)
			want := [2]uint64{recordAnswer(entry, 1), recordAnswer(entry, 2)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				got, err := h.run(vm, entry, 1+uint64(i%2))
				if err != nil || got != want[i%2] {
					b.Fatalf("got %d, %v; want %d", got, err, want[i%2])
				}
			}
			b.ReportMetric(customerCount, "rows/op")
		})
	}
}

func TestRecordTransactions(t *testing.T) {
	for _, b := range []struct {
		name         string
		bolt, legacy bool
		layout       row2.Layout
	}{{"FastMemory", false, false, row2.Fast2}, {"FastBolt", true, false, row2.Fast2}, {"CompactMemory", false, false, row2.Compact2}, {"CompactBolt", true, false, row2.Compact2}, {"MessagePackBolt", true, true, row2.Compact2}} {
		t.Run(b.name, func(t *testing.T) {
			path, opts := edb.InMemory, edb.Options{}
			if b.bolt {
				path, opts = filepath.Join(t.TempDir(), "db"), edb.Options{IsTesting: true}
			}
			testRecordTransaction(t, path, opts, b.layout, b.legacy)
		})
	}
}

func testRecordTransaction(t *testing.T, path string, opts edb.Options, layout row2.Layout, legacy bool) {
	t.Helper()
	h, vm := newRecordFixture(t, `
func Fail(ctx m.Context, revision uint64) uint64 {
 return update(ctx,func(tx m.Context)uint64{
  c:=m.Customer.Edit(tx,1);c.profile.balance=98765;tx.Put(1,c);panic("abort")
 })
}`, customerCount, path, opts, layout, legacy)
	for _, entry := range []string{"ReadCustomers", "WriteCustomers", "Fail"} {
		got, err := h.run(vm, entry, 17)
		if entry == "Fail" {
			if err == nil {
				t.Fatal("write should abort")
			}
		} else if err != nil || got != recordAnswer(entry, 17) {
			t.Fatalf("%s: %d, %v", entry, got, err)
		}
	}
	h.db.Read(func(tx *edb.Tx) {
		for id := uint64(1); id <= customerCount; id++ {
			row := stored(h.table, tx, id)
			balance := 100 + id%37
			revision := uint64(0)
			if id%5 != 0 {
				balance = id*(1+id%9) + 17
				revision = 17
			}
			if row.GetValue(revisionKey).AsUint() != revision || row.Child(row2.Step{Key: profileKey}).GetValue(balanceKey).AsUint() != balance {
				t.Fatalf("wrong persisted state for row %d", id)
			}
		}
	})
}

func TestTableBindings(t *testing.T) {
	schema := new(edb.Schema)
	models := kvo.NewSchema()
	id := kvo.NewProp(models, 1, "id", kvo.TUint64, nil)
	type account struct {
		Key     uint64 `edb:"-"`
		ID      uint64 `edb:"id"`
		Balance uint64 `edb:"balance"`
	}
	type archivedAccount account
	type order struct {
		Key      uint64 `edb:"-"`
		ID       uint64 `edb:"id"`
		Quantity uint64 `edb:"quantity"`
	}
	define := func(entity string, code uint64, property string, types ...reflect.Type) *kvo.Model {
		value := kvo.NewProp(models, code, property, kvo.TUint64, nil)
		return kvo.NewModel(models, kvo.NewEntityType(models, entity, types...), func(b *kvo.ModelBuilder) { b.Prop(models.MustPropByCode(id)); b.Prop(models.MustPropByCode(value)) })
	}
	am := define("account", 2, "balance", reflect.TypeFor[account]())
	archivedModel := kvo.NewModel(models, kvo.NewEntityType(models, "archived_account", reflect.TypeFor[archivedAccount]()), func(b *kvo.ModelBuilder) { b.Prop(models.MustPropByCode(id)); b.Prop(models.MustPropByCode(2)) })
	om := define("order", 3, "quantity", reflect.TypeFor[order]())
	accounts := edb.DefineTable(schema, "accounts", am, func(b *edb.TableBuilder[uint64]) {
		b.SetRecordStorage(edb.RecordStorage{Format: edb.Fast2})
	}).TableDef()
	archived := edb.DefineTable(schema, "archived_accounts", archivedModel, func(b *edb.TableBuilder[uint64]) {
		b.SetRecordStorage(edb.RecordStorage{Format: edb.Fast2})
	}).TableDef()
	orders := edb.DefineTable(schema, "orders", om, func(b *edb.TableBuilder[uint64]) {
		b.SetRecordStorage(edb.RecordStorage{Format: edb.Fast2})
	}).TableDef()
	ctxType := handleType("m", "Context")
	pkg, err := evm.ModelPackage("m", models)
	if err != nil {
		t.Fatal(err)
	}
	pkg.Types = append(pkg.Types, ctxType)
	var h recordHost
	h.bindPut(&pkg, ctxType)
	h.bindTable(&pkg, ctxType, "Accounts", "Account", accounts, id)
	h.bindTable(&pkg, ctxType, "Archived", "ArchivedAccount", archived, id)
	h.bindTable(&pkg, ctxType, "Orders", "Order", orders, id)

	const header = `import "m"; func Apply(tx m.Context){`
	if _, err := evmcompiler.Compile("bad.go", header+`tx.Put(1,m.Account.Scan(tx).Next())}`, pkg); err == nil {
		t.Fatal("accepted read-only row in tx.Put")
	}
	m, err := evmcompiler.Compile("test.go", header+`
 a:=m.Account.Edit(tx,1)
 q:=m.Order.Scan(tx).Next().quantity
 a.balance+=q
 arch:=m.ArchivedAccount.Edit(tx,1)
 arch.balance=93
 tx.Put(1,arch)
 tx.Put(1,a)
 }`, pkg)
	if err != nil {
		t.Fatal(err)
	}
	program, err := evm.Link(m, pkg)
	if err != nil {
		t.Fatal(err)
	}
	db, err := edb.Open(edb.InMemory, schema, edb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Write(edb.TxMetadata{}, func(tx *edb.Tx) {
		for _, table := range []*edb.TableDef{accounts, orders, archived} {
			var w row2.Writer
			w.Begin(0, "", row2.Object, row2.Fast2)
			w.Put(id, "", values.Uint(1), row2.Word)
			if table == orders {
				w.Put(3, "", values.Uint(7), row2.Word)
			}
			w.End()
			insert(table, tx, 1, row2.Trusted(w.BytesWritten()))
		}
		ctx, scope := evm.WithRecords(context.Background())
		defer scope.Close()
		h.tx, h.scope = tx, scope
		if err := evm.New(program).Run(ctx, "Apply", []values.Value{{}}, nil); err != nil {
			t.Fatal(err)
		}
	})
	db.Read(func(tx *edb.Tx) {
		if n := stored(archived, tx, 1).GetValue(2).AsUint(); n != 93 {
			t.Fatalf("archived balance %d", n)
		}
		if a, q := stored(accounts, tx, 1).GetValue(2).AsUint(), stored(orders, tx, 1).GetValue(3).AsUint(); a != 7 || q != 7 {
			t.Fatalf("balance %d, quantity %d", a, q)
		}
	})
}

// Once each read site has run, increasing the row count must add no heap allocations.
func TestReadAllocationsDoNotGrowWithRows(t *testing.T) {
	allocations := func(rows int) float64 {
		h, vm := newRecordBench(t, "", rows)
		return testing.AllocsPerRun(5, func() {
			if _, err := h.run(vm, "ReadCustomers", 0); err != nil {
				t.Fatal(err)
			}
		})
	}
	small, large := allocations(16), allocations(customerCount)
	t.Logf("16 rows: %.0f allocations; %d rows: %.0f", small, customerCount, large)
	if large > small {
		t.Fatalf("scan allocations grew from %v to %v", small, large)
	}
}

func TestRetainedRecordIdentity(t *testing.T) {
	h, vm := newRecordBench(t, `
func Kept(tx m.Context, _ uint64) uint64 {
 rows:=m.Customer.Scan(tx)
 var first *m.Customer
 var child *m.Profile
 for i:=0;i<4;i++{
  row:=rows.Next()
  profile:=row.profile
  if i==0 {first=row}
  if i==1 {child=profile}
 }
 return first.id*1000+child.balance
}
func descend(rows m.CustomersCursor, n int) uint64 {
 row:=rows.Next()
 if n==1 {return row.id}
 rest:=descend(rows,n-1)
 return rest+row.id
}
func Recursive(tx m.Context, _ uint64) uint64 {return descend(m.Customer.Scan(tx),4)}
`, 16)
	for entry, want := range map[string]uint64{"Kept": 1102, "Recursive": 10} {
		h.tx = h.db.BeginRead()
		got, err := h.run(vm, entry, 0)
		if err != nil || got != want {
			t.Fatalf("%s: got %d, %v; want %d", entry, got, err, want)
		}
	}
}
