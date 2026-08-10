package edb

import (
	"reflect"
	"testing"
)

type scanEdgeRow struct {
	ID    ID     `msgpack:"-"`
	Group string `msgpack:"g"`
	Sort  int    `msgpack:"s"`
	Code  string `msgpack:"c"`
}

type scanEdgeGroupSort struct {
	Group string
	Sort  int
}

type scanEdgeTenantCode struct {
	Tenant ID
	Code   string
}

type scanEdgeCompoundKey struct {
	A int
	B int
}

type scanEdgeCompoundRow struct {
	Key   scanEdgeCompoundKey `msgpack:"-"`
	Label string              `msgpack:"l"`
}

var (
	scanEdgeSchema        = &Schema{}
	scanEdgesByGroup      = AddIndex[string]("group")
	scanEdgesByGroupSort  = AddIndex[scanEdgeGroupSort]("group_sort")
	scanEdgesByCode       = AddIndex[string]("code").Unique()
	scanEdgesByTenantCode = AddIndex[scanEdgeTenantCode]("tenant_code").Unique()
	scanEdgesTable        = AddTable(scanEdgeSchema, "scan_edges", 1, func(row *scanEdgeRow, ib *IndexBuilder) {
		ib.Add(scanEdgesByGroup, row.Group)
		ib.Add(scanEdgesByGroupSort, scanEdgeGroupSort{Group: row.Group, Sort: row.Sort})
		ib.Add(scanEdgesByCode, row.Code)
		ib.Add(scanEdgesByTenantCode, scanEdgeTenantCode{Tenant: 1, Code: row.Code})
	}, nil, []*Index{scanEdgesByGroup, scanEdgesByGroupSort, scanEdgesByCode, scanEdgesByTenantCode})
	scanEdgeCompoundTable = AddTable[scanEdgeCompoundRow](scanEdgeSchema, "scan_compound_edges", 1, nil, nil, nil)
)

type scanEdgeIDCase struct {
	name string
	opt  ScanOptions
	want []ID
}

type scanEdgeCompoundCase struct {
	name string
	opt  ScanOptions
	want []scanEdgeCompoundKey
}

func TestTableScan_ScalarBoundaryMatrix(t *testing.T) {
	db := setupScanEdgeDB(t)
	cases := []scanEdgeIDCase{
		{"full forward", FullScan(), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"full reverse", FullScan().Reversed(), scanEdgeIDs(70, 60, 50, 45, 40, 30, 20, 10)},
		{"range nil bounds", RangeScan(nil, nil, false, false), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"exact first", ExactScan(ID(10)), scanEdgeIDs(10)},
		{"exact middle", ExactScan(ID(45)), scanEdgeIDs(45)},
		{"exact last", ExactScan(ID(70)), scanEdgeIDs(70)},
		{"exact below first", ExactScan(ID(5)), nil},
		{"exact gap", ExactScan(ID(44)), nil},
		{"exact above last", ExactScan(ID(80)), nil},
		{"lower before first inclusive", LowerBoundScan(ID(5), true), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"lower before first exclusive", LowerBoundScan(ID(5), false), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"lower first inclusive", LowerBoundScan(ID(10), true), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"lower first exclusive", LowerBoundScan(ID(10), false), scanEdgeIDs(20, 30, 40, 45, 50, 60, 70)},
		{"lower gap inclusive", LowerBoundScan(ID(44), true), scanEdgeIDs(45, 50, 60, 70)},
		{"lower gap exclusive", LowerBoundScan(ID(44), false), scanEdgeIDs(45, 50, 60, 70)},
		{"lower middle inclusive", LowerBoundScan(ID(45), true), scanEdgeIDs(45, 50, 60, 70)},
		{"lower middle exclusive", LowerBoundScan(ID(45), false), scanEdgeIDs(50, 60, 70)},
		{"lower last inclusive", LowerBoundScan(ID(70), true), scanEdgeIDs(70)},
		{"lower last exclusive", LowerBoundScan(ID(70), false), nil},
		{"lower above last inclusive", LowerBoundScan(ID(80), true), nil},
		{"upper below first inclusive", UpperBoundScan(ID(5), true), nil},
		{"upper below first exclusive", UpperBoundScan(ID(5), false), nil},
		{"upper first inclusive", UpperBoundScan(ID(10), true), scanEdgeIDs(10)},
		{"upper first exclusive", UpperBoundScan(ID(10), false), nil},
		{"upper gap inclusive", UpperBoundScan(ID(44), true), scanEdgeIDs(10, 20, 30, 40)},
		{"upper gap exclusive", UpperBoundScan(ID(44), false), scanEdgeIDs(10, 20, 30, 40)},
		{"upper middle inclusive", UpperBoundScan(ID(45), true), scanEdgeIDs(10, 20, 30, 40, 45)},
		{"upper middle exclusive", UpperBoundScan(ID(45), false), scanEdgeIDs(10, 20, 30, 40)},
		{"upper last inclusive", UpperBoundScan(ID(70), true), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"upper last exclusive", UpperBoundScan(ID(70), false), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60)},
		{"upper above last inclusive", UpperBoundScan(ID(80), true), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"range inclusive inclusive", RangeScan(ID(30), ID(50), true, true), scanEdgeIDs(30, 40, 45, 50)},
		{"range inclusive exclusive", RangeScan(ID(30), ID(50), true, false), scanEdgeIDs(30, 40, 45)},
		{"range exclusive inclusive", RangeScan(ID(30), ID(50), false, true), scanEdgeIDs(40, 45, 50)},
		{"range exclusive exclusive", RangeScan(ID(30), ID(50), false, false), scanEdgeIDs(40, 45)},
		{"same bound inclusive inclusive", RangeScan(ID(45), ID(45), true, true), scanEdgeIDs(45)},
		{"same bound inclusive exclusive", RangeScan(ID(45), ID(45), true, false), nil},
		{"same bound exclusive inclusive", RangeScan(ID(45), ID(45), false, true), nil},
		{"same bound exclusive exclusive", RangeScan(ID(45), ID(45), false, false), nil},
		{"crossed bounds", RangeScan(ID(50), ID(30), true, true), nil},
		{"missing bounds around middle", RangeScan(ID(15), ID(55), true, false), scanEdgeIDs(20, 30, 40, 45, 50)},
		{"outside bounds", RangeScan(ID(5), ID(80), true, true), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"reverse lower first inclusive", LowerBoundScan(ID(10), true).Reversed(), scanEdgeIDs(70, 60, 50, 45, 40, 30, 20, 10)},
		{"reverse lower first exclusive", LowerBoundScan(ID(10), false).Reversed(), scanEdgeIDs(70, 60, 50, 45, 40, 30, 20)},
		{"reverse lower middle inclusive", LowerBoundScan(ID(45), true).Reversed(), scanEdgeIDs(70, 60, 50, 45)},
		{"reverse lower middle exclusive", LowerBoundScan(ID(45), false).Reversed(), scanEdgeIDs(70, 60, 50)},
		{"reverse upper middle inclusive", UpperBoundScan(ID(50), true).Reversed(), scanEdgeIDs(50, 45, 40, 30, 20, 10)},
		{"reverse upper middle exclusive", UpperBoundScan(ID(50), false).Reversed(), scanEdgeIDs(45, 40, 30, 20, 10)},
		{"reverse range inclusive inclusive", RangeScan(ID(30), ID(60), true, true).Reversed(), scanEdgeIDs(60, 50, 45, 40, 30)},
		{"reverse range inclusive exclusive", RangeScan(ID(30), ID(60), true, false).Reversed(), scanEdgeIDs(50, 45, 40, 30)},
		{"reverse range exclusive inclusive", RangeScan(ID(30), ID(60), false, true).Reversed(), scanEdgeIDs(60, 50, 45, 40)},
		{"reverse missing bounds around middle", RangeScan(ID(15), ID(55), true, false).Reversed(), scanEdgeIDs(50, 45, 40, 30, 20)},
		{"reverse crossed bounds", RangeScan(ID(50), ID(30), true, true).Reversed(), nil},
	}
	runScanEdgeIDCases(t, db, cases, func(tx *Tx, opt ScanOptions) []ID {
		return AllKeys[ID](TableScan[scanEdgeRow](tx, opt).Raw())
	})
}

func TestTableScan_CompositePrefixAndRangeMatrix(t *testing.T) {
	db := setupScanEdgeDB(t)
	cases := []scanEdgeCompoundCase{
		{"full forward", FullScan(), scanEdgeKeys(scanEdgeCompoundKey{1, 1}, scanEdgeCompoundKey{1, 2}, scanEdgeCompoundKey{1, 4}, scanEdgeCompoundKey{2, 1}, scanEdgeCompoundKey{2, 3}, scanEdgeCompoundKey{3, 1})},
		{"full reverse", FullScan().Reversed(), scanEdgeKeys(scanEdgeCompoundKey{3, 1}, scanEdgeCompoundKey{2, 3}, scanEdgeCompoundKey{2, 1}, scanEdgeCompoundKey{1, 4}, scanEdgeCompoundKey{1, 2}, scanEdgeCompoundKey{1, 1})},
		{"exact present", ExactScan(scanEdgeCompoundKey{1, 2}), scanEdgeKeys(scanEdgeCompoundKey{1, 2})},
		{"exact missing inside prefix", ExactScan(scanEdgeCompoundKey{1, 3}), nil},
		{"prefix first run", ExactScan(scanEdgeCompoundKey{1, 0}).Prefix(1), scanEdgeKeys(scanEdgeCompoundKey{1, 1}, scanEdgeCompoundKey{1, 2}, scanEdgeCompoundKey{1, 4})},
		{"prefix first run reverse", ExactScan(scanEdgeCompoundKey{1, 0}).Prefix(1).Reversed(), scanEdgeKeys(scanEdgeCompoundKey{1, 4}, scanEdgeCompoundKey{1, 2}, scanEdgeCompoundKey{1, 1})},
		{"prefix middle run", ExactScan(scanEdgeCompoundKey{2, 0}).Prefix(1), scanEdgeKeys(scanEdgeCompoundKey{2, 1}, scanEdgeCompoundKey{2, 3})},
		{"prefix last run reverse", ExactScan(scanEdgeCompoundKey{3, 0}).Prefix(1).Reversed(), scanEdgeKeys(scanEdgeCompoundKey{3, 1})},
		{"prefix below first", ExactScan(scanEdgeCompoundKey{0, 0}).Prefix(1), nil},
		{"prefix above last", ExactScan(scanEdgeCompoundKey{4, 0}).Prefix(1), nil},
		{"range inclusive inclusive", RangeScan(scanEdgeCompoundKey{1, 2}, scanEdgeCompoundKey{2, 3}, true, true), scanEdgeKeys(scanEdgeCompoundKey{1, 2}, scanEdgeCompoundKey{1, 4}, scanEdgeCompoundKey{2, 1}, scanEdgeCompoundKey{2, 3})},
		{"range inclusive exclusive", RangeScan(scanEdgeCompoundKey{1, 2}, scanEdgeCompoundKey{2, 3}, true, false), scanEdgeKeys(scanEdgeCompoundKey{1, 2}, scanEdgeCompoundKey{1, 4}, scanEdgeCompoundKey{2, 1})},
		{"range exclusive inclusive", RangeScan(scanEdgeCompoundKey{1, 2}, scanEdgeCompoundKey{2, 3}, false, true), scanEdgeKeys(scanEdgeCompoundKey{1, 4}, scanEdgeCompoundKey{2, 1}, scanEdgeCompoundKey{2, 3})},
		{"range gap bounds", RangeScan(scanEdgeCompoundKey{1, 3}, scanEdgeCompoundKey{2, 2}, true, true), scanEdgeKeys(scanEdgeCompoundKey{1, 4}, scanEdgeCompoundKey{2, 1})},
		{"reverse range inclusive exclusive", RangeScan(scanEdgeCompoundKey{1, 2}, scanEdgeCompoundKey{2, 3}, true, false).Reversed(), scanEdgeKeys(scanEdgeCompoundKey{2, 1}, scanEdgeCompoundKey{1, 4}, scanEdgeCompoundKey{1, 2})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db.Read(func(tx *Tx) {
				got := AllKeys[scanEdgeCompoundKey](TableScan[scanEdgeCompoundRow](tx, tc.opt).Raw())
				assertScanEdgeKeys(t, got, tc.want)
			})
		})
	}
}

func TestIndexScan_NonUniqueScalarBoundaryMatrix(t *testing.T) {
	db := setupScanEdgeDB(t)
	cases := []scanEdgeIDCase{
		{"full forward", FullScan(), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"full reverse", FullScan().Reversed(), scanEdgeIDs(70, 60, 50, 45, 40, 30, 20, 10)},
		{"exact first duplicate run", ExactScan("ant"), scanEdgeIDs(10, 20)},
		{"exact first duplicate run reverse", ExactScan("ant").Reversed(), scanEdgeIDs(20, 10)},
		{"exact middle duplicate run", ExactScan("bee"), scanEdgeIDs(30, 40, 45, 50)},
		{"exact middle duplicate run reverse", ExactScan("bee").Reversed(), scanEdgeIDs(50, 45, 40, 30)},
		{"exact single run", ExactScan("cat"), scanEdgeIDs(60)},
		{"exact below first", ExactScan("aardvark"), nil},
		{"exact gap", ExactScan("bet"), nil},
		{"exact above last", ExactScan("elk"), nil},
		{"range inclusive inclusive", RangeScan("bee", "cat", true, true), scanEdgeIDs(30, 40, 45, 50, 60)},
		{"range inclusive exclusive", RangeScan("bee", "cat", true, false), scanEdgeIDs(30, 40, 45, 50)},
		{"range exclusive inclusive", RangeScan("bee", "cat", false, true), scanEdgeIDs(60)},
		{"range exclusive exclusive", RangeScan("bee", "cat", false, false), nil},
		{"same bound inclusive inclusive", RangeScan("bee", "bee", true, true), scanEdgeIDs(30, 40, 45, 50)},
		{"same bound inclusive exclusive", RangeScan("bee", "bee", true, false), nil},
		{"same bound exclusive inclusive", RangeScan("bee", "bee", false, true), nil},
		{"same bound exclusive exclusive", RangeScan("bee", "bee", false, false), nil},
		{"lower duplicate inclusive", LowerBoundScan("bee", true), scanEdgeIDs(30, 40, 45, 50, 60, 70)},
		{"lower duplicate exclusive", LowerBoundScan("bee", false), scanEdgeIDs(60, 70)},
		{"lower gap inclusive", LowerBoundScan("bet", true), scanEdgeIDs(60, 70)},
		{"lower gap exclusive", LowerBoundScan("bet", false), scanEdgeIDs(60, 70)},
		{"upper duplicate inclusive", UpperBoundScan("bee", true), scanEdgeIDs(10, 20, 30, 40, 45, 50)},
		{"upper duplicate exclusive", UpperBoundScan("bee", false), scanEdgeIDs(10, 20)},
		{"upper gap inclusive", UpperBoundScan("cow", true), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60)},
		{"upper gap exclusive", UpperBoundScan("cow", false), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60)},
		{"outside bounds", RangeScan("aardvark", "zebra", true, true), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"bounds between existing runs", RangeScan("bat", "cow", true, true), scanEdgeIDs(30, 40, 45, 50, 60)},
		{"lower above last", LowerBoundScan("elk", true), nil},
		{"upper below first", UpperBoundScan("aardvark", true), nil},
		{"reverse range inclusive inclusive", RangeScan("bee", "cat", true, true).Reversed(), scanEdgeIDs(60, 50, 45, 40, 30)},
		{"reverse range inclusive exclusive", RangeScan("bee", "cat", true, false).Reversed(), scanEdgeIDs(50, 45, 40, 30)},
		{"reverse range exclusive inclusive", RangeScan("bee", "cat", false, true).Reversed(), scanEdgeIDs(60)},
		{"reverse lower duplicate exclusive", LowerBoundScan("bee", false).Reversed(), scanEdgeIDs(70, 60)},
		{"reverse upper duplicate exclusive", UpperBoundScan("bee", false).Reversed(), scanEdgeIDs(20, 10)},
		{"reverse upper gap exclusive", UpperBoundScan("cow", false).Reversed(), scanEdgeIDs(60, 50, 45, 40, 30, 20, 10)},
		{"reverse bounds between existing runs", RangeScan("bat", "cow", true, true).Reversed(), scanEdgeIDs(60, 50, 45, 40, 30)},
	}
	runScanEdgeIDCases(t, db, cases, func(tx *Tx, opt ScanOptions) []ID {
		return AllKeys[ID](IndexScan[scanEdgeRow](tx, scanEdgesByGroup, opt).Raw())
	})
}

func TestIndexScan_CompositePrefixRangeAndPrimaryKeyMatrix(t *testing.T) {
	db := setupScanEdgeDB(t)
	cases := []scanEdgeIDCase{
		{"full forward", FullScan(), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"full reverse", FullScan().Reversed(), scanEdgeIDs(70, 60, 50, 45, 40, 30, 20, 10)},
		{"exact duplicate composite key", ExactScan(scanEdgeGroupSort{"bee", 2}), scanEdgeIDs(40, 45)},
		{"exact duplicate composite key reverse", ExactScan(scanEdgeGroupSort{"bee", 2}).Reversed(), scanEdgeIDs(45, 40)},
		{"exact missing composite key", ExactScan(scanEdgeGroupSort{"bee", 3}), nil},
		{"prefix middle group", ExactScan(scanEdgeGroupSort{"bee", 0}).Prefix(1), scanEdgeIDs(30, 40, 45, 50)},
		{"prefix middle group reverse", ExactScan(scanEdgeGroupSort{"bee", 0}).Prefix(1).Reversed(), scanEdgeIDs(50, 45, 40, 30)},
		{"prefix missing group", ExactScan(scanEdgeGroupSort{"cow", 0}).Prefix(1), nil},
		{"range exact duplicate lower inclusive", RangeScan(scanEdgeGroupSort{"bee", 2}, scanEdgeGroupSort{"bee", 4}, true, true), scanEdgeIDs(40, 45, 50)},
		{"range exact duplicate lower exclusive", RangeScan(scanEdgeGroupSort{"bee", 2}, scanEdgeGroupSort{"bee", 4}, false, true), scanEdgeIDs(50)},
		{"range exact duplicate upper exclusive", RangeScan(scanEdgeGroupSort{"bee", 1}, scanEdgeGroupSort{"bee", 2}, true, false), scanEdgeIDs(30)},
		{"range gap lower", RangeScan(scanEdgeGroupSort{"bee", 3}, scanEdgeGroupSort{"dog", 1}, true, true), scanEdgeIDs(50, 60)},
		{"range reverse exact duplicate lower inclusive", RangeScan(scanEdgeGroupSort{"bee", 2}, scanEdgeGroupSort{"bee", 4}, true, true).Reversed(), scanEdgeIDs(50, 45, 40)},
		{"prefix range", RangeScan(scanEdgeGroupSort{"bee", 2}, scanEdgeGroupSort{"bee", 4}, true, true).Prefix(1), scanEdgeIDs(40, 45, 50)},
		{"prefix range reverse", RangeScan(scanEdgeGroupSort{"bee", 2}, scanEdgeGroupSort{"bee", 4}, true, true).Prefix(1).Reversed(), scanEdgeIDs(50, 45, 40)},
		{"exact primary inclusive inclusive", ExactScan("bee").PrimaryKeyRange(ID(30), ID(50), true, true), scanEdgeIDs(30, 40, 45, 50)},
		{"exact primary exclusive exclusive", ExactScan("bee").PrimaryKeyRange(ID(30), ID(50), false, false), scanEdgeIDs(40, 45)},
		{"exact primary missing bounds", ExactScan("bee").PrimaryKeyRange(ID(35), ID(46), true, false), scanEdgeIDs(40, 45)},
		{"exact primary lower before upper gap", ExactScan("bee").PrimaryKeyRange(ID(1), ID(39), true, true), scanEdgeIDs(30)},
		{"exact primary upper before run", ExactScan("bee").PrimaryKeyRange(nil, ID(25), false, true), nil},
		{"exact primary lower after run", ExactScan("bee").PrimaryKeyRange(ID(55), nil, true, false), nil},
		{"exact primary lower only inclusive", ExactScan("bee").PrimaryKeyRange(ID(40), nil, true, false), scanEdgeIDs(40, 45, 50)},
		{"exact primary lower only exclusive", ExactScan("bee").PrimaryKeyRange(ID(40), nil, false, false), scanEdgeIDs(45, 50)},
		{"exact primary upper only inclusive", ExactScan("bee").PrimaryKeyRange(nil, ID(45), false, true), scanEdgeIDs(30, 40, 45)},
		{"exact primary upper only exclusive", ExactScan("bee").PrimaryKeyRange(nil, ID(45), false, false), scanEdgeIDs(30, 40)},
		{"exact primary reverse inclusive inclusive", ExactScan("bee").PrimaryKeyRange(ID(30), ID(50), true, true).Reversed(), scanEdgeIDs(50, 45, 40, 30)},
		{"exact primary reverse missing bounds", ExactScan("bee").PrimaryKeyRange(ID(35), ID(46), true, false).Reversed(), scanEdgeIDs(45, 40)},
		{"range primary lower suffix", RangeScan("bee", "dog", true, false).PrimaryKeyRange(ID(40), nil, false, false), scanEdgeIDs(45, 50, 60)},
		{"range primary upper suffix", RangeScan("ant", "bee", false, true).PrimaryKeyRange(nil, ID(45), false, true), scanEdgeIDs(30, 40, 45)},
		{"range primary suffix both sides", RangeScan("ant", "cat", true, true).PrimaryKeyRange(ID(20), ID(60), false, false), scanEdgeIDs(30, 40, 45, 50)},
		{"range primary suffix reverse", RangeScan("ant", "cat", true, true).PrimaryKeyRange(ID(20), ID(60), false, false).Reversed(), scanEdgeIDs(50, 45, 40, 30)},
	}
	runScanEdgeIDCases(t, db, cases, func(tx *Tx, opt ScanOptions) []ID {
		idx := scanEdgesByGroupSort
		if opt.Method == ScanMethodExact && opt.Lower.IsValid() && opt.Lower.Type() == reflect.TypeOf("") {
			idx = scanEdgesByGroup
		}
		if opt.Method == ScanMethodRange && opt.Lower.IsValid() && opt.Lower.Type() == reflect.TypeOf("") {
			idx = scanEdgesByGroup
		}
		return AllKeys[ID](IndexScan[scanEdgeRow](tx, idx, opt).Raw())
	})
}

func TestIndexScan_UniqueScalarBoundaryMatrix(t *testing.T) {
	db := setupScanEdgeDB(t)
	cases := []scanEdgeIDCase{
		{"full forward", FullScan(), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"full reverse", FullScan().Reversed(), scanEdgeIDs(70, 60, 50, 45, 40, 30, 20, 10)},
		{"range nil bounds", RangeScan(nil, nil, false, false), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70)},
		{"exact first", ExactScan("c10"), scanEdgeIDs(10)},
		{"exact middle", ExactScan("c45"), scanEdgeIDs(45)},
		{"exact last", ExactScan("c70"), scanEdgeIDs(70)},
		{"exact missing before", ExactScan("c05"), nil},
		{"exact missing gap", ExactScan("c44"), nil},
		{"exact missing after", ExactScan("c80"), nil},
		{"exact reverse", ExactScan("c45").Reversed(), scanEdgeIDs(45)},
		{"range inclusive inclusive", RangeScan("c30", "c50", true, true), scanEdgeIDs(30, 40, 45, 50)},
		{"range inclusive exclusive", RangeScan("c30", "c50", true, false), scanEdgeIDs(30, 40, 45)},
		{"range exclusive inclusive", RangeScan("c30", "c50", false, true), scanEdgeIDs(40, 45, 50)},
		{"range exclusive exclusive", RangeScan("c30", "c50", false, false), scanEdgeIDs(40, 45)},
		{"lower middle exclusive", LowerBoundScan("c45", false), scanEdgeIDs(50, 60, 70)},
		{"upper middle exclusive", UpperBoundScan("c45", false), scanEdgeIDs(10, 20, 30, 40)},
		{"reverse range inclusive exclusive", RangeScan("c30", "c50", true, false).Reversed(), scanEdgeIDs(45, 40, 30)},
		{"reverse lower exclusive", LowerBoundScan("c45", false).Reversed(), scanEdgeIDs(70, 60, 50)},
		{"reverse upper exclusive", UpperBoundScan("c45", false).Reversed(), scanEdgeIDs(40, 30, 20, 10)},
		{"raw prefix full unique key", ExactScan("c40").Prefix(1), scanEdgeIDs(40)},
		{"raw prefix full unique key reverse", ExactScan("c40").Prefix(1).Reversed(), scanEdgeIDs(40)},
	}
	runScanEdgeIDCases(t, db, cases, func(tx *Tx, opt ScanOptions) []ID {
		return AllKeys[ID](IndexScan[scanEdgeRow](tx, scanEdgesByCode, opt).Raw())
	})

	db.Write(func(tx *Tx) {
		Put(tx, &scanEdgeRow{ID: 39, Group: "bee", Sort: 2, Code: "c4\x00x"})
		Put(tx, &scanEdgeRow{ID: 41, Group: "bee", Sort: 2, Code: "c4\x01x"})
		Put(tx, &scanEdgeRow{ID: 42, Group: "bee", Sort: 2, Code: "c4"})
	})
	afterC4Prefix := ExactScan("c4").Prefix(1)
	afterC4Prefix.Method = ScanMethodRange
	afterC4Prefix.AfterIndexKey = reflect.ValueOf("c4")
	beforeC4Prefix := afterC4Prefix.Reversed()
	beforeC4Prefix.AfterIndexKey = reflect.Value{}
	beforeC4Prefix.BeforeIndexKey = reflect.ValueOf("c4")
	cases = []scanEdgeIDCase{
		{"after NUL suffix", ScanOptions{Method: ScanMethodRange, AfterIndexKey: reflect.ValueOf("c4\x00x")}, scanEdgeIDs(42, 41, 40, 45, 50, 60, 70)},
		{"before short key", ScanOptions{Reverse: true, Method: ScanMethodRange, BeforeIndexKey: reflect.ValueOf("c4")}, scanEdgeIDs(39, 30, 20, 10)},
		{"prefix lower exclusive", LowerBoundScan("c4", false).Prefix(1), scanEdgeIDs(39, 41, 40, 45)},
		{"prefix after short key", afterC4Prefix, scanEdgeIDs(41, 40, 45)},
		{"prefix before short key", beforeC4Prefix, scanEdgeIDs(39)},
	}
	runScanEdgeIDCases(t, db, cases, func(tx *Tx, opt ScanOptions) []ID {
		return AllKeys[ID](IndexScan[scanEdgeRow](tx, scanEdgesByCode, opt).Raw())
	})

	afterComposite := ScanOptions{
		Method:        ScanMethodRange,
		AfterIndexKey: reflect.ValueOf(scanEdgeTenantCode{Tenant: 1, Code: "c4\x01x"}),
	}
	beforeComposite := ScanOptions{
		Reverse:        true,
		Method:         ScanMethodRange,
		BeforeIndexKey: reflect.ValueOf(scanEdgeTenantCode{Tenant: 1, Code: "c4"}),
	}
	cases = []scanEdgeIDCase{
		{"composite after control suffix", afterComposite, scanEdgeIDs(42, 40, 45, 50, 60, 70)},
		{"composite before short key", beforeComposite, scanEdgeIDs(41, 39, 30, 20, 10)},
	}
	runScanEdgeIDCases(t, db, cases, func(tx *Tx, opt ScanOptions) []ID {
		return AllKeys[ID](IndexScan[scanEdgeRow](tx, scanEdgesByTenantCode, opt).Raw())
	})
}

func TestIndexScan_MalformedOptionsPanic(t *testing.T) {
	db := setupScanEdgeDB(t)
	unownedIndex := AddIndex[string]("unowned")
	cases := []struct {
		name string
		run  func(tx *Tx)
	}{
		{"table wrong lower type", func(tx *Tx) {
			_ = TableScan[scanEdgeRow](tx, LowerBoundScan("bad", true))
		}},
		{"table wrong exact type", func(tx *Tx) {
			_ = TableScan[scanEdgeRow](tx, ExactScan("bad"))
		}},
		{"table wrong upper type", func(tx *Tx) {
			_ = TableScan[scanEdgeRow](tx, UpperBoundScan("bad", true))
		}},
		{"table wrong prefix type", func(tx *Tx) {
			_ = TableScan[scanEdgeCompoundRow](tx, ExactScan(ID(1)).Prefix(1))
		}},
		{"table exact without lower", func(tx *Tx) {
			_ = TableScan[scanEdgeRow](tx, ScanOptions{Method: ScanMethodExact})
		}},
		{"table unsupported scan method", func(tx *Tx) {
			_ = TableScan[scanEdgeRow](tx, ScanOptions{Method: ScanMethod(99)})
		}},
		{"index wrong lower type", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, LowerBoundScan(ID(1), true))
		}},
		{"index wrong exact type", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, ExactScan(ID(1)))
		}},
		{"index wrong upper type", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, UpperBoundScan(ID(1), true))
		}},
		{"index wrong prefix type", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, ExactScan(ID(1)).Prefix(1))
		}},
		{"index exact without lower", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, ScanOptions{Method: ScanMethodExact})
		}},
		{"index unsupported scan method", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, ScanOptions{Method: ScanMethod(99)})
		}},
		{"positioned exact scan", func(tx *Tx) {
			opt := ExactScan("c10")
			opt.AfterIndexKey = reflect.ValueOf("c10")
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByCode, opt)
		}},
		{"positioned unsupported scan method", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByCode, ScanOptions{
				Method:         ScanMethod(99),
				BeforeIndexKey: reflect.ValueOf("c10"),
			})
		}},
		{"index from another table", func(tx *Tx) {
			_ = IndexScan[scanEdgeCompoundRow](tx, scanEdgesByGroup, FullScan())
		}},
		{"index without table", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, unownedIndex, FullScan())
		}},
		{"raw exact without lower", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, ScanOptions{
				Method:      ScanMethodExact,
				PrefixValue: reflect.ValueOf("bee"),
				PrefixEls:   1,
			})
		}},
		{"raw exact wrong lower type", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, ScanOptions{
				Method:      ScanMethodExact,
				Lower:       reflect.ValueOf(ID(1)),
				PrefixValue: reflect.ValueOf("bee"),
				PrefixEls:   1,
			})
		}},
		{"raw range wrong bound type", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, RangeScan(ID(1), nil, true, false).PrimaryKeyRange(ID(10), nil, true, false))
		}},
		{"primary lower without index lower", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, FullScan().PrimaryKeyRange(ID(10), nil, true, false))
		}},
		{"primary upper without index upper", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, FullScan().PrimaryKeyRange(nil, ID(10), false, true))
		}},
		{"primary wrong type", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByGroup, ExactScan("bee").PrimaryKeyRange("bad", nil, true, false))
		}},
		{"unique primary range", func(tx *Tx) {
			_ = IndexScan[scanEdgeRow](tx, scanEdgesByCode, ExactScan("c10").PrimaryKeyRange(ID(10), ID(20), true, true))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db.Read(func(tx *Tx) {
				assertPanics(t, func() {
					tc.run(tx)
				})
			})
		})
	}
	assertPanics(t, func() {
		_ = FullScan().Prefix(1)
	})
}

func TestScanConvenienceHelpersAndEmptyResults(t *testing.T) {
	db := setupScanEdgeDB(t)
	db.Read(func(tx *Tx) {
		assertScanEdgeIDs(t, AllKeys[ID](FullTableScan[scanEdgeRow](tx).Raw()), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70))
		assertScanEdgeIDs(t, AllKeys[ID](FullReverseTableScan[scanEdgeRow](tx).Raw()), scanEdgeIDs(70, 60, 50, 45, 40, 30, 20, 10))
		assertScanEdgeIDs(t, AllKeys[ID](RangeTableScan[scanEdgeRow](tx, ID(30), ID(45), true, true).Raw()), scanEdgeIDs(30, 40, 45))
		assertScanEdgeIDs(t, AllKeys[ID](ReverseRangeTableScan[scanEdgeRow](tx, ID(30), ID(45), true, true).Raw()), scanEdgeIDs(45, 40, 30))
		assertScanEdgeIDs(t, AllKeys[ID](ExactTableScan[scanEdgeRow](tx, ID(40)).Raw()), scanEdgeIDs(40))

		assertScanEdgeIDs(t, AllKeys[ID](FullIndexScan[scanEdgeRow](tx, scanEdgesByGroup).Raw()), scanEdgeIDs(10, 20, 30, 40, 45, 50, 60, 70))
		assertScanEdgeIDs(t, AllKeys[ID](ExactIndexScan[scanEdgeRow](tx, scanEdgesByGroup, "cat").Raw()), scanEdgeIDs(60))
		assertScanEdgeIDs(t, AllKeys[ID](ReverseExactIndexScan[scanEdgeRow](tx, scanEdgesByGroup, "bee").Raw()), scanEdgeIDs(50, 45, 40, 30))
		assertScanEdgeIDs(t, AllKeys[ID](RangeIndexScan[scanEdgeRow](tx, scanEdgesByGroup, "bee", "cat", true, false).Raw()), scanEdgeIDs(30, 40, 45, 50))
		assertScanEdgeIDs(t, AllKeys[ID](ReverseRangeIndexScan[scanEdgeRow](tx, scanEdgesByGroup, "bee", "cat", true, false).Raw()), scanEdgeIDs(50, 45, 40, 30))
		assertScanEdgeIDs(t, AllKeys[ID](PrefixIndexScan[scanEdgeRow](tx, scanEdgesByGroupSort, 1, scanEdgeGroupSort{"bee", 0}).Raw()), scanEdgeIDs(30, 40, 45, 50))
		assertScanEdgeIDs(t, AllKeys[ID](ReversePrefixIndexScan[scanEdgeRow](tx, scanEdgesByGroupSort, 1, scanEdgeGroupSort{"bee", 0}).Raw()), scanEdgeIDs(50, 45, 40, 30))

		if First(TableScan[scanEdgeRow](tx, ExactScan(ID(999)))) != nil {
			t.Fatalf("First on empty scan returned a row")
		}
		if got := FirstKey[ID](TableScan[scanEdgeRow](tx, ExactScan(ID(999)))); got != 0 {
			t.Fatalf("FirstKey on empty scan = %v, wanted 0", got)
		}
		if Select(TableScan[scanEdgeRow](tx, FullScan()), func(row *scanEdgeRow) bool { return row.ID == 999 }) != nil {
			t.Fatalf("Select on unmatched scan returned a row")
		}
	})
}

func TestScanOptions_LogStringVariants(t *testing.T) {
	options := []ScanOptions{
		FullScan().Reversed(),
		ExactScan("bee"),
		RangeScan(nil, "cat", false, true),
		ExactScan(scanEdgeGroupSort{"bee", 0}).Prefix(1),
		ExactScan("bee").PrimaryKeyRange(ID(30), ID(50), true, false),
		ExactScan("bee").PrimaryKeyRange(ID(30), ID(50), false, true),
		ScanOptions{Method: ScanMethod(99)},
	}
	for _, opt := range options {
		if got := opt.LogString(); got == "" {
			t.Fatalf("empty LogString for %#v", opt)
		}
	}
}

func TestIndex_KeyStringParseAndDecodeRoundTrip(t *testing.T) {
	key := scanEdgeGroupSort{Group: "bee", Sort: 2}
	raw := scanEdgesByGroupSort.EncodeKey(key)
	got := scanEdgesByGroupSort.DecodeKeyVal(raw).Interface().(scanEdgeGroupSort)
	if got != key {
		t.Fatalf("DecodeKeyVal(EncodeKey(%v)) = %v", key, got)
	}

	var into scanEdgeGroupSort
	scanEdgesByGroupSort.DecodeKeyValInto(reflect.ValueOf(&into).Elem(), raw)
	if into != key {
		t.Fatalf("DecodeKeyValInto(EncodeKey(%v)) = %v", key, into)
	}

	var tryInto scanEdgeGroupSort
	if err := scanEdgesByGroupSort.TryDecodeKeyValInto(reflect.ValueOf(&tryInto).Elem(), raw); err != nil {
		t.Fatalf("TryDecodeKeyValInto failed: %v", err)
	}
	if tryInto != key {
		t.Fatalf("TryDecodeKeyValInto(EncodeKey(%v)) = %v", key, tryInto)
	}

	keyString := scanEdgesByGroupSort.KeyString(key)
	if got := scanEdgesByGroupSort.RawKeyString(raw); got != keyString {
		t.Fatalf("RawKeyString(EncodeKey(%v)) = %q, wanted %q", key, got, keyString)
	}
	parsed, err := scanEdgesByGroupSort.ParseNakedIndexKey(keyString)
	if err != nil {
		t.Fatalf("ParseNakedIndexKey(%q) failed: %v", keyString, err)
	}
	if parsed != key {
		t.Fatalf("ParseNakedIndexKey(KeyString(%v)) = %T %v", key, parsed, parsed)
	}
	parsedVal, err := scanEdgesByGroupSort.ParseNakedIndexKeyVal(keyString)
	if err != nil {
		t.Fatalf("ParseNakedIndexKeyVal(%q) failed: %v", keyString, err)
	}
	if got := parsedVal.Interface().(scanEdgeGroupSort); got != key {
		t.Fatalf("ParseNakedIndexKeyVal(KeyString(%v)) = %v", key, got)
	}

	if scanEdgesByGroupSort.IsUnique() {
		t.Fatalf("non-unique index reports unique")
	}
	if !scanEdgesByCode.IsUnique() {
		t.Fatalf("unique index reports non-unique")
	}
	if _, err := scanEdgesByGroupSort.ParseNakedIndexKey("bee"); err == nil {
		t.Fatalf("ParseNakedIndexKey accepted an incomplete composite key")
	}
}

func TestIndexScan_EmptyStrategyProducesEmptyCursor(t *testing.T) {
	ik, iv, itup, dk := (emptyIndexScanStrategy{}).Next(nil, true, false, scanEdgesByGroup)
	if ik != nil || iv != nil || itup != nil || dk != nil {
		t.Fatalf("empty strategy returned %x %x %v %x", ik, iv, itup, dk)
	}
}

func setupScanEdgeDB(t testing.TB) *DB {
	t.Helper()
	db := setup(t, scanEdgeSchema)
	db.Write(func(tx *Tx) {
		for _, row := range scanEdgeRows() {
			Put(tx, row)
		}
		for _, row := range scanEdgeCompoundRows() {
			Put(tx, row)
		}
	})
	return db
}

func scanEdgeRows() []*scanEdgeRow {
	return []*scanEdgeRow{
		{ID: 10, Group: "ant", Sort: 1, Code: "c10"},
		{ID: 20, Group: "ant", Sort: 3, Code: "c20"},
		{ID: 30, Group: "bee", Sort: 1, Code: "c30"},
		{ID: 40, Group: "bee", Sort: 2, Code: "c40"},
		{ID: 45, Group: "bee", Sort: 2, Code: "c45"},
		{ID: 50, Group: "bee", Sort: 4, Code: "c50"},
		{ID: 60, Group: "cat", Sort: 1, Code: "c60"},
		{ID: 70, Group: "dog", Sort: 2, Code: "c70"},
	}
}

func scanEdgeCompoundRows() []*scanEdgeCompoundRow {
	return []*scanEdgeCompoundRow{
		{Key: scanEdgeCompoundKey{1, 1}, Label: "a11"},
		{Key: scanEdgeCompoundKey{1, 2}, Label: "a12"},
		{Key: scanEdgeCompoundKey{1, 4}, Label: "a14"},
		{Key: scanEdgeCompoundKey{2, 1}, Label: "a21"},
		{Key: scanEdgeCompoundKey{2, 3}, Label: "a23"},
		{Key: scanEdgeCompoundKey{3, 1}, Label: "a31"},
	}
}

func scanEdgeIDs(ids ...ID) []ID {
	return ids
}

func scanEdgeKeys(keys ...scanEdgeCompoundKey) []scanEdgeCompoundKey {
	return keys
}

func runScanEdgeIDCases(t *testing.T, db *DB, cases []scanEdgeIDCase, scan func(*Tx, ScanOptions) []ID) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db.Read(func(tx *Tx) {
				got := scan(tx, tc.opt)
				assertScanEdgeIDs(t, got, tc.want)
			})
		})
	}
}

func assertScanEdgeIDs(t testing.TB, got, want []ID) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got ids %v, wanted %v", got, want)
	}
}

func assertScanEdgeKeys(t testing.TB, got, want []scanEdgeCompoundKey) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got keys %v, wanted %v", got, want)
	}
}
