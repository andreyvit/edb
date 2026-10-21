package edb

import (
	"bytes"
	"path/filepath"
	"testing"
)

type benchmarkRow struct {
	Key     uint64 `msgpack:"-"`
	Group   uint64 `msgpack:"g"`
	Payload []byte `msgpack:"p"`
}

type benchmarkFixture struct {
	db    *DB
	index *Index
	rows  []*benchmarkRow
	keys  []any
}

func BenchmarkGet(b *testing.B) {
	f := newBenchmarkFixture(b)
	tx := f.db.BeginRead()
	defer tx.Close()
	var row *benchmarkRow
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row = Get[benchmarkRow](tx, f.keys[i%len(f.keys)])
	}
	b.StopTimer()
	if row == nil || row.Key != f.rows[(b.N-1)%len(f.rows)].Key || len(row.Payload) != 128 {
		b.Fatal("unexpected row")
	}
}

func BenchmarkLookup(b *testing.B) {
	f := newBenchmarkFixture(b)
	tx := f.db.BeginRead()
	defer tx.Close()
	var row *benchmarkRow
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row = Lookup[benchmarkRow](tx, f.index, f.keys[i%16])
	}
	b.StopTimer()
	if row == nil || row.Group != f.rows[(b.N-1)%16].Group || len(row.Payload) != 128 {
		b.Fatal("unexpected indexed row")
	}
}

func BenchmarkPut(b *testing.B) {
	for _, operation := range []string{"insert", "update"} {
		b.Run(operation+"/batch=100", func(b *testing.B) {
			benchmarkMutation(b, operation)
		})
	}
}

func BenchmarkDelete(b *testing.B) {
	b.Run("batch=100", func(b *testing.B) {
		benchmarkMutation(b, "delete")
	})
}

func BenchmarkScan(b *testing.B) {
	for _, source := range []string{"table", "index"} {
		b.Run(source, func(b *testing.B) {
			f := newBenchmarkFixture(b)
			tx := f.db.BeginRead()
			defer tx.Close()
			count := len(f.rows)
			if source == "index" {
				count = 63
			}
			var row *benchmarkRow
			var total int64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var cur Cursor[benchmarkRow]
				if source == "index" {
					cur = IndexScan[benchmarkRow](tx, f.index, ExactScan(uint64(1)))
				} else {
					cur = TableScan[benchmarkRow](tx, FullScan())
				}
				for cur.Next() {
					row = cur.Row()
					total++
				}
			}
			b.StopTimer()
			if total != int64(b.N)*int64(count) || row == nil || len(row.Payload) != 128 {
				b.Fatal("unexpected scan result")
			}
			b.ReportMetric(float64(count), "rows/op")
		})
	}
}

func benchmarkMutation(b *testing.B, operation string) {
	b.StopTimer()
	f := newBenchmarkFixture(b)
	rows := f.rows[:100]
	var mutate func(*Tx, *benchmarkRow) bool
	var restore func(*Tx)
	switch operation {
	case "insert":
		rows = make([]*benchmarkRow, len(rows))
		for i := range rows {
			row := *f.rows[i]
			row.Key += uint64(len(f.rows))
			rows[i] = &row
		}
		mutate = func(tx *Tx, row *benchmarkRow) bool {
			return Put(tx, row)
		}
		restore = func(tx *Tx) {
			for _, row := range rows {
				DeleteRow(tx, row)
			}
		}
	case "update":
		mutate = func(tx *Tx, row *benchmarkRow) bool {
			row.Group ^= 16
			row.Payload[0] ^= 0xff
			return Put(tx, row)
		}
	case "delete":
		mutate = func(tx *Tx, row *benchmarkRow) bool {
			return DeleteRow(tx, row)
		}
		restore = func(tx *Tx) {
			for _, row := range rows {
				Put(tx, row)
			}
		}
	default:
		b.Fatalf("unknown mutation %q", operation)
	}
	var total int64
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		f.db.Write(func(tx *Tx) {
			for _, row := range rows {
				if mutate(tx, row) {
					total++
				}
			}
		})
		if restore != nil {
			b.StopTimer()
			f.db.Write(restore)
			b.StartTimer()
		}
	}
	b.StopTimer()
	if total != int64(b.N)*int64(len(rows)) {
		b.Fatal("unexpected mutation count")
	}
	f.db.Read(func(tx *Tx) {
		if Count(TableScan[benchmarkRow](tx, FullScan()).Raw()) != len(f.rows) {
			b.Fatal("unexpected row count")
		}
	})
	b.ReportMetric(float64(len(rows)), "rows/op")
}

func newBenchmarkFixture(b *testing.B) *benchmarkFixture {
	b.Helper()
	schema := &Schema{}
	index := AddIndex[uint64]("group")
	DefineTable[benchmarkRow, uint64](schema, "rows", func(builder *TableBuilder[benchmarkRow, uint64]) {
		builder.AddIndex(index)
		builder.Indexer(func(row *benchmarkRow, ib *IndexBuilder) {
			ib.Add(index, row.Group)
		})
	})
	db, err := Open(filepath.Join(b.TempDir(), "bench.db"), schema, Options{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(db.Close)
	f := &benchmarkFixture{db: db, index: index, rows: make([]*benchmarkRow, 1000), keys: make([]any, 1000)}
	for i := range f.rows {
		f.rows[i] = &benchmarkRow{Key: uint64(i + 1), Group: uint64(i%16 + 1), Payload: bytes.Repeat([]byte{0x35}, 128)}
		f.keys[i] = f.rows[i].Key
	}
	db.Write(func(tx *Tx) {
		for _, row := range f.rows {
			Put(tx, row)
		}
	})
	return f
}
