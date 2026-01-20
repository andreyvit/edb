package edb

import (
	"log/slog"
	"testing"
)

func TestRawRangeCursor_BoundsPrefixAndReverse(t *testing.T) {
	s := newMemStorage()

	wtx := must(s.BeginTx(true))
	buck := must(wtx.CreateBucket("b", ""))
	mustPut(t, buck, []byte{0x10, 0x01}, []byte("a"))
	mustPut(t, buck, []byte{0x10, 0x02}, []byte("b"))
	mustPut(t, buck, []byte{0x10, 0x03}, []byte("c"))
	mustPut(t, buck, []byte{0x11, 0x01}, []byte("x"))
	ensure(wtx.Commit())

	rtx := must(s.BeginTx(false))
	defer rtx.Rollback()
	rbuck := nonNil(rtx.Bucket("b", ""))
	logger := slog.Default()

	{
		cur := (&RawRange{Prefix: []byte{0x10}}).newCursor(rbuck.Cursor(), logger)
		var got []string
		for cur.Next() {
			got = append(got, string(cur.Value()))
		}
		if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
			t.Fatalf("prefix scan values = %v, wanted [a b c]", got)
		}
	}

	{
		cur := (&RawRange{Prefix: []byte{0x10}, Reverse: true}).newCursor(rbuck.Cursor(), logger)
		var got []string
		for cur.Next() {
			got = append(got, string(cur.Value()))
		}
		if len(got) != 3 || got[0] != "c" || got[1] != "b" || got[2] != "a" {
			t.Fatalf("prefix reverse scan values = %v, wanted [c b a]", got)
		}
	}

	{
		cur := (&RawRange{Lower: []byte{0x10, 0x01}, LowerInc: false}).newCursor(rbuck.Cursor(), logger)
		if !cur.Next() || string(cur.Value()) != "b" {
			t.Fatalf("lower exclusive start = %q, wanted b", cur.Value())
		}
	}

	{
		cur := (&RawRange{Upper: []byte{0x10, 0x03}, UpperInc: false, Reverse: true}).newCursor(rbuck.Cursor(), logger)
		if !cur.Next() || string(cur.Value()) != "b" {
			t.Fatalf("upper exclusive reverse start = %q, wanted b", cur.Value())
		}
	}
}

func TestRawRangeCursor_PrefixMismatchPanics(t *testing.T) {
	s := newMemStorage()
	wtx := must(s.BeginTx(true))
	buck := must(wtx.CreateBucket("b", ""))
	mustPut(t, buck, []byte{0x10}, []byte("a"))
	ensure(wtx.Commit())

	rtx := must(s.BeginTx(false))
	defer rtx.Rollback()
	rbuck := nonNil(rtx.Bucket("b", ""))
	logger := slog.Default()

	assertPanics(t, func() {
		cur := (&RawRange{Prefix: []byte{0x10}, Lower: []byte{0x11}, LowerInc: true}).newCursor(rbuck.Cursor(), logger)
		_ = cur.Next()
	})
	assertPanics(t, func() {
		cur := (&RawRange{Prefix: []byte{0x10}, Upper: []byte{0x11}, UpperInc: true, Reverse: true}).newCursor(rbuck.Cursor(), logger)
		_ = cur.Next()
	})
}

func mustPut(t *testing.T, buck storageBucket, k, v []byte) {
	t.Helper()
	ensure(buck.Put(k, v))
}

