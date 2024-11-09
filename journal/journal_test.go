package journal_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/andreyvit/edb/journal"
	"github.com/andreyvit/edb/journal/journaltest"
)

var bytesEq = journaltest.BytesEq

const magic = "'JOURNLAT"
const header1 = "0/ver 0/pad 0_0/flags 0../pad"
const header2 = "0*32/journal_inv 0*32/seg_inv 0...*3/reserved"

func TestJournal_trivial(t *testing.T) {
	j := journaltest.Writable(t, journal.Options{})
	ensure(j.WriteRecord(0, []byte("hello")))
	ensure(j.WriteRecord(0, []byte("w")))
	j.Advance(1000 * time.Second)
	ensure(j.WriteRecord(0, []byte("orld")))
	ensure(j.Commit())
	j.FinishWriting()

	files := j.FileNames()
	deepEq(t, files, []string{"j000000000001-20240101T000000-0000000000000001.wal"})

	j.Eq(files[0], shdr("1.. 80_00_92_65 0...", "e984dc85563d5731"),
		"#10 #0 'hello",
		"#2 #0 'w",
		"#8 #1000 'orld",
		"7d_33_a6_68_73_e0_8f_ee",
	)
}

func shdr(inside, check string) string {
	return magic + " " + header1 + " " +
		inside + " " + header2 + " " + check
}

func deepEq[T any](t testing.TB, a, e T) bool {
	if !reflect.DeepEqual(a, e) {
		t.Helper()
		t.Errorf("** got %v, wanted %v", a, e)
		return false
	}
	return true
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func ensure(err error) {
	if err != nil {
		panic(err)
	}
}
