package journaltest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/andreyvit/edb/journal"
)

var Start = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

type TestJournal struct {
	*journal.Journal

	T   testing.TB
	FS  fstest.MapFS
	Dir string

	now time.Time
}

func Writable(t *testing.T, o journal.Options) *TestJournal {
	dir := t.TempDir()
	j := &TestJournal{
		T:   t,
		FS:  make(fstest.MapFS),
		Dir: dir,

		now: Start,
	}
	o.FileName = "j*.wal"
	o.Now = func() time.Time { return j.now }
	o.Logger = slog.New(slog.NewTextHandler(&logWriter{t}, &slog.HandlerOptions{
		AddSource: false,
		Level:     slog.LevelDebug,
	}))
	o.Verbose = true

	j.Journal = journal.New(dir, o)
	j.StartWriting()
	t.Cleanup(func() {
		err := j.FinishWriting()
		if err != nil {
			t.Error(err)
		}
	})
	return j
}

func (j *TestJournal) Eq(fileName string, expected ...string) {
	j.T.Helper()
	BytesEq(j.T, j.Data(fileName), Expand(expected...))
}

func (j *TestJournal) Put(fileName string, expected ...string) {
	ensure(os.WriteFile(filepath.Join(j.Dir, fileName), Expand(expected...), 0o644))
}

func (j *TestJournal) Data(fileName string) []byte {
	b, err := os.ReadFile(filepath.Join(j.Dir, fileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		j.T.Fatalf("when reading %v: %v", fileName, err)
	}
	return b
}

func (j *TestJournal) Now() time.Time {
	return j.now
}

func (j *TestJournal) Advance(d time.Duration) {
	j.now = j.now.Add(d)
}

func (j *TestJournal) FileNames() []string {
	var names []string
	for _, env := range must(os.ReadDir(j.Dir)) {
		names = append(names, env.Name())
	}
	slices.Sort(names)
	return names
}

type logWriter struct{ t testing.TB }

func (c *logWriter) Write(buf []byte) (int, error) {
	msg := string(buf)
	origLen := len(msg)
	msg = strings.TrimSuffix(msg, "\n")
	c.t.Log(msg)
	return origLen, nil
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

func Expand(specs ...string) []byte {
	var b []byte
	for _, spec := range specs {
		for _, elem := range strings.Fields(spec) {
			base, _, _ := strings.Cut(elem, "/") // comment
			if base == "" {
				continue
			}

			base, repStr, _ := strings.Cut(base, "*")

			rep := 1
			if repStr != "" {
				var err error
				rep, err = strconv.Atoi(repStr)
				if err != nil {
					panic(fmt.Sprintf("invalid repeat count %q in element %q", repStr, elem))
				}
			}

			base, right, padTo8 := strings.Cut(base, "...")
			var padTo4 bool
			if !padTo8 {
				base, right, padTo4 = strings.Cut(base, "..")
			}

			baseBytes, err := appendHexDecoding(nil, base)
			if err != nil {
				panic(fmt.Errorf("%w in element %q", err, elem))
			}

			rightBytes, err := appendHexDecoding(nil, right)
			if err != nil {
				panic(fmt.Errorf("%w in element %q", err, elem))
			}

			for range rep {
				b = append(b, baseBytes...)

				n := len(baseBytes) + len(rightBytes)
				if padTo8 && n < 8 {
					for range 8 - n {
						b = append(b, 0)
					}
				} else if padTo4 && n < 4 {
					for range 4 - n {
						b = append(b, 0)
					}
				}

				b = append(b, rightBytes...)
			}
		}
	}
	return b
}

func appendHexDecoding(data []byte, hex string) ([]byte, error) {
	const none byte = 0xFF

	if decimal, ok := strings.CutPrefix(hex, "#"); ok {
		v, err := strconv.ParseUint(decimal, 10, 64)
		if err != nil {
			return nil, err
		}
		return binary.AppendUvarint(data, v), nil
	} else if alpha, ok := strings.CutPrefix(hex, "'"); ok {
		return append(data, alpha...), nil
	}

	prev := none
	for _, b := range []byte(hex) {
		var half byte
		switch b {
		case '_', ' ':
			if prev != none {
				data = append(data, prev)
				prev = none
			}
			continue
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			half = b - '0'
		case 'a', 'b', 'c', 'd', 'e', 'f':
			half = b - 'a' + 10
		case 'A', 'B', 'C', 'D', 'E', 'F':
			half = b - 'A' + 10
		default:
			return nil, fmt.Errorf("invalid char '%c'", b)
		}
		if prev == none {
			prev = half
		} else {
			data = append(data, prev<<4|half)
			prev = none
		}
	}
	if prev != none {
		data = append(data, prev)
	}
	return data, nil
}

func HexDump(b []byte, highlightOff int) string {
	var buf strings.Builder
	var off int
	n := len(b)
	for {
		fmt.Fprintf(&buf, "%08x", off)
		if off >= n {
			buf.WriteByte('\n')
			break
		}
		buf.WriteByte(' ')
		for i := range 8 {
			if off+i >= n {
				buf.WriteByte(' ')
				buf.WriteByte(' ')
				buf.WriteByte(' ')
			} else {
				if highlightOff >= 0 && off+i == highlightOff {
					buf.WriteByte('>')
				} else {
					buf.WriteByte(' ')
				}
				fmt.Fprintf(&buf, "%02x", b[off+i])
			}
		}
		buf.WriteByte(' ')
		buf.WriteByte(' ')
		buf.WriteByte('|')
		for i := range 8 {
			if off+i < n {
				v := b[off+i]
				if v >= 32 && v <= 126 {
					buf.WriteByte(v)
				} else {
					buf.WriteByte('.')
				}
			}
		}
		off += 8
		buf.WriteByte('|')
		buf.WriteByte('\n')
		if off >= n {
			break
		}
	}
	return buf.String()
}

func BytesEq(t testing.TB, a, e []byte) bool {
	if !bytes.Equal(a, e) {
		an, en := len(a), len(e)
		off := min(an, en)
		for i := range min(an, en) {
			if a[i] != e[i] {
				off = i
				break
			}
		}

		t.Helper()
		t.Errorf("** got:\n%v\nwanted:\n%v\nfirst difference offset: 0x%x (%d)", HexDump(a, off), HexDump(e, off), off, off)
		return false
	}
	return true
}
