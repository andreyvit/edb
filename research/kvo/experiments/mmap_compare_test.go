package kvolab

// These read real mmap-backed Bolt values. Heap-array cache controls elsewhere
// remain useful, but cannot substitute for row placement inside Bolt pages.
import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/andreyvit/edb/kvo"
	draft "github.com/andreyvit/edb/research/kvo/draft"
	"go.etcd.io/bbolt"
)

func mmapFixture(name string) Value {
	switch name {
	case "local128":
		return numeric(128, 0)
	case "global128":
		return numeric(128, 1<<50)
	case "nested":
		v := Obj()
		for i := 0; i < 8; i++ {
			v.Fields = append(v.Fields, Field{uint64(i + 1), numeric(16, 0)})
		}
		return v
	default:
		panic(name)
	}
}
func mmapEncode(source Value, codec string) []byte {
	switch codec {
	case "immutable":
		return oldKVO(source).Bytes()
	case "draft-indexed":
		return draftObject(source, draft.Indexed, draft.Indexed)
	case "indexed-all", "indexed-root", "compact":
		var w lengthWriter
		return lengthEncode(&w, nil, source, func(depth int, _ Value) bool {
			return codec == "indexed-all" || codec == "indexed-root" && depth == 0
		})
	default:
		panic(codec)
	}
}
func mmapRead(raw []byte, codec, fixture string, q uint64) uint64 {
	key := q&127 + 1
	if fixture == "global128" {
		key += 1 << 50
	}
	if fixture == "nested" {
		parent, child := q&7+1, q>>3&15+1
		switch codec {
		case "immutable":
			return kvo.LoadRecord(raw, nil).Root().GetMap(parent).Get(child)
		case "draft-indexed":
			return draft.Trusted(draft.Trusted(raw).Get(parent).Bytes()).Get(child).Uint()
		default:
			return lengthTrusted(raw).Get(parent).Object().Get(child).Uint()
		}
	}
	switch codec {
	case "immutable":
		return kvo.LoadRecord(raw, nil).Root().Get(key)
	case "draft-indexed":
		return draft.Trusted(raw).Get(key).Uint()
	default:
		return lengthTrusted(raw).Get(key).Uint()
	}
}

type mmapDataset struct {
	db                             *bbolt.DB
	tx                             *bbolt.Tx
	bucket                         *bbolt.Bucket
	rows                           [][]byte // Borrowed from tx, never copied into Go-heap payloads.
	keys                           [][8]byte
	rowBytes, fileBytes, unaligned int
}

func newMmapDataset(t testing.TB, data []byte, rows int) *mmapDataset {
	t.Helper()
	path := filepath.Join(t.TempDir(), "read-fixture.db")
	db, err := bbolt.Open(path, 0600, &bbolt.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([][8]byte, rows)
	err = db.Update(func(tx *bbolt.Tx) error {
		bucket, e := tx.CreateBucket([]byte("rows"))
		if e != nil {
			return e
		}
		for i := range keys {
			binary.BigEndian.PutUint64(keys[i][:], uint64(i))
			if e = bucket.Put(keys[i][:], data); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen read-only: every value must come from the file mapping, not an
	// uncommitted writable node or a heap copy cached by this benchmark.
	db, err = bbolt.Open(path, 0600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	bucket := tx.Bucket([]byte("rows"))
	d := &mmapDataset{db: db, tx: tx, bucket: bucket, keys: keys, rowBytes: len(data), rows: make([][]byte, rows)}
	t.Cleanup(func() {
		if e := tx.Rollback(); e != nil {
			t.Error(e)
		}
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	d.fileBytes = int(info.Size())
	for i := range keys {
		d.rows[i] = bucket.Get(keys[i][:])
		if len(d.rows[i]) != len(data) {
			t.Fatal("row length")
		}
		if uintptr(unsafe.Pointer(unsafe.SliceData(d.rows[i])))&7 != 0 {
			d.unaligned++
		}
	}
	return d
}
func TestMmapComparison(t *testing.T) {
	for _, fixture := range []string{"local128", "global128", "nested"} {
		source := mmapFixture(fixture)
		for _, codec := range []string{"immutable", "draft-indexed", "indexed-all", "indexed-root", "compact"} {
			t.Run(fixture+"/"+codec, func(t *testing.T) {
				data := mmapEncode(source, codec)
				d := newMmapDataset(t, data, 16)
				for q := uint64(0); q < 128; q++ {
					var f Field
					if fixture == "nested" {
						f = source.Fields[q&7].Value.Fields[q>>3&15]
					} else {
						f = source.Fields[q&127]
					}
					if got := mmapRead(d.rows[q&15], codec, fixture, q); got != f.Value.U {
						t.Fatalf("got %d want %d", got, f.Value.U)
					}
				}
				t.Logf("row=%d file=%d misaligned=%d/%d", d.rowBytes, d.fileBytes, d.unaligned, len(d.rows))
			})
		}
	}
}
func BenchmarkMmapRead(b *testing.B) {
	for _, fixture := range []string{"local128", "global128", "nested"} {
		source := mmapFixture(fixture)
		for _, rows := range []int{1, 32768} {
			codecs := []string{"immutable", "draft-indexed", "indexed-all", "compact"}
			if fixture == "nested" {
				codecs = append(codecs, "indexed-root")
			}
			for _, codec := range codecs {
				b.Run(fmt.Sprintf("%s/rows%d/%s", fixture, rows, codec), func(b *testing.B) {
					data := mmapEncode(source, codec)
					d := newMmapDataset(b, data, rows)
					for _, op := range []string{"navigation", "bucket", "transaction"} {
						b.Run(op, func(b *testing.B) {
							b.ReportAllocs()
							b.ResetTimer()
							var sum uint64
							rng := uint64(0xdedabcde12345678)
							for i := 0; i < b.N; i++ {
								rng ^= rng << 13
								rng ^= rng >> 7
								rng ^= rng << 17
								row := int(rng>>16) & (rows - 1)
								switch op {
								case "navigation":
									sum += mmapRead(d.rows[row], codec, fixture, rng)
								case "bucket":
									sum += mmapRead(d.bucket.Get(d.keys[row][:]), codec, fixture, rng)
								case "transaction":
									err := d.db.View(func(tx *bbolt.Tx) error {
										sum += mmapRead(tx.Bucket([]byte("rows")).Get(d.keys[row][:]), codec, fixture, rng)
										return nil
									})
									if err != nil {
										b.Fatal(err)
									}
								}
							}
							sinkU = sum
							b.ReportMetric(float64(d.rowBytes), "row-B")
							b.ReportMetric(float64(rows*d.rowBytes), "payload-B")
							b.ReportMetric(float64(d.fileBytes), "file-B")
							b.ReportMetric(float64(d.unaligned)*100/float64(rows), "unaligned-percent")
						})
					}
				})
			}
		}
	}
}
