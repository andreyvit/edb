//go:build ignore

// Hash complete unregistered buckets before/after EDB opens a private database.
// No native-codec coverage is claimed for these obsolete values.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"go.etcd.io/bbolt"
)

func main() {
	if len(os.Args) != 3 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("private database and bucket names required")
	}
	db, e := bbolt.Open(os.Args[1], 0600, &bbolt.Options{ReadOnly: true})
	if e != nil {
		panic(e)
	}
	defer db.Close()
	e = db.View(func(tx *bbolt.Tx) error {
		for _, name := range strings.Split(os.Args[2], ",") {
			h := sha256.New()
			rows := 0
			total := uint64(0)
			largest := 0
			var visit func(*bbolt.Bucket) error
			visit = func(b *bbolt.Bucket) error {
				if b == nil {
					panic("missing selected bucket")
				}
				return b.ForEach(func(k, v []byte) error {
					h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(k))))
					h.Write(k)
					if v == nil {
						h.Write([]byte{1})
						return visit(b.Bucket(k))
					}
					h.Write([]byte{0})
					h.Write(binary.BigEndian.AppendUint64(nil, uint64(len(v))))
					h.Write(v)
					rows++
					total += uint64(len(v))
					largest = max(largest, len(v))
					return nil
				})
			}
			if e := visit(tx.Bucket([]byte(name))); e != nil {
				return e
			}
			if e := json.NewEncoder(os.Stdout).Encode(map[string]any{"bucket": name, "rows_including_metadata": rows, "value_bytes": total, "largest_value_bytes": largest, "sha256": fmt.Sprintf("%x", h.Sum(nil))}); e != nil {
				return e
			}
		}
		return nil
	})
	if e != nil {
		panic(e)
	}
}
