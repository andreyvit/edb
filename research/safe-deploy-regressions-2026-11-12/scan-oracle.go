//go:build ignore

// Independent raw-bucket selection oracle, built unchanged in old/new Fire.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/fdb"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func emit(v any)           { must(json.NewEncoder(os.Stdout).Encode(v)) }

func parts(raw []byte) [][]byte {
	pop := func() (n uint64) {
		var shift uint
		for {
			if len(raw) == 0 || shift > 35 {
				panic("invalid tuple")
			}
			b := raw[len(raw)-1]
			raw = raw[:len(raw)-1]
			n |= uint64(b&127) << shift
			if b&128 == 0 {
				return
			}
			shift += 7
		}
	}
	n := int(pop())
	if n < 1 || n > 100 {
		panic("bad tuple arity")
	}
	lens := make([]int, n-1)
	for i := n - 2; i >= 0; i-- {
		lens[i] = int(pop())
	}
	out := make([][]byte, n)
	for i, k := range lens {
		if k > len(raw) {
			panic("tuple length")
		}
		out[i] = raw[:k]
		raw = raw[k:]
	}
	out[n-1] = raw
	return out
}
func pack(p [][]byte) []byte {
	var out []byte
	for _, b := range p {
		out = append(out, b...)
	}
	appendReverse := func(n uint64) {
		b := binary.AppendUvarint(nil, n)
		for i := len(b) - 1; i >= 0; i-- {
			out = append(out, b[i])
		}
	}
	for _, b := range p[:len(p)-1] {
		appendReverse(uint64(len(b)))
	}
	appendReverse(uint64(len(p)))
	return out
}
func equal(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
func prefix(a, b [][]byte) bool { return len(a) >= len(b) && equal(a[:len(b)], b) }

type entry struct {
	pk, payload []byte
	components  [][]byte
	index       reflect.Value
}

func main() {
	if len(os.Args) != 2 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("disposable sample path required")
	}
	db, err := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true})
	must(err)
	defer db.Close()
	checks, failures := 0, 0
	for _, tbl := range fdb.Schema.Tables() {
		for _, idx := range tbl.Indices() {
			var entries []entry
			db.Read(func(tx *edb.Tx) {
				b := tx.BoltTx().Bucket([]byte(tbl.Name())).Bucket([]byte("i_" + idx.ShortName()))
				must(b.ForEach(func(k, v []byte) error {
					comp := parts(bytes.Clone(k))
					var pk []byte
					if idx.IsUnique() {
						vals := parts(bytes.Clone(v))
						if len(vals) != 1 {
							panic("unique value arity")
						}
						pk = vals[0]
					} else {
						pk = comp[len(comp)-1]
						comp = comp[:len(comp)-1]
					}
					entries = append(entries, entry{pk, bytes.Join(comp, nil), comp, idx.DecodeKeyVal(pack(comp))})
					return nil
				}))
			})
			check := func(label string, opt edb.ScanOptions, accept func(entry) bool) {
				checks++
				wanted := make([]int, 0, len(entries))
				for i, e := range entries {
					if accept(e) {
						wanted = append(wanted, i)
					}
				}
				if opt.Reverse {
					for i, j := 0, len(wanted)-1; i < j; i, j = i+1, j-1 {
						wanted[i], wanted[j] = wanted[j], wanted[i]
					}
				}
				got, mismatch := 0, 0
				failure := ""
				func() {
					defer func() {
						if e := recover(); e != nil {
							failure = fmt.Sprintf("%T", e)
							fmt.Fprintf(os.Stderr, "%s/%s %s: %v\n", tbl.Name(), idx.ShortName(), label, e)
						}
					}()
					db.Read(func(tx *edb.Tx) {
						c := tx.IndexScan(idx, opt)
						for c.Next() {
							if got >= len(wanted) {
								mismatch++
							} else {
								e := entries[wanted[got]]
								if !bytes.Equal(c.RawKey(), e.pk) || !reflect.DeepEqual(c.IndexKey(), e.index.Interface()) {
									mismatch++
								}
							}
							got++
							if got > len(entries)+1 {
								panic("scan did not terminate")
							}
						}
					})
				}()
				ok := failure == "" && mismatch == 0 && got == len(wanted)
				if !ok {
					failures++
				}
				emit(map[string]any{"table": tbl.Name(), "index": idx.ShortName(), "case": label, "reverse": opt.Reverse, "expected": len(wanted), "got": got, "mismatches": mismatch, "error": failure, "ok": ok})
			}
			for _, rev := range []bool{false, true} {
				o := edb.FullScan()
				o.Reverse = rev
				check("full", o, func(entry) bool { return true })
			}
			if len(entries) == 0 {
				continue
			}
			for a := 0; a < 8; a++ {
				x := entries[a*(len(entries)-1)/7]
				y := entries[(a+3)%8*(len(entries)-1)/7]
				for _, rev := range []bool{false, true} {
					o := edb.ExactScanVal(x.index)
					o.Reverse = rev
					check(fmt.Sprintf("exact-%d", a), o, func(e entry) bool { return equal(e.components, x.components) })
					for n := 1; n <= len(x.components); n++ {
						o := edb.ExactScanVal(x.index).Prefix(n)
						o.Reverse = rev
						check(fmt.Sprintf("prefix-%d-%d", a, n), o, func(e entry) bool { return prefix(e.components, x.components[:n]) })
					}
					for flags := 0; flags < 4; flags++ {
						li, ui := flags&1 != 0, flags&2 != 0
						o := edb.RangeScanVal(x.index, y.index, li, ui)
						o.Reverse = rev
						check(fmt.Sprintf("range-%d-%d", a, flags), o, func(e entry) bool {
							l, u := bytes.Compare(e.payload, x.payload), bytes.Compare(e.payload, y.payload)
							return (l > 0 || l == 0 && li) && (u < 0 || u == 0 && ui)
						})
						if !idx.IsUnique() {
							o := edb.ExactIDRangeScanVal(x.index, tbl.DecodeKeyVal(x.pk), tbl.DecodeKeyVal(y.pk), li, ui)
							o.Reverse = rev
							check(fmt.Sprintf("exact-primary-%d-%d", a, flags), o, func(e entry) bool {
								l, u := bytes.Compare(e.pk, x.pk), bytes.Compare(e.pk, y.pk)
								return equal(e.components, x.components) && (l > 0 || l == 0 && li) && (u < 0 || u == 0 && ui)
							})
						}
					}
				}
			}
		}
	}
	emit(map[string]any{"finished": true, "checks": checks, "failures": failures})
}
