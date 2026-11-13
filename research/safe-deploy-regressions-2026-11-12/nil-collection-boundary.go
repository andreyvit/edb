//go:build ignore

// Synthetic invalid-input boundary: indexer rejection versus later consumers.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/andreyvit/edb"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/bm/mcoupon"
	"github.com/prairiegroupinc/fire/fdb"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func caught(fn func()) (result string) {
	defer func() {
		if err := recover(); err != nil {
			result = fmt.Sprintf("%T", err)
		}
	}()
	fn()
	return
}
func main() {
	if len(os.Args) != 2 || !strings.Contains(os.Args[1], "/fire-edb-regressions-") {
		panic("fresh private disposable DB required")
	}
	if _, err := os.Stat(os.Args[1]); !os.IsNotExist(err) {
		panic("database already exists")
	}
	db, err := edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true})
	must(err)
	defer func() { db.Close() }()
	product := &bm.Product{ID: 1, TenantID: 1, ExtID: "audit", Variants: []*bm.ProductVariant{{ID: 2, ExtID: "audit-variant"}}}
	coupon := &bm.ExtraCoupon{ID: 3, TenantID: 1, CustomerID: 4, Coupons: mcoupon.CouponSet{{Code: "audit"}}}
	db.Write(edb.TxMetadata{}, func(tx *edb.Tx) { edb.Put(tx, product, coupon) })
	for _, kind := range []string{"product", "coupon"} {
		writeError := caught(func() {
			db.Write(edb.TxMetadata{}, func(tx *edb.Tx) {
				if kind == "product" {
					row := edb.Get[bm.Product](tx, product.ID)
					row.Variants = []*bm.ProductVariant{nil}
					edb.Put(tx, row)
				} else {
					row := edb.Get[bm.ExtraCoupon](tx, coupon.ID)
					row.Coupons = mcoupon.CouponSet{nil}
					edb.Put(tx, row)
				}
			})
		})
		db.Close()
		db, err = edb.Open(os.Args[1], fdb.Schema, edb.Options{IsTesting: true})
		must(err)
		var storedNil bool
		consumerError := caught(func() {
			db.Read(func(tx *edb.Tx) {
				if kind == "product" {
					row := edb.Get[bm.Product](tx, product.ID)
					storedNil = len(row.Variants) == 1 && row.Variants[0] == nil
					_ = row.Variant(2)
				} else {
					row := edb.Get[bm.ExtraCoupon](tx, coupon.ID)
					storedNil = len(row.Coupons) == 1 && row.Coupons[0] == nil
					_ = row.Coupons.MainCoupon()
				}
			})
		})
		must(json.NewEncoder(os.Stdout).Encode(map[string]any{"kind": kind, "write_error": writeError, "stored_nil": storedNil, "consumer_error": consumerError}))
	}
}
