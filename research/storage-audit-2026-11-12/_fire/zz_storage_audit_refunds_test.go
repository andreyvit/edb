package fdb

import (
	"fmt"
	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/kvo"
	"github.com/andreyvit/edb/kvo/codec/msgpack"
	"github.com/prairiegroupinc/fire/bm"
	"github.com/prairiegroupinc/fire/bpub"
	"github.com/prairiegroupinc/fire/bubbleflake"
	"github.com/prairiegroupinc/fire/nm"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestDeepRefundJoinAudit(t *testing.T) {
	rng := rand.New(rand.NewPCG(67, 28))
	cases := 0
	for scenario := 0; scenario < 256; scenario++ {
		db, err := edb.Open(edb.InMemory, Schema, edb.Options{IsTesting: true})
		if err != nil {
			t.Fatal(err)
		}
		src := &bm.Order{ID: 42, TenantID: 4, CustomerID: 5, ObsoleteIsPointsUsedRefunded: true, PtsRedeemedDirectly: int64(rng.IntN(100)), PointsUsed: map[bm.ChangeID]bm.Points{}, RedemptionRefunds: map[bm.ChangeID]bm.OrderRedemptionRefund{}}
		for i := range 5 {
			id := bubbleflake.ID(i + 1)
			src.PointsUsed[id] = int64(rng.IntN(100)) - 20
			src.RedemptionRefunds[id] = bm.OrderRedemptionRefund{AmountApplied: 17, AmountRefunded: 3, PointsRefunded: int64(rng.IntN(100))}
		}
		db.Write(func(tx *edb.Tx) {
			for i := range 30 {
				r := &bm.PointBalanceChange{ID: bubbleflake.ID(i + 100), TenantID: 4, CustomerID: 5, OrderID: 42, Type: bpub.ChangeTypePointsRefundRedeemedTowardsOrder, RelatedChangeID: bubbleflake.ID(rng.IntN(8)), Delta: int64(rng.IntN(100)) - 20}
				switch rng.IntN(8) {
				case 0:
					r.TenantID = 9
				case 1:
					r.CustomerID = 9
				case 2:
					r.Reverted = true
				case 3:
					r.Type = bpub.ChangeType(0)
				case 4:
					r.OrderID = 43
				}
				if scenario%11 == 0 && i%5 == 0 {
					r.Delta = math.MaxInt64
				}
				edb.Put(tx, r)
			}
		})
		p := kvo.Project(reflect.TypeFor[bm.Order](), nm.MOrder, nil)
		raw := msgpack.Encode(nil, reflect.ValueOf(src))
		for _, format := range []string{"native", "MessagePack"} {
			cases++
			db.Read(func(tx *edb.Tx) {
				want, got := new(bm.Order), new(bm.Order)
				msgpack.Decode(raw, reflect.ValueOf(want))
				want.ID = src.ID
				want.UpgradeToV4(auditOrderChanges(tx, src.ID))
				input := new(bm.Order)
				msgpack.Decode(raw, reflect.ValueOf(input))
				input.ID = src.ID
				r := p.Record(reflect.ValueOf(input))
				if format == "MessagePack" {
					r = p.MessagePack(raw)
				}
				e := r.Edit()
				migrateOrderRefunds(tx, src.ID, e)
				data := e.MessagePack()
				if data == nil {
					data = msgpack.Encode(nil, reflect.ValueOf(e.Record().Native()))
				}
				msgpack.Decode(data, reflect.ValueOf(got))
				got.ID = src.ID
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("refund join scenario=%d %s: %s", scenario, format, auditDifference(reflect.ValueOf(want).Elem(), reflect.ValueOf(got).Elem(), ""))
				}
			})
		}
		db.Close()
	}
	fmt.Printf("refund join audit: cases=%d failures=0\n", cases)
}
