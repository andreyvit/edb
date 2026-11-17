package edb

import (
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestDescribeOpenTxnsMetadataAndOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := observationOpen(t, true, false, nil)
		oldest := db.BeginRead(TxMetadata{Kind: "system:oldest", Tenant: "first"})
		defer oldest.Close()
		time.Sleep(100 * time.Millisecond)
		removed := db.BeginRead(TxMetadata{})
		defer removed.Close()
		time.Sleep(100 * time.Millisecond)
		writer := db.BeginUpdate(TxMetadata{Kind: "system:writer", Tenant: "second"})
		defer writer.Close()
		time.Sleep(100 * time.Millisecond)
		youngest := db.BeginRead(TxMetadata{})
		defer youngest.Close()
		removed.Close()

		want := "3 OPEN TRANSACTIONS:\n" +
			"\n---\nopen for 300 ms (read) Kind=system:oldest Tenant=first\n" +
			"\n---\nopen for 100 ms (write) Kind=system:writer Tenant=second\n" +
			"\n---\nopen for 0 ms (read) Kind=system:unknown Tenant=unknown\n"
		if got := db.DescribeOpenTxns(); got != want {
			t.Fatalf("report = %q, wanted %q", got, want)
		}
		if youngest.Metadata() != (TxMetadata{}) {
			t.Fatal("report changed read metadata")
		}
		oldest.Close()
		writer.Close()
		youngest.Close()
		if got := db.DescribeOpenTxns(); got != "NO OPEN TRANSACTIONS" {
			t.Fatalf("report after cleanup = %q", got)
		}
	})
}

func TestDescribeOpenTxnsFallback(t *testing.T) {
	for _, test := range []struct {
		metadata TxMetadata
		want     string
	}{
		{TxMetadata{}, "Kind=system:unknown Tenant=unknown"},
		{TxMetadata{Kind: "system:read"}, "Kind=system:read Tenant=unknown"},
		{TxMetadata{Tenant: "shop"}, "Kind=system:unknown Tenant=shop"},
		{TxMetadata{Kind: "system:read", Tenant: "none"}, "Kind=system:read Tenant=none"},
		{TxMetadata{Kind: "system:read", Tenant: "mixed"}, "Kind=system:read Tenant=mixed"},
	} {
		for _, writable := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/write=%v", test.want, writable), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					db := observationOpen(t, true, false, nil)
					var tx *Tx
					mode := "read"
					if writable {
						tx = db.BeginUpdate(test.metadata)
						mode = "write"
					} else {
						tx = db.BeginRead(test.metadata)
					}
					defer tx.Close()
					want := fmt.Sprintf("1 OPEN TRANSACTIONS:\n\n---\nopen for 0 ms (%s) %s\n", mode, test.want)
					if got := db.DescribeOpenTxns(); got != want {
						t.Fatalf("report = %q, wanted %q", got, want)
					}
				})
			})
		}
	}
}

func TestDescribeOpenTxnsOptionalStack(t *testing.T) {
	for _, capture := range []bool{false, true} {
		t.Run(fmt.Sprintf("capture=%v", capture), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				db, err := Open(InMemory, basicSchema, Options{IsTesting: true, CaptureTxStack: capture})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				tx := db.BeginRead(TxMetadata{Kind: "system:stack", Tenant: "shop"})
				defer tx.Close()
				if capture && !strings.Contains(string(tx.stack), "TestDescribeOpenTxnsOptionalStack") {
					t.Fatalf("missing origin stack: %s", tx.stack)
				}
				for _, age := range []time.Duration{0, 99 * time.Millisecond, 100 * time.Millisecond, 101 * time.Millisecond} {
					time.Sleep(age - time.Since(tx.StartTime()))
					want := fmt.Sprintf("1 OPEN TRANSACTIONS:\n\n---\nopen for %d ms (read) Kind=system:stack Tenant=shop", age.Milliseconds())
					if capture && age >= 100*time.Millisecond {
						want += ":\n" + string(tx.stack)
					} else {
						want += "\n"
					}
					if got := db.DescribeOpenTxns(); got != want {
						t.Fatalf("report at %v = %q, wanted %q", age, got, want)
					}
				}
			})
		})
	}
}
