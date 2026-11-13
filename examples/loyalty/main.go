// This example uses a temporary database; it leaves no files behind.
package main

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/andreyvit/edb"
	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/gox"
	"github.com/andreyvit/edb/values"
)

// Guest Go files live under testdata so the native Go compiler leaves them alone.
//
//go:embed testdata/bonus.go
var bonusSource string

//go:embed testdata/card.gox
var cardSource string

func main() {
	if err := run(context.Background(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, out io.Writer) error {
	// Register and compile once. A linked program can serve many executions.
	models, err := evm.ModelPackage("m", Models)
	if err != nil {
		return err
	}
	module, err := evmcompiler.Compile("bonus.go", bonusSource, models)
	if err != nil {
		return err
	}
	program, err := evm.Link(module, models)
	if err != nil {
		return err
	}
	vm := evm.New(program)

	template, err := gox.Compile("card.gox", cardSource, models)
	if err != nil {
		return err
	}
	card, err := template.Link(models)
	if err != nil {
		return err
	}

	dir, err := os.MkdirTemp("", "edb-loyalty-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	db, err := edb.Open(filepath.Join(dir, "customers.db"), DBSchema, edb.Options{})
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Tx(edb.TxMetadata{}, true, func(tx *edb.Tx) error {
		CustomerRows.Put(tx, &Customer{
			ID: 101, FirstName: "Alice & Bob", Email: "alice@example.com",
			Tags: []string{"vip", "newsletter"}, PointsEarned: 1250, PointsRedeemed: 250,
		})
		CustomerRows.Put(tx, &Customer{
			ID: 102, FirstName: "Maria", Email: "maria@example.com",
			Tags: []string{"newsletter"}, PointsEarned: 300, PointsRedeemed: 100,
		})
		CustomerRows.Put(tx, &Customer{
			ID: 103, FirstName: "Sam", Email: "sam@example.com",
			Tags: []string{"vip"}, PointsEarned: 800, PointsRedeemed: 300,
		})
		return nil
	}); err != nil {
		return err
	}

	if err := db.Tx(edb.TxMetadata{}, false, func(tx *edb.Tx) error {
		id, customer := CustomerRows.Lookup(tx, customersByEmailIndex, "alice@example.com")
		if customer == nil {
			return fmt.Errorf("customer not found")
		}
		fmt.Fprintf(out, "%s: %d points\n", customer.FirstName, customer.PointsEarned-customer.PointsRedeemed)
		fmt.Fprintf(out, "VIP balance (Go structs): %d\n", balanceFromStructs(tx, "vip"))
		fmt.Fprintf(out, "VIP balance (KVO): %d\n", balanceFromRecords(tx, "vip"))

		// The VM and renderer read this record while its transaction is open.
		r := Customers.Get(tx, id)
		args := []values.Value{r.Value()}
		var result [1]values.Value
		if err := vm.RunWithBudget(ctx, "BonusPoints", args, result[:], evm.Budget{Steps: 10_000}); err != nil {
			return err
		}
		fmt.Fprintf(out, "VIP bonus (EVM): %d\n", result[0].AsInt())
		html, err := card.Render(ctx, args, gox.RenderOptions{Strict: true})
		if err != nil {
			return err
		}
		fmt.Fprintln(out, html)
		return nil
	}); err != nil {
		return err
	}

	if err := redeem(db, 101, 200); err != nil {
		return err
	}
	return db.Tx(edb.TxMetadata{}, false, func(tx *edb.Tx) error {
		fmt.Fprintf(out, "After redeeming 200 points: %d\n", PointBalance(Customers.Get(tx, 101)))
		return nil
	})
}
