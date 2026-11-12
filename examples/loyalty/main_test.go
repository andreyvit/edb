package main

import (
	"context"
	"os"
)

// This exercises the README's database, KVO, EVM and GoX walkthrough together.
func Example() {
	if err := run(context.Background(), os.Stdout); err != nil {
		panic(err)
	}
	// Output:
	// Alice & Bob: 1000 points
	// VIP balance (Go structs): 1500
	// VIP balance (KVO): 1500
	// VIP bonus (EVM): 100
	// <section class="loyalty-card"><h2>Alice &amp; Bob</h2><p>1000 points available</p><span class="tag">vip</span><span class="tag">newsletter</span></section>
	// After redeeming 200 points: 800
}
