//go:build evmguest

package fixture

func Roundtrip(ctx Context, a, b *Row) (Context, *Row, Row)

func Pair() (*Row, *Row)

func Enabled(n Count) bool
func Name(prefix Label, n Count) Label
func Lookup(id Count) *Row
func Ratio(x float32) float64
func Mark(n Count)
func Marked() Count
