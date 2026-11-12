package fixture

//go:generate go run ../generate

type Count int32
type Flag bool
type Child struct{ Number int64 }
type Row struct {
	Count    Count
	Flag     Flag
	Text     string
	Fraction float32
	Ratio    float64
	Child    Child
	Next     *Row
	Items    []int
	Labels   map[string]int
	hidden   int
}
