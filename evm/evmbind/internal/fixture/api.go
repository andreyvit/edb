package fixture

type Count int

const Start Count = 2
const Huge = 1 << 100

type Detail struct{ Text string }
type Row struct {
	Next    *Row
	Counter *Count
	Detail  Detail
	Count   Count
	Active  bool
	Ratio   float32
	Amount  float64
	Bits    uint16
}

func (r *Row) Bump(n Count) Count { r.Count += n; return r.Count }
func (r *Row) Mark(n Count)       { r.Count = n }

type Observer interface {
	During(func())
	Observe([]string, map[string]any)
	Mark()
	ResetMark()
}

type Context interface {
	Observer
	Tenant() string
	Credit(int) int
}

type Label string

func (r *Row) Tagged(s Label) bool { return r != nil && r.Detail.Text == string(s) }
func (r *Row) Choose(other *Row) *Row {
	if r == nil {
		return other
	}
	r.Count++
	if other == nil {
		return r
	}
	other.Count += 2
	return other
}
