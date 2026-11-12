package evm

import "math"

type NumericKind uint8

const (
	NumericSigned NumericKind = iota + 1
	NumericUnsigned
	NumericFloat
)

type NumericType struct {
	Kind NumericKind
	Bits uint8
}
type Conversion struct{ From, To NumericType }

func (t NumericType) valid() bool {
	if t.Kind == NumericFloat {
		return t.Bits == 32 || t.Bits == 64
	}
	return (t.Kind == NumericSigned || t.Kind == NumericUnsigned) && (t.Bits == 8 || t.Bits == 16 || t.Bits == 32 || t.Bits == 64)
}
func (t NumericType) normalize(x uint64) uint64 {
	if t.Bits == 64 {
		return x
	}
	x &= (uint64(1) << t.Bits) - 1
	if t.Kind == NumericSigned {
		return uint64(int64(x<<(64-t.Bits)) >> (64 - t.Bits))
	}
	return x
}
func (c Conversion) convert(x uint64) uint64 {
	if c.From.Kind == NumericFloat {
		var v float64
		if c.From.Bits == 32 {
			v = float64(math.Float32frombits(uint32(x)))
		} else {
			v = math.Float64frombits(x)
		}
		switch c.To.Kind {
		case NumericSigned:
			return c.To.normalize(uint64(int64(v)))
		case NumericUnsigned:
			return c.To.normalize(uint64(v))
		case NumericFloat:
			if c.To.Bits == 32 {
				return uint64(math.Float32bits(float32(v)))
			}
			return math.Float64bits(v)
		}
	}
	x = c.From.normalize(x)
	if c.To.Kind == NumericFloat {
		if c.To.Bits == 32 {
			if c.From.Kind == NumericSigned {
				return uint64(math.Float32bits(float32(int64(x))))
			}
			return uint64(math.Float32bits(float32(x)))
		}
		var v float64
		if c.From.Kind == NumericSigned {
			v = float64(int64(x))
		} else {
			v = float64(x)
		}
		return math.Float64bits(v)
	}
	return c.To.normalize(x)
}
