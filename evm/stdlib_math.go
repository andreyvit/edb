package evm

import (
	"fmt"
	"github.com/andreyvit/edb/values"
	"math"
)

func standardMath(c *Call) error {
	switch c.native.Name {
	case "Abs":
		v0 := math.Abs(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Acos":
		v0 := math.Acos(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Acosh":
		v0 := math.Acosh(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Asin":
		v0 := math.Asin(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Asinh":
		v0 := math.Asinh(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Atan":
		v0 := math.Atan(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Atan2":
		v0 := math.Atan2(c.argument(0).AsFloat(), c.argument(1).AsFloat())
		c.result(0, values.Float(v0))
	case "Atanh":
		v0 := math.Atanh(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Cbrt":
		v0 := math.Cbrt(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Ceil":
		v0 := math.Ceil(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Copysign":
		v0 := math.Copysign(c.argument(0).AsFloat(), c.argument(1).AsFloat())
		c.result(0, values.Float(v0))
	case "Cos":
		v0 := math.Cos(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Cosh":
		v0 := math.Cosh(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Dim":
		v0 := math.Dim(c.argument(0).AsFloat(), c.argument(1).AsFloat())
		c.result(0, values.Float(v0))
	case "Erf":
		v0 := math.Erf(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Erfc":
		v0 := math.Erfc(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Exp":
		v0 := math.Exp(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Exp2":
		v0 := math.Exp2(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Expm1":
		v0 := math.Expm1(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Floor":
		v0 := math.Floor(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "FMA":
		v0 := math.FMA(c.argument(0).AsFloat(), c.argument(1).AsFloat(), c.argument(2).AsFloat())
		c.result(0, values.Float(v0))
	case "Hypot":
		v0 := math.Hypot(c.argument(0).AsFloat(), c.argument(1).AsFloat())
		c.result(0, values.Float(v0))
	case "Inf":
		v0 := math.Inf(int(c.argument(0).R))
		c.result(0, values.Float(v0))
	case "IsInf":
		v0 := math.IsInf(c.argument(0).AsFloat(), int(c.argument(1).R))
		c.result(0, values.Value{R: boolWord(v0)})
	case "IsNaN":
		v0 := math.IsNaN(c.argument(0).AsFloat())
		c.result(0, values.Value{R: boolWord(v0)})
	case "Log":
		v0 := math.Log(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Log10":
		v0 := math.Log10(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Log1p":
		v0 := math.Log1p(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Log2":
		v0 := math.Log2(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Max":
		v0 := math.Max(c.argument(0).AsFloat(), c.argument(1).AsFloat())
		c.result(0, values.Float(v0))
	case "Min":
		v0 := math.Min(c.argument(0).AsFloat(), c.argument(1).AsFloat())
		c.result(0, values.Float(v0))
	case "Mod":
		v0 := math.Mod(c.argument(0).AsFloat(), c.argument(1).AsFloat())
		c.result(0, values.Float(v0))
	case "Modf":
		v0, v1 := math.Modf(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
		c.result(1, values.Float(v1))
	case "NaN":
		v0 := math.NaN()
		c.result(0, values.Float(v0))
	case "Nextafter":
		v0 := math.Nextafter(c.argument(0).AsFloat(), c.argument(1).AsFloat())
		c.result(0, values.Float(v0))
	case "Pow":
		v0 := math.Pow(c.argument(0).AsFloat(), c.argument(1).AsFloat())
		c.result(0, values.Float(v0))
	case "Pow10":
		v0 := math.Pow10(int(c.argument(0).R))
		c.result(0, values.Float(v0))
	case "Remainder":
		v0 := math.Remainder(c.argument(0).AsFloat(), c.argument(1).AsFloat())
		c.result(0, values.Float(v0))
	case "Round":
		v0 := math.Round(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "RoundToEven":
		v0 := math.RoundToEven(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Signbit":
		v0 := math.Signbit(c.argument(0).AsFloat())
		c.result(0, values.Value{R: boolWord(v0)})
	case "Sin":
		v0 := math.Sin(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Sincos":
		v0, v1 := math.Sincos(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
		c.result(1, values.Float(v1))
	case "Sinh":
		v0 := math.Sinh(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Sqrt":
		v0 := math.Sqrt(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Tan":
		v0 := math.Tan(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Tanh":
		v0 := math.Tanh(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Trunc":
		v0 := math.Trunc(c.argument(0).AsFloat())
		c.result(0, values.Float(v0))
	case "Float32bits":
		v0 := math.Float32bits(math.Float32frombits(uint32(c.argument(0).R)))
		c.result(0, values.Value{R: uint64(v0)})
	case "Float32frombits":
		v0 := math.Float32frombits(uint32(c.argument(0).R))
		c.result(0, values.Value{R: uint64(math.Float32bits(v0))})
	case "Float64bits":
		v0 := math.Float64bits(c.argument(0).AsFloat())
		c.result(0, values.Value{R: uint64(v0)})
	case "Float64frombits":
		v0 := math.Float64frombits(uint64(c.argument(0).R))
		c.result(0, values.Float(v0))
	default:
		return fmt.Errorf("unknown math primitive")
	}
	return nil
}
