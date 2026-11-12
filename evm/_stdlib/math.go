package math

func Abs(x float64) float64
func Acos(x float64) float64
func Acosh(x float64) float64
func Asin(x float64) float64
func Asinh(x float64) float64
func Atan(x float64) float64
func Atan2(x float64, y float64) float64
func Atanh(x float64) float64
func Cbrt(x float64) float64
func Ceil(x float64) float64
func Copysign(x float64, y float64) float64
func Cos(x float64) float64
func Cosh(x float64) float64
func Dim(x float64, y float64) float64
func Erf(x float64) float64
func Erfc(x float64) float64
func Exp(x float64) float64
func Exp2(x float64) float64
func Expm1(x float64) float64
func Floor(x float64) float64
func FMA(x float64, y float64, z float64) float64
func Hypot(x float64, y float64) float64
func Inf(sign int) float64
func IsInf(f float64, sign int) bool
func IsNaN(f float64) bool
func Log(x float64) float64
func Log10(x float64) float64
func Log1p(x float64) float64
func Log2(x float64) float64
func Max(x float64, y float64) float64
func Min(x float64, y float64) float64
func Mod(x float64, y float64) float64
func Modf(f float64) (float64, float64)
func NaN() float64
func Nextafter(x float64, y float64) float64
func Pow(x float64, y float64) float64
func Pow10(n int) float64
func Remainder(x float64, y float64) float64
func Round(x float64) float64
func RoundToEven(x float64) float64
func Signbit(f float64) bool
func Sin(x float64) float64
func Sincos(f float64) (float64, float64)
func Sinh(x float64) float64
func Sqrt(x float64) float64
func Tan(x float64) float64
func Tanh(x float64) float64
func Trunc(x float64) float64
func Float32bits(f float32) uint32
func Float32frombits(b uint32) float32
func Float64bits(f float64) uint64
func Float64frombits(b uint64) float64

const (
	E                      = 2.71828182845904523536028747135266249775724709369995957496696763
	Pi                     = 3.14159265358979323846264338327950288419716939937510582097494459
	Phi                    = 1.61803398874989484820458683436563811772030917980576286213544862
	Sqrt2                  = 1.41421356237309504880168872420969807856967187537694807317667974
	SqrtE                  = 1.64872127070012814684865078781416357165377610071014801157507931
	SqrtPi                 = 1.77245385090551602729816748334114518279754945612238712821380779
	SqrtPhi                = 1.27201964951406896425242246173749149171560804184009624861664038
	Ln2                    = 0.693147180559945309417232121458176568075500134360255254120680009
	Log2E                  = 1 / Ln2
	Ln10                   = 2.30258509299404568401799145468436420760110148862877297603332790
	Log10E                 = 1 / Ln10
	MaxFloat32             = 0x1p127 * (1 + (1 - 0x1p-23))
	MaxFloat64             = 0x1p1023 * (1 + (1 - 0x1p-52))
	SmallestNonzeroFloat32 = 0x1p-126 * 0x1p-23
	SmallestNonzeroFloat64 = 0x1p-1022 * 0x1p-52
)
