package row2

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"strconv"
	"unsafe"

	"github.com/andreyvit/edb/values"
)

// Encoding specifies how a typed runtime value maps to row2 payload bytes.
// Strings and immutable byte spans share Data; Float32 uses the historical
// float64 wire encoding, while runtime values retain float32 bits.
type Encoding uint8

const (
	Word Encoding = iota
	Signed
	Float32
	Data
	Timestamp
)

func (e Encoding) String() string {
	switch e {
	case Word:
		return "word"
	case Signed:
		return "signed"
	case Float32:
		return "float32"
	case Timestamp:
		return "timestamp"
	case Data:
		return "data"
	case rawEncoding:
		return "raw"
	default:
		return "Encoding(" + strconv.Itoa(int(e)) + ")"
	}
}

// Field is raw codec metadata, not a runtime value. Payloads borrow the source
// container; inline words retain their full width when copied without a model.
// Decode requires the schema's encoding. Copying unknown fields uses PutRaw.
type Field struct {
	ptr *byte
	num uint64
}

func rawWord(n uint64) Field { return Field{num: n} }
func payloadField(b []byte) Field {
	if len(b) == 0 {
		return Field{}
	}
	return Field{unsafe.SliceData(b), uint64(len(b))}
}
func (v Field) IsZero() bool { return v.num == 0 }
func (v Field) Payload() []byte {
	if v.ptr == nil {
		if v.num != 0 {
			panic(ErrType)
		}
		return nil
	}
	return unsafe.Slice(v.ptr, int(v.num))
}
func (v Field) Container() View { return Trusted(v.Payload()) }
func (v Field) Decode(enc Encoding) values.Value {
	if enc == Timestamp && v.ptr != nil && v.num > 8 {
		b := v.Payload()
		if len(b) == 12 {
			x := values.Bytes(b)
			x.AsTime()
			return x
		}
		// Read records written by the former RFC3339 timestamp representation.
		return values.Time(values.Bytes(b).AsTime())
	}
	if enc == Data {
		return values.Bytes(v.Payload())
	}
	n := v.num
	if v.ptr != nil {
		if v.num > 8 {
			panic(ErrType)
		}
		n = readWord(v.Payload())
		if enc == Signed && v.num > 0 {
			shift := uint(64 - 8*v.num)
			n = uint64(int64(n<<shift) >> shift)
		}
	}
	if enc == Float32 {
		f := math.Float64frombits(n)
		x := float32(f)
		if float64(x) != f && !math.IsNaN(f) {
			panic(ErrValue)
		}
		return values.Float32(x)
	}
	return values.Uint(n)
}
func wireWord(v values.Value, enc Encoding) uint64 {
	if enc == Float32 {
		return math.Float64bits(float64(v.AsFloat32()))
	}
	return v.R
}
func scalarWidth(n uint64, enc Encoding) int {
	if enc == Signed {
		n = n ^ uint64(int64(n)>>63)
		return (bits.Len64(n) + 1 + 7) / 8
	}
	return (bits.Len64(n) + 7) / 8
}

func readWord(b []byte) uint64 {
	switch len(b) {
	case 0:
		return 0
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	case 8:
		return binary.LittleEndian.Uint64(b)
	}
	if len(b) > 8 {
		panic(ErrFormat)
	}
	var u uint64
	for i, x := range b {
		u |= uint64(x) << (uint(i) * 8)
	}
	return u
}
func appendWord(dst []byte, u uint64, n int) []byte {
	switch n {
	case 0:
		return dst
	case 1:
		return append(dst, byte(u))
	case 2:
		return binary.LittleEndian.AppendUint16(dst, uint16(u))
	case 4:
		return binary.LittleEndian.AppendUint32(dst, uint32(u))
	case 8:
		return binary.LittleEndian.AppendUint64(dst, u)
	}
	for range n {
		dst = append(dst, byte(u))
		u >>= 8
	}
	return dst
}
func readUvarint(b []byte) (uint64, []byte) {
	u, n := binary.Uvarint(b)
	if n <= 0 || n != max(1, (bits.Len64(u)+6)/7) {
		panic(ErrFormat)
	}
	return u, b[n:]
}

// A reversed canonical varint is decoded from the end of the bounded object.
// It avoids reserving a fixed four/eight-byte count or moving the entries when
// the final count crosses a varint threshold.
func appendTail(dst []byte, u uint64) []byte {
	start := len(dst)
	dst = binary.AppendUvarint(dst, u)
	for i, j := start, len(dst)-1; i < j; i, j = i+1, j-1 {
		dst[i], dst[j] = dst[j], dst[i]
	}
	return dst
}
func readTail(b []byte) (uint64, []byte) {
	var u uint64
	for i := 0; i < 10 && i < len(b); i++ {
		x := b[len(b)-1-i]
		if i == 9 && x > 1 {
			panic(ErrFormat)
		}
		u |= uint64(x&127) << (uint(i) * 7)
		if x < 128 {
			if i != 0 && x == 0 {
				panic(ErrFormat)
			}
			return u, b[:len(b)-1-i]
		}
	}
	panic(ErrFormat)
}

var (
	ErrFormat = errors.New("row2: invalid row")
	ErrLayout = errors.New("row2: invalid layout")
	ErrKind   = errors.New("row2: wrong container kind")
	ErrType   = errors.New("row2: incompatible value")
	ErrOrder  = errors.New("row2: keys must be ordered and unique")
	ErrLimit  = errors.New("row2: container exceeds limit")
	ErrValue  = errors.New("row2: invalid input")
	ErrIndex  = errors.New("row2: index out of bounds")
)

const MaxEntries = 1 << 20
const MaxBytes = 64 << 20

// PayloadSize bounds a scalar or immutable byte-span payload before encoding.
func PayloadSize(v values.Value) uint64 {
	if v.P != nil {
		return v.R
	}
	return 8
}
