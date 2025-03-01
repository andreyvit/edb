package edb

import (
	"encoding/binary"
	"fmt"
)

const (
	valueFormatVer1      = 1
	valueFormatVerLatest = valueFormatVer1
)

type valueFlags uint64

const (
	vfVerBit0 = valueFlags(1 << iota)
	vfVerBit1
	vfVerBit2
	vfVerBit3
	vfCompressionBit0

	vfVerMask       = (vfVerBit0 | vfVerBit1 | vfVerBit2 | vfVerBit3)
	vfVer1          = vfVerBit0
	vfGzip          = vfCompressionBit0
	vfSupportedMask = (vfVer1 | vfGzip)
	vfDefault       = vfVer1

	minValueSize       = 5
	maxValueHeaderSize = binary.MaxVarintLen64 * 5
	maxSchemaVersion   = 32768 // just a sanity value, can be increased
)

func (vf valueFlags) ver() valueFlags {
	return vf & vfVerMask
}

func (vf valueFlags) encoding() encodingMethod {
	return MsgPack
}

type value struct {
	Flags     valueFlags
	SchemaVer uint64
	ModCount  uint64
	Data      []byte
	Index     []byte
}

func (vle value) ValueMeta() ValueMeta {
	return ValueMeta{
		SchemaVer: vle.SchemaVer,
		ModCount:  vle.ModCount,
	}
}

func reserveValueHeader(buf []byte) []byte {
	if len(buf) != 0 {
		panic("value must be written to an empty buffer")
	}
	return buf[:maxValueHeaderSize]
}

func putValueHeader(buf []byte, flags valueFlags, schemaVer uint64, modCount uint64, indexOff int) []byte {
	if indexOff > len(buf) {
		panic(fmt.Errorf("invalid indexOff=%d", indexOff)) // sanity check
	}
	if (flags &^ vfSupportedMask) != 0 {
		panic(fmt.Errorf("invalid flags %x", flags))
	}
	dataSize := indexOff - maxValueHeaderSize
	indexSize := len(buf) - indexOff

	var off = 0
	n := binary.PutUvarint(buf[off:], uint64(flags))
	off += n
	n = binary.PutUvarint(buf[off:], uint64(schemaVer))
	off += n
	n = binary.PutUvarint(buf[off:], uint64(modCount))
	off += n
	n = binary.PutUvarint(buf[off:], uint64(dataSize))
	off += n
	n = binary.PutUvarint(buf[off:], uint64(indexSize))
	off += n
	headerSize := off
	if headerSize > maxValueHeaderSize {
		panic("internal error")
	}
	if headerSize < maxValueHeaderSize {
		// move the header closer to data
		start := maxValueHeaderSize - headerSize
		copy(buf[start:maxValueHeaderSize], buf[:headerSize])
		return buf[start:]
	} else {
		return buf
	}
}

func briefRawValue(data []byte) ([]byte, error) {
	var vle value
	err := vle.decode(data, false)
	if err != nil {
		return nil, err
	}
	return data[:len(data)-len(vle.Index)], nil
}

// isBrief = true if data is expected to miss the index part
func (vle *value) decode(data []byte, isMemento bool) error {
	orig := data
	if len(data) < minValueSize {
		return dataErrf(orig, len(data)-len(orig), nil, "invalid value: at least %d bytes required", minValueSize)
	}

	v, n := binary.Uvarint(data)
	if n <= 0 {
		return dataErrf(orig, len(data)-len(orig), nil, "invalid value: bad flags")
	}
	if (v & ^uint64(vfSupportedMask)) != 0 {
		return dataErrf(orig, len(data)-len(orig), nil, "invalid value: unsupported flags %x", v)
	}
	vle.Flags, data = valueFlags(v), data[n:]

	v, n = binary.Uvarint(data)
	if n <= 0 || v > maxSchemaVersion {
		return dataErrf(orig, len(data)-len(orig), nil, "invalid value: bad schema version")
	}
	vle.SchemaVer, data = v, data[n:]

	v, n = binary.Uvarint(data)
	if n <= 0 {
		return dataErrf(orig, len(data)-len(orig), nil, "invalid value: bad mod count")
	}
	vle.ModCount, data = v, data[n:]

	dataSize, n := binary.Uvarint(data)
	if n <= 0 {
		return dataErrf(orig, len(data)-len(orig), nil, "invalid value: bad data size")
	}
	data = data[n:]

	indexSize, n := binary.Uvarint(data)
	if n <= 0 {
		return dataErrf(orig, len(data)-len(orig), nil, "invalid value: bad data size")
	}
	data = data[n:]

	if isMemento {
		if uint64(len(data)) != dataSize {
			return dataErrf(orig, len(data)-len(orig), nil, "invalid value: got %d bytes for memento data, expected %d bytes", len(data), dataSize)
		}
		vle.Data = data
	} else {
		expectedSize := dataSize + indexSize
		if uint64(len(data)) != expectedSize {
			return dataErrf(orig, len(data)-len(orig), nil, "invalid value: got %d bytes for data+index, expected %d bytes", len(data), expectedSize)
		}

		vle.Data, data = data[:dataSize], data[dataSize:]
		vle.Index, data = data[:indexSize], data[indexSize:]
	}
	return nil
}
