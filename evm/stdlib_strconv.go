package evm

import (
	"fmt"
	"github.com/andreyvit/edb/values"
	"strconv"
)

func standardStrconv(c *Call) error {
	name := c.native.Name
	var args [4]values.Value
	for i := range c.native.Signature.Params {
		args[i] = c.argument(i)
	}
	size := uint64(128)
	switch name {
	case "atoi", "parseBool", "parseInt", "parseUint", "parseFloat", "Quote", "QuoteToASCII", "unquote":
		size = args[0].R*8 + 256
	case "FormatFloat":
		if args[3].R != 32 && args[3].R != 64 {
			return &standardPanic{"strconv: illegal AppendFloat/FormatFloat bitSize"}
		}
		if int64(args[2].R) > 0 {
			size += args[2].R
		}
		if size < args[2].R && int64(args[2].R) > 0 {
			return ErrAllocation
		}
	case "FormatInt", "FormatUint":
		if args[1].R < 2 || args[1].R > 36 {
			return &standardPanic{"strconv: illegal AppendInt/FormatInt base"}
		}
	}
	if err := c.reserve(size); err != nil {
		return err
	}
	switch name {
	case "atoi":
		v0, v1 := strconv.Atoi(args[0].AsString())
		c.result(0, values.Value{R: uint64(v0)})
		text := ""
		if v1 != nil {
			text = v1.Error()
		}
		c.result(1, values.String(text))
	case "Itoa":
		v0 := strconv.Itoa(int(args[0].R))
		c.result(0, values.String(v0))
	case "parseBool":
		v0, v1 := strconv.ParseBool(args[0].AsString())
		c.result(0, values.Bool(v0))
		text := ""
		if v1 != nil {
			text = v1.Error()
		}
		c.result(1, values.String(text))
	case "parseInt":
		v0, v1 := strconv.ParseInt(args[0].AsString(), int(args[1].R), int(args[2].R))
		c.result(0, values.Value{R: uint64(v0)})
		text := ""
		if v1 != nil {
			text = v1.Error()
		}
		c.result(1, values.String(text))
	case "parseUint":
		v0, v1 := strconv.ParseUint(args[0].AsString(), int(args[1].R), int(args[2].R))
		c.result(0, values.Value{R: uint64(v0)})
		text := ""
		if v1 != nil {
			text = v1.Error()
		}
		c.result(1, values.String(text))
	case "parseFloat":
		v0, v1 := strconv.ParseFloat(args[0].AsString(), int(args[1].R))
		c.result(0, values.Float(v0))
		text := ""
		if v1 != nil {
			text = v1.Error()
		}
		c.result(1, values.String(text))
	case "FormatBool":
		v0 := strconv.FormatBool(args[0].R != 0)
		c.result(0, values.String(v0))
	case "FormatInt":
		v0 := strconv.FormatInt(int64(args[0].R), int(args[1].R))
		c.result(0, values.String(v0))
	case "FormatUint":
		v0 := strconv.FormatUint(uint64(args[0].R), int(args[1].R))
		c.result(0, values.String(v0))
	case "FormatFloat":
		v0 := strconv.FormatFloat(args[0].AsFloat(), byte(args[1].R), int(args[2].R), int(args[3].R))
		c.result(0, values.String(v0))
	case "Quote":
		v0 := strconv.Quote(args[0].AsString())
		c.result(0, values.String(v0))
	case "QuoteToASCII":
		v0 := strconv.QuoteToASCII(args[0].AsString())
		c.result(0, values.String(v0))
	case "unquote":
		v0, v1 := strconv.Unquote(args[0].AsString())
		c.result(0, values.String(v0))
		text := ""
		if v1 != nil {
			text = v1.Error()
		}
		c.result(1, values.String(text))
	default:
		return fmt.Errorf("unknown strconv primitive")
	}
	return nil
}
