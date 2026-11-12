package evm

import (
	"fmt"
	"github.com/andreyvit/edb/values"
	"math"
	"strings"
)

func standardStrings(c *Call) error {
	name := c.native.Name
	var args [4]values.Value
	g := c.native
	for i := range g.Signature.Params {
		args[i] = c.argument(i)
	}
	size := uint64(0)
	switch name {
	case "Clone", "ToLower", "ToUpper", "ToTitle":
		size = args[0].R * 3
	case "Repeat":
		n := int64(args[1].R)
		if n < 0 {
			return &standardPanic{"strings: negative Repeat count"}
		}
		if args[0].R != 0 && uint64(n) > math.MaxInt/args[0].R {
			return &standardPanic{"strings: Repeat output length overflow"}
		}
		size = args[0].R * uint64(n)
	case "Replace", "ReplaceAll":
		n := strings.Count(args[0].AsString(), args[1].AsString())
		if name == "Replace" && int64(args[3].R) >= 0 {
			n = min(n, int(args[3].R))
		}
		size = args[0].R
		if args[2].R > args[1].R {
			delta := args[2].R - args[1].R
			if uint64(n) > math.MaxInt/delta {
				return ErrAllocation
			}
			size += uint64(n) * delta
		}
	case "Fields", "Split", "SplitAfter", "SplitN", "SplitAfterN":
		n := args[0].R + 1
		if name == "SplitN" || name == "SplitAfterN" {
			if int64(args[2].R) >= 0 {
				n = min(n, args[2].R)
			}
		}
		size = n * 24
	}
	if err := c.reserve(size); err != nil {
		return err
	}
	switch name {
	case "Clone":
		v0 := strings.Clone(args[0].AsString())
		c.result(0, values.String(v0))
	case "ToLower":
		v0 := strings.ToLower(args[0].AsString())
		c.result(0, values.String(v0))
	case "ToUpper":
		v0 := strings.ToUpper(args[0].AsString())
		c.result(0, values.String(v0))
	case "ToTitle":
		v0 := strings.ToTitle(args[0].AsString())
		c.result(0, values.String(v0))
	case "TrimSpace":
		v0 := strings.TrimSpace(args[0].AsString())
		c.result(0, values.String(v0))
	case "Compare":
		v0 := strings.Compare(args[0].AsString(), args[1].AsString())
		c.result(0, values.Int(int64(v0)))
	case "Count":
		v0 := strings.Count(args[0].AsString(), args[1].AsString())
		c.result(0, values.Int(int64(v0)))
	case "Index":
		v0 := strings.Index(args[0].AsString(), args[1].AsString())
		c.result(0, values.Int(int64(v0)))
	case "LastIndex":
		v0 := strings.LastIndex(args[0].AsString(), args[1].AsString())
		c.result(0, values.Int(int64(v0)))
	case "IndexAny":
		v0 := strings.IndexAny(args[0].AsString(), args[1].AsString())
		c.result(0, values.Int(int64(v0)))
	case "LastIndexAny":
		v0 := strings.LastIndexAny(args[0].AsString(), args[1].AsString())
		c.result(0, values.Int(int64(v0)))
	case "Contains":
		v0 := strings.Contains(args[0].AsString(), args[1].AsString())
		c.result(0, values.Bool(v0))
	case "ContainsAny":
		v0 := strings.ContainsAny(args[0].AsString(), args[1].AsString())
		c.result(0, values.Bool(v0))
	case "EqualFold":
		v0 := strings.EqualFold(args[0].AsString(), args[1].AsString())
		c.result(0, values.Bool(v0))
	case "HasPrefix":
		v0 := strings.HasPrefix(args[0].AsString(), args[1].AsString())
		c.result(0, values.Bool(v0))
	case "HasSuffix":
		v0 := strings.HasSuffix(args[0].AsString(), args[1].AsString())
		c.result(0, values.Bool(v0))
	case "ContainsRune":
		v0 := strings.ContainsRune(args[0].AsString(), rune(args[1].R))
		c.result(0, values.Bool(v0))
	case "IndexRune":
		v0 := strings.IndexRune(args[0].AsString(), rune(args[1].R))
		c.result(0, values.Int(int64(v0)))
	case "IndexByte":
		v0 := strings.IndexByte(args[0].AsString(), byte(args[1].R))
		c.result(0, values.Int(int64(v0)))
	case "LastIndexByte":
		v0 := strings.LastIndexByte(args[0].AsString(), byte(args[1].R))
		c.result(0, values.Int(int64(v0)))
	case "Cut":
		v0, v1, v2 := strings.Cut(args[0].AsString(), args[1].AsString())
		c.result(0, values.String(v0))
		c.result(1, values.String(v1))
		c.result(2, values.Bool(v2))
	case "CutPrefix":
		v0, v1 := strings.CutPrefix(args[0].AsString(), args[1].AsString())
		c.result(0, values.String(v0))
		c.result(1, values.Bool(v1))
	case "CutSuffix":
		v0, v1 := strings.CutSuffix(args[0].AsString(), args[1].AsString())
		c.result(0, values.String(v0))
		c.result(1, values.Bool(v1))
	case "Fields":
		v0 := strings.Fields(args[0].AsString())
		return c.stringSliceResult(0, v0)
	case "Repeat":
		v0 := strings.Repeat(args[0].AsString(), int(args[1].R))
		c.result(0, values.String(v0))
	case "Replace":
		v0 := strings.Replace(args[0].AsString(), args[1].AsString(), args[2].AsString(), int(args[3].R))
		c.result(0, values.String(v0))
	case "ReplaceAll":
		v0 := strings.ReplaceAll(args[0].AsString(), args[1].AsString(), args[2].AsString())
		c.result(0, values.String(v0))
	case "Split":
		v0 := strings.Split(args[0].AsString(), args[1].AsString())
		return c.stringSliceResult(0, v0)
	case "SplitAfter":
		v0 := strings.SplitAfter(args[0].AsString(), args[1].AsString())
		return c.stringSliceResult(0, v0)
	case "SplitN":
		v0 := strings.SplitN(args[0].AsString(), args[1].AsString(), int(args[2].R))
		return c.stringSliceResult(0, v0)
	case "SplitAfterN":
		v0 := strings.SplitAfterN(args[0].AsString(), args[1].AsString(), int(args[2].R))
		return c.stringSliceResult(0, v0)
	case "Trim":
		v0 := strings.Trim(args[0].AsString(), args[1].AsString())
		c.result(0, values.String(v0))
	case "TrimLeft":
		v0 := strings.TrimLeft(args[0].AsString(), args[1].AsString())
		c.result(0, values.String(v0))
	case "TrimRight":
		v0 := strings.TrimRight(args[0].AsString(), args[1].AsString())
		c.result(0, values.String(v0))
	case "TrimPrefix":
		v0 := strings.TrimPrefix(args[0].AsString(), args[1].AsString())
		c.result(0, values.String(v0))
	case "TrimSuffix":
		v0 := strings.TrimSuffix(args[0].AsString(), args[1].AsString())
		c.result(0, values.String(v0))
	case "Join":
		n := uint64(args[0].SliceLen())
		if err := c.reserve(n * 24); err != nil {
			return err
		}
		elems := make([]string, n)
		cells := values.CellSlice(args[0].P, n)
		size := uint64(0)
		if n > 0 {
			size = (n - 1) * args[1].R
		}
		for i, v := range cells {
			if i&255 == 255 {
				if err := c.budget.contextErr(); err != nil {
					return err
				}
			}
			elems[i] = v.AsString()
			size += v.R
			if size > math.MaxInt {
				return ErrAllocation
			}
		}
		if err := c.reserve(size); err != nil {
			return err
		}
		c.result(0, values.String(strings.Join(elems, args[1].AsString())))
	default:
		return fmt.Errorf("unknown strings primitive")
	}
	return nil
}
func (c *Call) stringSliceResult(i int, ss []string) error {
	if len(ss) > math.MaxUint32 {
		return ErrAllocation
	}
	p, err := allocCells(uint64(len(ss)), &c.budget.allocated, c.budget.limits)
	if err != nil {
		return err
	}
	cells := values.CellSlice(p, uint64(len(ss)))
	for i, s := range ss {
		if i&255 == 255 {
			if err := c.budget.contextErr(); err != nil {
				return err
			}
		}
		cells[i] = values.String(s)
	}
	v := values.SliceValue(p, uint32(len(ss)), uint32(len(ss)))
	if ss == nil {
		v = values.Value{}
	}
	c.result(i, v)
	return nil
}
