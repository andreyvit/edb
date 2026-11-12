package kvo

import (
	"strconv"
	"strings"
)

type FmtContext struct{}

func Hex(v uint64) string { return strings.ToUpper(strconv.FormatUint(v, 16)) + "h" }
