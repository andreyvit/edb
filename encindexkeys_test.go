package edb

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestIndexDiffing(t *testing.T) {
	tests := []struct {
		old     string
		new     string
		removed string
	}{
		{"", "", ""},
		{"", "1:a", ""},
		{"1:a", "", "1:a"},
		{"1:abc", "", "1:abc"},
		{"1:a 1:b", "1:a", "1:b"},
		{"1:a 1:b", "1:b", "1:a"},
		{"1:a 1:b", "1:a 1:b", ""},
		{"1:a 2:a 2:b", "", "1:a 2:a 2:b"},
		{"1:a 2:a 2:b", "1:a", "2:a 2:b"},
		{"1:a 2:a 2:b", "2:a", "1:a 2:b"},
		{"1:a 2:a 2:b", "2:b", "1:a 2:a"},
		{"1:a 2:a 2:b", "1:a 2:a", "2:b"},
		{"1:a 2:a 2:b", "2:a 2:b", "1:a"},
		{"1:a 2:a 2:b", "1:a 2:b", "2:a"},
		{"1:a 2:a 2:b", "1:a 2:a 2:b", ""},
	}
	for _, tt := range tests {
		oldKeys := parseIndexKeys(tt.old)
		newKeys := parseIndexKeys(tt.new)
		oldKeysData := appendIndexKeys(nil, oldKeys)
		var removedKeys []string
		findRemovedIndexKeys(oldKeysData, newKeys, func(ord uint64, key []byte) {
			removedKeys = append(removedKeys, fmt.Sprintf("%d:%s", ord, key))
		})
		actual := strings.Join(removedKeys, " ")
		if actual != tt.removed {
			t.Errorf("** Removed(%s => %s) == %q, expected %q", tt.old, tt.new, actual, tt.removed)
		}
	}
}

func parseIndexKeys(s string) indexRows {
	cc := strings.Fields(s)
	rows := make(indexRows, len(cc))
	for i, c := range cc {
		ordStr, keyStr, ok := splitByte(c, ':')
		if !ok {
			panic("invalid entry: " + c)
		}
		ord := must(strconv.ParseUint(ordStr, 10, 64))
		rows[i] = IndexRow{IndexOrd: ord, KeyRaw: []byte(keyStr)}
	}
	return rows
}
