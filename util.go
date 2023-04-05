package edb

import (
	"bytes"
	"strings"

	"go.etcd.io/bbolt"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func ensure(err error) {
	if err != nil {
		panic(err)
	}
}

func nonNil[T any](v *T) *T {
	if v == nil {
		panic("nil")
	}
	return v
}

func splitByte(s string, sep byte) (string, string, bool) {
	i := strings.IndexByte(s, sep)
	if i < 0 {
		return s, "", false
	} else {
		return s[:i], s[i+1:], true
	}
}

func rpad(s string, n int, pad rune) string {
	rem := n - len(s)
	if rem <= 0 {
		return s
	}
	return s + strings.Repeat(string(pad), rem)
}

func boltSeek(c *bbolt.Cursor, prefix []byte, reverse bool) ([]byte, []byte) {
	if reverse {
		return boltSeekLast(c, prefix)
	} else {
		return c.Seek(prefix)
	}
}

func boltSeekLast(c *bbolt.Cursor, prefix []byte) ([]byte, []byte) {
	// NOTE: this could be made much faster by incrementing the prefix temporarily, but then we'd need to deal with overflow
	k, _ := c.Seek(prefix)
	if k == nil {
		return nil, nil
	}
	for k != nil && bytes.HasPrefix(k, prefix) {
		k, _ = c.Next()
	}
	return c.Prev()
}

func boltFirstLast(c *bbolt.Cursor, reverse bool) ([]byte, []byte) {
	if reverse {
		return c.Last()
	} else {
		return c.First()
	}
}

func boltAdvance(c *bbolt.Cursor, reverse bool) ([]byte, []byte) {
	if reverse {
		return c.Prev()
	} else {
		return c.Next()
	}
}
