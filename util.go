package edb

import (
	"bytes"
	"encoding/hex"
	"log/slog"
	"strings"

	"go.etcd.io/bbolt"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func must2[T1, T2 any](v1 T1, v2 T2, err error) (T1, T2) {
	if err != nil {
		panic(err)
	}
	return v1, v2
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
	if k == nil {
		return c.Last()
	} else {
		return c.Prev()
	}
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

type hexBytes []byte

func (b hexBytes) String() string {
	return hex.EncodeToString(b)
}

func hexstr(b []byte) string {
	if b == nil {
		return "<nil>"
	}
	if len(b) == 0 {
		return "<empty>"
	}
	return hex.EncodeToString(b)
}

func hexAttr(key string, b []byte) slog.Attr {
	return slog.String(key, hexstr(b))
}
