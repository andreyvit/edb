package msgpack

import (
	"bytes"
	mp "github.com/vmihailenco/msgpack/v5"
	"reflect"
	"testing"
	"time"
)

func TestDirectTime(t *testing.T) {
	for _, v := range []any{time.Time{}, time.Unix(10, 0), time.Unix(10, 123456789), time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC), []int64{10, 123456789}, "2026-11-10T12:00:00.123456789Z"} {
		raw := Encode(nil, reflect.ValueOf(&v))
		want, err := mp.NewDecoder(bytes.NewReader(raw)).DecodeTime()
		if err != nil {
			t.Fatal(err)
		}
		if got := Trusted(raw).Time().AsTime(); !got.Equal(want) {
			t.Fatal(v, got, want)
		}
		if n := testing.AllocsPerRun(100, func() { _ = Trusted(raw).Time().AsTime() }); n != 0 {
			t.Fatal(v, n)
		}
	}
	for _, raw := range [][]byte{{0xd6, 13, 0, 0, 0, 10}, {0xd6, 255, 0, 0, 0, 10}} {
		if got := Trusted(raw).Time().AsTime(); got.Unix() != 10 {
			t.Fatal(got)
		}
	}
	for _, raw := range [][]byte{{0xd6, 1, 0, 0, 0, 10}, {0xd4, 255, 1}, {0xd6, 255, 0}, {1}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("accepted %x", raw)
				}
			}()
			Trusted(raw).Time()
		}()
	}
}
