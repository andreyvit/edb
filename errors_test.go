package edb

import (
	"errors"
	"strings"
	"testing"
)

func TestDataError_ErrorAndUnwrap(t *testing.T) {
	t.Run("small data", func(t *testing.T) {
		inner := errors.New("inner")
		err := dataErrf([]byte{0xAA, 0xBB}, 1, inner, "oops")
		var de *DataError
		if !errors.As(err, &de) {
			t.Fatalf("err = %T, wanted *DataError", err)
		}
		if !errors.Is(err, inner) {
			t.Fatalf("errors.Is(err, inner) = false, wanted true")
		}
		s := err.Error()
		if !strings.Contains(s, "oops") || !strings.Contains(s, "inner") || !strings.Contains(s, "(2)") {
			t.Fatalf("err.Error() = %q, wanted message with oops/inner/(2)", s)
		}
	})

	t.Run("large data includes prefix+suffix", func(t *testing.T) {
		data := make([]byte, 200)
		for i := range data {
			data[i] = byte(i)
		}
		err := dataErrf(data, 0, nil, "oops")
		s := err.Error()
		if !strings.Contains(s, "(200)") || !strings.Contains(s, "...") {
			t.Fatalf("err.Error() = %q, wanted message with (200) and ...", s)
		}
	})
}

func TestTableError_ErrorAndUnwrap(t *testing.T) {
	inner := errors.New("inner")
	err := tableErrf(usersTable, usersByEmail, []byte("k"), inner, "oops %d", 1)
	if !errors.Is(err, inner) {
		t.Fatalf("errors.Is(err, inner) = false, wanted true")
	}
	s := err.Error()
	if !strings.Contains(s, "Users.Email") || !strings.Contains(s, "\"k\"") || !strings.Contains(s, "oops 1") || !strings.Contains(s, "inner") {
		t.Fatalf("err.Error() = %q, wanted table/index/key/msg/inner", s)
	}

	s = (&TableError{Table: "T", Err: inner}).Error()
	if !strings.Contains(s, "T: inner") {
		t.Fatalf("TableError.Error() = %q, wanted %q", s, "T: inner")
	}

	err = kvtableErrf(kubets, nil, []byte("k"), inner, "oops")
	s = err.Error()
	if !strings.Contains(s, "kubets") || !strings.Contains(s, "\"k\"") || !strings.Contains(s, "oops") {
		t.Fatalf("kvtableErrf Error() = %q, wanted kubets/key/msg", s)
	}
}

func TestFormatErrMsg(t *testing.T) {
	if got := formatErrMsg(nil); got != "" {
		t.Fatalf("formatErrMsg(nil) = %q, wanted empty", got)
	}
	if got := formatErrMsg([]any{"hi %d", 1}); got != "hi 1: " {
		t.Fatalf("formatErrMsg = %q, wanted %q", got, "hi 1: ")
	}

	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic")
		}
	}()
	_ = formatErrMsg([]any{42})
}
