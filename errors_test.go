package edb

import (
	"bytes"
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

func TestDataError_OwnsData(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "nil"},
		{name: "empty", data: []byte{}},
		{name: "binary", data: []byte{0x00, 0x7f, 0xff}},
		{name: "large", data: bytes.Repeat([]byte{0x01, 0x00, 0xff}, 80)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := bytes.Clone(tc.data)
			inner := errors.New("inner")
			err := dataErrf(input, 0, inner, "retained data")
			var dataErr *DataError
			if !errors.As(err, &dataErr) {
				t.Fatalf("error type = %T, want *DataError", err)
			}
			text := err.Error()
			for i := range input {
				input[i] = 0xa5
			}
			if !bytes.Equal(dataErr.Data, tc.data) || (dataErr.Data == nil) != (tc.data == nil) {
				t.Fatalf("retained data = %#v, want %#v", dataErr.Data, tc.data)
			}
			if got := err.Error(); got != text {
				t.Fatalf("retained error = %q, want %q", got, text)
			}
			if !errors.Is(err, inner) {
				t.Fatal("retained error lost its cause")
			}
		})
	}
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

func TestTableError_OwnsKey(t *testing.T) {
	for _, constructor := range []struct {
		name      string
		makeError func([]byte, error) error
	}{
		{"table", func(key []byte, inner error) error {
			return tableErrf(usersTable, usersByEmail, key, inner, "retained key")
		}},
		{"kv", func(key []byte, inner error) error {
			return kvtableErrf(kubets, nil, key, inner, "retained key")
		}},
	} {
		t.Run(constructor.name, func(t *testing.T) {
			for _, tc := range []struct {
				name string
				key  []byte
			}{
				{name: "nil"},
				{name: "empty", key: []byte{}},
				{name: "binary", key: []byte{0x00, 0x22, 0x7f, 0xff}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					input := bytes.Clone(tc.key)
					inner := errors.New("inner")
					err := constructor.makeError(input, inner)
					var tableErr *TableError
					if !errors.As(err, &tableErr) {
						t.Fatalf("error type = %T, want *TableError", err)
					}
					text := err.Error()
					for i := range input {
						input[i] = 0xa5
					}
					if !bytes.Equal(tableErr.Key, tc.key) || (tableErr.Key == nil) != (tc.key == nil) {
						t.Fatalf("retained key = %#v, want %#v", tableErr.Key, tc.key)
					}
					if got := err.Error(); got != text {
						t.Fatalf("retained error = %q, want %q", got, text)
					}
					if !errors.Is(err, inner) {
						t.Fatal("retained error lost its cause")
					}
				})
			}
		})
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
