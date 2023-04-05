package edb

import (
	"fmt"
	"strings"
)

type DataError struct {
	Data []byte
	Off  int
	Err  error
	Msg  string
}

func dataErrf(data []byte, off int, err error, format string, args ...any) error {
	return &DataError{data, off, err, fmt.Sprintf(format, args...)}
}

func (e *DataError) Unwrap() error {
	return e.Err
}

func (e *DataError) Error() string {
	const prefixLen = 64
	const suffixLen = 32
	n := len(e.Data)
	if n <= prefixLen+suffixLen {
		if e.Err != nil {
			return fmt.Sprintf("%s: %v: (%d) %x", e.Msg, e.Err, n, e.Data)
		} else {
			return fmt.Sprintf("%s: (%d) %x", e.Msg, n, e.Data)
		}
	} else {
		p, s := e.Data[:prefixLen], e.Data[n-suffixLen:]
		if e.Err != nil {
			return fmt.Sprintf("%s: %v: (%d) %x...%x", e.Msg, e.Err, n, p, s)
		} else {
			return fmt.Sprintf("%s: (%d) %x...%x", e.Msg, n, p, s)
		}
	}
}

type TableError struct {
	Table *Table
	Index *Index
	Key   []byte
	Msg   string
	Err   error
}

func tableErrf(tbl *Table, idx *Index, key []byte, err error, format string, args ...any) error {
	return &TableError{tbl, idx, key, fmt.Sprintf(format, args...), err}
}

func (e *TableError) Unwrap() error {
	return e.Err
}

func (e *TableError) Error() string {
	var buf strings.Builder
	buf.WriteString(e.Table.Name())
	if e.Index != nil {
		buf.WriteByte('.')
		buf.WriteString(e.Index.ShortName())
	}
	if e.Key != nil {
		buf.WriteByte('/')
		buf.Write(e.Key)
	}
	if e.Msg != "" {
		buf.WriteString(": ")
		buf.WriteString(e.Msg)
		if e.Err != nil {
			buf.WriteString(": ")
			buf.WriteString(e.Err.Error())
		}
	} else if e.Err != nil {
		buf.WriteString(": ")
		buf.WriteString(e.Err.Error())
	}
	return buf.String()
}

func formatErrMsg(messageAndArgs []any) string {
	if len(messageAndArgs) == 0 {
		return ""
	}
	msg, ok := messageAndArgs[0].(string)
	if !ok {
		panic(fmt.Errorf("error's message arg is %T instead of string: %v", messageAndArgs[0], messageAndArgs[0]))
	}
	return fmt.Sprintf(msg, messageAndArgs[1:]...) + ": "
}
