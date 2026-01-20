package edb

import (
	"bytes"
	"testing"
)

func TestValueFlags_Ver(t *testing.T) {
	if (vfVer1 | vfGzip).ver() != vfVer1 {
		t.Fatalf("valueFlags.ver returned unexpected value")
	}
}

func TestBriefRawValue(t *testing.T) {
	buf := make([]byte, 0, maxValueHeaderSize+16)
	raw := reserveValueHeader(buf)

	raw = append(raw, []byte{1, 2, 3}...)
	indexOff := len(raw)
	raw = append(raw, []byte{9, 8}...)

	raw = putValueHeader(raw, vfDefault, 1, 2, indexOff)

	brief, err := briefRawValue(raw)
	if err != nil {
		t.Fatalf("briefRawValue failed: %v", err)
	}
	if bytes.HasSuffix(brief, []byte{9, 8}) {
		t.Fatalf("briefRawValue still contains index bytes: %x", brief)
	}

	_, err = briefRawValue([]byte{1, 2, 3})
	if err == nil {
		t.Fatalf("briefRawValue(invalid) err = nil, wanted error")
	}
}

