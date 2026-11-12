package msgpack

// FieldEdit replaces a string-keyed map field. A nil Value deletes the field;
// []byte{0xc0} stores explicit null. Values must contain one complete value.
type FieldEdit struct {
	Name  string
	Value []byte
}

// Apply copies the enclosing map once, preserving untouched bytes and key order.
// Repeated edits to a name use the last edit, including for duplicate wire keys.
// The input and result never share writable storage.
func (v View) Apply(edits []FieldEdit) View {
	if len(edits) == 0 {
		return v
	}
	capacity := len(v.raw) + 5
	for _, e := range edits {
		if e.Value != nil {
			if _, err := TryOpen(e.Value); err != nil {
				panic(err)
			}
			capacity += len(e.Name) + len(e.Value) + 5
		}
	}
	var scratch [16]bool
	seen := scratch[:min(len(edits), len(scratch))]
	if len(edits) > len(scratch) {
		seen = make([]bool, len(edits))
	}
	find := func(name string) int {
		for i := len(edits) - 1; i >= 0; i-- {
			if edits[i].Name == name {
				return i
			}
		}
		return -1
	}
	off, count := v.mapHeader()
	n := count
	dst := make([]byte, 5, capacity)
	for range count {
		start := off
		end := off + skip(v.raw[off:], 0)
		key := Trusted(v.raw[off:end]).Bytes()
		off = end + skip(v.raw[end:], 0)
		i := find(string(key))
		if i < 0 {
			dst = append(dst, v.raw[start:off]...)
		} else {
			seen[i] = true
			if edits[i].Value == nil {
				n--
			} else {
				dst = append(dst, v.raw[start:end]...)
				dst = append(dst, edits[i].Value...)
			}
		}
	}
	for i, e := range edits {
		if !seen[i] && e.Value != nil && find(e.Name) == i {
			dst = appendString(dst, e.Name)
			dst = append(dst, e.Value...)
			n++
		}
	}
	var header [5]byte
	h := appendMapHeader(header[:0], n)
	start := 5 - len(h)
	copy(dst[start:5], h)
	return Trusted(dst[start:])
}
