package kvolab

// Account for bytes in the actual encoded replay trees. Child containers are
// counted recursively, so their framing is not mistaken for application data.
// Immediate tagged values belong to entry metadata, where those bytes live.
import (
	"sort"
	"testing"
)

type selectorSizeParts struct {
	scalars, spans, dictionaryKeys, entryMetadata, headers, footers int
}

func (p selectorSizeParts) total() int {
	return p.scalars + p.spans + p.dictionaryKeys + p.entryMetadata + p.headers + p.footers
}

func (p *selectorSizeParts) add(q selectorSizeParts) {
	p.scalars += q.scalars
	p.spans += q.spans
	p.dictionaryKeys += q.dictionaryKeys
	p.entryMetadata += q.entryMetadata
	p.headers += q.headers
	p.footers += q.footers
}

func selectorMeasureParts(t *testing.T, raw []byte, source Value, mode selectorMode) selectorSizeParts {
	t.Helper()
	footer, body := tagTail(raw)
	catalog := body[len(body)-int(footer>>3):]
	_, entries := lengthU(catalog)
	parts := selectorSizeParts{headers: len(catalog) - len(entries), footers: len(raw) - len(body)}
	it := selectorIter(raw, mode)
	for {
		position, metadata := it.pos, len(it.meta)
		key, text, value, ok := it.Next()
		if !ok {
			break
		}
		parts.dictionaryKeys += len(text)
		parts.entryMetadata += metadata - len(it.meta) - len(text)
		var child Value
		switch source.Kind {
		case Record:
			i := sort.Search(len(source.Fields), func(i int) bool { return source.Fields[i].Key >= key })
			if i == len(source.Fields) || source.Fields[i].Key != key {
				t.Fatalf("unexpected Object key %d", key)
			}
			child = source.Fields[i].Value
		case Dict:
			i := sort.Search(len(source.Entries), func(i int) bool { return source.Entries[i].Key >= string(text) })
			if i == len(source.Entries) || source.Entries[i].Key != string(text) {
				t.Fatal("unexpected Dictionary key")
			}
			child = source.Entries[i].Value
		case List:
			child = source.Items[key]
		default:
			t.Fatal("expected container")
		}
		switch {
		case value.ptr == nil:
			parts.scalars += it.pos - position
		case child.Kind == Record || child.Kind == Dict || child.Kind == List:
			parts.add(selectorMeasureParts(t, value.Bytes(), child, mode))
		default:
			parts.spans += it.pos - position
		}
	}
	if parts.total() != len(raw) {
		t.Fatalf("unaccounted bytes: parts=%+v total=%d encoded=%d", parts, parts.total(), len(raw))
	}
	return parts
}

func TestSelectorReplayByteAccounting(t *testing.T) {
	for mode := selectorLiteral; mode <= selectorTaggedWide; mode++ {
		t.Run(mode.String(), func(t *testing.T) {
			var total selectorSizeParts
			for _, source := range workloadFixtures(t) {
				w := selectorWriter{mode: mode}
				raw := selectorEncode(&w, nil, source)
				total.add(selectorMeasureParts(t, raw, source, mode))
			}
			t.Logf("roots=%d scalars=%d spans=%d dictionary_keys=%d entry_metadata=%d headers=%d footers=%d total=%d",
				len(workloadFixtures(t)), total.scalars, total.spans, total.dictionaryKeys, total.entryMetadata, total.headers, total.footers, total.total())
		})
	}
}
