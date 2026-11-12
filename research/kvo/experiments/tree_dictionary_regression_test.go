package kvolab

import "testing"

// Dictionary edit metadata stores its string in an arena and uses the numeric
// key word as that arena offset. Emission must not confuse the offset with a
// path key when deciding whether an edited child needs recursive emission.
func TestTreeDictionaryChildren(t *testing.T) {
	source := Obj(Field{1, Map(
		Entry{"a", Obj(Field{1, U(3)}, Field{2, U(4)})},
		Entry{"bb", Obj(Field{1, U(5)}, Field{2, U(6)})},
		Entry{"ccc", Obj(Field{1, U(7)}, Field{2, U(8)})},
	)})
	paths := [][]perfStep{
		{{key: 1}, {kind: 1, text: "a"}, {key: 1}},
		{{key: 1}, {kind: 1, text: "bb"}, {key: 1}},
		{{key: 1}, {kind: 1, text: "ccc"}, {key: 1}},
	}
	policy := func(int, Value) bool { return true }
	for _, mode := range []string{"back", "front", "keys-last", "sorted-children", "hash-children"} {
		t.Run(mode, func(t *testing.T) {
			var out []byte
			switch mode {
			case "back":
				var w lengthWriter
				raw := lengthEncode(&w, nil, source, policy)
				var e treeEditor
				e.Reset(lengthTrusted(raw), "append", true)
				for i, path := range paths {
					e.Set(path, lengthWord(uint64(10+i)))
				}
				out = e.Append(&w, nil)
			case "front":
				var w unifiedWriter
				raw := unifiedEncode(&w, nil, source, policy)
				var e unifiedTreeEditor
				e.Reset(unifiedTrusted(raw), "append", true)
				for i, path := range paths {
					e.Set(path, lengthWord(uint64(10+i)))
				}
				out = e.Append(&w, nil)
			default:
				var w keysLastWriter
				raw := keysLastEncode(&w, nil, source, policy)
				if mode == "keys-last" {
					var e keysLastTreeEditor
					e.Reset(keysLastTrusted(raw), "append", true)
					for i, path := range paths {
						e.Set(path, lengthWord(uint64(10+i)))
					}
					out = e.Append(&w, nil)
				} else {
					var e indexedTreeEditor
					childMode := "sorted"
					if mode == "hash-children" {
						childMode = "hash"
					}
					e.Reset(keysLastTrusted(raw), "append", true, childMode)
					for i, path := range paths {
						e.Set(path, lengthWord(uint64(10+i)))
					}
					out = e.Append(&w, nil)
				}
			}
			read := func(path []perfStep) lengthValue {
				switch mode {
				case "back":
					return perfPath(lengthTrusted(out), path, "common")
				case "front":
					return unifiedPath(out, path)
				default:
					return keysLastPath(out, path)
				}
			}
			for i, path := range paths {
				if read(path).Uint() != uint64(10+i) {
					t.Fatal("changed leaf")
				}
				untouched := append([]perfStep(nil), path...)
				untouched[2].key = 2
				if read(untouched).Uint() != uint64(4+2*i) {
					t.Fatal("unchanged leaf lost")
				}
			}
		})
	}
}
