package kvodraft

import "slices"

// RecordModel is the draft's minimal field declaration. It checks key membership;
// semantic types and validation remain the responsibility of a full model/binder.
// The immutable declaration is shared, never stored in each value or editor.
type RecordModel struct{ keys []uint64 }

func NewRecordModel(keys ...uint64) *RecordModel {
	for i := 1; i < len(keys); i++ {
		if keys[i-1] >= keys[i] {
			panic(ErrOrder)
		}
	}
	return &RecordModel{keys: slices.Clone(keys)}
}
func (m *RecordModel) check(key uint64) {
	if m == nil {
		panic(ErrValue)
	}
	if _, ok := slices.BinarySearch(m.keys, key); !ok {
		panic(ErrUnknownKey)
	}
}
func (m *RecordModel) Get(view View, key uint64) Value {
	m.check(key)
	return view.Get(key)
}
func (m *RecordModel) Set(editor *Editor, key uint64, value Value) {
	m.check(key)
	if editor == nil {
		panic(ErrValue)
	}
	editor.Set(key, value)
}
func (m *RecordModel) Delete(editor *Editor, key uint64) {
	m.check(key)
	if editor == nil {
		panic(ErrValue)
	}
	editor.Delete(key)
}
