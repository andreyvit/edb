package kvolab

import "testing"

func TestCurrentAccessControls(t *testing.T) {
	for _, source := range workloadFixtures(t) {
		var reference lengthWriter
		leaves := perfCollect(source, lengthEncode(&reference, nil, source, func(int, Value) bool { return true }))
		for _, policy := range []func(int, Value) bool{nil, func(int, Value) bool { return true }, func(d int, _ Value) bool { return d == 0 }} {
			var w currentWriter
			raw := currentEncode(&w, nil, source, policy)
			for _, leaf := range leaves {
				data := raw
				for i, step := range leaf.path {
					view := currentTrusted(data)
					var fused, wrapped currentValue
					switch step.kind {
					case 0:
						fused, wrapped = view.Get(step.key), view.GetWrapped(step.key)
					case 1:
						fused, wrapped = view.GetString(step.text), view.GetStringWrapped(step.text)
					case 2:
						fused, wrapped = view.At(int(step.key)), view.AtWrapped(int(step.key))
					}
					if fused != wrapped {
						t.Fatal("access adapter changed value or boundary")
					}
					if i+1 < len(leaf.path) {
						data = fused.Bytes()
					}
				}
			}
		}
	}
}
