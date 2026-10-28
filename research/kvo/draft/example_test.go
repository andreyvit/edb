package kvodraft_test

import (
	"fmt"
	kvo "github.com/andreyvit/edb/research/kvo/draft"
)

// A generated wrapper knows the field's meaning and needs no schema pointer.
type Widget struct{ Data kvo.View }

func (w Widget) Count() uint64 { return w.Data.Get(1).Uint() }

var widgetModel = kvo.NewRecordModel(1, 2)

func Example() {
	attributes := kvo.AppendDictionary(nil, []kvo.Pair{{Key: "region", Value: kvo.ByteString([]byte("west"))}}, kvo.Compact)
	row := kvo.AppendNumeric(nil, []kvo.Field{{Key: 1, Value: kvo.Word(12)}, {Key: 2, Value: kvo.ObjectValue(kvo.Trusted(attributes))}}, kvo.Indexed)
	view, err := kvo.Open(row) // An explicit boundary for external bytes.
	if err != nil {
		panic(err)
	}
	widget := Widget{view}
	fmt.Println(widget.Count())
	var scratch [8]kvo.Edit
	editor := kvo.NewEditor(view, scratch[:], nil)
	widgetModel.Set(&editor, 1, kvo.Word(13))
	updated := editor.Append(nil, kvo.Indexed)
	widget = Widget{kvo.Trusted(updated)}
	fmt.Println(widget.Count())
	fmt.Println(string(widget.Data.Get(2).Object().GetString("region").Bytes()))
	widgetModel.Set(&editor, 1, kvo.Word(0))
	widget = Widget{kvo.Trusted(editor.Append(nil, kvo.Indexed))}
	fmt.Println(widget.Count()) // Missing is the same ordinary Go zero.
	// Output:
	// 12
	// 13
	// west
	// 0
}
func ExampleAppendChanges() {
	log := kvo.AppendChanges(nil, kvo.Changes{Base: 41, Next: 42, Items: []kvo.Change{{Path: []kvo.Step{{Kind: kvo.FieldStep, Number: 1}}, Value: kvo.Word(0)}}})
	var path [8]kvo.PathPart
	reader, err := kvo.OpenChanges(log, path[:])
	if err != nil {
		panic(err)
	}
	change, ok, err := reader.NextChange()
	if err != nil {
		panic(err)
	}
	fmt.Println(ok, reader.Base, reader.Next, change.Path[0].Number, change.Value.Uint(), change.Delete)
	// Output: true 41 42 1 0 false
}
