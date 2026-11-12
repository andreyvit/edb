package main

import (
	"github.com/andreyvit/edb/internal/inlinerow"
	"github.com/andreyvit/edb/kvo"
	"os"
	"reflect"
)

func main() {
	b := kvo.Bind(reflect.TypeFor[inlinerow.Row](), inlinerow.Model)
	if err := os.WriteFile("bindings_gen.go", b.Generate("inlinerow", "Static"), 0644); err != nil {
		panic(err)
	}
}
