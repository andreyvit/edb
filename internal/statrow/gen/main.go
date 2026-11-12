package main

import (
	"os"
	"reflect"

	"github.com/andreyvit/edb/internal/statrow"
	"github.com/andreyvit/edb/kvo"
)

func main() {
	_, m := statrow.Model()
	b, err := kvo.Bind(reflect.TypeFor[statrow.State](), m).TryGenerate("statrow", "Static")
	if err != nil {
		panic(err)
	}
	if err = os.WriteFile("bindings_gen.go", b, 0644); err != nil {
		panic(err)
	}
}
