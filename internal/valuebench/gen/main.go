package main

import (
	"github.com/andreyvit/edb/internal/valuebench"
	"github.com/andreyvit/edb/kvo"
	"os"
	"reflect"
)

func main() {
	_, m := valuebench.Model()
	b, e := kvo.Bind(reflect.TypeFor[valuebench.Row](), m).TryGenerate("valuebench", "Static")
	if e != nil {
		panic(e)
	}
	if e = os.WriteFile("bindings_gen.go", b, 0644); e != nil {
		panic(e)
	}
}
