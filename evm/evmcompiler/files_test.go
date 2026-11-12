package evmcompiler_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/andreyvit/edb/evm"
	"github.com/andreyvit/edb/evm/evmcompiler"
	"github.com/andreyvit/edb/values"
)

func TestGoFiles_shared_declarations_initialization_and_positions(t *testing.T) {
	sources := map[string]string{
		"main.go": `package main
import "fmt"
var first = seed.Value()
func Run() string { return fmt.Sprintf("%d/%d", first, seed.Value()) }
func Fail() { broken() }
`,
		"model.go": `type Item struct { N int }
var seed = Item{N: 41}
func (v Item) Value() int { return v.N }
func init() { seed.N++ }
func broken() {
 var p *Item
 _ = p.N
}
`,
	}
	m, err := evmcompiler.CompileFiles(sources)
	if err != nil {
		t.Fatal(err)
	}
	p, err := evm.Link(m)
	if err != nil {
		t.Fatal(err)
	}
	vm := evm.New(p)
	out := make([]values.Value, 1)
	if err := vm.Run(context.Background(), "Run", nil, out); err != nil || out[0].AsString() != "41/42" {
		t.Fatalf("%q: %v", out[0].AsString(), err)
	}
	err = vm.Run(context.Background(), "Fail", nil, nil)
	var execution *evm.ExecutionError
	if !errors.As(err, &execution) || execution.Position.File != "model.go" || execution.Position.Line != 7 {
		t.Fatalf("runtime position: %+v", execution)
	}
	sources["model.go"] += "func bad() string { return fmt.Sprint(1) }\n"
	if _, err := evmcompiler.CompileFiles(sources); err == nil || !strings.Contains(err.Error(), "model.go:9:") || !strings.Contains(err.Error(), "undefined: fmt") {
		t.Fatalf("file-local imports: %v", err)
	}
}
