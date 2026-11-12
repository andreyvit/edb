// Package evmcompiler compiles sequential Go 1.23 to EVM bytecode.
// Only supplied package declarations are visible; it never imports native Go
// packages or uses reflection. Unsupported syntax is a compile error.
package evmcompiler

import (
	"fmt"
	"strings"

	"github.com/andreyvit/edb/evm"
)

const MaxSourceSize = 1 << 20

type Diagnostic struct {
	Position evm.SourcePosition
	Message  string
}

func (d Diagnostic) Error() string {
	return fmt.Sprintf("%s:%d:%d: %s", d.Position.File, d.Position.Line, d.Position.Column, d.Message)
}

type Diagnostics []Diagnostic

func (d Diagnostics) Error() string {
	lines := make([]string, len(d))
	for i, e := range d {
		lines[i] = e.Error()
	}
	return strings.Join(lines, "\n")
}

// Compile type-checks Go 1.23 source against explicitly imported virtual
// packages and lowers supported operations to fixed64 EVM bytecode.
// Unsupported operations return diagnostics; no partial module is returned.
func Compile(filename, source string, packages ...evm.Package) (*evm.Module, error) {
	return Options{}.Compile(filename, source, packages...)
}

// Options controls optional EVM code generation. The zero value enables all passes.
type Options struct {
	NoOptimization bool
}

// Compile emits Go SSA operations directly when NoOptimization is set.
// Link honors the same choice for the lifetime of the resulting program.
func (o Options) Compile(filename, source string, packages ...evm.Package) (*evm.Module, error) {
	return compileTyped(filename, source, packages, o)
}

// CompileFiles compiles one package from named source files. Files share
// declarations; imports keep ordinary Go file scope. Input order is deterministic.
func CompileFiles(sources map[string]string, packages ...evm.Package) (*evm.Module, error) {
	return Options{}.CompileFiles(sources, packages...)
}
func (o Options) CompileFiles(sources map[string]string, packages ...evm.Package) (*evm.Module, error) {
	return compileTypedFiles(sources, packages, o)
}
