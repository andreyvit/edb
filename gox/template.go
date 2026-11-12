// Package gox compiles GoX templates into typed EVM functions.
package gox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"unsafe"

	"github.com/andreyvit/edb/evm"
	ht "github.com/andreyvit/edb/gox/internal/htmltemplate"
	"github.com/andreyvit/edb/values"
)

// Trusted content types bypass selected escaping steps. Only construct them
// from trusted source, never by casting unvalidated input.
type HTML = ht.HTML
type CSS = ht.CSS
type JS = ht.JS
type JSStr = ht.JSStr
type URL = ht.URL
type Srcset = ht.Srcset

// Format selects output analysis at compile time.
type Format uint8

const (
	HTMLFormat Format = iota
	TextFormat
	CSSFormat
)

// Options fixes the output language during compilation. The zero value is HTML.
type Options struct{ Format Format }

// Parameter describes an entry input in Render argument order.
type Parameter struct{ Name, Type string }

// Class identifies a statically known CSS class, including classes in helpers.
type Class struct {
	Name     string
	Position evm.SourcePosition
}
type Diagnostic struct {
	Position evm.SourcePosition
	Message  string
}

func (d Diagnostic) Error() string {
	return fmt.Sprintf("%s:%d:%d: %s", d.Position.File, d.Position.Line, d.Position.Column, d.Message)
}

// Template holds bytecode and its immutable rendering plan. Keep the plan with
// the module; the module alone does not contain the contextual escaping filters.
// Treat exported metadata as read-only after compilation.
type Template struct {
	Entry            string
	GeneratedSources map[string]string
	DynamicClasses   bool
	format           Format
	Module           *evm.Module
	Parameters       []Parameter
	Classes          []Class
	GeneratedSource  string
	operations       []operation
	sources          map[string]string
}

// Program binds a template to host implementations and supports concurrent renders.
type Program struct {
	pool     sync.Pool
	template *Template
	program  *evm.Program
}
type RenderOptions struct {
	// Strict fails on nil field access in output instead of reporting and omitting it.
	Strict bool
	// Report receives recoverable output diagnostics. Nil logs through slog.
	Report func(Diagnostic)
	// Budget applies the same execution limits as an EVM script.
	Budget evm.Budget
	// MaxOutputBytes defaults to 16 MiB. A failed render returns no partial output.
	MaxOutputBytes int
}

// Compile compiles an HTML GoX file using EVM's standard and supplied packages.
func Compile(filename, source string, packages ...evm.Package) (*Template, error) {
	return Options{}.Compile(filename, source, packages...)
}
func (o Options) Compile(filename, source string, packages ...evm.Package) (*Template, error) {
	return o.CompileFiles(filename, map[string]string{filename: source}, packages...)
}

// Link binds the module to host packages with matching signatures. It does not run it.
func (t *Template) Link(packages ...evm.Package) (*Program, error) {
	p, err := evm.Link(t.Module, append(packages, runtimePackage())...)
	if err != nil {
		return nil, err
	}
	return &Program{template: t, program: p}, nil
}

// Render executes with EVM values in Parameters order. Globals and temporary
// state reset between renders. Host package implementations retain their own state.
func (p *Program) Render(ctx context.Context, args []values.Value, options RenderOptions) (string, error) {
	return p.RenderFunc(ctx, p.template.Entry, args, options)
}

// EVM returns the linked program for ordinary Go entry points in this package.
func (p *Program) EVM() *evm.Program { return p.program }

// RenderFunc executes a named renderer. args excludes its explicit builder parameter.
func (p *Program) RenderFunc(ctx context.Context, name string, args []values.Value, options RenderOptions) (string, error) {
	session, _ := p.pool.Get().(*renderSession)
	if session == nil {
		session = &renderSession{vm: evm.New(p.program)}
	}
	b := &session.builder
	b.operations = p.template.operations
	b.options = options
	if b.options.Report == nil {
		b.options.Report = defaultReport
	}
	b.format = p.template.format
	b.content = ht.Context{}
	if b.format == CSSFormat {
		_, _ = b.content.Text("<style>")
	}
	defer func() {
		_ = session.vm.Reset()
		clear(session.args)
		session.args = session.args[:0]
		clear(b.tags[:cap(b.tags)])
		b.tags = b.tags[:0]
		b.attr = ""
		b.attrbuf = b.attrbuf[:0]
		clear(b.attributeScratch)
		b.attributeScratch = b.attributeScratch[:0]
		if cap(b.attributeScratch) > 1024 {
			b.attributeScratch = nil
		}
		clear(b.classParts)
		b.classParts = b.classParts[:0]
		if cap(b.classParts) > 4096 {
			b.classParts = nil
		}
		b.classBytes = 0
		b.buf = b.buf[:0]
		b.options = RenderOptions{}
		if cap(b.buf) > 1<<20 {
			b.buf = nil
		}
		if cap(b.attrbuf) > 1<<20 {
			b.attrbuf = nil
		}
		p.pool.Put(session)
	}()
	if b.options.MaxOutputBytes == 0 {
		b.options.MaxOutputBytes = 16 << 20
	}
	if b.options.MaxOutputBytes < 0 {
		return "", fmt.Errorf("negative output limit")
	}
	ctx = context.WithValue(ctx, builderKey{}, b)
	session.args = append(session.args, values.Value{})
	in := session.args
	in[0] = values.Value{P: unsafe.Pointer(b)}
	in = append(in, args...)
	session.args = in
	err := session.vm.RunWithBudget(ctx, name, in, nil, options.Budget)
	if err != nil {
		var execution *evm.ExecutionError
		if errors.As(err, &execution) && execution.Position.File != "" {
			pos := execution.Position
			return "", fmt.Errorf("%s:%d:%d: %w", pos.File, pos.Line, pos.Column, err)
		}
		return "", err
	}
	if err := b.flush(); err != nil {
		return "", err
	}
	if len(b.tags) != 0 {
		return "", fmt.Errorf("unclosed output element")
	}
	return string(b.buf), nil
}

type renderSession struct {
	vm      *evm.VM
	builder Builder
	args    []values.Value
}

func defaultReport(d Diagnostic) { slog.Warn("GoX output omitted", "source", d.Error()) }

// Package declares and binds the rendering API for mixed EVM packages.
func Package() evm.Package      { return runtimePackage() }
func BuilderType() *values.Type { return builderPointer.Type }
func HTMLType() *values.Type    { return trustedTypes["HTML"] }
