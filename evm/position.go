package evm

import "fmt"

type SourcePosition struct {
	File         string
	Line, Column int
}

func (f *Function) position(pc int) SourcePosition {
	if len(f.Positions) != 0 {
		return f.Positions[pc]
	}
	if len(f.Lines) != 0 {
		return SourcePosition{Line: f.Lines[pc]}
	}
	return SourcePosition{}
}

// ExecutionError retains the source location and wrapped host/VM error.
type ExecutionError struct {
	Function string
	PC       int
	Position SourcePosition
	Err      error
}

func (e *ExecutionError) Error() string {
	if e.Position.Line != 0 {
		return fmt.Sprintf("%s pc %d (line %d): %v", e.Function, e.PC, e.Position.Line, e.Err)
	}
	return fmt.Sprintf("%s pc %d: %v", e.Function, e.PC, e.Err)
}
func (e *ExecutionError) Unwrap() error { return e.Err }
