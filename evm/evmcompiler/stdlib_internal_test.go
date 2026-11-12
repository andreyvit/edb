package evmcompiler

import (
	"go/token"
	"testing"
)

func TestVirtualLibrariesCannotInitializeState(t *testing.T) {
	for _, source := range []string{`package sample;var x int`, `package sample;func init(){}`} {
		env, err := newTypeEnvironment(nil)
		if err != nil {
			t.Fatal(err)
		}
		env.files = token.NewFileSet()
		if _, err := env.importStandard("sample", source); err == nil {
			t.Fatalf("accepted stateful library: %s", source)
		}
	}
}
