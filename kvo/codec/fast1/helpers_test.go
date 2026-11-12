package fast1

import "testing"

func eq[T comparable](t testing.TB, a, e T) {
	t.Helper()
	if a != e {
		t.Fatalf("got %v, want %v", a, e)
	}
}
func assertPanics(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s: expected panic", name)
		}
	}()
	fn()
}
