// Package nativebind selects native bindings once, when a schema is built.
package nativebind

type Mode uint8

const (
	Automatic Mode = iota
	StaticOnly
)

// Select uses generated operations only if the declaration and its native
// operations both match. StaticOnly treats a stale binding as a setup error.
func (m Mode) Select(name, want, provided string, compatible bool) bool {
	if m > StaticOnly {
		panic("invalid native binding mode")
	}
	match := compatible && provided == want
	if !match && m == StaticOnly {
		panic("missing or stale generated bindings for " + name)
	}
	return match
}
