package kvo

import "fmt"

// Preserve error identity at Try API recovery boundaries, including errors
// raised by user-provided codecs. Non-error panics retain their diagnostic text.
func recoveredError(context string, value any) error {
	if err, ok := value.(error); ok {
		return fmt.Errorf("%s: %w", context, err)
	}
	return fmt.Errorf("%s: %v", context, value)
}
