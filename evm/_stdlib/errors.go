package errors

func comparable(value any) bool

type errorString struct{ text string }

func (e *errorString) Error() string { return e.text }
func New(text string) error          { return &errorString{text} }
func Unwrap(err error) error {
	if e, ok := err.(interface{ Unwrap() error }); ok {
		return e.Unwrap()
	}
	return nil
}
func Is(err, target error) bool {
	if err == nil || target == nil {
		return err == target
	}
	if comparable(target) && err == target {
		return true
	}
	if e, ok := err.(interface{ Is(error) bool }); ok && e.Is(target) {
		return true
	}
	if e, ok := err.(interface{ Unwrap() error }); ok {
		return Is(e.Unwrap(), target)
	}
	if e, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range e.Unwrap() {
			if Is(child, target) {
				return true
			}
		}
	}
	return false
}

type joinError struct{ errs []error }

func (e *joinError) Error() string {
	text := ""
	for i, err := range e.errs {
		if i > 0 {
			text += "\n"
		}
		text += err.Error()
	}
	return text
}
func (e *joinError) Unwrap() []error { return e.errs }
func Join(errs ...error) error {
	var out []error
	for _, err := range errs {
		if err != nil {
			out = append(out, err)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return &joinError{out}
}
