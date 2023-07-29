// Package multierr helps report multiple errors as a single error value.
//
// Because typed nil values stored in interfaces are non-nil, callers
// should take care to only return instances of MultiErr if an error
// actually occurred.
//
//	var errs []error
//	// ...
//	if len(errs) > 0 {
//	    return multierr.New(errs)
//	}
//	return nil
//
// The New function helps catch this mistake by panicking on empty
// input.
package multierr

import (
	"strings"
)

type MultiErr []error

func New(errs []error) error {
	if len(errs) == 0 {
		panic("programmer error: multierr.New called with no errors")
	}
	return MultiErr(errs)
}

var _ error = MultiErr{}

func (m MultiErr) Error() string {
	if len(m) == 1 {
		return m[0].Error()
	}
	var b strings.Builder
	b.WriteString("multiple errors:")
	for _, e := range m {
		b.WriteString("\n\t")
		b.WriteString(e.Error())
	}
	b.WriteString("\n")
	return b.String()
}

// All reports whether all errors in a MultiErr (or, the singular
// non-multi error) pass the test.
func All(err error, test func(err error) bool) bool {
	errs, ok := err.(MultiErr)
	if !ok {
		return test(err)
	}
	for _, err := range errs {
		if !test(err) {
			return false
		}
	}
	return true
}
