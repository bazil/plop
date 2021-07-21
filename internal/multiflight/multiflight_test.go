package multiflight_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"bazil.org/plop/internal/multierr"
	"bazil.org/plop/internal/multiflight"
)

func TestBypassErrorsWhenSuccess(t *testing.T) {
	m := multiflight.New()
	wakeup := make(chan struct{})
	const greeting = "Hello, world"
	m.Add(0, func(ctx context.Context) (interface{}, error) {
		defer close(wakeup)
		return nil, errors.New("fail for test")
	})
	m.Add(1*time.Millisecond, func(ctx context.Context) (interface{}, error) {
		// Improve the chances that the failing action is processed first.
		<-wakeup
		time.Sleep(5 * time.Millisecond)
		return greeting, nil
	})
	ctx := context.Background()
	result, err := m.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if g, e := result, greeting; g != e {
		t.Fatalf("unexpected result: %v != %v", g, e)
	}
}

func TestAllError(t *testing.T) {
	m := multiflight.New()
	var (
		errOne = errors.New("one")
		errTwo = errors.New("two")
	)
	m.Add(0, func(ctx context.Context) (interface{}, error) {
		return nil, errOne
	})
	m.Add(0, func(ctx context.Context) (interface{}, error) {
		return nil, errTwo
	})
	ctx := context.Background()
	result, err := m.Run(ctx)
	if err == nil {
		t.Fatalf("expected an error: %#v", result)
	}
	merr, ok := err.(multierr.MultiErr)
	if !ok {
		t.Fatalf("expected a MultiErr: %#v", err)
	}
	// The ordering is not guaranteed.
	seen := make(map[error]struct{})
	for _, err := range merr {
		seen[err] = struct{}{}
	}
	if g, e := len(seen), 2; g != e {
		t.Fatalf("unexpected errors: %v", seen)
	}
	if _, ok := seen[errOne]; !ok {
		t.Fatalf("missing errOne: %v", seen)
	}
	if _, ok := seen[errTwo]; !ok {
		t.Fatalf("missing errOne: %v", seen)
	}
}
