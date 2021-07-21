package multiflight_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"bazil.org/plop/internal/multiflight"
)

func Example() {
	m := multiflight.New()
	m.SetMaxWorkers(10)
	m.Add(0, func(ctx context.Context) (interface{}, error) {
		// So ridiculously long that this should never win the race,
		// to keep the example reproducible.
		time.Sleep(1 * time.Second)
		return "slow", nil
	})
	m.Add(10*time.Millisecond, func(ctx context.Context) (interface{}, error) {
		return nil, errors.New("failing")
	})
	m.Add(20*time.Millisecond, func(ctx context.Context) (interface{}, error) {
		time.Sleep(5 * time.Millisecond)
		return "fast", nil
	})
	ctx := context.Background()
	result, err := m.Run(ctx)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
	// This will practically always output "fast", but on a
	// ridiculously loaded system might also result in "slow".
	fmt.Printf("result: %v\n", result)
	// Output:
	// result: fast
}
