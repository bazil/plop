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
		time.Sleep(100 * time.Millisecond)
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
	// This will most commonly output "fast", but a loaded system
	// might also result in "slow".
	fmt.Printf("result: %v\n", result)
}
