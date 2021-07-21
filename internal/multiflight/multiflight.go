// Make multiple attempts, even concurrently.
package multiflight

import (
	"context"
	"errors"
	"log"
	"sort"
	"time"

	"bazil.org/plop/internal/multierr"
)

const debugLog = false

func debugf(fmt string, args ...interface{}) {
	if debugLog {
		log.Printf("multiflight: "+fmt, args...)
	}
}

type op struct {
	delay time.Duration
	fn    func(context.Context) (interface{}, error)
}

type Multiflight struct {
	maxWorkers int
	actions    []op
}

func New() *Multiflight {
	m := &Multiflight{
		maxWorkers: 4,
	}
	return m
}

func (m *Multiflight) SetMaxWorkers(n int) {
	m.maxWorkers = n
}

// Add an action to run later.
//
// In general, avoid side effects in the action function, and where unavoidable make them goroutine safe.
// Especially avoid mimicking function return by assigning results to variables, as alternate actions may succeed concurrently.
//
// Each individual action function may or may not be called.
//
// The returned value of some successful action will be returned from Run.
// Note that concurrent actions may end successfully.
func (m *Multiflight) Add(delay time.Duration, action func(context.Context) (interface{}, error)) {
	m.actions = append(m.actions, op{delay: delay, fn: action})
}

type result struct {
	success interface{}
	err     error
}

// Run the actions added until at least one succeeds.
//
// No other methods may be called after calling Run.
//
// The returned error may be a MultiErr.
func (m *Multiflight) Run(ctx context.Context) (interface{}, error) {
	actions := m.actions
	if len(actions) == 0 {
		return nil, errors.New("multiflight: no actions to try")
	}
	// Sort ops by shortest delay first.
	sort.SliceStable(actions, func(i, j int) bool {
		return actions[i].delay < actions[j].delay
	})
	// Adjust all delays so first one is always at 0.
	min := actions[0].delay
	for i := range actions {
		actions[i].delay -= min
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()
	var errs []error
	exited := make(chan result, 1)
	numWorkers := 0
	next := time.NewTimer(0)

	for {
		debugf("loop: numWorkers=%d actions=%d", numWorkers, len(actions))
		wantMore := numWorkers == 0
		if numWorkers > 0 {
			// Only worry about *when* to start more if there's
			// currently at least some workers.

			if len(actions) > 0 {
				debugf("delay=%v", actions[0].delay)
				d := time.Until(start.Add(actions[0].delay))
				if !next.Stop() {
					<-next.C
				}
				next.Reset(d)
			}

			timeBased := next.C
			if m.maxWorkers > 0 && numWorkers >= m.maxWorkers {
				// Can't start more until some workers exit.
				timeBased = nil
			}
			// Wait until we have a reason to act.
			select {
			case <-timeBased:
				// Slow progress, spawn an attempt with the next
				// fallback.
				debugf("trigger by time")
				wantMore = true

			case r := <-exited:
				if r.err == nil {
					// Success!
					debugf("worker success")
					return r.success, nil
				}
				debugf("worker error: %v", r.err)
				errs = append(errs, r.err)
				numWorkers -= 1
			}
		}

		if m.maxWorkers > 0 && numWorkers >= m.maxWorkers {
			// Never go over the limit.
			debugf("limit workers: num=%d max=%d", numWorkers, m.maxWorkers)
			wantMore = false
		}

		if wantMore && len(actions) > 0 {
			act := actions[0]
			actions = actions[1:]
			numWorkers += 1
			debugf("start worker: num=%d", numWorkers)
			go func() {
				success, err := act.fn(ctx)
				exited <- result{success: success, err: err}
			}()
		}

		if numWorkers == 0 && len(actions) == 0 {
			// Nothing in flight and we're out of things to try.
			debugf("out of options")
			break
		}

	}
	if len(errs) > 0 {
		err := multierr.MultiErr(errs)
		debugf("all fail: %v", err)
		return nil, err
	}
	panic("not reached")
}
