package task

import (
	"context"

	"github.com/xtls/xray-core/common/signal/semaphore"
)

// OnSuccess executes g() after f() returns nil.
func OnSuccess(f func() error, g func() error) func() error {
	return func() error {
		if err := f(); err != nil {
			return err
		}
		return g()
	}
}

// Lifetime retains asynchronous work even when Run returns before its tasks.
// The release receives the task's actual result, not Run's early cancellation.
type Lifetime interface {
	RetainTask() func(error)
}

type lifetimeKey struct{}

func ContextWithLifetime(ctx context.Context, lifetime Lifetime) context.Context {
	return context.WithValue(ctx, lifetimeKey{}, lifetime)
}

// Run executes a list of tasks in parallel, returns the first error encountered or nil if all tasks pass.
func Run(ctx context.Context, tasks ...func() error) error {
	n := len(tasks)
	s := semaphore.New(n)
	done := make(chan error, 1)
	lifetime, _ := ctx.Value(lifetimeKey{}).(Lifetime)

	for _, task := range tasks {
		<-s.Wait()
		var release func(error)
		if lifetime != nil {
			release = lifetime.RetainTask()
		}
		go func(f func() error, release func(error)) {
			err := f()
			if release != nil {
				release(err)
			}
			if err == nil {
				s.Signal()
				return
			}

			select {
			case done <- err:
			default:
			}
		}(task, release)
	}

	/*
		if altctx := ctx.Value("altctx"); altctx != nil {
			ctx = altctx.(context.Context)
		}
	*/

	for i := 0; i < n; i++ {
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-s.Wait():
		}
	}

	/*
		if cancel := ctx.Value("cancel"); cancel != nil {
			cancel.(context.CancelFunc)()
		}
	*/

	return nil
}
