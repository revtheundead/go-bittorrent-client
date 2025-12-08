package shutdown

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// Closer is an interface for resources that can be closed
type Closer interface {
	Close() error
}

// CloserFunc wraps a close function
type CloserFunc func() error

func (f CloserFunc) Close() error {
	return f()
}

// CloseHandler creates a shutdown handler for a Closer
func CloseHandler(name string, closer Closer) Handler {
	return func(ctx context.Context) error {
		if closer == nil {
			return nil
		}
		return closer.Close()
	}
}

// CloseHandlerFunc creates a shutdown handler from a close function
func CloseHandlerFunc(fn func() error) Handler {
	return func(ctx context.Context) error {
		if fn == nil {
			return nil
		}
		return fn()
	}
}

// MultiCloseHandler creates a handler that closes multiple resources
func MultiCloseHandler(closers ...Closer) Handler {
	return func(ctx context.Context) error {
		var errs []error

		for _, closer := range closers {
			if closer != nil {
				if err := closer.Close(); err != nil {
					errs = append(errs, err)
				}
			}
		}

		if len(errs) > 0 {
			return fmt.Errorf("multiple close errors: %v", errs)
		}

		return nil
	}
}

// SaveStateHandler creates a handler that saves state to a writer
func SaveStateHandler(w io.Writer, data []byte) Handler {
	return func(ctx context.Context) error {
		_, err := w.Write(data)
		return err
	}
}

// ChannelCloseHandler creates a handler that closes a channel
func ChannelCloseHandler(ch chan struct{}) Handler {
	var once sync.Once
	return func(ctx context.Context) error {
		once.Do(func() {
			close(ch)
		})
		return nil
	}
}

// WaitGroupHandler creates a handler that waits for a WaitGroup
func WaitGroupHandler(wg *sync.WaitGroup) Handler {
	return func(ctx context.Context) error {
		done := make(chan struct{})

		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// FuncHandler wraps a function as a shutdown handler
func FuncHandler(fn func() error) Handler {
	return func(ctx context.Context) error {
		if fn == nil {
			return nil
		}
		return fn()
	}
}

// CancelHandler creates a handler that calls a context cancel function
func CancelHandler(cancel context.CancelFunc) Handler {
	return func(ctx context.Context) error {
		if cancel != nil {
			cancel()
		}
		return nil
	}
}

// CompositeHandler creates a handler that runs multiple handlers sequentially
func CompositeHandler(handlers ...Handler) Handler {
	return func(ctx context.Context) error {
		var errs []error

		for _, handler := range handlers {
			if handler != nil {
				if err := handler(ctx); err != nil {
					errs = append(errs, err)
				}
			}
		}

		if len(errs) > 0 {
			return fmt.Errorf("composite handler errors: %v", errs)
		}

		return nil
	}
}

// ParallelHandler creates a handler that runs multiple handlers in parallel
func ParallelHandler(handlers ...Handler) Handler {
	return func(ctx context.Context) error {
		var wg sync.WaitGroup
		errCh := make(chan error, len(handlers))

		for _, handler := range handlers {
			if handler == nil {
				continue
			}

			wg.Add(1)
			go func(h Handler) {
				defer wg.Done()
				if err := h(ctx); err != nil {
					errCh <- err
				}
			}(handler)
		}

		// Wait for all handlers to complete
		wg.Wait()
		close(errCh)

		// Collect errors
		var errs []error
		for err := range errCh {
			errs = append(errs, err)
		}

		if len(errs) > 0 {
			return fmt.Errorf("parallel handler errors: %v", errs)
		}

		return nil
	}
}

// DelayHandler creates a handler that executes after a delay
func DelayHandler(delay func() error) Handler {
	return func(ctx context.Context) error {
		if delay == nil {
			return nil
		}
		return delay()
	}
}

// NoOpHandler returns a handler that does nothing
func NoOpHandler() Handler {
	return func(ctx context.Context) error {
		return nil
	}
}
