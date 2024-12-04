package batch

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// BatchFunc is called for each batch.
// Any error will cancel the batching operation but returning Abort
// indicates it was deliberate, and not an error case.
// [start:end)
type BatchFunc func(start, end int) error

type ParallelBatchFunc func(ctx context.Context, start, end int) error

// Abort indicates a batch operation should abort early.
// nolint:golint
var (
	Abort      = errors.New("done")
	defaultErr = errors.New("default")
	nilErr     = errors.New("nil")
)

// All calls eachFn for all items
// Returns any error from eachFn except for Abort it returns nil.
func All(count, batchSize int, eachFn BatchFunc) error {
	for i := 0; i < count; i += batchSize {
		end := i + batchSize
		if end > count {
			end = count
		}
		err := eachFn(i, end)
		if err == Abort {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// parallelOptions holds the options for the Parallel function.
type parallelOptions struct {
	timeout time.Duration // Timeout duration for the entire execution
}

// WithTimeout sets the timeout duration for the Parallel function.
// In case the ctx context.Context already has a deadline / timeout,
// adding the longer timeout will not be applied.
func WithTimeout(timeout time.Duration) func(*parallelOptions) {
	return func(opts *parallelOptions) {
		opts.timeout = timeout
	}
}

func Parallel(
	ctx context.Context,
	count, batchSize, maxWorker int,
	eachFn ParallelBatchFunc,
	options ...func(*parallelOptions),
) error {
	if count == 0 {
		return nil
	}
	maxWorker, totalInvokeCount, err := splitParallel(count, batchSize, maxWorker)
	if err != nil {
		return err
	}
	// Create a parallelOptions struct and apply any specified options
	opts := &parallelOptions{
		timeout: 0, // Default timeout duration (no timeout)
	}
	for _, opt := range options {
		opt(opts)
	}
	return parallelInternal(ctx, count, maxWorker, totalInvokeCount, batchSize, eachFn, opts.timeout)
}

func splitParallel(count, batchSize, maxWorker int) (int, int, error) {
	if count == 0 {
		return 0, 0, nil
	}
	if batchSize <= 0 {
		return 0, 0, errors.New("parallel batch size cannot be 0")
	}
	if maxWorker <= 0 {
		return 0, 0, errors.New("max worker cannot be 0")
	}

	totalInvokeCount := count / batchSize // how many times eachFn is called
	if count%batchSize != 0 {             // have leftovers, increase totalInvokeCount by 1
		totalInvokeCount++
	}
	if maxWorker > totalInvokeCount { // some workers will not get a job, let's make maxWorker = totalInvokeCount
		maxWorker = totalInvokeCount
	}

	return maxWorker, totalInvokeCount, nil
}

func overwriteTimeoutIfSet(
	ctx context.Context,
	timeout time.Duration,
) (context.Context, context.CancelFunc) {
	// When we want to overwrite timeout in ctx
	if timeout > 0 {
		return context.WithTimeout(ctx, timeout)
	}
	// When the deadline in ctx is set, use it
	if _, deadlineSet := ctx.Deadline(); deadlineSet {
		return context.WithCancel(ctx)
	}
	// No timeout
	return context.WithCancel(ctx)
}

// nolint:funlen
func parallelInternal(
	ctx context.Context,
	count, maxWorker, totalInvokeCount, batchSize int,
	eachFn ParallelBatchFunc,
	timeout time.Duration,
) error {
	dataChan := make(chan int, totalInvokeCount)
	errChan := make(chan error, totalInvokeCount)

	batchCtx, cancel := overwriteTimeoutIfSet(ctx, timeout)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(maxWorker)

	for worker := 0; worker < maxWorker; worker++ {
		go func() {
			err := defaultErr
			defer wg.Done()
			defer func() {
				r := recover()
				if r != nil {
					errChan <- errors.New(fmt.Sprintf("parallel batch panic recovered: %v, %s", r, string(debug.Stack())))
					return
				}
				//To handle the runtime.Goexit() of gomock
				//This means the eachFn exit and doesn't throw any panic
				if errors.Is(defaultErr, err) {
					errChan <- errors.New(fmt.Sprintf("goroutine exited without panic, %s", string(debug.Stack())))
				}
			}()
			for {
				select {
				case <-batchCtx.Done():
					err = nilErr
					return
				case start, hasJob := <-dataChan:
					if !hasJob {
						err = nilErr
						return
					}
					end := start + batchSize
					if end > count {
						end = count
					}
					err = eachFn(batchCtx, start, end)
					errChan <- err
				}
			}
		}()
	}

	for i := 0; i < count; i += batchSize {
		dataChan <- i
	}
	close(dataChan)

	wg.Wait()

	// Close the error channel to signal that no more errors will be sent
	close(errChan)

	finishCount := 0
	for err := range errChan {
		if err != nil {
			if errors.Is(err, Abort) {
				return nil
			}
			return err
		}
		finishCount++
		if finishCount >= totalInvokeCount {
			return nil
		}
	}
	return nil
}
