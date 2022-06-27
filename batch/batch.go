package batch

import "errors"

// DoBatchFunc is called for each batch.
// Any error will cancel the batching operation but returning ErrAbort
// indicates it was deliberate, and not an error case.
// [start:end)
type DoBatchFunc func(start, end int) error

// ErrAbort indicates a batch operation should abort early.
var ErrAbort = errors.New("done")

// All calls eachFn for all items
// Returns any error from eachFn except for ErrAbort it returns nil.
func All(count, batchSize int, eachFn DoBatchFunc) error {
	for i := 0; i < count; i += batchSize {
		end := i + batchSize
		if end > count {
			end = count
		}
		err := eachFn(i, end)
		if err == ErrAbort {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}
