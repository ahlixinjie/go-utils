package batch

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func ExampleAll() {
	ids := []int{1, 2, 3, 4, 5, 6, 7, 8}
	var output []int
	err := All(len(ids), 5, func(start, end int) error {
		output = append(output, ids[start:end]...)
		return nil
	})
	if err != nil {

	}
	fmt.Println(output)
	// Output: [1 2 3 4 5 6 7 8]
}

func Test(t *testing.T) {
	type r struct {
		start, end int
	}
	var ranges []r
	err := All(100, 10, func(start, end int) error {
		ranges = append(ranges, r{
			start: start,
			end:   end,
		})
		return nil
	})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(ranges) != 10 {
		t.Errorf("want length 10, got length %d", len(ranges))
	}

	expectedRanges := []r{
		{start: 0, end: 10},
		{start: 10, end: 20},
		{start: 20, end: 30},
		{start: 30, end: 40},
		{start: 40, end: 50},
		{start: 50, end: 60},
		{start: 60, end: 70},
		{start: 70, end: 80},
		{start: 80, end: 90},
		{start: 90, end: 100},
	}
	if !reflect.DeepEqual(expectedRanges, ranges) {
		t.Errorf("want %v, got %v", expectedRanges, ranges)
	}
}

func TestHalfPages(t *testing.T) {
	type r struct {
		start, end int
	}
	var ranges []r
	err := All(15, 10, func(start, end int) error {
		ranges = append(ranges, r{
			start: start,
			end:   end,
		})
		return nil
	})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(ranges) != 2 {
		t.Errorf("want length 2, got length %d", len(ranges))
	}

	expectedRanges := []r{
		{start: 0, end: 10},
		{start: 10, end: 15},
	}
	if !reflect.DeepEqual(expectedRanges, ranges) {
		t.Errorf("want %v, got %v", expectedRanges, ranges)
	}
}

func TestTinyPages(t *testing.T) {
	type r struct {
		start, end int
	}
	var ranges []r
	err := All(1, 10, func(start, end int) error {
		ranges = append(ranges, r{
			start: start,
			end:   end,
		})
		return nil
	})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(ranges) != 1 {
		t.Errorf("want length 1, got length %d", len(ranges))
	}
	expectedRanges := []r{
		{start: 0, end: 1},
	}
	if !reflect.DeepEqual(expectedRanges, ranges) {
		t.Errorf("want %v, got %v", expectedRanges, ranges)
	}
}

func TestAbort(t *testing.T) {
	type r struct {
		start, end int
	}
	var ranges []r
	err := All(20, 10, func(start, end int) error {
		ranges = append(ranges, r{
			start: start,
			end:   end,
		})
		return Abort
	})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(ranges) != 1 {
		t.Errorf("want length 1, got length %d", len(ranges))
	}
	expectedRanges := []r{
		{start: 0, end: 10},
	}
	if !reflect.DeepEqual(expectedRanges, ranges) {
		t.Errorf("want %v, got %v", expectedRanges, ranges)
	}
}

func TestErr(t *testing.T) {
	type r struct {
		start, end int
	}
	var ranges []r
	errTest := errors.New("something went wrong")
	err := All(20, 10, func(start, end int) error {
		ranges = append(ranges, r{
			start: start,
			end:   end,
		})
		return errTest
	})
	if !errors.Is(err, errTest) {
		t.Errorf("Unexpected error: %v", err)
	}
}

func Test_Parallel_OkCase(t *testing.T) {
	type testCase struct {
		name      string
		count     int
		batchSize int
		maxWorker int
		err       error
	}
	testCases := []testCase{
		{name: "parallel 1", count: 122, batchSize: 10, maxWorker: 4},
		{name: "parallel 2", count: 122, batchSize: 30, maxWorker: 100},
		{name: "parallel 3", count: 418, batchSize: 7, maxWorker: 13},
		{name: "parallel 4", count: 122, batchSize: 9, maxWorker: 10},
		{name: "parallel 5", count: 152, batchSize: 10, maxWorker: 10},
		{name: "parallel 6", count: 11, batchSize: 10, maxWorker: 10},
		{name: "parallel 7", count: 0, batchSize: 1, maxWorker: 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			looped := make([]int, tc.count)

			err := Parallel(context.Background(), tc.count, tc.batchSize, tc.maxWorker, func(ctx context.Context, start, end int) error {
				for i := start; i < end; i++ {
					looped[i] += 1
				}
				return nil
			})
			assert.NoError(t, err)

			for i, loop := range looped {
				assert.Equal(t, 1, loop, "index: "+strconv.Itoa(i))
			}
		})
	}
}

func Test_Parallel_ErrorCase(t *testing.T) {
	type testCase struct {
		name      string
		count     int
		batchSize int
		maxWorker int
		err       error
	}
	testCases := []testCase{
		{name: "0 batch size", count: 10, batchSize: 0, maxWorker: 10},
		{name: "0 max worker", count: 10, batchSize: 10, maxWorker: 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			looped := make([]int, tc.count)
			for i := range looped {
				looped[i] = 0
			}

			err := Parallel(context.Background(), tc.count, tc.batchSize, tc.maxWorker, func(ctx context.Context, start, end int) error {
				return nil
			})
			assert.Error(t, err)
		})
	}

	t.Run("batch err", func(t *testing.T) {
		err := Parallel(context.Background(), 100, 1, 10, func(ctx context.Context, start, end int) error {
			return errors.New("batch err")
		})
		assert.Error(t, err)
	})

	t.Run("abort early", func(t *testing.T) {
		err := Parallel(context.Background(), 100, 1, 10, func(ctx context.Context, start, end int) error {
			return Abort
		})
		assert.NoError(t, err)
	})

	t.Run("panic", func(t *testing.T) {
		counter := int32(0)
		err := Parallel(context.Background(), 100, 10, 10, func(ctx context.Context, start, end int) error {
			if atomic.AddInt32(&counter, 1) == 1 {
				panic("some panic here")
			}
			return nil
		})
		assert.Error(t, err)
	})

	// gomock uses this, and it will cause deadlock
	t.Run("goexit", func(t *testing.T) {
		counter := int32(0)
		err := Parallel(context.Background(), 100, 10, 10, func(ctx context.Context, start, end int) error {
			if atomic.AddInt32(&counter, 1) == 1 {
				runtime.Goexit()
			}
			return nil
		})
		assert.Error(t, err)
	})
}

func TestParallel(t *testing.T) {
	// Define a test case with a batch function that sleeps for a specific duration
	testCase := struct {
		count     int
		batchSize int
		maxWorker int
		sleepTime time.Duration
	}{
		count:     10,
		batchSize: 3,
		maxWorker: 5,
		sleepTime: 100 * time.Millisecond,
	}

	// Define the batch function for the test case
	batchFn := func(ctx context.Context, start, end int) error {
		select {
		case <-ctx.Done():
			return ctx.Err() // Return context error if canceled
		default:
			// Simulate work by sleeping for a specified duration
			time.Sleep(testCase.sleepTime)
			fmt.Printf("Processed batch [%d, %d)\n", start, end)
			return nil
		}
	}

	// Run the Parallel function with the test case configuration
	err := Parallel(context.Background(), testCase.count, testCase.batchSize, testCase.maxWorker, batchFn)
	assert.NoError(t, err)
}

func TestParallel_WithErrors(t *testing.T) {
	// Define a test case with a batch function that returns an error for a specific range
	testCase := struct {
		count      int
		batchSize  int
		maxWorker  int
		errorRange [2]int
	}{
		count:      10,
		batchSize:  3,
		maxWorker:  5,
		errorRange: [2]int{4, 7}, // Causes an error for range [4, 7)
	}

	// Define the batch function for the test case
	batchFn := func(ctx context.Context, start, end int) error {
		select {
		case <-ctx.Done():
			return ctx.Err() // Return context error if canceled
		default:
			// Simulate work
			fmt.Printf("Processing batch [%d, %d)\n", start, end)

			// Simulate an error for the specified range
			if start >= testCase.errorRange[0] && start < testCase.errorRange[1] {
				return errors.New("error occurred in batch")
			}

			return nil
		}
	}

	// Run the Parallel function with the test case configuration
	err := Parallel(context.Background(), testCase.count, testCase.batchSize, testCase.maxWorker, batchFn)
	assert.Error(t, err)
	assert.EqualError(t, err, "error occurred in batch")
}

func TestParallel_CancelContext(t *testing.T) {
	// Define a test case where the context is canceled before completing the batches
	testCase := struct {
		count     int
		batchSize int
		maxWorker int
	}{
		count:     10,
		batchSize: 3,
		maxWorker: 5,
	}

	// Define the batch function for the test case
	batchFn := func(ctx context.Context, start, end int) error {
		select {
		case <-ctx.Done():
			return ctx.Err() // Return context error if canceled
		default:
			// Simulate work
			fmt.Printf("Processing batch [%d, %d)\n", start, end)
			return nil
		}
	}

	// Create a cancellation context and cancel it immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Run the Parallel function with the canceled context and test case configuration
	err := Parallel(ctx, testCase.count, testCase.batchSize, testCase.maxWorker, batchFn)
	assert.Error(t, err)
	assert.EqualError(t, err, "context canceled")
}

func TestWorkerStarvationWithTimeout(t *testing.T) {
	// Create a context with a timeout for the batch
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up the necessary variables for parallel execution
	count := 10    // Total number of tasks
	batchSize := 2 // Number of tasks processed in each batch
	maxWorker := 2 // Maximum number of workers

	// Define a ParallelBatchFunc that performs a task that never completes
	eachFn := func(ctx context.Context, start, end int) error {
		// Simulate a task that blocks indefinitely
		<-ctx.Done()
		return nil
	}

	// Call the Parallel function and expect a deadlock error
	err := Parallel(ctx, count, batchSize, maxWorker, eachFn, WithTimeout(1*time.Second))
	assert.NoError(t, err)
}

func TestWorkerWithTimeout(t *testing.T) {
	// Create a context with a timeout for the batch
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Set up the necessary variables for parallel execution
	count := 10    // Total number of tasks
	batchSize := 2 // Number of tasks processed in each batch
	maxWorker := 2 // Maximum number of workers

	// Define a ParallelBatchFunc that performs a task that never completes
	eachFn := func(ctx context.Context, start, end int) error {
		// Simulate a task that blocks indefinitely
		<-ctx.Done()
		return nil
	}

	// Call the Parallel function and expect a deadlock error
	err := Parallel(ctx, count, batchSize, maxWorker, eachFn)
	assert.NoError(t, err)
}

func TestInadequateWorkerCount(t *testing.T) {
	// Create a context with a cancellation function
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up the necessary variables for parallel execution
	count := 10    // Total number of tasks
	batchSize := 2 // Number of tasks processed in each batch
	maxWorker := 2 // Maximum number of workers

	// Define a ParallelBatchFunc that returns Abort after processing a task
	eachFn := func(ctx context.Context, start, end int) error {
		// Simulate a task that aborts early
		if start == 2 {
			return Abort
		}
		// Simulate other tasks that take some time to complete
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	// Call the Parallel function and expect an error due to inadequate workers
	err := Parallel(ctx, count, batchSize, maxWorker, eachFn)
	assert.NoError(t, err)
}

func TestGoRoutineFn(t *testing.T) {
	// Create a context with a cancellation function
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up the necessary variables for parallel execution
	count := 10    // Total number of tasks
	batchSize := 2 // Number of tasks processed in each batch
	maxWorker := 2 // Maximum number of workers

	// Define a ParallelBatchFunc that performs a go routine task
	eachFn := func(ctx context.Context, start, end int) error {
		go func() {
			time.Sleep(1 * time.Second)
		}()
		return nil
	}

	// Call the Parallel function and expect a deadlock error
	err := Parallel(ctx, count, batchSize, maxWorker, eachFn)
	assert.NoError(t, err)
}

func benchmarkParallel(b *testing.B, count, batchSize, maxWorker int) {
	// Define the batch function for the benchmark
	batchFn := func(ctx context.Context, start, end int) error {
		// Simulate work by sleeping for a small duration
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	// Run the Parallel function in the benchmark
	for n := 0; n < b.N; n++ {
		_ = Parallel(context.Background(), count, batchSize, maxWorker, batchFn)
	}
}

// Before:
// goos: darwin
// goarch: amd64
// pkg: git.garena.com/shopee/promotion/promotion-common/batch
// cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
// Benchmark_Parallel_10_1_1-12    	      10	 104547270 ns/op	     768 B/op	       8 allocs/op

// After:
// goos: darwin
// goarch: amd64
// pkg: git.garena.com/shopee/promotion/promotion-common/batch
// cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
// === RUN   Benchmark_Parallel_10_1_1
// Benchmark_Parallel_10_1_1
// Benchmark_Parallel_10_1_1-12                  10         104460434 ns/op             956 B/op         13 allocs/op
func Benchmark_Parallel_10_1_1(b *testing.B) {
	benchmarkParallel(b, 10, 1, 1)
}

// Before:
// goos: darwin
// goarch: amd64
// pkg: git.garena.com/shopee/promotion/promotion-common/batch
// cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
// Benchmark_Parallel_10_5_5-12    	     100	  10669803 ns/op	     755 B/op	      10 allocs/op

// After:
// goos: darwin
// goarch: amd64
// pkg: git.garena.com/shopee/promotion/promotion-common/batch
// cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
// Benchmark_Parallel_10_5_5-12    	     100	  10495422 ns/op	     956 B/op	      15 allocs/op
func Benchmark_Parallel_10_5_5(b *testing.B) {
	benchmarkParallel(b, 10, 5, 5)
}

// Before:
// goos: darwin
// goarch: amd64
// pkg: git.garena.com/shopee/promotion/promotion-common/batch
// cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
// Benchmark_Parallel_1000_100_10-12    	     100	  10681186 ns/op	    2600 B/op	      27 allocs/op

// After:
// goos: darwin
// goarch: amd64
// pkg: git.garena.com/shopee/promotion/promotion-common/batch
// cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
// Benchmark_Parallel_1000_100_10-12    	     100	  10521900 ns/op	    2947 B/op	      32 allocs/op
func Benchmark_Parallel_1000_100_10(b *testing.B) {
	benchmarkParallel(b, 1000, 100, 10)
}

// Before:
// goos: darwin
// goarch: amd64
// pkg: git.garena.com/shopee/promotion/promotion-common/batch
// cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
// Benchmark_Parallel_10000_1000_100-12    	     100	  10684849 ns/op	    2567 B/op	      27 allocs/op

// After:
// goos: darwin
// goarch: amd64
// pkg: git.garena.com/shopee/promotion/promotion-common/batch
// cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
// Benchmark_Parallel_10000_1000_100-12    	     100	  10754753 ns/op	    2997 B/op	      33 allocs/op
func Benchmark_Parallel_10000_1000_100(b *testing.B) {
	benchmarkParallel(b, 10000, 1000, 100)
}
