package batch

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestExampleAll(t *testing.T) {
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
		return ErrAbort
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
	if err != errTest {
		t.Errorf("Unexpected error: %v", err)
	}
}
