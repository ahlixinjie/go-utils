package set

import (
	"testing"
)

func TestSetExample(t *testing.T) {
	set1 := NewSet(1, 2, 3)
	set2 := NewSet([]int{3, 4, 5}...)

	d1 := set1.DifferenceSet(set2)
	d1Expect := []int{1, 2} //set1-set2
	if len(d1) != len(d1Expect) {
		t.Errorf("DifferenceSet not expected")
	}
	for i := range d1 {
		if d1[i] != d1Expect[i] {
			t.Errorf("DifferenceSet not expected")
		}
	}

	d2 := set2.DifferenceSet(set1)
	d2Expect := []int{4, 5} //set1-set2
	if len(d2) != len(d2Expect) {
		t.Errorf("DifferenceSet not expected")
	}
	for i := range d2 {
		if d2[i] != d2Expect[i] {
			t.Errorf("DifferenceSet not expected")
		}
	}
}
