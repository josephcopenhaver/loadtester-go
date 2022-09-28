package loadtester

import (
	"testing"
)

func TestTaskResultFlagsIsZero(t *testing.T) {
	var trf taskResultFlags

	if !trf.isZero() {
		t.Error("zero initialized taskResultFlags isZero() call did not return true")
	}

	trf = taskResultFlags{0, 0, 0, 0}
	if !trf.isZero() {
		t.Error("zero initialized taskResultFlags isZero() call did not return true")
	}

	trf = taskResultFlags{1, 1, 1, 1}
	if trf.isZero() {
		t.Error("non-zero initialized taskResultFlags isZero() call did not return false")
	}

	trf = taskResultFlags{1, 0, 0, 0}
	if trf.isZero() {
		t.Error("non-zero initialized taskResultFlags isZero() call did not return false")
	}
}
