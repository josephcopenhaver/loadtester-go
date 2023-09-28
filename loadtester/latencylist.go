package loadtester

import (
	"math"
	"slices"
	"time"
)

type latencyList struct {
	data []time.Duration
}

func newLatencyList() *latencyList {
	return &latencyList{}
}

func (ll *latencyList) add(d time.Duration) {
	ll.data = append(ll.data, d)
}

func (ll *latencyList) reset() {
	ll.data = ll.data[:0]
}

func (ll *latencyList) stringPercentile(p uint8) string {

	maxIdx := len(ll.data) - 1
	if maxIdx < 0 {
		return ""
	}

	slices.Sort(ll.data)

	const (
		denominator             = 100
		integerMathRoundingTerm = denominator / 2
	)

	// check for integer overflow
	if maxIdx == 0 || int(p) <= ((math.MaxInt-integerMathRoundingTerm)/maxIdx) {
		// integer math multiplication operation will not overflow

		v := ll.data[(maxIdx*int(p)+integerMathRoundingTerm)/denominator]

		return v.String()
	}

	// p * size would overflow, time to use expensive floats
	fidx := math.Round(float64(p) / float64(denominator) * float64(maxIdx))
	idx := int(fidx)
	v := ll.data[idx]

	return v.String()
}
