package loadtester

import (
	"math"
	"slices"
	"time"
)

type latencyList struct {
	data []time.Duration
}

func (ll *latencyList) add(d time.Duration) {
	ll.data = append(ll.data, d)
}

func (ll *latencyList) reset() {
	ll.data = ll.data[:0]
}

type percentileTarget struct {
	n  int // numerator
	d  int // denominator
	rt int // integerRoundingTerm
}

// newPTarget constructs a new integer rational multiplication lookup table record and validates the input elements
func newPTarget(percentileTimes100, n, d int) percentileTarget {
	if percentileTimes100 <= 0 || n <= 0 || d <= 0 || n >= d || d%2 != 0 || (10000%d != 0) || ((10000/d)*n != percentileTimes100) {
		panic("should never happen")
	}
	return percentileTarget{n, d, d >> 1}
}

const numPercentiles = 10

var percentileTargets = [numPercentiles]percentileTarget{
	newPTarget(2500, 1, 4), // 0
	newPTarget(5000, 1, 2), // 1
	newPTarget(7500, 3, 4), // 2
	// next val's n & d are intentionally doubled so last argument (d) can be even for integer rounding term ( d/2 )
	newPTarget(8000, 8, 10),       // 3
	newPTarget(8500, 17, 20),      // 4
	newPTarget(9000, 9, 10),       // 5
	newPTarget(9500, 19, 20),      // 6
	newPTarget(9900, 99, 100),     // 7
	newPTarget(9990, 999, 1000),   // 8
	newPTarget(9999, 9999, 10000), // 9
}

var percentileComputeOrder = [numPercentiles]uint8{
	0, // 1
	1, // 1
	2, // 3
	3, // 8
	5, // 9
	4, // 17
	6, // 19
	7, // 99
	8, // 999
	9, // 9999
}

func (ll *latencyList) readPercentileStrings(out *[numPercentiles]string) {

	maxIdx := len(ll.data) - 1
	if maxIdx < 0 {
		for i := range out {
			out[i] = ""
		}
		return
	}

	if maxIdx == 0 {
		s := durationToNanoString(ll.data[0])
		for i := range out {
			out[i] = s
		}
		return
	}

	slices.Sort(ll.data)

	for pci, pcv := range percentileComputeOrder {
		pt := percentileTargets[pcv]

		// check for integer overflow
		if pt.n <= ((math.MaxInt - pt.rt) / maxIdx) {
			// integer math multiplication operation will not overflow

			v := ll.data[((maxIdx*pt.n)+pt.rt)/pt.d]

			out[pcv] = durationToNanoString(v)
			continue
		}

		// would overflow, time to use expensive floats
		for {
			fidx := math.Round(float64(pt.n) / float64(pt.d) * float64(maxIdx))

			idx := int(fidx)
			v := ll.data[idx]
			out[pcv] = durationToNanoString(v)

			pci++
			if pci >= numPercentiles {
				return
			}

			pcv = percentileComputeOrder[pci]
		}
	}
}
