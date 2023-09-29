package loadtester

import (
	"math"
)

// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm

type welfordVarianceAlg struct {
	count    int
	mean, m2 float64
}

func (wva *welfordVarianceAlg) Update(v float64) {
	wva.count++
	delta := v - wva.mean
	wva.mean += delta / float64(wva.count)
	delta2 := v - wva.mean
	wva.m2 += delta * delta2
}

func (wva *welfordVarianceAlg) Variance() float64 {
	if wva.count < 2 {
		return math.NaN()
	}

	return wva.m2 / float64(wva.count)
}

// func (wva *welfordVarianceAlg) Mean() float64 {
// 	if wva.count < 2 {
// 		return math.NaN()
// 	}

// 	return wva.mean
// }

// func (wva *welfordVarianceAlg) SampleVariance() float64 {
// 	if wva.count < 2 {
// 		return math.NaN()
// 	}

// 	return wva.m2 / (float64(wva.count) - float64(1.0))
// }
