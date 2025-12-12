package loadtester

import (
	"math"
)

// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm

type welfordVariance struct {
	mean, m2 float64
}

// Update requires the value v to always be a valid finite non-negative float64
func (wv *welfordVariance) Update(count int, v float64) {
	// the non-negative invariant exists because the implementation
	// defends against negative epsilons due to multiplications using
	// extremely small non-negative values
	delta := v - wv.mean
	wv.mean += delta / float64(count)
	delta2 := v - wv.mean
	// if statement defends against the addition of negative epsilon
	if m2Delta := delta * delta2; m2Delta > 0 {
		wv.m2 += m2Delta
	}
}

func (wv *welfordVariance) Variance(count int) float64 {
	if count < 2 {
		return math.NaN()
	}

	result := wv.m2 / float64(count)
	// if statement defends against returning a negative value
	// given all update float values are non-negative and
	// multiplication / division operations using extremely small
	// non-negative values can introduce negative epsilons
	if result < 0 {
		result = 0
	}
	return result
}

// func (wv *welfordVariance) Mean(count int) float64 {
// 	if count < 2 {
// 		return math.NaN()
// 	}

// 	return wv.mean
// }

// func (wv *welfordVariance) SampleVariance(count int) float64 {
// 	if count < 2 {
// 		return math.NaN()
// 	}

// 	return wv.m2 / (float64(count) - float64(1.0))
// }
