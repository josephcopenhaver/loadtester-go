package loadtester

import (
	"math"
	"strconv"
)

// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm

type welfordVariance struct {
	mean, m2 float64
}

func (wv *welfordVariance) Update(count int, v float64) {
	delta := v - wv.mean
	wv.mean += delta / float64(count)
	delta2 := v - wv.mean
	wv.m2 += delta * delta2
}

func (wv *welfordVariance) Variance(count int) float64 {
	if count < 2 {
		return math.NaN()
	}

	return wv.m2 / float64(count)
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

// varianceFloatString takes a zero or greater than zero or NaN float value representing welford variance of a sample of floats
// and returns a "unit-squared" representation integer as a string
//
// Note that if you copy this function it really should panic if the input is negative. Variance cannot be negative!
func varianceFloatString(f float64) string {
	// round first thing because it can cause an overflow on various CPU architectures
	f = math.Round(f)

	if math.IsNaN(f) {
		return ""
	}

	v := int64(math.MaxInt64)
	if !math.IsInf(f, 1) {
		if n := int64(f); n >= 0 {
			v = n
		}
	}

	return strconv.FormatInt(v, 10)
}
