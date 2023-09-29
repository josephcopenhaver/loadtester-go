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

func varianceFloatString(f float64) string {
	if math.IsNaN(f) {
		return ""
	}

	v := int64(math.MaxInt64)
	if !math.IsInf(f, 1) {
		v = int64(math.Round(f)) & math.MaxInt64
	}

	return strconv.FormatInt(v, 10) + "ns"
}
