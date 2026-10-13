package proxytron

// Thompson sampling draws each route's P(valid) from its Beta posterior
// (Pool.Acquire). The standard library has no Gamma or Beta variates, so
// they are here.

import (
	"fmt"
	"math"
	"math/rand/v2"
)

// gammaVariate draws from Gamma(alpha, 1) by Marsaglia and Tsang's method,
// boosted by U^(1/alpha) below alpha = 1.
//
// alpha must be positive: it panics otherwise, since the method would never
// return. Selection never asks for that, as a posterior is a positive prior
// plus non-negative outcome counts.
func gammaVariate(r *rand.Rand, alpha float64) float64 {
	if !(alpha > 0) {
		panic(fmt.Sprintf("proxy: Gamma variate of shape %v, want > 0", alpha))
	}
	if alpha < 1 {
		u := r.Float64()
		return gammaVariate(r, alpha+1) * math.Pow(u, 1/alpha)
	}
	d := alpha - 1.0/3.0
	c := 1 / math.Sqrt(9*d)
	for {
		var x, v float64
		for {
			x = r.NormFloat64()
			v = 1 + c*x
			if v > 0 {
				break
			}
		}
		v = v * v * v
		u := r.Float64()
		if u < 1-0.0331*x*x*x*x {
			return d * v
		}
		if math.Log(u) < 0.5*x*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
}

// betaVariate draws from Beta(alpha, beta) as y / (y + z) of two Gamma
// draws, and 0 when the first is 0 (without drawing the second).
func betaVariate(r *rand.Rand, alpha, beta float64) float64 {
	y := gammaVariate(r, alpha)
	if y != 0 {
		return y / (y + gammaVariate(r, beta))
	}
	return 0.0
}
