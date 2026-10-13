package proxytron

import (
	"math"
	"testing"
)

// The sampler is a Beta(a, b): its mean and variance over many draws, for
// the shapes selection uses (a prior below 1 and a record well above).
func TestBetaVariate_Distribution(t *testing.T) {
	r := seeded(42)
	for _, c := range [][2]float64{{0.5, 1.5}, {7.01, 1.5}, {1, 1}, {0.4, 2.6}} {
		a, b := c[0], c[1]
		const n = 200000
		sum, sq := 0.0, 0.0
		for range n {
			x := betaVariate(r, a, b)
			if x < 0 || x > 1 {
				t.Fatalf("Beta(%v, %v) drew %v", a, b, x)
			}
			sum += x
			sq += x * x
		}
		mean := sum / n
		variance := sq/n - mean*mean
		wantMean := a / (a + b)
		wantVar := a * b / ((a + b) * (a + b) * (a + b + 1))
		if math.Abs(mean-wantMean) > 0.005 || math.Abs(variance-wantVar) > 0.005 {
			t.Errorf("Beta(%v, %v): mean %.4f var %.4f, want %.4f and %.4f", a, b, mean, variance, wantMean, wantVar)
		}
	}
}

// A shape that is not positive has no Gamma distribution: drawing one is a
// bug, and it panics rather than looping forever.
func TestGammaVariate_PanicsOnAShapeThatIsNotPositive(t *testing.T) {
	for _, alpha := range []float64{0, -1, math.NaN(), math.Inf(-1)} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("gammaVariate(%v) returned", alpha)
				}
			}()
			gammaVariate(seeded(1), alpha)
		}()
	}
}
