package guest

import "sensors"

// Price an equal-principal loan portfolio under one discount-rate scenario.
func Price(scenario int) int {
	total := 0.0
	discount := 1.0 + float64(40+scenario)/10000.0
	for loan := 0; loan < 32; loan++ {
		balance := float64(80000 + loan*1250)
		principal := balance / 120.0
		coupon := float64(35+loan%11) / 10000.0
		factor := 1.0
		for month := 0; month < 120; month++ {
			factor /= discount
			total += (principal + balance*coupon) * factor
			balance -= principal
		}
	}
	return int(total * 100.0)
}

// Scan 128 sensors and sum the excess above their configured thresholds.
func Scan(batch int) int {
	total := 0
	base := batch * 31
	for i := base; i < base+128; i++ {
		value := sensors.Reading(i)
		limit := sensors.Limit(i)
		if value > limit {
			total += value - limit
		}
	}
	return total
}

// Fib traverses the recursive call tree without memoization.
func Fib(seed int) int {
	return fibonacci(14 + seed)
}

func fibonacci(n int) int {
	if n < 2 {
		return n
	}
	return fibonacci(n-2) + fibonacci(n-1)
}

// Sieve strikes each multiple starting at twice its prime.
func Sieve(seed int) int {
	n := 5000 + 31*seed
	flags := make([]bool, n)
	for i := 0; i < n; i++ {
		flags[i] = true
	}
	count := 0
	for i := 2; i <= n; i++ {
		if flags[i-1] {
			count++
			for k := i + i; k <= n; k += i {
				flags[k-1] = false
			}
		}
	}
	return count
}

// Mandelbrot counts pixels still inside after 50 iterations.
func Mandelbrot(seed int) int {
	n := 24 + 2*seed
	scale := 2.0 / float64(n)
	inside := 0
	for y := 0; y < n; y++ {
		ci := float64(y)*scale - 1.0
		for x := 0; x < n; x++ {
			cr := float64(x)*scale - 1.5
			zr, zi := cr, ci
			rr, ii := zr*zr, zi*zi
			inside++
			for i := 1; i < 50; i++ {
				zi = zr*zi*2.0 + ci
				zr = rr - ii + cr
				ii, rr = zi*zi, zr*zr
				if rr+ii > 4.0 {
					inside--
					break
				}
			}
		}
	}
	return inside
}
