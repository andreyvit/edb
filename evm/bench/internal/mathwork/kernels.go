// Package mathwork holds the independent numeric workloads. Both the Go
// compiler and the EVM compiler consume this file.
package mathwork

func Filter(n int, x float64, a, b []float64) float64 {
	var fast, slow float64
	for i := 0; i < n; i++ {
		fast += 0.07 * (x - fast)
		slow += 0.03 * (fast - slow)
	}
	return slow
}

func Oscillator(n int, x float64, a, b []float64) float64 {
	position, velocity := x, 0.0
	for i := 0; i < n; i++ {
		accel := -0.8*position - 0.03*velocity
		velocity += 0.01 * accel
		position += 0.01 * velocity
	}
	return position
}

func Midpoint(n int, x float64, a, b []float64) float64 {
	dx := x / float64(n)
	total := 0.0
	for i := 0; i < n; i++ {
		t := (float64(i) + 0.5) * dx
		total += 1.0 / (1.0 + t*t)
	}
	return total * dx
}

func Chebyshev(n int, x float64, a, b []float64) float64 {
	prev, current := 1.0, x
	for i := 1; i < n; i++ {
		prev, current = current, 2*x*current-prev
	}
	return current
}

func Horner(n int, x float64, a, b []float64) float64 {
	acc := 0.0
	for i := 0; i < n; i++ {
		acc = acc*x + a[i]
	}
	return acc
}

func Dot(n int, x float64, a, b []float64) float64 {
	sum := 0.0
	for i := 0; i < n; i++ {
		sum += a[i] * b[i]
	}
	return sum
}

func Kahan(n int, x float64, a, b []float64) float64 {
	sum, correction := 0.0, 0.0
	for i := 0; i < n; i++ {
		value := a[i] - correction
		next := sum + value
		correction = (next - sum) - value
		sum = next
	}
	return sum
}

func Variance(n int, x float64, a, b []float64) float64 {
	mean, m2 := 0.0, 0.0
	for i := 0; i < n; i++ {
		delta := a[i] - mean
		mean += delta / float64(i+1)
		m2 += delta * (a[i] - mean)
	}
	return m2 / float64(n)
}

// Calibrate and save each sample. Return the energy of the calibrated signal.
func Calibrate(n int, x float64, a, b []float64) float64 {
	energy := 0.0
	for i := 0; i < n; i++ {
		v := a[i]*x + 0.125
		b[i] = v
		energy += v * v
	}
	return energy
}

func Autocorrelation(n int, x float64, a, b []float64) float64 {
	var sum, power float64
	for i := 1; i < n; i++ {
		sum += a[i] * a[i-1]
		power += a[i] * a[i]
	}
	return sum / power
}

func SoftClip(n int, x float64, a, b []float64) float64 {
	total := 0.0
	for i := 0; i < n; i++ {
		v := a[i] * x
		if v > 0.3 {
			v = 0.3 + (v-0.3)*0.1
		} else if v < -0.3 {
			v = -0.3 + (v+0.3)*0.1
		}
		total += v * v
	}
	return total
}

func Newton(n int, x float64, a, b []float64) float64 {
	root := 1.0
	for i := 0; i < n; i++ {
		next := 0.5 * (root + x/root)
		err := next - root
		if err*err < 1e-24 {
			return next
		}
		root = next
	}
	return root
}
