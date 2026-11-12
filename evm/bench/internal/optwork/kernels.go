// Package optwork holds workloads compiled by both Go and EVM.
package optwork

func mix(word, key uint64) uint64 {
	return (word ^ key) * 16777619
}

// Hash separately keyed fields, with the key reused across records.
func Digest(n, seed int) int {
	state := uint64(seed)
	key := uint64(seed + 17)
	for i := 0; i < n; i++ {
		state = mix(state, uint64(i))
		if i&3 == 1 {
			state = mix(state, key)
		}
	}
	return int(state)
}

func adjusted(base, change int) int { return base*3 + change }
func within(x, limit int) bool      { return x < limit }

// Independently calculate a charge and a threshold from the same inputs.
func Tariff(n, seed int) int {
	total := 0
	for i := 0; i < n; i++ {
		base := (i + seed) & 255
		charge := adjusted(base, seed) * 2
		if within(adjusted(base, seed), 200) {
			total += charge
		} else {
			total += charge / 3
		}
	}
	return total
}

func Histogram(n, seed int) int {
	bins := make([]int, n/4+1)
	for i := 0; i < n; i++ {
		bins[(i*31+seed)%len(bins)]++
	}
	sum := 0
	for i, v := range bins {
		sum += v * v * (i + 1)
	}
	return sum
}

type sample struct{ weight int }

func Queue(n, seed int) int {
	pool := [4]sample{{1}, {3}, {5}, {seed}}
	queue := make([]*sample, n)
	for i := 0; i < n; i++ {
		queue[i] = &pool[(i+seed)&3]
	}
	total := 0
	for _, v := range queue {
		total += v.weight
	}
	return total
}

// Three mixing rounds per input word, with an unused induction variable.
func Rounds(n, seed int) int {
	state := uint64(seed + 1)
	for i := 0; i < n; i++ {
		state += uint64(i)
		for round := 0; round < 3; round++ {
			state = (state ^ (state >> 13)) * 0x85ebca6b
		}
	}
	return int(state)
}

// Sum a weighted neighborhood. Here the induction variable enters a multiply.
func Stencil(n, seed int) int {
	total := 0
	for i := 0; i < n; i++ {
		for offset := -1; offset <= 1; offset++ {
			total += ((i + offset) & 255) * (seed + offset*offset)
		}
	}
	return total
}

func taps(a, b, c, d int) int { return (a+b)*3 + (c+d)*5 }
func Channels(n, seed int) int {
	acc := seed
	for i := 0; i < n; i++ {
		weight := 0
		for lane := 0; lane < 4; lane++ {
			weight += taps(i, lane, i&31, seed)
		}
		acc = (acc + weight) & 0xffffff
	}
	return acc
}
