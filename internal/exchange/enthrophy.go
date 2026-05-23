package exchange

import "math"

func shannonEntropy(data []byte) float64 {
	if len(data) == 0 {
		return 0
	}
	var freq [256]int

	for _, b := range data {
		freq[b]++
	}

	var entropy float64
	length := float64(len(data))

	for _, count := range freq {
		if count == 0 {
			continue
		}

		p := float64(count) / length
		entropy -= p * math.Log2(p)
	}
	return entropy
}
