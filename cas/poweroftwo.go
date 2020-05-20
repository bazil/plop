package cas

import "math/bits"

func nextPowerOfTwo(v uint32) uint32 {
	// http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}

func nearestPowerOfTwo(v uint32) uint32 {
	next := nextPowerOfTwo(v)
	prev := next >> 1
	if v-prev < next-v {
		return prev
	}
	return next
}

// bitsOfPowerOfTwo rounds to nearest power of two and reports the
// number of bits needed to store it.
func bitsOfPowerOfTwo(v uint32) int {
	v = nearestPowerOfTwo(v)
	return bits.TrailingZeros32(v)
}
