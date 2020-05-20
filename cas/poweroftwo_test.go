package cas

import (
	"strconv"
	"testing"
)

func TestNearestPowerOfTwo(t *testing.T) {
	run := func(input uint32, want uint32) {
		t.Run(strconv.FormatUint(uint64(input), 10),
			func(t *testing.T) {
				got := nearestPowerOfTwo(input)
				if got != want {
					t.Errorf("%d != %d", got, want)
				}
			},
		)
	}
	run(42, 32)
	run(50, 64)
	run(1<<25-10, 1<<25)
	run(1<<25+1, 1<<25)
}

func TestBitsOfPowerOfTwo(t *testing.T) {
	run := func(input uint32, want int) {
		t.Run(strconv.FormatUint(uint64(input), 10),
			func(t *testing.T) {
				got := bitsOfPowerOfTwo(input)
				if got != want {
					t.Errorf("%d != %d", got, want)
				}
			},
		)
	}
	run(42, 5)
	run(50, 6)
	run(1*1024*1024, 20)
	run(1<<25-10, 25)
	run(1<<25+1, 25)
}
