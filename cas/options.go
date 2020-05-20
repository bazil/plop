package cas

type option func(*config)

type Option option

// WithChunkLimits sets the minimum and maximum chunk size.
//
// Zero will leave the previous value in effect.
//
// An invalid combination (min > max, including when one is left to
// zero and uses the previous value) will set both values to max.
func WithChunkLimits(min, max uint32) Option {
	fn := func(cfg *config) {
		if min != 0 {
			cfg.chunkMin = min
		}
		if max != 0 {
			cfg.chunkMax = max
		}
		if cfg.chunkMin > cfg.chunkMax {
			cfg.chunkMin = cfg.chunkMax
		}
	}
	return fn
}

// WithChunkGoal sets the desired average chunk size for chunking.
//
// Zero will leave the previous value in effect.
func WithChunkGoal(size uint32) Option {
	fn := func(cfg *config) {
		if size != 0 {
			cfg.chunkAvgBits = bitsOfPowerOfTwo(size)
		}
	}
	return fn
}
