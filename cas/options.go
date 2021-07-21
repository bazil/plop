package cas

import (
	"time"

	"gocloud.dev/blob"
)

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

// WithBucket add a bucket as an alternate destination for reads and writes.
func WithBucket(bucket *blob.Bucket) Option {
	return WithBucketAfter(0, bucket)
}

// WithBucketAfter add a bucket as an alternate destination for reads and writes.
//
// It will only be tried after delay has passed, or if all earlier possible buckets have failed.
func WithBucketAfter(delay time.Duration, bucket *blob.Bucket) Option {
	fn := func(cfg *config) {
		cfg.buckets = append(cfg.buckets, alternativeBucket{delay: delay, bucket: bucket})
	}
	return fn
}
