package config

import (
	"context"

	"bazil.org/plop/cas"
	"gocloud.dev/blob"
)

func OpenVolume(ctx context.Context, cfg *Config, vol *Volume) (*cas.Store, *blob.Bucket, error) {
	bucket, err := blob.OpenBucket(ctx, vol.Bucket.URL)
	if err != nil {
		return nil, nil, err
	}
	var opts []cas.Option
	opts = append(opts, cfg.Chunker.CASOptions()...)
	opts = append(opts, vol.Chunker.CASOptions()...)
	store := cas.NewStore(bucket, vol.Passphrase, opts...)
	return store, bucket, nil
}
