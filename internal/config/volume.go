package config

import (
	"context"
	"path/filepath"

	"bazil.org/plop/cas"
	aws_config "github.com/aws/aws-sdk-go-v2/config"
	gocloud_aws "gocloud.dev/aws"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

func openBucket(ctx context.Context, cfg *Config, bucketConfig *Bucket) (*blob.Bucket, error) {
	if bucketConfig.url.Scheme == "s3" {
		// It seems with how `gocloud.dev` uses `github.com/aws/aws-sdk-go-v2`, we can't inject per-use-case credentials without reimplementing the (admittedly trivial) bucket URL parsing.
		// The API split between `V2ConfigFromURLParams` and the actual AWS SDK is incredibly awkward.
		// We used to be able to pass a config to `gocloud.dev/blob/s3blob.URLOpener`, but that's no longer in the API, so now we have to reimplement parts of that.
		bucketName := bucketConfig.url.Host
		awsConfigForBucket, err := gocloud_aws.V2ConfigFromURLParams(ctx, bucketConfig.url.Query())
		if err != nil {
			return nil, err
		}
		if creds := bucketConfig.AWS.CredentialsFile; creds != nil {
			filename := ""
			if creds.Path != nil {
				filename = *creds.Path
				// Interpret paths relative to Plop config file.
				filename = filepath.Join(filepath.Dir(cfg.path), filename)
			}
			profile := ""
			if creds.Profile != nil {
				profile = *creds.Profile
			}
			// Load credentials from the specified file and profile.
			// This repeats work one inside `gocloud_aws.V2ConfigFromURLParams` but we can't control that.
			credsCfg, err := aws_config.LoadDefaultConfig(ctx,
				aws_config.WithSharedCredentialsFiles([]string{filename}),
				aws_config.WithSharedConfigProfile(profile),
			)
			if err != nil {
				return nil, err
			}
			awsConfigForBucket.Credentials = credsCfg.Credentials
		}
		s3Client := s3blob.Dial(awsConfigForBucket)
		bucket, err := s3blob.OpenBucket(ctx, s3Client, bucketName, nil)
		return bucket, err
	}

	bucket, err := blob.OpenBucket(ctx, bucketConfig.URL)
	return bucket, err
}

func openBuckets(ctx context.Context, cfg *Config, vol *Volume) ([]*blob.Bucket, error) {
	var buckets []*blob.Bucket
	defer func() {
		// Close any buckets left.
		// The variable is zeroed on success.
		for _, b := range buckets {
			_ = b.Close()
		}
	}()
	for _, bucketConfig := range vol.Buckets {
		bucket, err := openBucket(ctx, cfg, bucketConfig)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, bucket)
	}
	// Clear out local variable to disarm defer.
	result := buckets
	buckets = nil
	return result, nil
}

func OpenVolume(ctx context.Context, cfg *Config, vol *Volume) (*cas.Store, []*blob.Bucket, error) {
	var buckets []*blob.Bucket
	buckets, err := openBuckets(ctx, cfg, vol)
	if err != nil {
		return nil, nil, err
	}
	var opts []cas.Option
	for i, b := range buckets {
		bucketConfig := vol.Buckets[i]
		opts = append(opts, cas.WithBucket(b,
			cas.BucketAfter(bucketConfig.delay),
			cas.BucketShardBits(bucketConfig.ShardBits),
			cas.BucketReadOnly(bucketConfig.ReadOnly),
		))
	}
	opts = append(opts, cfg.Chunker.CASOptions()...)
	opts = append(opts, vol.Chunker.CASOptions()...)
	store := cas.NewStore(vol.Passphrase, opts...)
	return store, buckets, nil
}
