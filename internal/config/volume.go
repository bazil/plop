package config

import (
	"context"
	"path/filepath"

	"bazil.org/plop/cas"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

func openBucket(ctx context.Context, cfg *Config, bucketConfig *Bucket) (*blob.Bucket, error) {
	if bucketConfig.AWS != nil {
		options := aws_session.Options{}
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
			options.Config.Credentials = aws_credentials.NewSharedCredentials(filename, profile)
		}
		session, err := aws_session.NewSessionWithOptions(options)
		if err != nil {
			return nil, err
		}
		opener := &s3blob.URLOpener{
			ConfigProvider: session,
		}
		bucket, err := opener.OpenBucketURL(ctx, &bucketConfig.url)
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
	for _, b := range buckets {
		opts = append(opts, cas.WithBucket(b))
	}
	opts = append(opts, cfg.Chunker.CASOptions()...)
	opts = append(opts, vol.Chunker.CASOptions()...)
	store := cas.NewStore(vol.Passphrase, opts...)
	return store, buckets, nil
}
