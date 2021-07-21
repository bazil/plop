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

func openBucket(ctx context.Context, cfg *Config, vol *Volume) (*blob.Bucket, error) {
	if vol.Bucket.AWS != nil {
		options := aws_session.Options{}
		if creds := vol.Bucket.AWS.CredentialsFile; creds != nil {
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
		bucket, err := opener.OpenBucketURL(ctx, &vol.Bucket.url)
		return bucket, err
	}

	bucket, err := blob.OpenBucket(ctx, vol.Bucket.URL)
	return bucket, err
}

func OpenVolume(ctx context.Context, cfg *Config, vol *Volume) (*cas.Store, *blob.Bucket, error) {
	bucket, err := openBucket(ctx, cfg, vol)
	if err != nil {
		return nil, nil, err
	}
	var opts []cas.Option
	opts = append(opts, cas.WithBucket(bucket))
	opts = append(opts, cfg.Chunker.CASOptions()...)
	opts = append(opts, vol.Chunker.CASOptions()...)
	store := cas.NewStore(vol.Passphrase, opts...)
	return store, bucket, nil
}
