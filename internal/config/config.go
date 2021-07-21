package config

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"bazil.org/plop/cas"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsimple"
	"github.com/zclconf/go-cty/cty"
	"gocloud.dev/blob/s3blob"
)

type Config struct {
	// Path from which this config was read from.
	path string

	MountPoint string `hcl:"mountpoint"`
	// SymlinkTarget is the prefix path added to symlinks created by `plop add`.
	// Defaults to MountPoint.
	SymlinkTarget string    `hcl:"symlink_target,optional"`
	DefaultVolume string    `hcl:"default_volume,optional"`
	Volumes       []*Volume `hcl:"volume,block"`
	volumes       map[string]*Volume
	Chunker       *ChunkerConfig `hcl:"chunker,block"`
}

func (cfg *Config) GetDefaultVolume() (*Volume, error) {
	if cfg.DefaultVolume == "" {
		return nil, errors.New("default volume not set")
	}
	vol := cfg.volumes[cfg.DefaultVolume]
	return vol, nil
}

func (cfg *Config) GetVolume(name string) (_ *Volume, ok bool) {
	vol, ok := cfg.volumes[name]
	return vol, ok
}

type Volume struct {
	Name       string         `hcl:"volume,label"`
	Passphrase string         `hcl:"passphrase"`
	Buckets    []*Bucket      `hcl:"bucket,block"`
	Chunker    *ChunkerConfig `hcl:"chunker,block"`
}

type Bucket struct {
	Delay *string `hcl:"delay"`
	delay time.Duration
	URL   string `hcl:"url"`
	url   url.URL
	AWS   *AWSConfig `hcl:"aws,block"`
}

type AWSConfig struct {
	CredentialsFile *AWSCredentialsFile `hcl:"credentials_file,block"`
}

type AWSCredentialsFile struct {
	// Path to AWS credentials.
	//
	// Relative paths are interpreted relative to the Plop
	// configuration directory.
	//
	// Empty string means AWS SDK should use the shared credentials
	// file at its default location.
	Path    *string `hcl:"path"`
	Profile *string `hcl:"profile"`
}

type ChunkerConfig struct {
	// hcl doesn't have convenient custom unmarshaling, so we're doing
	// byte sizes by defining "MiB" etc variables and letting config
	// writers multiply by them.
	//
	// https://github.com/hashicorp/hcl/issues/349

	// these fields are uint32 because a chunk is held in RAM and the
	// chunker library uses uint datatype.

	Min uint32 `hcl:"min,optional"`
	Max uint32 `hcl:"max,optional"`
	// Average chunk size to aim for. Will be rounded to the nearest
	// power of two.
	Average uint32 `hcl:"average,optional"`
}

// CASOptions returns the cas.Option values that enact this
// configuration. It is safe to call on nil values.
func (c *ChunkerConfig) CASOptions() []cas.Option {
	if c == nil {
		return nil
	}
	opts := []cas.Option{
		// rely on the options themselves to handle zero values
		cas.WithChunkLimits(c.Min, c.Max),
		cas.WithChunkGoal(c.Average),
	}
	return opts
}

var evalCtx = &hcl.EvalContext{
	Variables: map[string]cty.Value{
		"KiB": cty.NumberUIntVal(1024),
		"MiB": cty.NumberUIntVal(1024 * 1024),
		"GiB": cty.NumberUIntVal(1024 * 1024 * 1024),
		"kB":  cty.NumberUIntVal(1000),
		"MB":  cty.NumberUIntVal(1000 * 1000),
		"GB":  cty.NumberUIntVal(1000 * 1000 * 1000),
	},
}

func ParseConfig(filename string, src []byte) (*Config, error) {
	var cfg Config
	if err := hclsimple.Decode(filename, src, evalCtx, &cfg); err != nil {
		return nil, fmt.Errorf("cannot read config: %w", err)
	}
	cfg.path = filename
	if err := parseConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func ReadConfig(p string) (*Config, error) {
	var cfg Config
	if err := hclsimple.DecodeFile(p, evalCtx, &cfg); err != nil {
		return nil, fmt.Errorf("cannot read config: %w", err)
	}
	cfg.path = p
	// fold in any local config; TODO flag to disable?
	local, err := ReadLocalConfig()
	if err != nil {
		return nil, err
	}
	if n := local.DefaultVolume; n != "" {
		cfg.DefaultVolume = n
	}

	if err := parseConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func parseConfig(cfg *Config) error {
	if p := cfg.MountPoint; p == "" || !filepath.IsAbs(p) {
		return errors.New("config field mountpoint must be an absolute path")
	}

	if p := cfg.SymlinkTarget; p != "" {
		if !filepath.IsAbs(p) {
			return errors.New("config field symlink_target must be an absolute path, if set")
		}
	}

	if len(cfg.Volumes) == 0 {
		return errors.New("must have at least one volume")
	}

	cfg.volumes = make(map[string]*Volume, len(cfg.Volumes))
	for _, vol := range cfg.Volumes {
		if _, found := cfg.volumes[vol.Name]; found {
			return fmt.Errorf("duplicate volume: %q", vol.Name)
		}
		cfg.volumes[vol.Name] = vol
	}

	if n := cfg.DefaultVolume; n != "" {
		if _, ok := cfg.volumes[n]; !ok {
			return fmt.Errorf("default volume %q not found", n)
		}
	}

	for _, vol := range cfg.Volumes {
		if strings.ContainsAny(vol.Name, "/\x00") {
			return fmt.Errorf("config field volume %q name must not contain slashes or zero bytes", vol.Name)
		}
		if vol.Passphrase == "" {
			return fmt.Errorf("config block volume %q passphrase must be set", vol.Name)
		}
		if len(vol.Buckets) == 0 {
			return fmt.Errorf("config block volume %q bucket must be present", vol.Name)
		}
		for idx, bucket := range vol.Buckets {
			{
				if bucket.URL == "" {
					return fmt.Errorf("config block volume %q bucket #%d url must be set", vol.Name, idx+1)
				}
				bucket_url, err := url.Parse(bucket.URL)
				if err != nil {
					return fmt.Errorf("config block volume %q invalid bucket URL: %v: %v", vol.Name, bucket.URL, err)
				}
				bucket.url = *bucket_url
				if bucket.AWS != nil {
					if bucket.url.Scheme != s3blob.Scheme {
						return fmt.Errorf("config block volume %q bucket %v has aws config with non-s3 url", vol.Name, bucket.url.String())
					}
				}
			}

			if bucket.Delay != nil {
				d, err := time.ParseDuration(*bucket.Delay)
				if err != nil {
					return fmt.Errorf("config block volume %q bucket %v invalid time: %v", vol.Name, bucket.url.String(), err)
				}
				bucket.delay = d
			}
		}
	}

	return nil
}
