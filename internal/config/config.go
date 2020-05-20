package config

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"bazil.org/plop/cas"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsimple"
	"github.com/zclconf/go-cty/cty"
)

type Config struct {
	MountPoint string `hcl:"mountpoint"`
	// SymlinkTarget is the prefix path added to symlinks created by `plop add`.
	// Defaults to MountPoint.
	SymlinkTarget string    `hcl:"symlink_target,optional"`
	DefaultVolume string    `hcl:"default_volume"`
	Volumes       []*Volume `hcl:"volume,block"`
	volumes       map[string]*Volume
	Chunker       *ChunkerConfig `hcl:"chunker,block"`
}

func (cfg *Config) GetDefaultVolume() *Volume {
	return cfg.volumes[cfg.DefaultVolume]
}

func (cfg *Config) GetVolume(name string) (_ *Volume, ok bool) {
	vol, ok := cfg.volumes[name]
	return vol, ok
}

type Volume struct {
	Name       string         `hcl:"volume,label"`
	Passphrase string         `hcl:"passphrase"`
	Bucket     *Bucket        `hcl:"bucket,block"`
	Chunker    *ChunkerConfig `hcl:"chunker,block"`
}

type Bucket struct {
	URL string `hcl:"url"`
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
	return parseConfig(&cfg)
}

func ReadConfig(p string) (*Config, error) {
	var cfg Config
	if err := hclsimple.DecodeFile(p, evalCtx, &cfg); err != nil {
		return nil, fmt.Errorf("cannot read config: %w", err)
	}
	return parseConfig(&cfg)
}

func parseConfig(cfg *Config) (*Config, error) {
	if p := cfg.MountPoint; p == "" || !filepath.IsAbs(p) {
		return nil, errors.New("config field mountpoint must be an absolute path")
	}

	if p := cfg.SymlinkTarget; p != "" {
		if !filepath.IsAbs(p) {
			return nil, errors.New("config field symlink_target must be an absolute path, if set")
		}
	}

	if len(cfg.Volumes) == 0 {
		return nil, errors.New("must have at least one volume")
	}

	cfg.volumes = make(map[string]*Volume, len(cfg.Volumes))
	for _, vol := range cfg.Volumes {
		if _, found := cfg.volumes[vol.Name]; found {
			return nil, fmt.Errorf("duplicate volume: %q", vol.Name)
		}
		cfg.volumes[vol.Name] = vol
	}

	if _, ok := cfg.volumes[cfg.DefaultVolume]; !ok {
		return nil, fmt.Errorf("default volume %q not found", cfg.DefaultVolume)
	}

	for _, vol := range cfg.Volumes {
		if strings.ContainsAny(vol.Name, "/\x00") {
			return nil, fmt.Errorf("config field volume %q name must not contain slashes or zero bytes", vol.Name)
		}
		if vol.Passphrase == "" {
			return nil, fmt.Errorf("config field volume %q passphrase must be set", vol.Name)
		}
		if vol.Bucket == nil {
			return nil, fmt.Errorf("config block volume %q bucket must be present", vol.Name)
		}
		if vol.Bucket.URL == "" {
			return nil, errors.New("config field Bucket must be set")
		}
	}

	return cfg, nil
}
