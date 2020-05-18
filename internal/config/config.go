package config

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/hashicorp/hcl/v2/hclsimple"
)

type Config struct {
	MountPoint    string    `hcl:"mountpoint"`
	DefaultVolume string    `hcl:"default_volume"`
	Volumes       []*Volume `hcl:"volume,block"`
	volumes       map[string]*Volume
}

func (cfg *Config) GetDefaultVolume() *Volume {
	return cfg.volumes[cfg.DefaultVolume]
}

func (cfg *Config) GetVolume(name string) (_ *Volume, ok bool) {
	vol, ok := cfg.volumes[name]
	return vol, ok
}

type Volume struct {
	Name       string  `hcl:"volume,label"`
	Passphrase string  `hcl:"passphrase"`
	Bucket     *Bucket `hcl:"bucket,block"`
}

type Bucket struct {
	URL string `hcl:"url"`
}

func ReadConfig(p string) (*Config, error) {
	var cfg Config
	if err := hclsimple.DecodeFile(p, nil, &cfg); err != nil {
		return nil, fmt.Errorf("cannot read config: %w", err)
	}

	if p := cfg.MountPoint; p == "" || !filepath.IsAbs(p) {
		return nil, errors.New("config field mountpoint must be an absolute path")
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

	return &cfg, nil
}
