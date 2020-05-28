package add

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"bazil.org/plop/cas"
	cliplop "bazil.org/plop/internal/cli"
	"bazil.org/plop/internal/config"
	"github.com/tv42/cliutil/subcommands"
)

type addCommand struct {
	subcommands.Description
	flag.FlagSet
	Flags struct {
		Volume string
	}
	Arguments struct {
		File []string
	}
}

func replaceWithSymlink(target, path string) error {
	dir := filepath.Dir(path)
	var tmp string
	tries := 0
	for {
		tries++
		if tries > 1000 {
			return errors.New("failed to pick an unused temp file name")
		}
		// we don't really need crypto-safe random numbers but this
		// way we don't have to worry about seeding
		random := make([]byte, 4)
		if _, err := io.ReadFull(rand.Reader, random); err != nil {
			return fmt.Errorf("cannot get randomness: %w", err)
		}
		tmp = filepath.Join(dir, fmt.Sprintf(".plop.%x.tmp", random))
		if err := os.Symlink(target, tmp); err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return err
		}
		break
	}
	cleanTmp := true
	defer func() {
		if cleanTmp {
			_ = os.Remove(tmp)
		}
	}()
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	cleanTmp = false
	return nil
}

func (c *addCommand) addPath(ctx context.Context, store *cas.Store, targetPrefix string, p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	key, err := store.Create(ctx, f)
	if err != nil {
		return err
	}
	target := filepath.Join(targetPrefix, key)
	if err := replaceWithSymlink(target, p); err != nil {
		return fmt.Errorf("cannot make symlink: %w", err)
	}
	return nil
}

func (c *addCommand) Run() error {
	ctx := context.TODO()
	cfg, err := cliplop.Plop.Config()
	if err != nil {
		return err
	}
	// TODO because of the shape of the cliplop.Plop.Store API, we
	// look up the volume twice
	var vol *config.Volume
	if n := c.Flags.Volume; n != "" {
		v, ok := cfg.GetVolume(n)
		if !ok {
			return fmt.Errorf("volume not found: %v", n)
		}
		vol = v
	}
	if vol == nil {
		v, err := cfg.GetDefaultVolume()
		if err != nil {
			return err
		}
		vol = v
	}

	store, err := cliplop.Plop.Store(c.Flags.Volume)
	if err != nil {
		return err
	}
	targetPrefix := cfg.SymlinkTarget
	if targetPrefix == "" {
		targetPrefix = cfg.MountPoint
	}
	targetPrefix = filepath.Join(targetPrefix, vol.Name)
	for _, p := range c.Arguments.File {
		if err := c.addPath(ctx, store, targetPrefix, p); err != nil {
			return fmt.Errorf("cannot add to plop: %v", err)
		}
	}
	return nil
}

var add = addCommand{
	Description: "add files to plop and replace them with symlinks",
}

func init() {
	add.StringVar(&add.Flags.Volume, "volume", "", "volume to add file to")
	subcommands.Register(&add)
}
