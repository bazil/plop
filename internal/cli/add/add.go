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
	"github.com/tv42/cliutil/subcommands"
	"golang.org/x/sys/unix"
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
	target, err := os.Readlink(p)
	if err != nil {
		if errors.Is(err, unix.EINVAL) {
			// it's not a symlink
			return c.addRegularFile(ctx, store, targetPrefix, p)
		}
		// error from readlink
		return err
	}

	// check if it's a plop symlink, to this same volume
	if dir, file := filepath.Split(target); dir == targetPrefix+"/" && file != "" && filepath.Ext(file) == "" {
		// It's been added already.
		//
		// Note that filepath.Split leaves a trailing slash in place.
		//
		// Ensuring file name is not empty rules out links to the
		// volume directory.
		//
		// Ensuring file has no extension rules out symlinks to any
		// non-content data the filesystem might expose in the future,
		// e.g. metadata on extents.
		//
		// We could parse the filename zbase32 too, but that seems
		// brittle and unnecessary; now we have a path to other name
		// formats.
		return nil
	}

	// not a recognized or acceptable symlink; do add
	return c.addRegularFile(ctx, store, targetPrefix, p)
}

func (c *addCommand) addRegularFile(ctx context.Context, store *cas.Store, targetPrefix string, p string) error {
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
	vol, err := cliplop.Plop.Volume(c.Flags.Volume)
	if err != nil {
		return err
	}
	store, err := cliplop.Plop.Store(vol)
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
