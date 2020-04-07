package add

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	cliplop "bazil.org/plop/internal/cli"
	"github.com/tv42/cliutil/subcommands"
)

type addCommand struct {
	subcommands.Description
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

func (c *addCommand) addPath(p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	store, err := cliplop.Plop.Store()
	if err != nil {
		return err
	}
	ctx := context.TODO()
	key, err := store.Create(ctx, f)
	if err != nil {
		return err
	}
	// TODO un-hardcode path to plop
	plopPath := "mnt"
	target := filepath.Join(plopPath, key)
	if err := replaceWithSymlink(target, p); err != nil {
		return fmt.Errorf("cannot make symlink: %w", err)
	}
	return nil
}

func (c *addCommand) Run() error {
	for _, p := range c.Arguments.File {
		if err := c.addPath(p); err != nil {
			return fmt.Errorf("cannot add to plop: %v", err)
		}
	}
	return nil
}

var add = addCommand{
	Description: "add files to plop and replace them with symlinks",
}

func init() {
	subcommands.Register(&add)
}
