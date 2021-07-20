package write

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"bazil.org/plop/cas"
	cliplop "bazil.org/plop/internal/cli"
	"github.com/tv42/cliutil/positional"
	"github.com/tv42/cliutil/subcommands"
	"golang.org/x/term"
)

type writeCommand struct {
	subcommands.Description
	flag.FlagSet
	Flags struct {
		Volume string
	}
	Arguments struct {
		positional.Optional
		File []string
	}
}

func (c *writeCommand) writeFromReader(ctx context.Context, store *cas.Store, r io.Reader) error {
	key, err := store.Create(ctx, r)
	if err != nil {
		return err
	}
	fmt.Println(key)
	return nil
}

func (c *writeCommand) writeFromPath(ctx context.Context, store *cas.Store, p string) error {
	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	return c.writeFromReader(ctx, store, f)
}

func (c *writeCommand) Run() error {
	ctx := context.TODO()
	vol, err := cliplop.Plop.Volume(c.Flags.Volume)
	if err != nil {
		return err
	}
	store, err := cliplop.Plop.Store(vol)
	if err != nil {
		return err
	}

	if len(c.Arguments.File) == 0 {
		if term.IsTerminal(0) {
			return errors.New("refusing to read from terminal")
		}
		if err := c.writeFromReader(ctx, store, os.Stdin); err != nil {
			return fmt.Errorf("cannot write to plop: %v", err)
		}
		return nil
	}

	for _, p := range c.Arguments.File {
		if err := c.writeFromPath(ctx, store, p); err != nil {
			return fmt.Errorf("cannot write to plop: %v", err)
		}
	}
	return nil
}

var write = writeCommand{
	Description: "write objects to plop",
}

func init() {
	write.StringVar(&write.Flags.Volume, "volume", "", "volume to write to")
	subcommands.Register(&write)
}
