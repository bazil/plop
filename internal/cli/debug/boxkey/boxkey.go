package boxkey

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"bazil.org/plop/cas"
	cliplop "bazil.org/plop/internal/cli"
	"github.com/tv42/cliutil/subcommands"
)

type boxkeyCommand struct {
	subcommands.Description
	flag.FlagSet
	Flags struct {
		Volume string
	}
	Arguments struct {
		Key []string
	}
}

func (c *boxkeyCommand) boxKey(ctx context.Context, store *cas.Store, k string, w io.Writer) error {
	boxed, err := store.DebugBoxKey(k)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "%s\n", boxed); err != nil {
		return err
	}
	return nil
}

func (c *boxkeyCommand) Run() error {
	ctx := context.TODO()
	vol, err := cliplop.Plop.Volume(c.Flags.Volume)
	if err != nil {
		return err
	}
	store, err := cliplop.Plop.Store(vol)
	if err != nil {
		return err
	}

	for _, k := range c.Arguments.Key {
		if err := c.boxKey(ctx, store, k, os.Stdout); err != nil {
			return fmt.Errorf("cannot box key: %v", err)
		}
	}
	return nil
}

var boxkey = boxkeyCommand{
	Description: "boxkey blobs",
}

func init() {
	boxkey.StringVar(&boxkey.Flags.Volume, "volume", "", "volume to boxkey to")
	subcommands.Register(&boxkey)
}
