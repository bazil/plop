package extents

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

type extentsCommand struct {
	subcommands.Description
	flag.FlagSet
	Flags struct {
		Volume string
	}
	Arguments struct {
		Key []string
	}
}

func (c *extentsCommand) extentsForKey(ctx context.Context, store *cas.Store, k string, w io.Writer) error {
	h, err := store.Open(ctx, k)
	if err != nil {
		return err
	}
	r := h.IO(ctx)
	ext, err := r.ExtentAt(0)
	if err != nil {
		return err
	}
	for {
		if _, err := fmt.Fprintf(w, "%s\t%d\t%d\n", ext.Key(), ext.Start(), ext.End()-ext.Start()); err != nil {
			return fmt.Errorf("writing to output: %w", err)
		}
		next, ok := ext.Next()
		if !ok {
			break
		}
		ext = next
	}
	return nil
}

func (c *extentsCommand) Run() error {
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
		if err := c.extentsForKey(ctx, store, k, os.Stdout); err != nil {
			return fmt.Errorf("cannot get extents from plop: %v", err)
		}
	}
	return nil
}

var extents = extentsCommand{
	Description: "show extents for a plop object",
}

func init() {
	extents.StringVar(&extents.Flags.Volume, "volume", "", "volume to extents to")
	subcommands.Register(&extents)
}
