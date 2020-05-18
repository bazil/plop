package read

import (
	"context"
	"fmt"
	"io"
	"os"

	"bazil.org/plop/cas"
	cliplop "bazil.org/plop/internal/cli"
	"github.com/tv42/cliutil/subcommands"
)

type readCommand struct {
	subcommands.Description
	Arguments struct {
		Key []string
	}
}

func (c *readCommand) readKey(ctx context.Context, store *cas.Store, k string, w io.Writer) error {
	h, err := store.Open(ctx, k)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, h.IO(ctx)); err != nil {
		return err
	}
	return nil
}

func (c *readCommand) Run() error {
	ctx := context.TODO()
	store, err := cliplop.Plop.Store()
	if err != nil {
		return err
	}

	for _, k := range c.Arguments.Key {
		if err := c.readKey(ctx, store, k, os.Stdout); err != nil {
			return fmt.Errorf("cannot read from plop: %v", err)
		}
	}
	return nil
}

var read = readCommand{
	Description: "read objects to plop",
}

func init() {
	subcommands.Register(&read)
}