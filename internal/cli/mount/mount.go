package mount

import (
	cliplop "bazil.org/plop/internal/cli"
	"bazil.org/plop/internal/plopfs"
	"github.com/tv42/cliutil/subcommands"
)

type mountCommand struct {
	subcommands.Description
}

func (c *mountCommand) Run() error {
	cfg, err := cliplop.Plop.Config()
	if err != nil {
		return err
	}
	// TODO `plop -debug mount` should enable fuse debug log

	// TODO unify the config usage plopfs.Mount vs cliplop.Plop.Store
	if err := plopfs.Mount(cfg); err != nil {
		return err
	}
	return nil
}

var mount = mountCommand{
	Description: "mount and serve plopfs",
}

func init() {
	subcommands.Register(&mount)
}
