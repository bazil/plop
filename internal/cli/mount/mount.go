package mount

import (
	"context"
	"log"

	"bazil.org/plop/cas"
	cliplop "bazil.org/plop/internal/cli"
	"bazil.org/plop/internal/plopfs"
	"github.com/tv42/cliutil/subcommands"
	"gocloud.dev/blob"
)

type mountCommand struct {
	subcommands.Description
}

func (c *mountCommand) Run() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cfg, err := cliplop.Plop.Config()
	if err != nil {
		return err
	}
	// TODO expose all volumes
	vol := cfg.GetDefaultVolume()
	bucket, err := blob.OpenBucket(ctx, vol.Bucket.URL)
	if err != nil {
		return err
	}
	defer func() {
		if err := bucket.Close(); err != nil {
			log.Printf("error closing bucket: %v", err)
		}
	}()
	store := cas.NewStore(bucket, vol.Passphrase)
	// TODO `plop -debug mount` should enable fuse debug log
	if err := plopfs.Mount(store, cfg.MountPoint); err != nil {
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
