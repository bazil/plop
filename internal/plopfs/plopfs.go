package plopfs

import (
	"context"
	"fmt"
	"log"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/plop/cas"
	"bazil.org/plop/internal/config"
	"bazil.org/plop/internal/multierr"
	"gocloud.dev/blob"
)

// an easily-recognizable number that essentially disables
// time-based expiration
const forever = 1000000 * time.Hour

type PlopFS struct {
	volumes map[string]*cas.Store
	buckets map[string][]*blob.Bucket
}

func New(cfg *config.Config) (*PlopFS, error) {
	filesys := &PlopFS{
		volumes: make(map[string]*cas.Store, len(cfg.Volumes)),
		buckets: make(map[string][]*blob.Bucket, len(cfg.Volumes)),
	}
	ctx := context.TODO()
	for _, vol := range cfg.Volumes {
		store, buckets, err := config.OpenVolume(ctx, cfg, vol)
		if err != nil {
			return nil, err
		}
		filesys.volumes[vol.Name] = store
		filesys.buckets[vol.Name] = buckets
	}
	return filesys, nil
}

func (f *PlopFS) Close() error {
	var errs []error
	for name, bs := range f.buckets {
		for i, b := range bs {
			if err := b.Close(); err != nil {
				err = fmt.Errorf("error closing bucket #%d for %q: %w", i, name, err)
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		return multierr.New(errs)
	}
	return nil
}

var _ = fs.FS(&PlopFS{})

func (f *PlopFS) Root() (fs.Node, error) {
	n := &Root{
		fs: f,
	}
	return n, nil
}

func Mount(cfg *config.Config) error {
	c, err := fuse.Mount(cfg.MountPoint,
		fuse.Subtype("plopfs"),
		fuse.ReadOnly(),
		fuse.AsyncRead(),
		fuse.MaxReadahead(8*1024*1024),
		fuse.DefaultPermissions(),
	)
	if err != nil {
		return err
	}
	defer c.Close()

	filesys, err := New(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err := filesys.Close(); err != nil {
			log.Printf("error closing filesystem: %v", err)
		}
	}()

	if err := fs.Serve(c, filesys); err != nil {
		return err
	}
	return nil
}
