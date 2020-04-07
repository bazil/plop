package plopfs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/plop/internal/config"
)

type PlopFS struct {
	cfg *config.Config
}

func New(cfg *config.Config) *PlopFS {
	filesys := &PlopFS{
		cfg: cfg,
	}
	return filesys
}

var _ = fs.FS(&PlopFS{})

func (f *PlopFS) Root() (fs.Node, error) {
	n := &Root{
		fs: f,
	}
	return n, nil
}

func Mount(cfg *config.Config) error {
	c, err := fuse.Mount(cfg.MountPoint)
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := New(cfg)
	if err := fs.Serve(c, filesys); err != nil {
		return err
	}
	return nil
}
