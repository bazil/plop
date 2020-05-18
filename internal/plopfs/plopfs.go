package plopfs

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/plop/cas"
)

type PlopFS struct {
	store *cas.Store
}

func New(store *cas.Store) *PlopFS {
	filesys := &PlopFS{
		store: store,
	}
	return filesys
}

var _ = fs.FS(&PlopFS{})

func (f *PlopFS) Root() (fs.Node, error) {
	n := &Dir{
		fs: f,
	}
	return n, nil
}

func Mount(store *cas.Store, mountpoint string) error {
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := New(store)
	if err := fs.Serve(c, filesys); err != nil {
		return err
	}
	return nil
}
