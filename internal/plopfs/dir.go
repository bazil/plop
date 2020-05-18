package plopfs

import (
	"context"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

type Dir struct {
	fs *PlopFS
}

var _ = fs.Node(&Dir{})

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0755
	return nil
}

var _ = fs.NodeRequestLookuper(&Dir{})

func (d *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	h, err := d.fs.store.Open(ctx, req.Name)
	if err != nil {
		// TODO map errors: ErrBadKey, gcerrors
		return nil, err
	}

	n := &File{
		handle: h,
	}
	return n, nil
}
