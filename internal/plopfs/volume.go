package plopfs

import (
	"context"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/plop/cas"
)

type Volume struct {
	fs    *PlopFS
	store *cas.Store
}

var _ = fs.Node(&Volume{})

func (v *Volume) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	return nil
}

var _ = fs.NodeRequestLookuper(&Volume{})

func (v *Volume) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	h, err := v.store.Open(ctx, req.Name)
	if err != nil {
		// TODO map errors: ErrBadKey, gcerrors
		return nil, err
	}

	n := &File{
		handle: h,
	}
	return n, nil
}
