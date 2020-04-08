package plopfs

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"

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
		if errors.Is(err, cas.ErrBadKey) {
			return nil, syscall.ENOENT
		}
		if errors.Is(err, cas.ErrNotExist) {
			return nil, syscall.ENOENT
		}
		return nil, err
	}

	n := &File{
		handle: h,
	}
	resp.EntryValid = 24 * time.Hour
	return n, nil
}
