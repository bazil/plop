package plopfs

import (
	"context"
	"os"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

type Root struct {
	fs *PlopFS
}

var _ = fs.Node(&Root{})

func (r *Root) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	return nil
}

var _ = fs.NodeRequestLookuper(&Root{})

func (r *Root) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (fs.Node, error) {
	store, ok := r.fs.volumes[req.Name]
	if !ok {
		return nil, syscall.ENOENT
	}
	n := &Volume{
		fs:    r.fs,
		store: store,
	}
	return n, nil
}

var _ fs.HandleReadDirAller = (*Root)(nil)

func (r *Root) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var res []fuse.Dirent
	for name := range r.fs.volumes {
		res = append(res, fuse.Dirent{
			Type: fuse.DT_Dir,
			Name: name,
		})
	}
	return res, nil
}
