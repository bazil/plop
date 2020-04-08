package plopfs

import (
	"context"
	"io"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/plop/cas"
)

type File struct {
	handle *cas.Handle
}

var _ = fs.Node(&File{})
var _ = fs.Handle(&File{})

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = 0444
	a.Size = uint64(f.handle.Size())
	a.Valid = 24 * time.Hour
	return nil
}

var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	resp.Flags |= fuse.OpenKeepCache
	return f, nil
}

var _ = fs.HandleReader(&File{})

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	resp.Data = resp.Data[:req.Size]
	n, err := f.handle.IO(ctx).ReadAt(resp.Data, req.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	resp.Data = resp.Data[:n]
	return nil
}
