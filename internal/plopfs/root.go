package plopfs

import (
	"context"
	"os"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/plop/cas"
	"gocloud.dev/blob"
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
	vol, ok := r.fs.cfg.GetVolume(req.Name)
	if !ok {
		return nil, syscall.ENOENT
	}

	// TODO cache buckets?
	bucket, err := blob.OpenBucket(ctx, vol.Bucket.URL)
	if err != nil {
		return nil, err
	}
	// TODO close buckets, figure out lifecycle
	// defer func() {
	//         if err := bucket.Close(); err != nil {
	//                 log.Printf("error closing bucket: %v", err)
	//         }
	// }()
	store := cas.NewStore(bucket, vol.Passphrase)

	n := &Volume{
		fs:    r.fs,
		store: store,
	}
	return n, nil
}

var _ fs.HandleReadDirAller = (*Root)(nil)

func (r *Root) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var res []fuse.Dirent
	for _, vol := range r.fs.cfg.Volumes {
		res = append(res, fuse.Dirent{
			Type: fuse.DT_Dir,
			Name: vol.Name,
		})
	}
	return res, nil
}
