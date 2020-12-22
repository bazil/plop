package plopfs_test

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"bazil.org/fuse/fs/fstestutil/spawntest/httpjson"
	"bazil.org/plop/cas"
	"gocloud.dev/blob/fileblob"
)

type benchReadSequentialRequest struct {
	Path     string
	WantSize int64
	Repeats  int64
}

func doBenchReadSequential(ctx context.Context, req benchReadSequentialRequest) (*struct{}, error) {
	f, err := os.Open(req.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	for i := int64(0); i < req.Repeats; i++ {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		n, err := io.Copy(ioutil.Discard, f)
		if err != nil {
			return nil, err
		}
		if g, e := n, int64(req.WantSize); g != e {
			return nil, fmt.Errorf("wrong file length: %v != %v", g, e)
		}
	}
	return nil, nil
}

var benchReadSequentialHelper = helpers.Register("benchReadSequential", httpjson.ServePOST(doBenchReadSequential))

func BenchmarkRead(b *testing.B) {
	tmp := tempDir(b)
	bucket, err := fileblob.OpenBucket(tmp, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer bucket.Close()
	store := cas.NewStore("s3kr1t", cas.WithBucket(bucket))
	prng := rand.New(rand.NewSource(42))
	const dataSize = 10 * 1024 * 1024
	data := make([]byte, dataSize)
	_, _ = prng.Read(data)
	key := mustWriteBlob(b, store, data)

	config := fmt.Sprintf(`
mountpoint = "/does-not-exist"
default_volume = "testvolume"
volume "testvolume" {
  passphrase = "s3kr1t"
  bucket {
    url = %q
  }
}
`, "file://"+tmp)

	withMount(b, config, func(mntpath string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		control := benchReadSequentialHelper.Spawn(ctx, b)
		defer control.Close()

		req := benchReadSequentialRequest{
			Path:     filepath.Join(mntpath, "testvolume", key),
			WantSize: dataSize,
			Repeats:  int64(b.N),
		}
		b.ReportAllocs()
		b.ResetTimer()

		var nothing struct{}
		if err := control.JSON("/").Call(ctx, req, &nothing); err != nil {
			b.Fatalf("calling helper: %v", err)
		}

		b.StopTimer()
	})
}
