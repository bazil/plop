package plopfs_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"bazil.org/fuse/fs/fstestutil"
	"bazil.org/plop/cas"
	"bazil.org/plop/internal/plopfs"
	"gocloud.dev/blob/memblob"
)

func withMount(t testing.TB, store *cas.Store, fn func(mntpath string)) {
	filesys := plopfs.New(store)
	mnt, err := fstestutil.MountedT(t, filesys, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mnt.Close()
	fn(mnt.Dir)
}

type fileInfo struct {
	name string
	size int64
	mode os.FileMode
}

func checkFI(t testing.TB, got os.FileInfo, expected fileInfo) {
	if g, e := got.Name(), expected.name; g != e {
		t.Errorf("file info has bad name: %q != %q", g, e)
	}
	if g, e := got.Size(), expected.size; g != e {
		t.Errorf("file info has bad size: %v != %v", g, e)
	}
	if g, e := got.Mode(), expected.mode; g != e {
		t.Errorf("file info has bad mode: %v != %v", g, e)
	}
}

func writeBlob(store *cas.Store, data []byte) (string, error) {
	ctx := context.Background()
	key, err := store.Create(ctx, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("Create: %v", err)
	}
	return key, nil
}

func mustWriteBlob(t *testing.T, store *cas.Store, data []byte) string {
	t.Helper()
	key, err := writeBlob(store, data)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestReaddir(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	store := cas.NewStore(bucket, "s3kr1t")
	// demonstrate that regular objects are not visible in readdir
	_ = mustWriteBlob(t, store, []byte("dummy"))
	withMount(t, store, func(mntpath string) {
		fis, err := ioutil.ReadDir(mntpath)
		if err != nil {
			t.Fatal(err)
		}
		if g, e := len(fis), 0; g != e {
			t.Fatalf("wrong readdir results: got %v", fis)
		}
	})
}

func TestRead(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	store := cas.NewStore(bucket, "s3kr1t")

	const greeting = "hello, world\n"
	key := mustWriteBlob(t, store, []byte(greeting))

	withMount(t, store, func(mntpath string) {
		f, err := os.Open(filepath.Join(mntpath, key))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			t.Errorf("Stat: %v", err)
		}
		checkFI(t, fi, fileInfo{name: key, size: int64(len(greeting)), mode: 0444})

		data, err := ioutil.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		if g, e := string(data), greeting; g != e {
			t.Fatalf("wrong read results: %q != %q", g, e)
		}
	})
}
