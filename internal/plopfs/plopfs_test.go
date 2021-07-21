package plopfs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"bazil.org/fuse/fs/fstestutil"
	"bazil.org/plop/cas"
	"bazil.org/plop/internal/config"
	"bazil.org/plop/internal/plopfs"
	"gocloud.dev/blob/fileblob"
)

func withMount(t testing.TB, configText string, fn func(mntpath string)) {
	t.Helper()
	cfg, err := config.ParseConfig("<test literal>.hcl", []byte(configText))
	if err != nil {
		t.Fatal(err)
	}
	filesys, err := plopfs.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := filesys.Close(); err != nil {
			t.Error(err)
		}
	}()
	mnt, err := fstestutil.MountedT(t, filesys, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mnt.Close()
	fn(mnt.Dir)
}

type fileInfo struct {
	name   string
	size   int64
	mode   os.FileMode
	blocks int64
}

func checkFI(t testing.TB, got os.FileInfo, expected fileInfo) {
	t.Helper()
	if g, e := got.Name(), expected.name; g != e {
		t.Errorf("file info has bad name: %q != %q", g, e)
	}
	if g, e := got.Size(), expected.size; g != e {
		t.Errorf("file info has bad size: %v != %v", g, e)
	}
	if g, e := got.Mode(), expected.mode; g != e {
		t.Errorf("file info has bad mode: %v != %v", g, e)
	}
	st := got.Sys().(*syscall.Stat_t)
	if g, e := st.Blocks, expected.blocks; g != e {
		t.Errorf("file info has bad block count: %v != %v", g, e)
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

func tempDir(tb testing.TB) string {
	// TODO with go1.15 use testing.TB.TempDir
	tb.Helper()
	p, err := ioutil.TempDir("", "plopfs-test-*.tmp")
	if err != nil {
		tb.Fatalf("cannot make temp dir: %v", err)
	}
	tb.Cleanup(func() {
		if err := os.RemoveAll(p); err != nil {
			tb.Errorf("cannot clean temp dir: %v", err)
		}
	})
	return p
}

func TestRootReaddir(t *testing.T) {
	tmp := tempDir(t)
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

	withMount(t, config, func(mntpath string) {
		fis, err := ioutil.ReadDir(mntpath)
		if err != nil {
			t.Fatal(err)
		}
		if g, e := len(fis), 1; g != e {
			t.Fatalf("wrong readdir results: got %v", fis)
		}
		checkFI(t, fis[0], fileInfo{
			name: "testvolume",
			mode: os.ModeDir | 0o555,
		})
	})
}

func TestStat(t *testing.T) {
	tmp := tempDir(t)
	bucket, err := fileblob.OpenBucket(tmp, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	store := cas.NewStore("s3kr1t", cas.WithBucket(bucket))
	const greeting = "hello, world\n"
	key := mustWriteBlob(t, store, []byte(greeting))

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

	withMount(t, config, func(mntpath string) {
		{
			// root
			fi, err := os.Stat(mntpath)
			if err != nil {
				t.Fatal(err)
			}
			checkFI(t, fi, fileInfo{
				name: fi.Name(), // random tempdir name
				mode: os.ModeDir | 0o555,
			})
		}

		{
			// volume
			fi, err := os.Stat(filepath.Join(mntpath, "testvolume"))
			if err != nil {
				t.Fatal(err)
			}
			checkFI(t, fi, fileInfo{
				name: "testvolume",
				mode: os.ModeDir | 0o555,
			})
		}

		{
			// blob
			fi, err := os.Stat(filepath.Join(mntpath, "testvolume", key))
			if err != nil {
				t.Fatal(err)
			}
			checkFI(t, fi, fileInfo{
				name:   key,
				size:   int64(len(greeting)),
				mode:   0o444,
				blocks: 1,
			})
		}
	})
}

func TestVolumeReaddir(t *testing.T) {
	tmp := tempDir(t)
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

	withMount(t, config, func(mntpath string) {
		fis, err := ioutil.ReadDir(filepath.Join(mntpath, "testvolume"))
		if err != nil {
			t.Fatal(err)
		}
		if g, e := len(fis), 0; g != e {
			t.Fatalf("wrong readdir results: got %v", fis)
		}
	})
}

func TestVolumeNotExist(t *testing.T) {
	tmp := tempDir(t)
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

	withMount(t, config, func(mntpath string) {
		_, err := os.Stat(filepath.Join(mntpath, "does-not-exist"))
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected ErrNotExist, got %v", err)
		}
	})
}

func TestRead(t *testing.T) {
	tmp := tempDir(t)
	bucket, err := fileblob.OpenBucket(tmp, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()
	store := cas.NewStore("s3kr1t", cas.WithBucket(bucket))
	const greeting = "hello, world\n"
	key := mustWriteBlob(t, store, []byte(greeting))

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

	withMount(t, config, func(mntpath string) {
		f, err := os.Open(filepath.Join(mntpath, "testvolume", key))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			t.Errorf("Stat: %v", err)
		}
		checkFI(t, fi, fileInfo{name: key, size: int64(len(greeting)), blocks: 1, mode: 0o444})

		data, err := ioutil.ReadAll(f)
		if err != nil {
			t.Fatal(err)
		}
		if g, e := string(data), greeting; g != e {
			t.Fatalf("wrong read results: %q != %q", g, e)
		}
	})
}

func TestKeyNotExist(t *testing.T) {
	tmp := tempDir(t)
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

	withMount(t, config, func(mntpath string) {
		for _, badkey := range []string{
			"ne5em96397gwhy4cow3jmifggc7ssewzbfaiaao77kq3ea83n5cy",
			"not-really-a-hash",
		} {
			f, err := os.Open(filepath.Join(mntpath, "testvolume", badkey))
			if !errors.Is(err, os.ErrNotExist) {
				t.Errorf("expected ErrNotExist, got: %v", err)
			}
			if err == nil {
				f.Close()
			}
		}
	})
}
