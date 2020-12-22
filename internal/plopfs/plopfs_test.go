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
	"bazil.org/fuse/fs/fstestutil/spawntest"
	"bazil.org/fuse/fs/fstestutil/spawntest/httpjson"
	"bazil.org/plop/cas"
	"bazil.org/plop/internal/config"
	"bazil.org/plop/internal/plopfs"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"gocloud.dev/blob/fileblob"
)

var helpers spawntest.Registry

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

type readdirResult struct {
	Entries []readdirEntry
}

type readdirEntry struct {
	Name string
	Mode os.FileMode
}

func doReaddir(ctx context.Context, dir string) (*readdirResult, error) {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	r := &readdirResult{
		// avoid null in JSON
		Entries: []readdirEntry{},
	}
	for _, fi := range fis {
		r.Entries = append(r.Entries, readdirEntry{
			Name: fi.Name(),
			Mode: fi.Mode(),
		})
	}
	return r, nil
}

var readdirHelper = helpers.Register("readdir", httpjson.ServePOST(doReaddir))

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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		control := readdirHelper.Spawn(ctx, t)
		defer control.Close()
		var got readdirResult
		if err := control.JSON("/").Call(ctx, mntpath, &got); err != nil {
			t.Fatalf("calling helper: %v", err)
		}
		wantEntries := []readdirEntry{
			{Name: "testvolume", Mode: os.ModeDir | 0o555},
		}
		if diff := cmp.Diff(got.Entries, wantEntries); diff != "" {
			t.Errorf("wrong readdir entries (-got +want)\n%s", diff)
		}
	})
}

type statResult struct {
	Name   string
	Size   int64
	Mode   os.FileMode
	Blocks int64
}

func doStat(ctx context.Context, path string) (*statResult, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	st := fi.Sys().(*syscall.Stat_t)
	r := &statResult{
		Name:   fi.Name(),
		Size:   fi.Size(),
		Mode:   fi.Mode(),
		Blocks: st.Blocks,
	}
	return r, nil
}

var statHelper = helpers.Register("stat", httpjson.ServePOST(doStat))

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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		control := statHelper.Spawn(ctx, t)
		defer control.Close()

		t.Run("root", func(t *testing.T) {
			var got statResult
			if err := control.JSON("/").Call(ctx, mntpath, &got); err != nil {
				t.Fatalf("calling helper: %v", err)
			}
			want := statResult{
				Mode: os.ModeDir | 0o555,
			}
			if diff := cmp.Diff(got, want,
				// random tempdir name
				cmpopts.IgnoreFields(statResult{}, "Name"),
			); diff != "" {
				t.Errorf("wrong stat result (-got +want)\n%s", diff)
			}
		})

		t.Run("volume", func(t *testing.T) {
			p := filepath.Join(mntpath, "testvolume")
			var got statResult
			if err := control.JSON("/").Call(ctx, p, &got); err != nil {
				t.Fatalf("calling helper: %v", err)
			}
			want := statResult{
				Name: "testvolume",
				Mode: os.ModeDir | 0o555,
			}
			if diff := cmp.Diff(got, want); diff != "" {
				t.Errorf("wrong stat (-got +want)\n%s", diff)
			}
		})

		t.Run("blob", func(t *testing.T) {
			p := filepath.Join(mntpath, "testvolume", key)
			var got statResult
			if err := control.JSON("/").Call(ctx, p, &got); err != nil {
				t.Fatalf("calling helper: %v", err)
			}
			want := statResult{
				Name:   key,
				Size:   int64(len(greeting)),
				Mode:   0o444,
				Blocks: 1,
			}
			if diff := cmp.Diff(got, want); diff != "" {
				t.Errorf("wrong stat (-got +want)\n%s", diff)
			}
		})
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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		control := readdirHelper.Spawn(ctx, t)
		defer control.Close()
		p := filepath.Join(mntpath, "testvolume")
		var got readdirResult
		if err := control.JSON("/").Call(ctx, p, &got); err != nil {
			t.Fatalf("calling helper: %v", err)
		}
		wantEntries := []readdirEntry{}
		if diff := cmp.Diff(got.Entries, wantEntries); diff != "" {
			t.Errorf("wrong readdir entries (-got +want)\n%s", diff)
		}
	})
}

func doCheckNotExist(ctx context.Context, path string) (*struct{}, error) {
	fi, err := os.Stat(path)
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("expected ErrNotExist, got %v for %v", err, fi)
	}
	return nil, nil
}

var checkNotExistHelper = helpers.Register("notExist", httpjson.ServePOST(doCheckNotExist))

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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		control := checkNotExistHelper.Spawn(ctx, t)
		defer control.Close()
		p := filepath.Join(mntpath, "does-not-exist")
		var nothing struct{}
		if err := control.JSON("/").Call(ctx, p, &nothing); err != nil {
			t.Fatalf("calling helper: %v", err)
		}
	})
}

type readFstatResult struct {
	Content []byte
	Stat    statResult
}

func doReadFstat(ctx context.Context, path string) (*readFstatResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("Open: %v", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("Stat: %v", err)
	}
	st := fi.Sys().(*syscall.Stat_t)

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("ReadAll: %v", err)
	}

	r := &readFstatResult{
		Content: data,
		Stat: statResult{
			Name:   fi.Name(),
			Size:   fi.Size(),
			Mode:   fi.Mode(),
			Blocks: st.Blocks,
		},
	}
	return r, nil
}

var readFstatHelper = helpers.Register("readFstat", httpjson.ServePOST(doReadFstat))

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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		control := readFstatHelper.Spawn(ctx, t)
		defer control.Close()

		p := filepath.Join(mntpath, "testvolume", key)
		var got readFstatResult
		if err := control.JSON("/").Call(ctx, p, &got); err != nil {
			t.Fatalf("calling helper: %v", err)
		}
		want := readFstatResult{
			Content: []byte(greeting),
			Stat: statResult{
				Name:   key,
				Size:   int64(len(greeting)),
				Blocks: 1,
				Mode:   0o444,
			},
		}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Errorf("wrong stat result (-got +want)\n%s", diff)
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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		control := checkNotExistHelper.Spawn(ctx, t)
		defer control.Close()
		for _, badkey := range []string{
			"ne5em96397gwhy4cow3jmifggc7ssewzbfaiaao77kq3ea83n5cy",
			"not-really-a-hash",
		} {
			p := filepath.Join(mntpath, "testvolume", badkey)
			var nothing struct{}
			if err := control.JSON("/").Call(ctx, p, &nothing); err != nil {
				t.Fatalf("calling helper: %v", err)
			}
		}
	})
}
