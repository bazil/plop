package cas_test

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"bazil.org/plop/cas"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

func set(l ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(l))
	for _, s := range l {
		m[s] = struct{}{}
	}
	return m
}

// checkBucket does a sanity check on blobstore contents.
func checkBucket(t testing.TB, bucket *blob.Bucket, want ...string) {
	t.Helper()

	ctx := context.Background()
	iter := bucket.List(nil)
	wantMap := set(want...)
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if _, ok := wantMap[obj.Key]; !ok {
			t.Errorf("bucket junk: %+v", obj)
			continue
		}
		delete(wantMap, obj.Key)
	}
	if len(wantMap) > 0 {
		t.Errorf("blobs not seen in bucket: %v", want)
	}
}

func checkExtent(t testing.TB, ext *cas.Extent, key string, start, end int64, content string) {
	t.Helper()
	if g, e := ext.Key(), key; g != e {
		t.Errorf("bad extent key: %q != %q", g, e)
	}
	if g, e := ext.Start(), start; g != e {
		t.Errorf("bad extent start: %d != %d", g, e)
	}
	if g, e := ext.End(), end; g != e {
		t.Errorf("bad extent end: %d != %d", g, e)
	}
	buf, err := ext.Bytes()
	if err != nil {
		t.Errorf("error reading extent content: %v", err)
		return
	}
	if g, e := string(buf), content; g != e {
		t.Errorf("bad extent content: %q != %q", g, e)
	}
}

func TestRoundtrip(t *testing.T) {
	b := memblob.OpenBucket(nil)
	s := cas.NewStore(b, "s3kr1t")

	// intentionally enforce harsh lifetimes on contexts to make
	// sure we don't remember them too long
	ctxCreate, cancelCreate := context.WithCancel(context.Background())
	defer cancelCreate()
	const greeting = "hello, world\n"
	key, err := s.Create(ctxCreate, strings.NewReader(greeting))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cancelCreate()
	t.Logf("created %s", key)

	ctxOpen, cancelOpen := context.WithCancel(context.Background())
	defer cancelOpen()
	h, err := s.Open(ctxOpen, key)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cancelOpen()

	if g, e := h.Size(), int64(len(greeting)); g != e {
		t.Errorf("wrong length: %d != %d", g, e)
	}

	ctxRead := context.Background()
	r := h.IO(ctxRead)
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if g, e := string(buf), greeting; g != e {
		t.Errorf("bad content: %q != %q", g, e)
	}

	checkBucket(t, b,
		"b3jci1t6o4wstq445g5hc6mguexbbq948kq7mm1kxbjwyzwdrh6o",
		"o3iaqfe94q73cqbw3s468pxoy444hotxmahoqkfi91htaigfheqy",
	)
}

func TestCreateSizeZero(t *testing.T) {
	b := memblob.OpenBucket(nil)
	s := cas.NewStore(b, "s3kr1t")

	// intentionally enforce harsh lifetimes on contexts to make
	// sure we don't remember them too long
	ctxCreate, cancelCreate := context.WithCancel(context.Background())
	defer cancelCreate()
	key, err := s.Create(ctxCreate, strings.NewReader(""))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cancelCreate()
	t.Logf("created %s", key)

	ctxOpen := context.Background()
	ctxOpen, cancelOpen := context.WithCancel(ctxOpen)
	defer cancelOpen()
	h, err := s.Open(ctxOpen, key)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cancelOpen()

	if g, e := h.Size(), int64(0); g != e {
		t.Errorf("wrong length: %d != %d", g, e)
	}

	ctxRead := context.Background()
	r := h.IO(ctxRead)
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if g, e := string(buf), ""; g != e {
		t.Errorf("bad content: %q != %q", g, e)
	}

	checkBucket(t, b,
		"kjbqmr44hxaqeebjd9b9r4dsukrf34ag8kbiacnbg9pd7cpk8t8y",
	)
}

func TestReadAt(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	s := cas.NewStore(b, "s3kr1t")
	const greeting = "hello, world\n"
	key, err := s.Create(ctx, strings.NewReader(greeting))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	h, err := s.Open(ctx, key)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	buf := make([]byte, 3)
	n, err := h.IO(ctx).ReadAt(buf, 4)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("ReadAt returned a weird length: %d", n)
	}
	if g, e := string(buf), greeting[4:4+3]; g != e {
		t.Errorf("bad content: %q != %q", g, e)
	}
}

func TestExtentAt(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	// Force an extent boundary at a known location.
	//
	// It seems chunker does not respect minsize < windowSize, which
	// is 64.
	const chunkSize = 100
	s := cas.NewStore(b, "s3kr1t",
		cas.WithChunkLimits(chunkSize, chunkSize),
	)
	greeting := strings.Repeat("hello, world\n", 10)
	if len(greeting) < chunkSize+10 {
		t.Fatal("test has too small content")
	}
	key, err := s.Create(ctx, strings.NewReader(greeting))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	h, err := s.Open(ctx, key)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	r := h.IO(ctx)
	ext, err := r.ExtentAt(4)
	if err != nil {
		t.Fatalf("ExtentAt: %v", err)
	}
	checkExtent(t, ext, "hzc3c3katri1w9ew996zjr3dtmw1a4nrigys7yoi8t9ab4km4oho",
		0, chunkSize, greeting[:chunkSize])

	ext2, ok := ext.Next()
	if !ok {
		t.Fatal("expected more extents")
	}
	checkExtent(t, ext2, "6reor7xnoecfy15x1xhr8hy4wezuw9o9sbhqu4sz1kgcqqdfybhy",
		chunkSize, int64(len(greeting)), greeting[chunkSize:])

	if _, ok := ext2.Next(); ok {
		t.Fatal("didn't expect this many extents")
	}
}

func TestReadAtPastEOF(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	s := cas.NewStore(b, "s3kr1t")
	const greeting = "hello, world\n"
	key, err := s.Create(ctx, strings.NewReader(greeting))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	h, err := s.Open(ctx, key)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	buf := make([]byte, 10)
	const (
		tail   = 4
		offset = int64(len(greeting)) - tail
	)
	n, err := h.IO(ctx).ReadAt(buf, offset)
	if err != io.EOF {
		t.Fatalf("bad error: %v", err)
	}
	if n != tail {
		t.Fatalf("ReadAt returned a weird length: %d", n)
	}
	if g, e := string(buf[:n]), greeting[offset:]; g != e {
		t.Errorf("bad content: %q != %q", g, e)
	}
}

func TestReadAtAcrossExtents(t *testing.T) {
	ctx := context.Background()
	b := memblob.OpenBucket(nil)
	// Force an extent boundary at a known location.
	//
	// It seems chunker does not respect minsize < windowSize, which
	// is 64.
	const chunkSize = 100
	s := cas.NewStore(b, "s3kr1t",
		cas.WithChunkLimits(chunkSize, chunkSize),
	)
	greeting := strings.Repeat("hello, world\n", 10)
	if len(greeting) < chunkSize+10 {
		t.Fatal("test has too small content")
	}
	key, err := s.Create(ctx, strings.NewReader(greeting))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	h, err := s.Open(ctx, key)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for offset := 0; offset < chunkSize+2; offset++ {
		t.Run(fmt.Sprintf("@%d", offset),
			func(t *testing.T) {
				buf := make([]byte, 20)
				n, err := h.IO(ctx).ReadAt(buf, int64(offset))
				if err != nil {
					t.Fatalf("ReadAt: %v", err)
				}
				if n != len(buf) {
					t.Fatalf("ReadAt returned a weird length: %d", n)
				}
				if g, e := string(buf), greeting[offset:offset+20]; g != e {
					t.Errorf("bad content: %q != %q", g, e)
				}
			},
		)
	}
}
