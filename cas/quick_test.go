package cas_test

import (
	"bytes"
	"encoding/binary"
	"flag"
	"io"
	"math/rand"
	"testing"
	"testing/quick"

	"context"

	"bazil.org/plop/cas"
	"gocloud.dev/blob/memblob"
)

var seed uint64

func init() {
	// keep this as uint64 just because negative numbers are uglier and can be confused with -opt
	flag.Uint64Var(&seed, "seed", 0, "seed to initialize random number generator")
}

type randReader struct {
	*rand.Rand
}

func (r randReader) Read(p []byte) (n int, err error) {
	for len(p) > 4 {
		binary.BigEndian.PutUint32(p, r.Uint32())
		n += 4
		p = p[4:]
	}
	if len(p) > 0 {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, r.Uint32())
		n += copy(p, buf)
	}
	return n, err
}

func NewRandReader(seed int64) randReader {
	src := rand.NewSource(seed)
	rnd := rand.New(src)
	return randReader{rnd}
}

func TestQuickCompareRead(t *testing.T) {
	randR := NewRandReader(42)
	const size = 10 * 1024 * 1024
	buf := make([]byte, size)
	randR.Read(buf)

	b := memblob.OpenBucket(nil)
	s := cas.NewStore(b, "s3kr1t")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key, err := s.Create(ctx, bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	h, err := s.Open(ctx, key)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	casR := h.IO(ctx)

	readFn := func(r io.ReaderAt) func(p []byte, off int64) ([]byte, int, error) {
		fn := func(p []byte, off int64) ([]byte, int, error) {
			if off < 0 {
				off = -off
			}
			off = off % (size + 1000)
			n, err := r.ReadAt(p, off)
			p = p[:n]
			return p, n, err
		}
		return fn
	}
	got := readFn(casR)
	rat := bytes.NewReader(buf)
	exp := readFn(rat)

	if err := quick.CheckEqual(got, exp, nil); err != nil {
		t.Error(err)
	}
}
